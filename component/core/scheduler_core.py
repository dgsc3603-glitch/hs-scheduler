import threading
import queue
import time
import datetime
import subprocess
import os
import sys
import re
import shlex
import shutil
import atexit
import logging
import uuid
from component.models import Project, ProjectTask
from component.data_validation import has_diagnostics, validate_scheduler_payload
from component.utils import atomic_write_json, load_json_file

class SchedulerEvent:
    # Event Types
    LOG_SUMMARY = "LOG_SUMMARY"
    LOG_DETAIL = "LOG_DETAIL"
    STATUS_UPDATE = "STATUS_UPDATE"
    PROGRESS_UPDATE = "PROGRESS_UPDATE"
    TASK_REFRESH = "TASK_REFRESH"
    PROJECT_REFRESH = "PROJECT_REFRESH"
    NOTIFICATION = "NOTIFICATION"
    TELEGRAM = "TELEGRAM"
    SAVE_DATA = "SAVE_DATA"
    CLEAR_LOGS = "CLEAR_LOGS"

    def __init__(self, event_type, data):
        self.type = event_type
        self.data = data

class UserStopException(Exception):
    pass

class SchedulerCore:
    #          Skipped   
    TASK_STATUS_SKIPPED = "SkippedCondition"

    def __init__(self, event_queue, credentials_manager, data_file):
        self.event_queue = event_queue
        self.credentials = credentials_manager
        self.data_file = data_file
        self.logger = logging.getLogger("Scheduler")
        self.active_processes = []
        self.active_processes_lock = threading.Lock()
        self._requested_task_stops = set()
        self._requested_task_stops_lock = threading.Lock()
        self.persistence_lock = threading.Lock()
        self.project_state_lock = threading.RLock()
        self.progress_lock = threading.Lock()
        self.shutdown_lock = threading.Lock()
        self.project_workers = {}
        self.project_workers_lock = threading.Lock()
        
        # The app intentionally allows many concurrent projects; users control timing through schedules.
        self.max_concurrent_projects = 999
        self.semaphore = threading.Semaphore(self.max_concurrent_projects)
        self.pending_queue = queue.Queue()
        self._last_schedule_diag = {}
        self._pending_set = set()  # Prevent duplicate pending entries.
        
        self._progress_pattern = re.compile(r'(?:[\(\s])?(\d+)/(\d+)(?:[\)\s]|$)')
        self._progress_percent_pattern = re.compile(r'(?<!\d)(\d{1,3})\s*%')
        
        # Throttle UI detail-log events to avoid unbounded memory growth.
        self._last_detail_log_time = 0
        self._detail_log_interval = 0.05  # 50 ms minimum interval.
        self._last_live_log_state = {}
        
        # Status constants (mirrored from main app for consistency)
        self.STATUS_WAITING = "Waiting"
        self.STATUS_RUNNING = "Running"
        self.STATUS_COMPLETED = "Done"
        self.STATUS_ERROR = "Error"
        self.STATUS_STOPPED = "Stopped"
        self.STATUS_DEPENDENCY_WAIT = "DependencyWait"
        
        self.TASK_STATUS_WAITING = "Waiting"
        self.TASK_STATUS_RUNNING = "Running"
        self.TASK_STATUS_COMPLETED = "Done"
        self.TASK_STATUS_ERROR = "Error"
        self.TASK_STATUS_TIMEOUT = "Timeout"
        self.TASK_STATUS_STOPPED = "Stopped"
        self.TASK_STATUS_FINAL_FAIL = "FinalFailed"
        self.TASK_STATUS_SYSTEM_ERROR = "SystemError"
        
        # Statuses that should not be treated as failures during recovery.
        self._NON_FAILURE_STATUSES = {
            self.TASK_STATUS_COMPLETED,
            self.TASK_STATUS_WAITING,
            self.TASK_STATUS_SKIPPED,
        }

        # Persistence paths
        base_dir = os.path.dirname(os.path.abspath(data_file))
        self.history_file = os.path.join(base_dir, "scheduler_history.json")
        self.session_state_file = os.path.join(base_dir, "scheduler_session_state.json")
        self.log_dir = os.path.join(base_dir, "logs")
        self.schedule_trace_lock = threading.Lock()
        self.projects = []
        self.running = True
        self._current_date = datetime.datetime.now().strftime("%Y-%m-%d")
        # Track minute changes without relying on second==0 timing.
        self._last_checked_minute = ""
        self.last_all_done_date = None
        self._event_drop_counts = {}
        self._stale_recovery_grace_seconds = 180
        self._started = False
        self._shutdown_complete = False

        # Register cleanup on exit
        atexit.register(self.shutdown)

    def shutdown(self):
        """Cleanup active processes on application exit"""
        with self.shutdown_lock:
            if self._shutdown_complete:
                return
            self._shutdown_complete = True

        with self.active_processes_lock:
            to_kill = list(self.active_processes)

        self._trace_schedule_event("SYSTEM", "SESSION_STOP", active_processes=len(to_kill))
        self.running = False
        self._started = False
        if to_kill:
            self.logger.info("Cleaning up %s active processes", len(to_kill))

        for proc, p_name, t_id in to_kill:
            try:
                if proc.poll() is None:
                    proc.kill()
                    proc.wait(timeout=5)
            except Exception:
                pass

        with self.active_processes_lock:
            self.active_processes = [entry for entry in self.active_processes if entry not in to_kill]

    def start(self):
        """Start the scheduler loop in a background thread"""
        if self._started:
            return
        self._started = True
        self.load_data()
        self._trace_schedule_event("SYSTEM", "SESSION_START", project_count=len(self.projects))
        threading.Thread(target=self._scheduler_loop, daemon=True).start()

    def load_data(self):
        if not os.path.exists(self.data_file): 
            self.projects = []
            return
        
        try:
            raw_data = load_json_file(self.data_file, default=[])
            data, diagnostics = validate_scheduler_payload(raw_data)
            self._write_data_validation_report(diagnostics)
            if diagnostics["errors"]:
                self.log(f"       Error {len(diagnostics['errors'])}    ")
            if diagnostics["warnings"] or diagnostics["repairs"]:
                self.log(
                    "         /   "
                    f"{len(diagnostics['warnings']) + len(diagnostics['repairs'])}    "
                )
            # Reconstruct Project objects
            self.projects = []
            for p in data:
                # Handle potential missing keys with defaults
                kwargs = {k:v for k,v in p.items() if k not in ["name", "run_time", "tasks"]}
                project = Project(p["name"], p["run_time"], p.get("tasks", []), **kwargs)
                self.projects.append(project)
            
            self._load_session_state()
            self._reconcile_today_trace_state()
            self.emit(SchedulerEvent.PROJECT_REFRESH, None)
            self.log("         Done")
        except Exception as e:
            self.log(f"       Error: {e}")

    def _write_data_validation_report(self, diagnostics):
        report_path = os.path.join(self.log_dir, "data_validation_latest.json")
        if not has_diagnostics(diagnostics):
            try:
                if os.path.exists(report_path):
                    os.remove(report_path)
            except OSError:
                pass
            return
        try:
            report = {
                "created_at": datetime.datetime.now().isoformat(timespec="seconds"),
                "data_file": self.data_file,
                **diagnostics,
            }
            atomic_write_json(report_path, report, indent=2, backup=False)
        except Exception as e:
            self.logger.warning("Data validation report write failed: %s", e)

    def save_data(self):
        try:
            data = [p.to_dict() for p in self.projects]
            with self.persistence_lock:
                atomic_write_json(self.data_file, data, indent=4)
                self._save_session_state()
            self.log("      Done")
        except Exception as e:
            self.log(f"      Failed: {e}")

    def _load_session_state(self):
        if not os.path.exists(self.session_state_file): return
        try:
            state = load_json_file(self.session_state_file, default={}) or {}
            
            today = datetime.datetime.now().strftime("%Y-%m-%d")
            if state.get("date") != today:
                # Remove stale state file
                try: os.remove(self.session_state_file)
                except: pass
                return

            projects_state = state.get("projects", {})
            for proj in self.projects:
                if proj.name in projects_state:
                    ps = projects_state[proj.name]
                    with self.project_state_lock:
                        loaded_status = ps.get("status", "Waiting")
                        today_ticket = self._get_today_schedule_ticket(proj, today)
                        saved_ticket = ps.get("last_consumed_ticket") or proj.last_consumed_ticket
                        if self._is_stale_daily_session_status(proj, loaded_status, saved_ticket, today_ticket):
                            self.log(
                                f"[{proj.name}]          ({loaded_status})                Waiting          ."
                            )
                            loaded_status = self.STATUS_WAITING
                            ps["completed_tasks"] = 0
                            ps["total_tasks"] = len(proj.tasks)
                            ps["tasks"] = {}
                        
                        # 2    : daily                          Done     Waiting      
                        if proj.schedule_type == "daily" and loaded_status in ["Done", "Done"]:
                            try:
                                target_time = datetime.datetime.strptime(proj.run_time, "%H:%M").time()
                                now_time = datetime.datetime.now().time()
                                if target_time > now_time:
                                    self.log(f"[{proj.name}]          :      ({proj.run_time})            Waiting              .")
                                    loaded_status = "Waiting"
                                    ps["completed_tasks"] = 0
                                    
                                    #    task             tasks         
                                    if "tasks" in ps:
                                        ps["tasks"] = {}
                            except Exception as e:
                                print(f"Defense mechanism time parse error: {e}")

                        proj.status = loaded_status
                        proj.completed_tasks = ps.get("completed_tasks", 0)
                        proj.total_tasks = ps.get("total_tasks", 0)
                        # Restore consumed ticket state to prevent duplicate reruns after restart.
                        if saved_ticket:
                            proj.last_consumed_ticket = saved_ticket
                        
                        tasks_state = ps.get("tasks", {})
                        for task in proj.tasks:
                            if task.filename in tasks_state:
                                ts = tasks_state[task.filename]
                                task.status = ts.get("status", "Unknown")

            self.log(f"      ({today})          Done")
        except Exception as e:
            self.log(f"Session state load error: {e}")

    def _get_today_schedule_ticket(self, proj, today):
        if proj.schedule_type != "daily":
            return None
        try:
            datetime.datetime.strptime(proj.run_time, "%H:%M")
        except ValueError:
            return None
        return f"{today} {proj.run_time}"

    def _is_stale_daily_session_status(self, proj, loaded_status, saved_ticket, today_ticket):
        if proj.schedule_type != "daily" or not today_ticket:
            return False
        if saved_ticket == today_ticket:
            return False
        if proj.last_run and str(proj.last_run).startswith(today_ticket[:10]):
            return False
        stale_statuses = {
            self.STATUS_COMPLETED,
            self.STATUS_ERROR,
            self.STATUS_STOPPED,
            "Done",
            "Error",
            "Stopped",
        }
        return loaded_status in stale_statuses

    def _reconcile_today_trace_state(self):
        trace_path = self._get_schedule_trace_path()
        if not os.path.exists(trace_path):
            return
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        latest = {}
        latest_ticket = {}
        try:
            with open(trace_path, "r", encoding="utf-8", errors="replace") as handle:
                for line in handle:
                    parts = line.rstrip("\n").split(" | ", 3)
                    if len(parts) < 3:
                        continue
                    timestamp, project_name, event = parts[:3]
                    fields = self._parse_trace_fields(parts[3] if len(parts) == 4 else "")
                    if event == "SCHEDULE_TICKET_CONSUMED":
                        ticket = fields.get("next_run")
                        if ticket and ticket.startswith(today):
                            latest_ticket[project_name] = ticket
                    elif event == "PROJECT_FINISH":
                        last_run = fields.get("last_run", "")
                        if last_run.startswith(today):
                            latest[project_name] = {
                                "timestamp": timestamp,
                                "status": fields.get("status"),
                                "last_run": last_run,
                                "next_run": fields.get("next_run"),
                            }
        except Exception as exc:
            self.logger.warning("Schedule trace reconciliation failed: %s", exc)
            return

        if not latest and not latest_ticket:
            return

        for proj in self.projects:
            finish = latest.get(proj.name)
            ticket = latest_ticket.get(proj.name)
            if ticket and proj.last_consumed_ticket != ticket:
                proj.last_consumed_ticket = ticket
            if not finish:
                continue
            status = self._normalize_trace_status(finish.get("status"))
            if status:
                proj.status = status
                if status == self.STATUS_COMPLETED:
                    proj.total_tasks = len(proj.tasks)
                    proj.completed_tasks = len(proj.tasks)
                    for task in proj.tasks:
                        task.status = self.TASK_STATUS_COMPLETED
            if finish.get("last_run"):
                proj.last_run = finish["last_run"]
            if finish.get("next_run"):
                proj.next_run = finish["next_run"]

    def _parse_trace_fields(self, raw):
        fields = {}
        if not raw:
            return fields
        for chunk in raw.split(", "):
            key, sep, value = chunk.partition("=")
            if sep:
                fields[key.strip()] = value.strip()
        return fields

    def _normalize_trace_status(self, status):
        if not status:
            return None
        known = {
            self.STATUS_WAITING: self.STATUS_WAITING,
            self.STATUS_RUNNING: self.STATUS_RUNNING,
            self.STATUS_COMPLETED: self.STATUS_COMPLETED,
            self.STATUS_ERROR: self.STATUS_ERROR,
            self.STATUS_STOPPED: self.STATUS_STOPPED,
            self.STATUS_DEPENDENCY_WAIT: self.STATUS_DEPENDENCY_WAIT,
            "Done": self.STATUS_COMPLETED,
            "Stopped": self.STATUS_STOPPED,
        }
        if status in known:
            return known[status]
        if "Stopped" in status:
            return self.STATUS_STOPPED
        return None

    def _save_session_state(self):
        try:
            today = datetime.datetime.now().strftime("%Y-%m-%d")
            projects_state = {}
            snapshot = []
            for proj in self.projects:
                 tasks_snapshot = {t.filename: {"status": t.status} for t in proj.tasks}
                 snapshot.append({
                     "name": proj.name,
                     "status": proj.status,
                     "completed_tasks": proj.completed_tasks,
                     "total_tasks": proj.total_tasks,
                     "tasks": tasks_snapshot,
                     # Persist consumed ticket state to prevent duplicate reruns after restart.
                     "last_consumed_ticket": proj.last_consumed_ticket,
                 })
            
            for p_data in snapshot:
                projects_state[p_data["name"]] = {k:v for k,v in p_data.items() if k!="name"}

            state_data = {"date": today, "projects": projects_state}
            atomic_write_json(self.session_state_file, state_data, indent=2)
        except Exception as e:
            self.log(f"Session state save error: {e}")

    def _scheduler_loop(self):
        while self.running:
            try:
                now = datetime.datetime.now()
                
                # Reset daily state when the date changes.
                current_date = now.strftime("%Y-%m-%d")
                if current_date != self._current_date:
                    self._current_date = current_date
                    self._reset_daily_state()
                
                # Process scheduling once per minute.
                current_minute = now.strftime("%Y-%m-%d %H:%M")
                if current_minute != self._last_checked_minute:
                    self._last_checked_minute = current_minute
                    running_projects = sum(1 for proj in self.projects if proj.status == self.STATUS_RUNNING)
                    enabled_projects = sum(1 for proj in self.projects if proj.enabled)
                    self._trace_schedule_event(
                        "SYSTEM",
                        "HEARTBEAT",
                        minute=current_minute,
                        enabled_projects=enabled_projects,
                        running_projects=running_projects,
                        pending_queue=self.pending_queue.qsize()
                    )
                    
                    # Recover projects left in a stale running state.
                    self._heal_ghost_running_projects()
                    
                    for proj in self.projects:
                        if proj.enabled:
                            self._diagnose_scheduled_project(proj, now)
                        # Only waiting projects are eligible for scheduled execution.
                        if proj.enabled and proj.status == self.STATUS_WAITING:
                            self.try_consume_ticket_atomic(proj, now)
                
                # Drain pending work when capacity is available.
                if not self.pending_queue.empty():
                    self._process_pending()
                
                time.sleep(1)
            except Exception as e:
                self.log(f"Scheduler loop error: {e}")
                self._trace_schedule_event("SYSTEM", "SCHEDULER_LOOP_ERROR", detail=e)
                time.sleep(5)

    def _heal_ghost_running_projects(self):
        """C-7: STATUS_RUNNING  Failed                         """
        try:
            with self.active_processes_lock:
                active_proj_names = {p_name for _, p_name, _ in self.active_processes}
            
            for proj in self.projects:
                if proj.status == self.STATUS_RUNNING and proj.name not in active_proj_names:
                    #         
                    self.log(f"[{proj.name}]         !      ...")
                    with self.project_state_lock:
                        proj.status = self.STATUS_ERROR
                        for task in proj.tasks:
                            if task.status.startswith(self.TASK_STATUS_RUNNING):
                                task.status = self.TASK_STATUS_SYSTEM_ERROR
                    
                    # C-5: Lock      
                    try:
                        if proj.execution_lock.locked():
                            proj.execution_lock.release()
                    except RuntimeError:
                        pass
                    
                    # FIX: semaphore      
                    try:
                        self.semaphore.release()
                        self.log(f"[{proj.name}] semaphore    Done")
                    except ValueError:
                        pass
                    
                    proj.calculate_next_run()
                    self.log(f"[{proj.name}]       Done -> next_run: {proj.next_run}")
                    self.emit(SchedulerEvent.PROJECT_REFRESH, None)
                    self.emit(SchedulerEvent.TASK_REFRESH, None)
                    self.save_data()
        except Exception as e:
            self.log(f"Self-healing error: {e}")

    def emit(self, event_type, data):
        event = SchedulerEvent(event_type, data)
        try:
            self.event_queue.put_nowait(event)
        except queue.Full:
            drop_key = event_type
            self._event_drop_counts[drop_key] = self._event_drop_counts.get(drop_key, 0) + 1
            if self._event_drop_counts[drop_key] in (1, 10, 100):
                self.logger.warning("Event queue full; dropped %s event (%s)", event_type, self._event_drop_counts[drop_key])

    def log(self, message):
        timestamp = datetime.datetime.now().strftime("[%H:%M:%S]")
        self.logger.info(message)
        self.emit(SchedulerEvent.LOG_SUMMARY, f"{timestamp} {message}\n")

    def _track_project_worker_start(self, proj_name, run_id, worker_thread):
        with self.project_workers_lock:
            self.project_workers[proj_name] = {
                "run_id": run_id,
                "thread": worker_thread,
                "started_at": time.time(),
                "last_activity": time.time(),
            }

    def _track_project_worker_finish(self, proj_name, run_id):
        with self.project_workers_lock:
            worker = self.project_workers.get(proj_name)
            if worker and worker.get("run_id") == run_id:
                self.project_workers.pop(proj_name, None)

    def _touch_project_worker(self, proj_name):
        with self.project_workers_lock:
            worker = self.project_workers.get(proj_name)
            if worker:
                worker["last_activity"] = time.time()

    def _get_project_worker_state(self, proj_name):
        with self.project_workers_lock:
            worker = self.project_workers.get(proj_name)
            if not worker:
                return None
            thread = worker.get("thread")
            return {
                "run_id": worker.get("run_id"),
                "thread_alive": bool(thread and thread.is_alive()),
                "started_at": worker.get("started_at"),
                "last_activity": worker.get("last_activity"),
            }

    def _get_project_active_process_count(self, proj_name):
        with self.active_processes_lock:
            return sum(1 for _, p_name, _ in self.active_processes if p_name == proj_name)

    def _has_live_project_runtime(self, proj_name):
        worker = self._get_project_worker_state(proj_name)
        if worker and worker["thread_alive"]:
            return True
        return self._get_project_active_process_count(proj_name) > 0

    def _recover_stale_project_state(self, proj, reason):
        worker = self._get_project_worker_state(proj.name)
        active_processes = self._get_project_active_process_count(proj.name)
        if worker and worker["thread_alive"]:
            return False
        if active_processes > 0:
            return False
        if not proj.execution_lock.locked():
            return False

        try:
            proj.execution_lock.release()
        except RuntimeError:
            return False

        with self.project_state_lock:
            if proj.status == self.STATUS_RUNNING:
                proj.status = self.STATUS_ERROR
                for task in proj.tasks:
                    if task.status == self.TASK_STATUS_RUNNING or task.status.startswith(self.TASK_STATUS_RUNNING):
                        task.status = self.TASK_STATUS_SYSTEM_ERROR
            elif proj.status != self.STATUS_WAITING:
                proj.status = self.STATUS_WAITING
                for task in proj.tasks:
                    if task.status == self.TASK_STATUS_RUNNING or task.status.startswith(self.TASK_STATUS_RUNNING):
                        task.status = self.TASK_STATUS_WAITING

            proj.stop_requested = False
            proj.calculate_next_run()

        self._pending_set.discard(proj.name)
        self._trace_schedule_event(
            proj.name,
            "STALE_LOCK_RECOVERED",
            reason=reason,
            status=proj.status,
            next_run=proj.next_run,
        )
        self.log(f"[{proj.name}] stale execution_lock    ({reason})")
        self.save_data()
        self.emit(SchedulerEvent.PROJECT_REFRESH, None)
        self.emit(SchedulerEvent.TASK_REFRESH, None)
        return True

    def _get_schedule_trace_path(self, date_str=None):
        if date_str is None:
            date_str = datetime.datetime.now().strftime("%Y-%m-%d")
        return os.path.join(self.log_dir, f"schedule_trace_{date_str}.log")

    def _write_schedule_trace(self, line, date_str=None):
        try:
            os.makedirs(self.log_dir, exist_ok=True)
            trace_path = self._get_schedule_trace_path(date_str)
            with self.schedule_trace_lock:
                with open(trace_path, "a", encoding="utf-8") as f:
                    f.write(line + "\n")
        except Exception as e:
            self.log(f"Schedule trace write error: {e}")

    def _trace_schedule_event(self, proj_name, event, **fields):
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        details = []
        for key, value in fields.items():
            safe_value = str(value).replace("\n", "\\n")
            details.append(f"{key}={safe_value}")
        suffix = " | " + ", ".join(details) if details else ""
        self._write_schedule_trace(f"{timestamp} | {proj_name} | {event}{suffix}")

    def _get_ticket_block_reason(self, proj, current_time):
        try:
            next_run_dt = datetime.datetime.strptime(proj.next_run, "%Y-%m-%d %H:%M")
        except Exception:
            return "invalid_next_run"

        if current_time < next_run_dt:
            return "not_due_yet"
        if proj.next_run == proj.last_consumed_ticket:
            return "ticket_already_consumed"

        grace_limit = current_time.replace(second=0, microsecond=0) - datetime.timedelta(minutes=5)
        if next_run_dt < grace_limit and not proj.catch_up_missed:
            today_str = current_time.strftime("%Y-%m-%d")
            if not proj.next_run.startswith(today_str):
                return "stale_past_schedule"

        return None

    def _log_schedule_diag_once(self, proj, diag_key, message):
        if self._last_schedule_diag.get(proj.name) == diag_key:
            return
        self._last_schedule_diag[proj.name] = diag_key
        self._trace_schedule_event(
            proj.name,
            "SCHEDULE_DIAG",
            next_run=proj.next_run,
            status=proj.status,
            last_ticket=proj.last_consumed_ticket,
            detail=message
        )

    def _diagnose_scheduled_project(self, proj, current_time):
        try:
            next_run_dt = datetime.datetime.strptime(proj.next_run, "%Y-%m-%d %H:%M")
        except Exception:
            return

        current_minute = current_time.replace(second=0, microsecond=0)
        if next_run_dt > current_minute:
            return

        if proj.status != self.STATUS_WAITING:
            diag_key = ("status_block", proj.next_run, proj.status, proj.last_consumed_ticket)
            self._log_schedule_diag_once(
                proj,
                diag_key,
                f"[{proj.name}] Scheduled run blocked by status - status={proj.status}, next_run={proj.next_run}, "
                f"last_run={proj.last_run}, last_ticket={proj.last_consumed_ticket}"
            )
            return

        block_reason = self._get_ticket_block_reason(proj, current_time)
        if block_reason in (None, "not_due_yet", "ticket_already_consumed"):
            return

        reason_map = {
            "invalid_next_run": "next_run parse failed",
            "missed_grace_window": "scheduled time is more than five minutes late",
            "stale_past_schedule": "stale past schedule remains",
        }
        diag_key = ("ticket_block", proj.next_run, block_reason, proj.last_consumed_ticket)
        self._log_schedule_diag_once(
            proj,
            diag_key,
            f"[{proj.name}] Scheduled run skipped - {reason_map.get(block_reason, block_reason)} "
            f"(next_run={proj.next_run}, last_ticket={proj.last_consumed_ticket})"
        )

    def _launch_project_with_acquired_slot(self, proj, only_checked, trigger_source):
        self._pending_set.discard(proj.name)

        source_label = "scheduled run" if trigger_source == "scheduled" else "manual run"
        if trigger_source == "scheduled":
            self.log(f"[{proj.name}] {source_label} started (ticket: {proj.last_consumed_ticket}, next_run: {proj.next_run})")
        else:
            self.log(f"[{proj.name}] {source_label} started")

        self._trace_schedule_event(
            proj.name,
            "PROJECT_START",
            trigger_source=trigger_source,
            only_checked=only_checked,
            next_run=proj.next_run,
            last_ticket=proj.last_consumed_ticket
        )
        run_id = proj.execution_id
        worker = threading.Thread(
            target=self._execute_wrapper,
            args=(proj, only_checked, trigger_source, run_id),
            daemon=True,
            name=f"scheduler:{proj.name}:{run_id}"
        )
        self._track_project_worker_start(proj.name, run_id, worker)
        worker.start()
        return True

    def detail_log(self, message, proj_name="", task_name="", log_type="stdout"):
        # Throttle high-volume stdout/stderr events before sending them to the UI.
        now = time.time()
        if log_type in ("stdout", "stderr") and (now - self._last_detail_log_time) < self._detail_log_interval:
            return  # File logging already captured the output; skip only this UI event.
        self._last_detail_log_time = now
        
        # Drop UI events when the queue is full.
        try:
            self.event_queue.put_nowait(SchedulerEvent(SchedulerEvent.LOG_DETAIL, {
                "message": message,
                "proj_name": proj_name,
                "task_name": task_name,
                "log_type": log_type
            }))
        except queue.Full:
            pass

    def run_project(self, proj, only_checked=False, trigger_source="scheduled"):
        if not proj.execution_lock.acquire(blocking=False):
            self.log(f"[{proj.name}] execution_lock    Failed (   Running)")
            self._trace_schedule_event(proj.name, "PROJECT_START_SKIPPED", trigger_source=trigger_source, reason="execution_lock_busy")
            return False
            
        if not self.semaphore.acquire(blocking=False):
            proj.execution_lock.release()
            if trigger_source != "scheduled":
                self.log(f"[{proj.name}]          Failed -            ")
                self._trace_schedule_event(proj.name, "PROJECT_START_SKIPPED", trigger_source=trigger_source, reason="semaphore_full")
                return False
            self.log(f"[{proj.name}]            , Waiting    ")
            self._trace_schedule_event(proj.name, "PROJECT_QUEUED", trigger_source=trigger_source, reason="semaphore_full", next_run=proj.next_run)
            if proj.name not in self._pending_set:
                self._pending_set.add(proj.name)
                self.pending_queue.put((proj, datetime.datetime.now(), only_checked, trigger_source))
            return False

        # FIX: pending_queue       last_consumed_ticket   
        if trigger_source == "scheduled" and proj.last_consumed_ticket != proj.next_run:
            proj.last_consumed_ticket = proj.next_run
            self.log(f"[{proj.name}]       (pending   ): {proj.next_run}")

        # pending_set   
        self._pending_set.discard(proj.name)
        self.save_data()

        self.log(f"[{proj.name}]          Done (next_run: {proj.next_run})")
        return self._launch_project_with_acquired_slot(proj, only_checked, trigger_source)

    def _execute_wrapper(self, proj, only_checked, trigger_source, run_id):
        try:
            self._execute_project_logic(proj, only_checked, trigger_source)
        finally:
            self._track_project_worker_finish(proj.name, run_id)
            # Release defensively; recovery code may already have released the lock.
            try:
                proj.execution_lock.release()
            except RuntimeError:
                pass
            self.semaphore.release()
            self._process_pending()
            self._check_all_projects_completed()

    def _process_pending(self):
        """Waiting                """
        processed = 0
        while not self.pending_queue.empty():
            try:
                proj, _, only_checked, trigger_source = self.pending_queue.get_nowait()
                self._pending_set.discard(proj.name)
                if not self.run_project(proj, only_checked=only_checked, trigger_source=trigger_source):
                    break  # semaphore        
                processed += 1
            except queue.Empty:
                break
        if processed > 0:
            self.log(f"Waiting    {processed}         ")

    def stop_task(self, proj_name, task_id):
        with self._requested_task_stops_lock:
            self._requested_task_stops.add((proj_name, task_id))
        with self.active_processes_lock:
            for i, (proc, p_name, t_id) in enumerate(self.active_processes):
                if p_name == proj_name and t_id == task_id:
                    self._terminate_process_safely(proc, f"task:{task_id}")
                    # Note: process removal and status update usually happens in the thread loop
                    # but we can force a refresh here if needed.
                    return True
        with self._requested_task_stops_lock:
            self._requested_task_stops.discard((proj_name, task_id))
        return False

    def stop_project(self, proj_name):
        """     Stopped -         + stop_requested    """
        self.log(f"  [{proj_name}]    Stopped     ...")
        
        # stop_requested    Failed   (FailedTask      )
        for proj in self.projects:
            if proj.name == proj_name:
                proj.stop_requested = True
                break
        
        with self.active_processes_lock:
            # Find all processes for this project
            to_kill = [(proc, t_id) for proc, p_name, t_id in self.active_processes if p_name == proj_name]
        
        for proc, t_id in to_kill:
            with self._requested_task_stops_lock:
                self._requested_task_stops.add((proj_name, t_id))
            self._terminate_process_safely(proc, f"task:{t_id}")
            # The _run_single_script thread will handle removal from active_processes
        
        self.log(f"[{proj_name}]                     Done")

    def _terminate_process_safely(self, process, task_name="Unknown"):
        try:
            if process.poll() is not None:
                return

            process.terminate()
            try:
                process.wait(timeout=5)
                self.log(f"    {task_name}           ")
                return
            except subprocess.TimeoutExpired:
                pass

            if os.name == "nt":
                self._terminate_windows_process_tree(process, task_name)
            else:
                process.kill()
                process.wait(timeout=5)
                self.log(f"    {task_name}           ")
        except Exception as e:
            self.log(f"    {task_name}         Failed: {e}")

    def _terminate_windows_process_tree(self, process, task_name):
        try:
            result = subprocess.run(
                ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=10,
                check=False,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)

            if result.returncode == 0:
                self.log(f"    {task_name}              ")
            else:
                self.log(f"    {task_name}           ")
        except Exception:
            process.kill()
            process.wait(timeout=5)
            self.log(f"    {task_name}           ")

    def _check_task_condition(self, task, proj_name, proj_dict):
        if not task.condition.get('enabled', False):
            return True
        
        cond_type = task.condition.get('type', 'always')
        cond_value = task.condition.get('value', '')
        
        if cond_type == 'file_exists':
            if os.path.exists(cond_value):
                self.log(f"[{proj_name}] '{task.filename}'      :       ({cond_value})")
                return True
            else:
                self.log(f"[{proj_name}] '{task.filename}' Skipped:       ({cond_value})")
                with self.project_state_lock:
                    task.status = self.TASK_STATUS_SKIPPED
                return False
        
        elif cond_type == 'prev_success':
            proj = proj_dict.get(proj_name)
            if proj:
                same_step_tasks = [t for t in proj.tasks if t.step == task.step and t.order < task.order]
                all_success = all(t.status == self.TASK_STATUS_COMPLETED for t in same_step_tasks)
                if all_success:
                    self.log(f"[{proj_name}] '{task.filename}'      :            ")
                    return True
                else:
                    self.log(f"[{proj_name}] '{task.filename}' Skipped:       Failed")
            with self.project_state_lock:
                task.status = self.TASK_STATUS_SKIPPED
            return False
        return True

    def _safe_path_component(self, value, fallback="item"):
        cleaned = re.sub(r'[<>:"/\\|*\x00-\x1f]', "_", str(value or "")).strip(" .")
        return cleaned or fallback

    def _run_single_script(self, task, proj_name, proj_dict):
        if not self._check_task_condition(task, proj_name, proj_dict):
            self.emit(SchedulerEvent.TASK_REFRESH, None)
            return

        max_attempts = task.max_retries + 1
        for attempt in range(1, max_attempts + 1):
            task_start_time = time.time()
            if attempt > 1:
                self.log(f"[{proj_name}]   {task.filename}       ({attempt}/{max_attempts})...")
                time.sleep(2)

            with self.project_state_lock:
                task.status = f"{self.TASK_STATUS_RUNNING} ({attempt}/{max_attempts})" if max_attempts > 1 else f"{self.TASK_STATUS_RUNNING}..."
            self.emit(SchedulerEvent.TASK_REFRESH, None)
            
            self.detail_log("", proj_name, task.filename, "task_header")
            
            cmd = [sys.executable, '-u', task.filepath]
            if task.args:
                safe_args = self.credentials.inject_to_args(task.args)
                try:
                    cmd.extend(shlex.split(safe_args))
                except ValueError:
                    cmd.extend(safe_args.split())
            
            script_dir = os.path.dirname(task.filepath)
            today_str = datetime.datetime.now().strftime("%Y-%m-%d")
            safe_project_name = self._safe_path_component(proj_name, "project")
            safe_task_name = self._safe_path_component(task.filename, "task")
            log_dir = os.path.join(os.path.dirname(self.data_file), "task_logs", safe_project_name, today_str)
            if not os.path.exists(log_dir): os.makedirs(log_dir)
            
            timestamp = datetime.datetime.now().strftime("%H%M%S")
            log_path = os.path.join(log_dir, f"{safe_task_name}_{timestamp}.txt")

            temp_token = uuid.uuid4().hex
            temp_stdout = os.path.join(log_dir, f"_temp_stdout_{safe_task_name}_{temp_token}.log")
            temp_stderr = os.path.join(log_dir, f"_temp_stderr_{safe_task_name}_{temp_token}.log")
            
            process = None
            try:
                env = os.environ.copy()
                env['PYTHONUNBUFFERED'] = '1'
                env['PYTHONIOENCODING'] = 'utf-8'
                workspace_root = os.path.dirname(os.path.abspath(self.data_file))
                env['SCHEDULER_WORKSPACE_ROOT'] = workspace_root
                env['SCHEDULER_CLOUD_COPY_ROOT'] = os.path.join(workspace_root, 'cloud_copies')
                env['SCHEDULER_CLOUD_SPOOL_ROOT'] = os.path.join(workspace_root, 'cloud_spool')
                env['SCHEDULER_PROJECT_NAME'] = proj_name
                env['SCHEDULER_TASK_ID'] = str(task.task_id)
                existing_pythonpath = env.get('PYTHONPATH', '')
                env['PYTHONPATH'] = (
                    workspace_root
                    if not existing_pythonpath
                    else workspace_root + os.pathsep + existing_pythonpath
                )
                runtime_hooks = getattr(self, "runtime_hooks", None)
                if runtime_hooks and hasattr(runtime_hooks, "build_task_env"):
                    env = runtime_hooks.build_task_env(proj_name, task, env)
                
                with open(temp_stdout, 'w', encoding='utf-8', buffering=1) as t_out, \
                     open(temp_stderr, 'w', encoding='utf-8', buffering=1) as t_err:
                    
                    creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
                    if os.name == "nt":
                        creationflags |= getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)

                    process = subprocess.Popen(
                        cmd, 
                        stdout=t_out,
                        stderr=t_err,
                        stdin=subprocess.DEVNULL,
                        cwd=script_dir,
                        env=env,
                        creationflags=creationflags,
                        close_fds=False
                    )
                    
                    with self.active_processes_lock:
                        self.active_processes.append((process, proj_name, task.task_id))
                    
                    # Stream logs through temporary files instead of keeping full stdout/stderr in memory.
                    
                    t_out_read = open(temp_stdout, 'r', encoding='utf-8', errors='replace')
                    t_err_read = open(temp_stderr, 'r', encoding='utf-8', errors='replace')
                    
                    try:
                        while True:
                            if task.timeout > 0:
                                if time.time() - task_start_time > task.timeout:
                                    self._terminate_process_safely(process, task.filename)
                                    raise TimeoutError(f"Timeout ({task.timeout}s) exceeded")
                            
                            poll = process.poll()
                            
                            def read_lines(f, l_type):
                                new = f.read()
                                if new:
                                    for line in new.splitlines():
                                        if line.strip():
                                            self.detail_log(line.rstrip(), proj_name, task.filename, l_type)
                                            if l_type == "stdout":
                                                match = self._progress_pattern.search(line)
                                                if match:
                                                    c, t = match.groups()
                                                    with self.project_state_lock:
                                                        task.status = f"{self.TASK_STATUS_RUNNING} ({c}/{t})"
                                                    self.emit(SchedulerEvent.TASK_REFRESH, None)
                            
                            read_lines(t_out_read, "stdout")
                            read_lines(t_err_read, "stderr")
                            
                            if poll is not None:
                                #          Failed         
                                read_lines(t_out_read, "stdout")
                                read_lines(t_err_read, "stderr")
                                break
                                
                            # Check for project-wide stop request
                            proj = proj_dict.get(proj_name)
                            if proj and proj.stop_requested:
                                self._terminate_process_safely(process, task.filename)
                                break
                                
                            time.sleep(1)
                    finally:
                        t_out_read.close()
                        t_err_read.close()
                
                # C-8: active_processes in-place   
                with self.active_processes_lock:
                    self.active_processes = [p for p in self.active_processes if p[0] is not process]
                
                # C-1:     Failed               (       Failed
                try:
                    with open(log_path, "w", encoding="utf-8") as log_f:
                        log_f.write("[STDOUT]\n")
                        if os.path.exists(temp_stdout):
                            with open(temp_stdout, "r", encoding="utf-8", errors="replace") as src:
                                shutil.copyfileobj(src, log_f)
                        log_f.write("\n\n[STDERR]\n")
                        if os.path.exists(temp_stderr):
                            with open(temp_stderr, "r", encoding="utf-8", errors="replace") as src:
                                shutil.copyfileobj(src, log_f)
                except Exception as log_err:
                    self.log(f"Log file write error: {log_err}")
                
                if process.returncode == 0:
                    with self.project_state_lock:
                        task.status = self.TASK_STATUS_COMPLETED
                    self.log(f"[{proj_name}]   {task.filename}   ")
                    
                    proj = proj_dict.get(proj_name)
                    if proj:
                        with self.progress_lock:
                            proj.completed_tasks += 1
                            if proj.total_tasks > 0:
                                progress = (proj.completed_tasks / proj.total_tasks) * 100
                                self.emit(SchedulerEvent.PROGRESS_UPDATE, progress)
                    return
                else:
                    requested_stop = False
                    with self._requested_task_stops_lock:
                        requested_stop = (proj_name, task.task_id) in self._requested_task_stops
                        if requested_stop:
                            self._requested_task_stops.discard((proj_name, task.task_id))
                    if requested_stop or process.returncode in [15, -15]:
                        with self.project_state_lock:
                            task.status = self.TASK_STATUS_STOPPED
                    else:
                        self.emit(SchedulerEvent.TELEGRAM, f"     Error: {task.filename} in {proj_name}")
                        with self.project_state_lock:
                            task.status = self.TASK_STATUS_ERROR
                    if attempt < max_attempts: continue
                    
            except TimeoutError:
                with self.project_state_lock:
                    task.status = self.TASK_STATUS_TIMEOUT
                self.log(f"[{proj_name}]   {task.filename}      ")
            except Exception as e:
                with self.project_state_lock:
                    task.status = self.TASK_STATUS_SYSTEM_ERROR
                self.log(f"Error in {task.filename}: {e}")
            finally:
                if process is not None:
                    with self.active_processes_lock:
                        self.active_processes = [p for p in self.active_processes if p[0] is not process]
                # C-3: temp          (             )
                for tmp_f in [temp_stdout, temp_stderr]:
                    try:
                        if os.path.exists(tmp_f): os.remove(tmp_f)
                    except: pass
            
            self.emit(SchedulerEvent.TASK_REFRESH, None)
        
        with self.project_state_lock:
            if task.status == self.TASK_STATUS_WAITING or task.status.startswith(self.TASK_STATUS_RUNNING):
                task.status = self.TASK_STATUS_FINAL_FAIL

    def _execute_project_logic(self, proj, only_checked=False, trigger_source="manual"):
        try:
            actual = [t for t in proj.tasks if not (only_checked and not t.checked)]
            with self.progress_lock:
                proj.completed_tasks = 0
                proj.total_tasks = len(actual)
            self.emit(SchedulerEvent.PROGRESS_UPDATE, 0)

            with self.project_state_lock:
                proj.status = self.STATUS_RUNNING
                proj.stop_requested = False
            self.emit(SchedulerEvent.STATUS_UPDATE, f"Running {proj.name}")
            self.emit(SchedulerEvent.PROJECT_REFRESH, None)
            
            self.log(f"         : {proj.name}")
            
            source_label = "     " if trigger_source == "scheduled" else "     "
            self.log(f"[{proj.name}]      : {source_label}")
            steps = proj.get_tasks_by_step()
            proj_dict = {proj.name: proj} # Simplified for internal call
            
            for step_num in sorted(steps.keys()):
                with self.project_state_lock:
                    if proj.stop_requested: raise UserStopException()
                
                tasks = steps[step_num]
                if only_checked:
                    tasks = [t for t in tasks if t.checked]
                    if not tasks: continue

                self.log(f"    Step {step_num}    ({len(tasks)} , {proj.step_mode})")
                
                if proj.step_mode == "sequential":
                    for task in tasks:
                        with self.project_state_lock:
                            if proj.stop_requested: raise UserStopException()
                        self._run_single_script(task, proj.name, proj_dict)
                        # Fix 6:        Stopped         
                        if task.status not in self._NON_FAILURE_STATUSES:
                            break # Step failure
                else:
                    threads = []
                    for task in tasks:
                        with self.project_state_lock:
                            if proj.stop_requested: raise UserStopException()
                        t = threading.Thread(target=self._run_single_script, args=(task, proj.name, proj_dict))
                        t.start()
                        threads.append(t)
                    for t in threads: t.join()
                    
                self.emit(SchedulerEvent.TASK_REFRESH, None)
                # Fix 6:        Stopped         
                if any(t.status not in self._NON_FAILURE_STATUSES for t in tasks):
                    break

            with self.project_state_lock:
                finished_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
                proj.last_run = finished_at
                proj.last_trigger_source = trigger_source
                if trigger_source == "scheduled":
                    proj.last_scheduled_run = finished_at
                else:
                    proj.last_manual_run = finished_at
                proj.calculate_next_run()
                if proj.stop_requested:
                    proj.status = self.STATUS_STOPPED
                elif any(t.status not in self._NON_FAILURE_STATUSES for t in actual):
                    proj.status = self.STATUS_ERROR
                else:
                    proj.status = self.STATUS_COMPLETED
            
            proj.calculate_next_run()
            self.log(f"[{proj.name}]         : {trigger_source}")
            self.log(f"         : {proj.name} ({proj.status})")
            self.emit(SchedulerEvent.PROJECT_REFRESH, None)
            self.emit(SchedulerEvent.NOTIFICATION, f"     '{proj.name}'   ")
            
            # Send project completion notification.
            status_icon = " " if proj.status == self.STATUS_COMPLETED else " " if proj.status == self.STATUS_ERROR else "  "
            tg_msg = f"{status_icon} [{proj.name}] {proj.status}\n   {proj.last_run}"
            self.emit(SchedulerEvent.TELEGRAM, tg_msg)
            
            self.save_data()
            
        except UserStopException:
            finished_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
            with self.project_state_lock:
                proj.last_run = finished_at
                proj.last_trigger_source = trigger_source
                if trigger_source == "scheduled":
                    proj.last_scheduled_run = finished_at
                else:
                    proj.last_manual_run = finished_at
                proj.status = self.STATUS_STOPPED
                for task in proj.tasks:
                    if task.status in [self.TASK_STATUS_WAITING, self.TASK_STATUS_RUNNING] or task.status.startswith(self.TASK_STATUS_RUNNING):
                        task.status = "Stopped"
                proj.calculate_next_run()
            self.save_data()
            self.emit(SchedulerEvent.PROJECT_REFRESH, None)
            self.emit(SchedulerEvent.TASK_REFRESH, None)
        except Exception as e:
            # C-4:             Error     (Running      )
            self.log(f"  Error: {e}")
            with self.project_state_lock:
                proj.status = self.STATUS_ERROR
                for task in proj.tasks:
                    if task.status.startswith(self.TASK_STATUS_RUNNING):
                        task.status = self.TASK_STATUS_SYSTEM_ERROR
            self.emit(SchedulerEvent.PROJECT_REFRESH, None)
            self.emit(SchedulerEvent.TASK_REFRESH, None)
        finally:
            with self.project_state_lock:
                proj.stop_requested = False
                # C-4:        -    RUNNING      Error   
                if proj.status == self.STATUS_RUNNING:
                    proj.status = self.STATUS_ERROR
                    self.log(f"[{proj.name}]     RUNNING          Error   ")

            self._trace_schedule_event(
                proj.name,
                "PROJECT_FINISH",
                trigger_source=trigger_source,
                status=proj.status,
                last_run=proj.last_run,
                next_run=proj.next_run
            )

    def can_consume_ticket(self, proj, current_time):
        return self._get_ticket_block_reason(proj, current_time) is None

    def try_consume_ticket_atomic(self, proj, current_time):
        with self.project_state_lock:
            block_reason = self._get_ticket_block_reason(proj, current_time)
            if block_reason is not None:
                if block_reason not in ("not_due_yet", "ticket_already_consumed"):
                    diag_key = ("ticket_block", proj.next_run, block_reason, proj.last_consumed_ticket)
                    self._log_schedule_diag_once(
                        proj,
                        diag_key,
                        f"  [{proj.name}]          Failed- reason={block_reason}, "
                        f"next_run={proj.next_run}, last_ticket={proj.last_consumed_ticket}"
                    )
                return False
            
            if not proj.execution_lock.acquire(blocking=False):
                self._trace_schedule_event(
                    proj.name,
                    "PROJECT_START_SKIPPED",
                    trigger_source="scheduled",
                    reason="execution_lock_busy",
                    next_run=proj.next_run
                )
                self.log(f"   [{proj.name}]       Failed: execution_lock    ")
                return False
            
            if not self.semaphore.acquire(blocking=False):
                if proj.execution_lock.locked():
                    proj.execution_lock.release()
                self._trace_schedule_event(
                    proj.name,
                    "PROJECT_QUEUED",
                    trigger_source="scheduled",
                    reason="semaphore_full",
                    next_run=proj.next_run
                )
                self.log(f"[{proj.name}]       Failed: semaphore      Waiting     ")
                if proj.name not in self._pending_set:
                    self._pending_set.add(proj.name)
                    self.pending_queue.put((proj, current_time, False, "scheduled"))
                return False
            
            proj.last_consumed_ticket = proj.next_run
            proj.execution_id += 1
            self._pending_set.discard(proj.name)
            self.save_data()

            self.log(f"[{proj.name}]       Done: {proj.next_run} (  ID: {proj.execution_id})")
            self._trace_schedule_event(proj.name, "SCHEDULE_TICKET_CONSUMED", next_run=proj.next_run, execution_id=proj.execution_id)
            return self._launch_project_with_acquired_slot(proj, False, "scheduled")
    #     /          /Stopped    
    def run_project_manual(self, proj, only_checked=False):
        """                     """
        if proj.status == self.STATUS_RUNNING:
            self._trace_schedule_event(
                proj.name,
                "PROJECT_START_SKIPPED",
                trigger_source="manual",
                reason="already_running"
            )
            return False
        
        if not proj.execution_lock.acquire(blocking=False):
            self._trace_schedule_event(
                proj.name,
                "PROJECT_START_SKIPPED",
                trigger_source="manual",
                reason="execution_lock_busy"
            )
            return False
        
        if not self.semaphore.acquire(blocking=False):
            proj.execution_lock.release()
            self._trace_schedule_event(
                proj.name,
                "PROJECT_START_SKIPPED",
                trigger_source="manual",
                reason="semaphore_full"
            )
            return False
        
        proj.execution_id += 1
        self._trace_schedule_event(
            proj.name,
            "MANUAL_TRIGGER_ACCEPTED",
            trigger_source="manual",
            only_checked=only_checked,
            execution_id=proj.execution_id
        )
        # Fix 5: only_checked=False (          task   )
        return self._launch_project_with_acquired_slot(proj, only_checked, "manual")
        self.log(f"[{proj.name}]          (     )")
        return True
    
    # The old duplicate stop_project implementation was removed because it did not
    # track active process tuples correctly.

    # Project CRUD wrappers
    def add_project(self, name):
        self.projects.append(Project(name, "09:00"))
        self.save_data()
        self.emit(SchedulerEvent.PROJECT_REFRESH, None)
    
    def remove_project(self, index):
        if 0 <= index < len(self.projects):
            del self.projects[index]
            self.save_data()
            self.emit(SchedulerEvent.PROJECT_REFRESH, None)

    def save_history(self, record):
        try:
            history = []
            if os.path.exists(self.history_file):
                try:
                    history = load_json_file(self.history_file, default=[]) or []
                except Exception:
                    pass
            
            history.append(record)
            # Limit history
            if len(history) > 1000:
                history = history[-1000:]

            with self.persistence_lock:
                atomic_write_json(self.history_file, history, indent=4)
        except Exception as e:
            self.log(f"        Error: {e}")

    def _reset_daily_state(self):
        self._last_schedule_diag.clear()
        self._trace_schedule_event("SYSTEM", "DAILY_RESET")
        """       -        /        """
        self.log("                 .           .")
        
        with self.project_state_lock:
            for proj in self.projects:
                proj.status = self.STATUS_WAITING
                proj.completed_tasks = 0
                proj.total_tasks = 0
                if hasattr(proj, 'last_executed_minute'):
                    proj.last_executed_minute = None
                proj.calculate_next_run()
                
                for task in proj.tasks:
                    task.status = self.TASK_STATUS_WAITING
        
        self.emit(SchedulerEvent.PROGRESS_UPDATE, 0)
        self.emit(SchedulerEvent.CLEAR_LOGS, None)
        
        try:
            if os.path.exists(self.session_state_file):
                os.remove(self.session_state_file)
        except Exception as e:
            self.log(f"Session file removal error: {e}")
            
        self.last_all_done_date = None
        self.emit(SchedulerEvent.PROJECT_REFRESH, None)
        self.emit(SchedulerEvent.TASK_REFRESH, None)

    def _check_all_projects_completed(self):
        """
        Send a daily notification after all enabled projects have finished and no project is running.
        """
        today_str = datetime.datetime.now().strftime("%Y-%m-%d")
        
        if self.last_all_done_date == today_str:
            return

        for proj in self.projects:
            if proj.status == self.STATUS_RUNNING:
                return 

        all_done = True
        for proj in self.projects:
            if not proj.enabled: continue
            
            if proj.last_run == "-" or not proj.last_run.startswith(today_str):
                all_done = False
                break
        
        if all_done:
            self.last_all_done_date = today_str
            msg = f"All projects completed for {today_str}.\n\nEvery enabled project has finished running."
            self.emit(SchedulerEvent.TELEGRAM, msg)
            self.log("                   Done")
