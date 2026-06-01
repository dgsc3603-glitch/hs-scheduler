import logging
import os
import queue
import threading
import time
import datetime
import json

from component.core.scheduler_core import SchedulerEvent
from component.data_validation import has_diagnostics, validate_scheduler_payload
from component.core.telegram_bot import SchedulerTelegramBot
from component.distributed import DistributedControlPlane, DistributedRuntimeConfig, ProjectPolicyCollection
from component.models import Project
from component.utils import CredentialManager, atomic_write_json, load_json_file, send_telegram_alert

from .runtime import EngineRuntimeCore
from .store import EngineStore


class EngineService:
    def __init__(self, base_dir, logger=None, runtime_config_path=None, policy_path=None):
        self.base_dir = base_dir
        self.db_path = os.path.join(base_dir, "scheduler_engine.db")
        self.legacy_data_path = os.path.join(base_dir, "scheduler_data.json")
        self.runtime_config_path = runtime_config_path or os.path.join(base_dir, "config", "distributed_runtime.json")
        self.policy_path = policy_path or os.path.join(base_dir, "config", "project_policies.json")
        self.store = EngineStore(self.db_path)
        self.logger = logger or logging.getLogger("SchedulerEngine")
        self.credentials = CredentialManager(self.legacy_data_path)
        self.runtime_config = DistributedRuntimeConfig.load(base_dir, self.runtime_config_path)
        self.project_policies = ProjectPolicyCollection.load(base_dir, self.policy_path)
        self.control_plane = DistributedControlPlane(
            self.runtime_config,
            self.project_policies,
            logger=self.logger,
        )
        self.event_queue = queue.Queue(maxsize=5000)
        self.core = EngineRuntimeCore(
            self.event_queue,
            self.credentials,
            self.legacy_data_path,
            runtime_hooks=self,
        )
        self.telegram_bot = SchedulerTelegramBot(self.core, self.credentials)
        self._running = False
        self._command_thread = None
        self._event_thread = None
        self._sync_thread = None
        self._active_run_ids = {}
        self._active_run_lock = threading.Lock()
        self._active_distributed_runs = {}
        self._active_artifact_captures = {}
        self._requested_task_stops = set()
        self._requested_task_stops_lock = threading.Lock()
        self._last_ticket_reconcile_at = 0.0

    def on_core_loaded(self, projects):
        self.control_plane.apply_runtime_overrides(projects)
        self._reconcile_control_plane_tickets(force=True)

    def start(self):
        self.store.initialize()
        recovery = self.store.recover_interrupted_runtime()
        if recovery["recovered_runs"] or recovery["recovered_running_commands"]:
            self.logger.warning(
                "Recovered interrupted engine state: runs=%s running_commands=%s",
                recovery["recovered_runs"],
                recovery["recovered_running_commands"],
            )
        self.control_plane.initialize()
        self.core.start()
        try:
            self.core._heal_ghost_running_projects()
        except Exception:
            self.logger.exception("Startup ghost-state recovery failed")
        self._sync_store_from_core()
        self.store.record_event(
            "INFO",
            "ENGINE_START",
            "engine service started",
            payload={
                "db_path": self.db_path,
                "distributed": self.control_plane.status(),
            },
        )
        self._running = True
        self._command_thread = threading.Thread(
            target=self._command_loop,
            name="engine-command-loop",
            daemon=True,
        )
        self._event_thread = threading.Thread(
            target=self._event_loop,
            name="engine-event-loop",
            daemon=True,
        )
        self._sync_thread = threading.Thread(
            target=self._sync_loop,
            name="engine-sync-loop",
            daemon=True,
        )
        self._command_thread.start()
        self._event_thread.start()
        self._sync_thread.start()
        self.telegram_bot.start()
        self.logger.info("Engine service started with db=%s", self.db_path)

    def stop(self):
        self._running = False
        try:
            self.telegram_bot.stop()
        except Exception:
            pass
        try:
            self.control_plane.release_lease()
        except Exception:
            pass
        try:
            self.core.running = False
            self.core.shutdown()
        except Exception:
            pass
        for thread in (self._command_thread, self._event_thread, self._sync_thread):
            if thread and thread.is_alive():
                thread.join(timeout=2.0)
        self.store.record_event("INFO", "ENGINE_STOP", "engine service stopped")

    def shutdown(self):
        self.stop()
        return {
            "stopped": True,
            "distributed": self.control_plane.status(),
        }

    def health(self):
        counts = self.store.get_counts()
        return {
            "running": self._running,
            "base_dir": self.base_dir,
            "db_path": self.db_path,
            "legacy_data_path": self.legacy_data_path,
            "counts": counts,
            "mode": "runtime",
            "telegram_bot_running": bool(self.telegram_bot.running),
            "telegram_bot_configured": bool(self.telegram_bot.is_configured),
            "distributed": self.control_plane.status(),
        }

    def list_projects(self):
        return self.store.list_projects()

    def get_project_tasks(self, project_name):
        return self.store.get_project_tasks(project_name)

    def list_events(self, after_event_id=0, limit=200):
        return self.store.list_events(after_event_id=after_event_id, limit=limit)

    def latest_event_id(self):
        return self.store.latest_event_id()

    def get_command(self, command_id):
        return self.store.get_command(command_id)

    def sync_from_legacy(self):
        reloaded = False
        merged = False
        if self._has_live_runtime():
            merged = self._merge_core_projects_from_legacy()
        else:
            self.core.load_data()
            self.control_plane.apply_runtime_overrides(self.core.projects)
            reloaded = True

        result = self._sync_store_from_core()
        result["reloaded_core"] = reloaded
        result["merged_core"] = merged
        self.store.record_event(
            "INFO",
            "ENGINE_SYNC",
            "legacy sync completed",
            payload=result,
        )
        return result

    def submit_run_command(self, project_name, trigger_source="manual", only_checked=False):
        command_id = self.store.submit_command(
            "run_project",
            project_name=project_name,
            payload={"trigger_source": trigger_source, "only_checked": bool(only_checked)},
        )
        self.store.record_event(
            "INFO",
            "COMMAND_ENQUEUED",
            "run command enqueued",
            project_name=project_name,
            payload={
                "command_id": command_id,
                "trigger_source": trigger_source,
                "only_checked": bool(only_checked),
            },
        )
        return command_id

    def submit_stop_command(self, project_name):
        command_id = self.store.submit_command("stop_project", project_name=project_name, payload={})
        self.store.record_event(
            "INFO",
            "COMMAND_ENQUEUED",
            "stop command enqueued",
            project_name=project_name,
            payload={"command_id": command_id},
        )
        return command_id

    def submit_stop_task_command(self, project_name, task_id):
        command_id = self.store.submit_command(
            "stop_task",
            project_name=project_name,
            payload={"task_id": task_id},
        )
        self.store.record_event(
            "INFO",
            "COMMAND_ENQUEUED",
            "stop task command enqueued",
            project_name=project_name,
            payload={"command_id": command_id, "task_id": task_id},
        )
        return command_id

    def submit_run_task_command(self, project_name, task_id):
        command_id = self.store.submit_command(
            "run_task",
            project_name=project_name,
            payload={"task_id": task_id},
        )
        self.store.record_event(
            "INFO",
            "COMMAND_ENQUEUED",
            "run task command enqueued",
            project_name=project_name,
            payload={"command_id": command_id, "task_id": task_id},
        )
        return command_id

    def on_project_launch(self, proj, trigger_source, only_checked):
        claim = self.control_plane.claim_project_run(proj, trigger_source)
        if not claim.allowed or not claim.claimed:
            self.store.record_event(
                "INFO",
                "DISTRIBUTED_RUN_SKIPPED",
                f"project skipped by distributed policy: {claim.reason}",
                project_name=proj.name,
                payload={
                    "trigger_source": trigger_source,
                    "run_key": claim.run_key,
                    "lane": claim.lane,
                },
            )
            return False

        with self._active_run_lock:
            if proj.name in self._active_run_ids:
                self.control_plane.finish_run(
                    proj.name,
                    claim.run_key,
                    result="cancelled",
                    message="local_active_run_exists",
                )
                return False
            run_id = self.store.start_project_run(
                proj.name,
                trigger_source,
                metadata={
                    "execution_id": proj.execution_id,
                    "only_checked": bool(only_checked),
                    "run_key": claim.run_key,
                    "lane": claim.lane,
                },
            )
            self._active_run_ids[proj.name] = run_id
            self._active_distributed_runs[proj.name] = {
                "run_key": claim.run_key,
                "lane": claim.lane,
                "trigger_source": trigger_source,
            }
            if self.runtime_config.artifact_capture_enabled:
                self._active_artifact_captures[proj.name] = self.control_plane.begin_artifact_capture(proj.name)
            else:
                self._active_artifact_captures[proj.name] = None
        return True

    def distributed_status(self):
        return self.control_plane.status()

    def distributed_config_document(self):
        raw = {}
        if os.path.exists(self.runtime_config_path):
            raw = load_json_file(self.runtime_config_path, default={}) or {}
        return {
            "document": raw,
            "resolved": self.runtime_config.to_dict(),
            "status": self.control_plane.status(),
            "config_path": self.runtime_config_path,
            "policy_path": self.policy_path,
        }

    def update_distributed_config(self, document):
        if not isinstance(document, dict):
            raise ValueError("distributed config payload must be an object")

        os.makedirs(os.path.dirname(self.runtime_config_path), exist_ok=True)
        atomic_write_json(self.runtime_config_path, document, indent=2)

        reload_error = ""
        try:
            self.runtime_config = DistributedRuntimeConfig.load(self.base_dir, self.runtime_config_path)
            self.project_policies = ProjectPolicyCollection.load(self.base_dir, self.policy_path)
            self.control_plane = DistributedControlPlane(
                self.runtime_config,
                self.project_policies,
                logger=self.logger,
            )
            self.control_plane.initialize()
            self.control_plane.apply_runtime_overrides(self.core.projects)
            self._reconcile_control_plane_tickets(force=True)
        except Exception as exc:
            reload_error = str(exc)
            self.logger.exception("Distributed runtime reload failed")

        payload = self.distributed_config_document()
        payload["reload_error"] = reload_error
        self.store.record_event(
            "INFO" if not reload_error else "ERROR",
            "DISTRIBUTED_CONFIG_UPDATED",
            "distributed runtime config updated",
            payload={
                "config_path": self.runtime_config_path,
                "reload_error": reload_error,
            },
        )
        return payload

    def build_task_env(self, project_name, task, env):
        policy = self.control_plane.policy_for(project_name)
        env["SCHEDULER_ORACLE_ENABLED"] = "1" if policy.get("oracle_enabled", True) else "0"
        env["SCHEDULER_BROWSER_MODE"] = str(policy.get("browser_mode", "headless"))
        env["SCHEDULER_AUTH_MODE"] = str(policy.get("auth_mode", "direct_login"))
        env["SCHEDULER_MAX_BROWSER_CONCURRENCY"] = str(policy.get("max_browser_concurrency", 5))
        env["SCHEDULER_FALLBACK_POLICY"] = str(policy.get("fallback_policy", "pc_original"))
        env["SCHEDULER_RETENTION_DAYS"] = str(policy.get("retention_days", 7))
        distributed_run = self._active_distributed_runs.get(project_name, {})
        if distributed_run.get("run_key"):
            env["SCHEDULER_RUN_KEY"] = str(distributed_run["run_key"])
        if self.runtime_config.node_role:
            env["SCHEDULER_NODE_ROLE"] = self.runtime_config.node_role
        return env

    def acknowledge_artifact_transfer(self, project_name, run_key, pc_archive_path, checksum=""):
        updated = self.control_plane.acknowledge_artifact_archived(
            run_key,
            pc_archive_path,
            checksum=checksum,
        )
        if updated:
            self.control_plane.cleanup_spool_after_archive(project_name, run_key)
            self.store.record_event(
                "INFO",
                "ARTIFACT_ARCHIVED_ON_PC",
                "artifact transfer acknowledged by PC archive",
                project_name=project_name,
                payload={
                    "run_key": run_key,
                    "pc_archive_path": pc_archive_path,
                    "checksum": checksum,
                },
            )
        return {"updated": bool(updated)}

    def on_project_finish(self, proj, trigger_source, only_checked):
        with self._active_run_lock:
            run_id = self._active_run_ids.pop(proj.name, None)
            distributed_run = self._active_distributed_runs.pop(proj.name, None)
            artifact_capture = self._active_artifact_captures.pop(proj.name, None)

        if not run_id:
            return

        result = self._map_project_result(proj.status)
        self.store.finish_project_run(
            run_id,
            proj.name,
            result=result,
            message=f"project finished with status {proj.status}",
        )
        if distributed_run:
            run_key = distributed_run.get("run_key")
            self.control_plane.sync_task_states(proj.name, run_key, proj.tasks)
            self.control_plane.finish_run(
                proj.name,
                run_key,
                result=result,
                message=f"project finished with status {proj.status}",
            )
            if self.runtime_config.artifact_capture_enabled:
                artifact_result = self.control_plane.finalize_artifact_capture(
                    proj.name,
                    run_key,
                    artifact_capture,
                )
                if artifact_result.get("spool_path"):
                    self.store.record_event(
                        "INFO",
                        "ARTIFACT_SPOOLED",
                        "project artifacts spooled for PC archive",
                        project_name=proj.name,
                        payload={
                            "run_key": run_key,
                            "spool_path": artifact_result.get("spool_path"),
                            "checksum": artifact_result.get("checksum"),
                            "file_count": len(artifact_result.get("files", [])),
                        },
                    )

    def _command_loop(self):
        while self._running:
            try:
                commands = self.store.list_pending_commands(limit=20)
                if not commands:
                    time.sleep(1.0)
                    continue

                for command in commands:
                    self._process_command(command)
            except Exception:
                self.logger.exception("Engine command loop failed")
                time.sleep(1.0)

    def _event_loop(self):
        while self._running:
            try:
                event = self.event_queue.get(timeout=1.0)
            except queue.Empty:
                continue
            try:
                self._handle_core_event(event)
            except Exception:
                self.logger.exception("Engine event loop failed")
                time.sleep(0.2)

    def _sync_loop(self):
        while self._running:
            try:
                self.control_plane.heartbeat()
                self._reconcile_control_plane_tickets()
                self._sync_store_from_core()
                self._heartbeat_active_runs()
                self._normalize_requested_task_stops()
                self.control_plane.archive_expired_rows()
            except Exception:
                self.logger.exception("Engine sync loop failed")
            time.sleep(2.0)

    def _reconcile_control_plane_tickets(self, force=False):
        if not self.control_plane.control_plane_enabled:
            return {"backfilled_count": 0, "reconciled_count": 0}

        now = time.time()
        if not force and (now - self._last_ticket_reconcile_at) < 15:
            return {"backfilled_count": 0, "reconciled_count": 0}

        self._last_ticket_reconcile_at = now
        backfill_result = self.control_plane.backfill_local_scheduled_runs(self.core.projects)
        reconcile_result = self.control_plane.reconcile_consumed_tickets(self.core.projects)
        changed = int(backfill_result.get("backfilled_count", 0)) + int(reconcile_result.get("reconciled_count", 0))
        if changed:
            self.core.save_data()
            self._sync_store_from_core()
            self.store.record_event(
                "INFO",
                "DISTRIBUTED_TICKETS_RECONCILED",
                "distributed scheduled tickets synchronized",
                payload={
                    "backfilled_count": int(backfill_result.get("backfilled_count", 0)),
                    "reconciled_count": int(reconcile_result.get("reconciled_count", 0)),
                },
            )
        return {
            "backfilled_count": int(backfill_result.get("backfilled_count", 0)),
            "reconciled_count": int(reconcile_result.get("reconciled_count", 0)),
        }

    def _process_command(self, command):
        command_id = command["command_id"]
        command_type = command["command_type"]
        project_name = command["project_name"]
        payload = command["payload"]

        self.store.mark_command_status(command_id, "running")
        try:
            project = self._find_project(project_name)
            if command_type == "run_project":
                if not project:
                    raise ValueError(f"project not found: {project_name}")
                accepted = self.core.run_project_manual(
                    project,
                    only_checked=bool(payload.get("only_checked", False)),
                )
                if not accepted:
                    raise RuntimeError(f"project launch rejected: {project_name}")
                self.store.mark_command_status(command_id, "completed", "accepted")
            elif command_type == "stop_project":
                if not project:
                    raise ValueError(f"project not found: {project_name}")
                self.core.stop_project(project_name)
                self.store.mark_command_status(command_id, "completed", "accepted")
            elif command_type == "stop_task":
                if not project:
                    raise ValueError(f"project not found: {project_name}")
                task_id = payload.get("task_id")
                if not task_id:
                    raise ValueError("task_id is required")
                with self._requested_task_stops_lock:
                    self._requested_task_stops.add((project_name, task_id))
                stopped = self.core.stop_task(project_name, task_id)
                if not stopped:
                    with self._requested_task_stops_lock:
                        self._requested_task_stops.discard((project_name, task_id))
                    raise RuntimeError(f"task stop rejected: {project_name}/{task_id}")
                self.store.mark_command_status(command_id, "completed", "accepted")
            elif command_type == "run_task":
                if not project:
                    raise ValueError(f"project not found: {project_name}")
                task_id = payload.get("task_id")
                if not task_id:
                    raise ValueError("task_id is required")
                accepted = self._run_single_task_only(project, task_id)
                if not accepted:
                    raise RuntimeError(f"task launch rejected: {project_name}/{task_id}")
                self.store.mark_command_status(command_id, "completed", "accepted")
            else:
                raise ValueError(f"unknown command type: {command_type}")
        except (ValueError, RuntimeError) as exc:
            self.logger.warning("Command rejected: %s", exc)
            self.store.mark_command_status(command_id, "failed", str(exc))
            self.store.record_event(
                "WARNING",
                "COMMAND_FAILED",
                str(exc),
                project_name=project_name,
                payload={"command_type": command_type},
            )
        except Exception as exc:
            self.logger.exception("Command processing failed")
            self.store.mark_command_status(command_id, "failed", str(exc))
            self.store.record_event(
                "ERROR",
                "COMMAND_FAILED",
                str(exc),
                project_name=project_name,
                payload={"command_type": command_type},
            )

    def _handle_core_event(self, event):
        if event.type == SchedulerEvent.LOG_SUMMARY:
            self.store.record_event("INFO", "LOG_SUMMARY", str(event.data))
            return

        if event.type == SchedulerEvent.LOG_DETAIL:
            data = event.data or {}
            log_type = data.get("log_type", "detail")
            self.store.record_event(
                "INFO",
                "LOG_DETAIL",
                data.get("message", ""),
                project_name=data.get("proj_name"),
                payload={"task_name": data.get("task_name"), "log_type": log_type},
            )
            return

        if event.type == SchedulerEvent.STATUS_UPDATE:
            self.store.record_event("INFO", "STATUS_UPDATE", str(event.data))
            return

        if event.type == SchedulerEvent.PROJECT_REFRESH:
            self._sync_store_from_core()
            return

        if event.type == SchedulerEvent.TASK_REFRESH:
            self._sync_store_from_core()
            return

        if event.type == SchedulerEvent.SAVE_DATA:
            self._sync_store_from_core()
            return

        if event.type == SchedulerEvent.TELEGRAM:
            self.store.record_event("INFO", "TELEGRAM", str(event.data))
            try:
                send_telegram_alert(
                    str(event.data),
                    self.credentials.load(),
                    log_func=lambda msg: self.logger.info(msg),
                )
            except Exception:
                self.logger.exception("Telegram send failed")
            return

        if event.type == SchedulerEvent.NOTIFICATION:
            self.store.record_event("INFO", "NOTIFICATION", str(event.data))
            return

        if event.type == SchedulerEvent.CLEAR_LOGS:
            self.store.record_event("INFO", "CLEAR_LOGS", "clear logs requested")
            return

        if event.type == SchedulerEvent.PROGRESS_UPDATE:
            return

    def _sync_store_from_core(self):
        return self.store.sync_from_models(self.core.projects)

    def _heartbeat_active_runs(self):
        with self._active_run_lock:
            active_items = list(self._active_run_ids.items())
        for project_name, run_id in active_items:
            project = self._find_project(project_name)
            status = "running"
            if project and project.status != self.core.STATUS_RUNNING:
                status = "finishing"
            self.store.heartbeat_run(run_id, status=status, project_name=project_name)
            distributed_run = self._active_distributed_runs.get(project_name)
            if distributed_run:
                self.control_plane.heartbeat_run(
                    project_name,
                    distributed_run.get("run_key"),
                    status=status,
                )
                if project:
                    self.control_plane.sync_task_states(
                        project_name,
                        distributed_run.get("run_key"),
                        project.tasks,
                    )

    def _find_project(self, project_name):
        for project in self.core.projects:
            if project.name == project_name:
                return project
        return None

    def _find_task(self, project, task_id):
        for task in project.tasks:
            if task.task_id == task_id:
                return task
        return None

    def _run_single_task_only(self, project, task_id):
        task = self._find_task(project, task_id)
        if not task:
            return False

        if project.status == self.core.STATUS_RUNNING and self.core._has_live_project_runtime(project.name):
            return False

        if not project.execution_lock.acquire(blocking=False):
            recovered = self.core._recover_stale_project_state(project, "engine_single_task")
            if not recovered or not project.execution_lock.acquire(blocking=False):
                return False

        if not self.core.semaphore.acquire(blocking=False):
            try:
                project.execution_lock.release()
            except RuntimeError:
                pass
            return False

        thread = threading.Thread(
            target=self._single_task_worker,
            args=(project, task),
            daemon=True,
            name=f"engine-single-task:{project.name}:{task.task_id}",
        )
        if self.on_project_launch(project, trigger_source="single_task", only_checked=False) is False:
            try:
                project.execution_lock.release()
            except RuntimeError:
                pass
            try:
                self.core.semaphore.release()
            except ValueError:
                pass
            return False
        try:
            thread.start()
        except Exception as exc:
            self._rollback_project_launch(project, str(exc))
            try:
                project.execution_lock.release()
            except RuntimeError:
                pass
            try:
                self.core.semaphore.release()
            except ValueError:
                pass
            return False
        return True

    def _single_task_worker(self, project, task):
        try:
            with self.core.project_state_lock:
                project.status = self.core.STATUS_RUNNING
                project.stop_requested = False
                project.total_tasks = 1
                project.completed_tasks = 0
                task.status = self.core.TASK_STATUS_WAITING

            self.core.log(f"[{project.name}] single task launch accepted: {task.filename}")
            self.core.emit(SchedulerEvent.PROJECT_REFRESH, None)
            self.core.emit(SchedulerEvent.TASK_REFRESH, None)

            project_map = {p.name: p for p in self.core.projects}
            self.core._run_single_script(task, project.name, project_map)

            with self.core.project_state_lock:
                finished_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
                project.last_run = finished_at
                project.last_trigger_source = "single_task"
                project.last_manual_run = finished_at
                if task.status in (
                    self.core.TASK_STATUS_COMPLETED,
                    self.core.TASK_STATUS_SKIPPED,
                ):
                    project.status = self.core.STATUS_COMPLETED
                elif task.status == self.core.TASK_STATUS_STOPPED:
                    project.status = self.core.STATUS_STOPPED
                else:
                    project.status = self.core.STATUS_ERROR
                project.calculate_next_run()

            self.core.save_data()
            self._sync_store_from_core()
            self.core.emit(SchedulerEvent.PROJECT_REFRESH, None)
            self.core.emit(SchedulerEvent.TASK_REFRESH, None)
        finally:
            self.on_project_finish(project, trigger_source="single_task", only_checked=False)
            with self.core.project_state_lock:
                project.stop_requested = False
            try:
                if project.execution_lock.locked():
                    project.execution_lock.release()
            except RuntimeError:
                pass
            try:
                self.core.semaphore.release()
            except ValueError:
                pass

    def _map_project_result(self, status):
        if status == self.core.STATUS_COMPLETED:
            return "success"
        if status == self.core.STATUS_ERROR:
            return "error"

        lowered = str(status).lower()
        if "stop" in lowered:
            return "stopped"
        return lowered or "completed"

    def _has_live_runtime(self):
        with self._active_run_lock:
            if self._active_run_ids:
                return True
        if any(self.core._has_live_project_runtime(project.name) for project in self.core.projects):
            return True
        with self.core.active_processes_lock:
            return bool(self.core.active_processes)

    def _rollback_project_launch(self, project, message):
        with self._active_run_lock:
            run_id = self._active_run_ids.pop(project.name, None)
            distributed_run = self._active_distributed_runs.pop(project.name, None)
            self._active_artifact_captures.pop(project.name, None)
        if run_id:
            self.store.finish_project_run(
                run_id,
                project.name,
                result="error",
                message=message,
            )
        if distributed_run:
            self.control_plane.finish_run(
                project.name,
                distributed_run.get("run_key"),
                result="error",
                message=message,
            )

    def _load_legacy_projects(self):
        if not os.path.exists(self.legacy_data_path):
            return []

        payload, diagnostics = validate_scheduler_payload(load_json_file(self.legacy_data_path, default=[]))
        if has_diagnostics(diagnostics):
            self.logger.warning(
                "Legacy project data validation completed: errors=%s warnings=%s repairs=%s quarantined_projects=%s quarantined_tasks=%s",
                len(diagnostics["errors"]),
                len(diagnostics["warnings"]),
                len(diagnostics["repairs"]),
                len(diagnostics["quarantined_projects"]),
                len(diagnostics["quarantined_tasks"]),
            )

        projects = []
        for item in payload:
            kwargs = {key: value for key, value in item.items() if key not in ("name", "run_time", "tasks")}
            projects.append(Project(item["name"], item["run_time"], item.get("tasks", []), **kwargs))
        return projects

    def _merge_core_projects_from_legacy(self):
        incoming_projects = self._load_legacy_projects()
        current_by_name = {project.name: project for project in self.core.projects}
        incoming_names = {project.name for project in incoming_projects}
        with self._active_run_lock:
            live_names = set(self._active_run_ids.keys())
        live_names.update(
            project.name
            for project in self.core.projects
            if self.core._has_live_project_runtime(project.name)
        )

        merged_projects = []
        for incoming in incoming_projects:
            existing = current_by_name.get(incoming.name)
            if existing and incoming.name in live_names:
                self._apply_legacy_config_to_live_project(existing, incoming)
                merged_projects.append(existing)
            else:
                merged_projects.append(incoming)

        for existing in self.core.projects:
            if existing.name in incoming_names:
                continue
            if existing.name in live_names:
                merged_projects.append(existing)

        with self.core.project_state_lock:
            self.core.projects = merged_projects
        self.control_plane.apply_runtime_overrides(self.core.projects)
        return True

    def _apply_legacy_config_to_live_project(self, target, source):
        target.last_executed_minute = source.last_executed_minute

    def _normalize_requested_task_stops(self):
        with self._requested_task_stops_lock:
            requested = list(self._requested_task_stops)

        if not requested:
            return

        changed = False
        for project_name, task_id in requested:
            project = self._find_project(project_name)
            task = self._find_task(project, task_id) if project else None
            if not task:
                with self._requested_task_stops_lock:
                    self._requested_task_stops.discard((project_name, task_id))
                continue

            if task.status == self.core.TASK_STATUS_STOPPED:
                with self._requested_task_stops_lock:
                    self._requested_task_stops.discard((project_name, task_id))
                continue

            if task.status in (
                self.core.TASK_STATUS_FINAL_FAIL,
                self.core.TASK_STATUS_ERROR,
                self.core.TASK_STATUS_SYSTEM_ERROR,
            ):
                with self.core.project_state_lock:
                    task.status = self.core.TASK_STATUS_STOPPED
                with self._requested_task_stops_lock:
                    self._requested_task_stops.discard((project_name, task_id))
                changed = True

        if changed:
            self.core.save_data()
            self._sync_store_from_core()
