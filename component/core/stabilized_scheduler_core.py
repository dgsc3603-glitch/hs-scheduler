import datetime

from component.core.scheduler_core import SchedulerCore, SchedulerEvent


class StabilizedSchedulerCore(SchedulerCore):
    def _heal_ghost_running_projects(self):
        try:
            for proj in self.projects:
                worker = self._get_project_worker_state(proj.name)
                active_processes = self._get_project_active_process_count(proj.name)
                worker_alive = bool(worker and worker["thread_alive"])

                if proj.status == self.STATUS_RUNNING and not worker_alive and active_processes == 0:
                    self.log(f"[{proj.name}] ghost running state detected; recovering")
                    with self.project_state_lock:
                        proj.status = self.STATUS_ERROR
                        for task in proj.tasks:
                            if task.status == self.TASK_STATUS_RUNNING or task.status.startswith(self.TASK_STATUS_RUNNING):
                                task.status = self.TASK_STATUS_SYSTEM_ERROR
                        proj.stop_requested = False
                        proj.calculate_next_run()

                    if proj.execution_lock.locked():
                        try:
                            proj.execution_lock.release()
                        except RuntimeError:
                            pass

                    try:
                        self.semaphore.release()
                    except ValueError:
                        pass

                    self.save_data()
                    self.emit(SchedulerEvent.PROJECT_REFRESH, None)
                    self.emit(SchedulerEvent.TASK_REFRESH, None)
                    continue

                if proj.status == self.STATUS_WAITING and proj.execution_lock.locked():
                    self._recover_stale_project_state(proj, "waiting_lock_without_runtime")
        except Exception as e:
            self.log(f"Self-healing error: {e}")

    def run_project(self, proj, only_checked=False, trigger_source="scheduled"):
        if not proj.execution_lock.acquire(blocking=False):
            recovered = self._recover_stale_project_state(proj, f"run_project:{trigger_source}")
            if not recovered or not proj.execution_lock.acquire(blocking=False):
                self.log(f"[{proj.name}] execution_lock acquire failed")
                self._trace_schedule_event(proj.name, "PROJECT_START_SKIPPED", trigger_source=trigger_source, reason="execution_lock_busy")
                return False

        if not self.semaphore.acquire(blocking=False):
            proj.execution_lock.release()
            if trigger_source != "scheduled":
                self.log(f"[{proj.name}] manual start blocked by semaphore")
                self._trace_schedule_event(proj.name, "PROJECT_START_SKIPPED", trigger_source=trigger_source, reason="semaphore_full")
                return False
            self.log(f"[{proj.name}] queued because semaphore is full")
            self._trace_schedule_event(proj.name, "PROJECT_QUEUED", trigger_source=trigger_source, reason="semaphore_full", next_run=proj.next_run)
            if proj.name not in self._pending_set:
                self._pending_set.add(proj.name)
                self.pending_queue.put((proj, datetime.datetime.now(), only_checked, trigger_source))
            return False

        if trigger_source == "scheduled" and proj.last_consumed_ticket != proj.next_run:
            proj.last_consumed_ticket = proj.next_run
            self.save_data()
            self.log(f"[{proj.name}] scheduled ticket consumed via pending path: {proj.next_run}")

        self._pending_set.discard(proj.name)
        self.log(f"[{proj.name}] project launch accepted (next_run: {proj.next_run})")
        return self._launch_project_with_acquired_slot(proj, only_checked, trigger_source)

    def _execute_wrapper(self, proj, only_checked, trigger_source, run_id):
        try:
            self._execute_project_logic(proj, only_checked, trigger_source)
        finally:
            self._track_project_worker_finish(proj.name, run_id)
            try:
                if proj.execution_lock.locked():
                    proj.execution_lock.release()
            except RuntimeError:
                pass
            try:
                self.semaphore.release()
            except ValueError:
                pass
            self._process_pending()
            self._check_all_projects_completed()

    def _execute_project_logic(self, proj, only_checked=False, trigger_source="manual"):
        try:
            return super()._execute_project_logic(proj, only_checked=only_checked, trigger_source=trigger_source)
        finally:
            self.save_data()

    def try_consume_ticket_atomic(self, proj, current_time):
        with self.project_state_lock:
            block_reason = self._get_ticket_block_reason(proj, current_time)
            if block_reason is not None:
                if block_reason not in ("not_due_yet", "ticket_already_consumed"):
                    diag_key = ("ticket_block", proj.next_run, block_reason, proj.last_consumed_ticket)
                    self._log_schedule_diag_once(
                        proj,
                        diag_key,
                        f"[{proj.name}] scheduled run blocked: {block_reason}, next_run={proj.next_run}, last_ticket={proj.last_consumed_ticket}",
                    )
                return False

            if not proj.execution_lock.acquire(blocking=False):
                recovered = self._recover_stale_project_state(proj, "scheduled_ticket_consume")
                if not recovered or not proj.execution_lock.acquire(blocking=False):
                    self._trace_schedule_event(
                        proj.name,
                        "PROJECT_START_SKIPPED",
                        trigger_source="scheduled",
                        reason="execution_lock_busy",
                        next_run=proj.next_run,
                    )
                    self.log(f"[{proj.name}] ticket consume failed: execution_lock busy")
                    return False

            if not self.semaphore.acquire(blocking=False):
                proj.execution_lock.release()
                self._trace_schedule_event(
                    proj.name,
                    "PROJECT_QUEUED",
                    trigger_source="scheduled",
                    reason="semaphore_full",
                    next_run=proj.next_run,
                )
                self.log(f"[{proj.name}] ticket consume failed: queued by semaphore pressure")
                if proj.name not in self._pending_set:
                    self._pending_set.add(proj.name)
                    self.pending_queue.put((proj, current_time, False, "scheduled"))
                return False

            proj.last_consumed_ticket = proj.next_run
            proj.execution_id += 1
            self._pending_set.discard(proj.name)
            self.save_data()

            self.log(f"[{proj.name}] scheduled ticket consumed: {proj.next_run} (execution_id={proj.execution_id})")
            self._trace_schedule_event(proj.name, "SCHEDULE_TICKET_CONSUMED", next_run=proj.next_run, execution_id=proj.execution_id)
            return self._launch_project_with_acquired_slot(proj, False, "scheduled")

    def run_project_manual(self, proj, only_checked=False):
        if proj.status == self.STATUS_RUNNING and self._has_live_project_runtime(proj.name):
            self._trace_schedule_event(
                proj.name,
                "PROJECT_START_SKIPPED",
                trigger_source="manual",
                reason="already_running",
            )
            return False

        if not proj.execution_lock.acquire(blocking=False):
            recovered = self._recover_stale_project_state(proj, "manual_trigger")
            if not recovered or not proj.execution_lock.acquire(blocking=False):
                self._trace_schedule_event(
                    proj.name,
                    "PROJECT_START_SKIPPED",
                    trigger_source="manual",
                    reason="execution_lock_busy",
                )
                return False

        if not self.semaphore.acquire(blocking=False):
            proj.execution_lock.release()
            self._trace_schedule_event(
                proj.name,
                "PROJECT_START_SKIPPED",
                trigger_source="manual",
                reason="semaphore_full",
            )
            return False

        proj.execution_id += 1
        self._trace_schedule_event(
            proj.name,
            "MANUAL_TRIGGER_ACCEPTED",
            trigger_source="manual",
            only_checked=only_checked,
            execution_id=proj.execution_id,
        )
        return self._launch_project_with_acquired_slot(proj, only_checked, "manual")

    def _reset_daily_state(self):
        self._last_schedule_diag.clear()
        self._trace_schedule_event("SYSTEM", "DAILY_RESET")
        self.log("Daily reset started")

        with self.project_state_lock:
            for proj in self.projects:
                if self._has_live_project_runtime(proj.name):
                    self.log(f"[{proj.name}] skipped during daily reset because runtime is still live")
                    continue

                if proj.execution_lock.locked():
                    self._recover_stale_project_state(proj, "daily_reset")

                proj.status = self.STATUS_WAITING
                proj.completed_tasks = 0
                proj.total_tasks = 0
                proj.stop_requested = False
                if hasattr(proj, "last_executed_minute"):
                    proj.last_executed_minute = None
                proj.calculate_next_run()

                for task in proj.tasks:
                    task.status = self.TASK_STATUS_WAITING

        self.emit(SchedulerEvent.PROGRESS_UPDATE, 0)
        self.emit(SchedulerEvent.CLEAR_LOGS, None)
        self.last_all_done_date = None
        self.save_data()
        self.emit(SchedulerEvent.PROJECT_REFRESH, None)
        self.emit(SchedulerEvent.TASK_REFRESH, None)
