import threading

from component.core.stabilized_scheduler_core import StabilizedSchedulerCore


class EngineRuntimeCore(StabilizedSchedulerCore):
    def __init__(self, event_queue, credentials_manager, data_file, runtime_hooks=None):
        super().__init__(event_queue, credentials_manager, data_file)
        self.runtime_hooks = runtime_hooks

    def start(self):
        if self._started:
            return
        self._started = True
        self.load_data()
        if self.runtime_hooks and hasattr(self.runtime_hooks, "on_core_loaded"):
            self.runtime_hooks.on_core_loaded(self.projects)
        self._trace_schedule_event("SYSTEM", "SESSION_START", project_count=len(self.projects))
        threading.Thread(target=self._scheduler_loop, daemon=True).start()

    def _launch_project_with_acquired_slot(self, proj, only_checked, trigger_source):
        if self.runtime_hooks:
            try:
                accepted = self.runtime_hooks.on_project_launch(
                    proj,
                    trigger_source=trigger_source,
                    only_checked=only_checked,
                )
                if accepted is False:
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
                    return False
            except Exception:
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
                raise
        return super()._launch_project_with_acquired_slot(proj, only_checked, trigger_source)

    def _execute_project_logic(self, proj, only_checked=False, trigger_source="manual"):
        try:
            return super()._execute_project_logic(
                proj,
                only_checked=only_checked,
                trigger_source=trigger_source,
            )
        finally:
            if self.runtime_hooks:
                self.runtime_hooks.on_project_finish(
                    proj,
                    trigger_source=trigger_source,
                    only_checked=only_checked,
                )
