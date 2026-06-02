import tkinter as tk
import os
import shutil
import tempfile
from types import SimpleNamespace

from component.ui.log_panel import LogPanel
from component.ui.project_panel import ProjectPanel
from component.ui.task_panel import TaskPanel
import component.app as app_module
from component.app import HSSchedulerApp


class DummyPanelApp:
    def add_project(self):
        pass

    def delete_project(self):
        pass

    def on_project_select(self, event=None):
        pass

    def edit_project(self, event=None):
        pass

    def run_project_now(self):
        pass

    def toggle_project_enabled(self):
        pass

    def stop_project(self):
        pass

    def add_task_to_project(self):
        pass

    def delete_task_from_project(self):
        pass

    def run_checked_tasks(self):
        pass

    def toggle_task_check(self, specific_item=None):
        pass

    def stop_selected_task(self):
        pass

    def save_data(self):
        return True

    def show_context_menu(self, event):
        pass

    def on_double_click_task(self, event):
        pass

    def on_space_task(self, event):
        return "break"

    def on_task_click(self, event):
        pass

    def on_drag_motion(self, event):
        pass

    def on_drag_release(self, event):
        pass

    def on_drop_files(self, event):
        pass


class FakeTree:
    def __init__(self, task_id=None):
        self._task_id = task_id

    def selection(self):
        return ("task-item",) if self._task_id else ()

    def item(self, item_id, key):
        if key == "values":
            return ("file.py", "", "", "", self._task_id)
        return ()


def panel_selection_checks():
    root = tk.Tk()
    root.withdraw()
    try:
        dummy = DummyPanelApp()

        project_host = tk.Frame(root)
        project_panel = ProjectPanel(project_host, dummy)
        projects = [
            SimpleNamespace(name="alpha", enabled=True, status="Waiting", next_run="2026-03-21 09:00", completed_tasks=0, total_tasks=0),
            SimpleNamespace(name="beta", enabled=True, status="Running", next_run="2026-03-21 10:00", completed_tasks=1, total_tasks=2),
        ]
        project_panel.refresh(
            projects,
            "Waiting",
            "Running",
            "Done",
            "Error",
            "DependencyWait",
            selected_project_name="beta",
        )
        assert project_panel.get_selected_name() == "beta", "project selection should be restored after refresh"

        task_host = tk.Frame(root)
        task_panel = TaskPanel(task_host, dummy)
        steps = {
            1: [
                SimpleNamespace(task_id="task-1", filename="first.py", args="", timeout=0, status="Waiting", checked=False),
                SimpleNamespace(task_id="task-2", filename="second.py", args="--flag", timeout=10, status="Running", checked=True),
            ]
        }
        status_constants = {
            "TASK_STATUS_COMPLETED": "Done",
            "TASK_STATUS_ERROR": "Error",
            "TASK_STATUS_RUNNING": "Running",
            "TASK_STATUS_TIMEOUT": "Timeout",
            "TASK_STATUS_STOPPED": "Stopped",
            "TASK_STATUS_FINAL_FAIL": "FinalFailed",
        }
        task_panel.refresh(steps, status_constants, selected_task_id="task-2")
        assert task_panel.get_selected_task_id() == "task-2", "task selection should be restored after refresh"

        latest_detail_log_checks(root)
    finally:
        root.destroy()


def latest_detail_log_checks(root):
    base = tempfile.mkdtemp(prefix="scheduler-log-panel-")
    try:
        project_dir = os.path.join(base, "task_logs", "demo", "2026-04-24")
        os.makedirs(project_dir, exist_ok=True)
        older = os.path.join(project_dir, "older.txt")
        latest = os.path.join(project_dir, "latest.txt")
        with open(older, "w", encoding="utf-8") as handle:
            handle.write("older content")
        with open(latest, "w", encoding="utf-8") as handle:
            handle.write("latest content")
        os.utime(older, (100, 100))
        os.utime(latest, (200, 200))

        panel = LogPanel(root, SimpleNamespace(base_dir=base))
        panel.update_project_list(["All Projects", "demo"])
        panel.detail_proj_var.set("demo")

        assert panel.load_latest_detail_log_for_selected_project()
        assert "latest content" in panel.detail_text.get("1.0", "end")
        assert panel._current_log_file == latest
    finally:
        shutil.rmtree(base, ignore_errors=True)


def engine_button_failure_checks():
    app = HSSchedulerApp.__new__(HSSchedulerApp)
    warnings = []
    project = SimpleNamespace(name="demo", enabled=True, calculate_next_run=lambda: None, stop_requested=False, tasks=[SimpleNamespace(task_id="task-1", filename="task.py")])

    app.engine_mode = True
    app.core = SimpleNamespace(projects=[project], stop_task=lambda *args, **kwargs: None, log=lambda *args, **kwargs: None)
    app.ui = SimpleNamespace(
        project_panel=SimpleNamespace(get_selected=lambda: 0),
        task_panel=SimpleNamespace(tree=FakeTree("task-1")),
    )
    app._submit_engine_run_command = lambda *args, **kwargs: False
    app._submit_engine_stop_command = lambda *args, **kwargs: False
    app._submit_engine_stop_task_command = lambda *args, **kwargs: False
    app._submit_engine_run_task_command = lambda *args, **kwargs: False
    app._show_engine_action_failed = lambda label: warnings.append(label)
    app.save_data = lambda: False
    app.refresh_project_list = lambda *args, **kwargs: None
    original_showwarning = app_module.messagebox.showwarning
    app_module.messagebox.showwarning = lambda title, message: warnings.append(message)

    try:
        HSSchedulerApp.run_project_now(app)
        HSSchedulerApp.stop_project(app)
        HSSchedulerApp.run_checked_tasks(app)
        HSSchedulerApp.stop_selected_task(app)
        HSSchedulerApp.toggle_project_enabled(app)
        HSSchedulerApp._run_single_task_only(app, project, project.tasks[0])
    finally:
        app_module.messagebox.showwarning = original_showwarning

    expected = {
        "Run project",
        "Stop project",
        "Run selected tasks",
        "Stop task",
        "Save project settings",
    }
    assert expected.issubset(set(warnings)), f"button actions should surface failures, got {warnings}"
    assert any("Failed to send single-task run command." in item for item in warnings), "single task button should surface a warning"


def engine_runtime_snapshot_refresh_check():
    app = HSSchedulerApp.__new__(HSSchedulerApp)
    task = SimpleNamespace(task_id="task-1", status="Error")
    project = SimpleNamespace(
        name="demo",
        status="Error",
        completed_tasks=0,
        total_tasks=1,
        last_run="2026-01-01 00:00",
        last_consumed_ticket="2026-01-01 00:00",
        last_trigger_source="-",
        last_manual_run="-",
        last_scheduled_run="-",
        tasks=[task],
    )

    app.engine_mode = True
    app.core = SimpleNamespace(projects=[project])
    app.engine_projects_cache = []
    app.engine_tasks_cache = {}
    app._engine_available_for_base = lambda: True

    def refresh_projects(force=False):
        app.engine_projects_cache = [
            {
                "project_name": "demo",
                "status": "Done",
                "completed_tasks": 1,
                "total_tasks": 1,
                "last_run_at": "2026-04-24 00:45",
                "last_consumed_ticket": "2026-04-24 00:40",
                "details": {"last_trigger_source": "scheduled"},
            }
        ]

    def refresh_tasks(project_name, force=False):
        app.engine_tasks_cache[project_name] = [
            {
                "task_id": "task-1",
                "status": "Done",
            }
        ]

    app._refresh_engine_projects = refresh_projects
    app._refresh_engine_tasks = refresh_tasks

    HSSchedulerApp._refresh_core_runtime_from_engine(app)

    assert project.status == "Done"
    assert project.completed_tasks == 1
    assert project.last_run == "2026-04-24 00:45"
    assert project.last_consumed_ticket == "2026-04-24 00:40"
    assert task.status == "Done"


def main():
    panel_selection_checks()
    engine_button_failure_checks()
    engine_runtime_snapshot_refresh_check()
    print("button smoke test passed")


if __name__ == "__main__":
    main()
