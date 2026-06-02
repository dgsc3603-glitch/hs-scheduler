import datetime
import os
import queue
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error

from component.core.scheduler_core import SchedulerCore
from component.engine.client import LocalEngineClient
from component.engine.service import EngineService
from component.engine.store import EngineStore
from component.models import Project, ProjectTask
from component.data_validation import validate_scheduler_payload
from component.utils import CredentialManager, atomic_write_json, load_json_file


def test_engine_startup_lock_and_shutdown():
    base = tempfile.mkdtemp(prefix="scheduler_engine_resilience_")
    port = 18951
    second_port = 18952
    proc = None
    second_proc = None
    try:
        proc = subprocess.Popen(
            [sys.executable, "scheduler_engine.py", "--base-dir", base, "--port", str(port)],
            cwd=os.getcwd(),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        client = LocalEngineClient(port=port, timeout=0.75)
        health = None
        for _ in range(30):
            try:
                health = client.health()
                break
            except Exception:
                if proc.poll() is not None:
                    raise AssertionError(f"engine exited early: {proc.returncode}")
                time.sleep(0.2)

        assert health, "engine health did not become available"
        assert os.path.abspath(health.get("base_dir", "")) == os.path.abspath(base)

        second_proc = subprocess.Popen(
            [sys.executable, "scheduler_engine.py", "--base-dir", base, "--port", str(second_port)],
            cwd=os.getcwd(),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        second_proc.wait(timeout=5)
        assert second_proc.returncode == 2

        client.shutdown()
        proc.wait(timeout=10)
    finally:
        for item in (second_proc, proc):
            if item and item.poll() is None:
                item.terminate()
                try:
                    item.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    item.kill()
        shutil.rmtree(base, ignore_errors=True)


def test_atomic_json_backup_recovery():
    base = tempfile.mkdtemp(prefix="scheduler_json_resilience_")
    try:
        path = os.path.join(base, "scheduler_data.json")
        original = [{"name": "project-a", "run_time": "09:00", "tasks": []}]
        updated = [{"name": "project-b", "run_time": "10:00", "tasks": []}]

        atomic_write_json(path, original, indent=2)
        atomic_write_json(path, updated, indent=2)

        assert load_json_file(path) == updated
        assert load_json_file(f"{path}.bak") == original

        with open(path, "w", encoding="utf-8") as handle:
            handle.write("{")

        assert load_json_file(path) == original
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_store_recovers_interrupted_runtime_state():
    base = tempfile.mkdtemp(prefix="scheduler_store_recovery_")
    try:
        store = EngineStore(os.path.join(base, "scheduler_engine.db"))
        store.initialize()

        run_id = store.start_project_run(
            "project-a",
            "manual",
            metadata={"case": "interrupted-runtime"},
        )
        store.heartbeat_run(run_id, status="running", project_name="project-a")
        command_id = store.submit_command("run_project", project_name="project-a")
        store.mark_command_status(command_id, "running")

        assert store.get_counts()["live_runs"] == 1

        result = store.recover_interrupted_runtime()

        assert result["recovered_runs"] == 1
        assert result["recovered_running_commands"] == 1
        assert store.get_counts()["live_runs"] == 0
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_scheduler_data_validation_quarantines_bad_entries():
    payload = [
        {
            "name": "demo",
            "run_time": "bad-time",
            "schedule_type": "interval",
            "schedule_value": "-1",
            "step_mode": "bad-mode",
            "tasks": [
                {
                    "task_id": "duplicate",
                    "filepath": "ok.py",
                    "step": "bad-step",
                    "timeout": -5,
                    "max_retries": "bad-retry",
                },
                {
                    "task_id": "duplicate",
                    "filepath": "",
                    "step": 1,
                },
            ],
        },
        {
            "name": "demo",
            "run_time": "09:00",
            "tasks": [],
        },
    ]

    projects, diagnostics = validate_scheduler_payload(payload)

    assert len(projects) == 1
    assert projects[0]["run_time"] == "09:00"
    assert projects[0]["schedule_value"] == "60"
    assert projects[0]["step_mode"] == "parallel"
    assert len(projects[0]["tasks"]) == 1
    assert projects[0]["tasks"][0]["step"] == 1
    assert projects[0]["tasks"][0]["timeout"] == 0
    assert diagnostics["quarantined_projects"], "duplicate project should be quarantined"
    assert diagnostics["quarantined_tasks"], "task without filepath should be quarantined"
    assert diagnostics["warnings"], "invalid scalar values should be reported"


def test_stale_daily_session_status_does_not_block_today_schedule():
    base = tempfile.mkdtemp(prefix="scheduler_session_recovery_")
    try:
        data_path = os.path.join(base, "scheduler_data.json")
        today = time.strftime("%Y-%m-%d")
        today_ticket = f"{today} 00:10"
        payload = [
            {
                "name": "stale-error",
                "run_time": "00:10",
                "schedule_type": "daily",
                "schedule_value": "",
                "enabled": True,
                "step_mode": "parallel",
                "tasks": [],
                "status": "Error",
                "last_run": "2026-01-01 00:10",
                "last_consumed_ticket": "2026-01-01 00:10",
            },
            {
                "name": "today-error",
                "run_time": "00:10",
                "schedule_type": "daily",
                "schedule_value": "",
                "enabled": True,
                "step_mode": "parallel",
                "tasks": [],
                "status": "Error",
                "last_run": today_ticket,
                "last_consumed_ticket": today_ticket,
            },
        ]
        atomic_write_json(data_path, payload, indent=2)
        atomic_write_json(
            os.path.join(base, "scheduler_session_state.json"),
            {
                "date": today,
                "projects": {
                    "stale-error": {
                        "status": "Error",
                        "completed_tasks": 1,
                        "total_tasks": 2,
                        "tasks": {},
                        "last_consumed_ticket": "2026-01-01 00:10",
                    },
                    "today-error": {
                        "status": "Error",
                        "completed_tasks": 1,
                        "total_tasks": 2,
                        "tasks": {},
                        "last_consumed_ticket": today_ticket,
                    },
                },
            },
            indent=2,
        )

        core = SchedulerCore(queue.Queue(), CredentialManager(data_path), data_path)
        core.load_data()

        by_name = {project.name: project for project in core.projects}
        assert by_name["stale-error"].status == core.STATUS_WAITING
        assert by_name["today-error"].status == core.STATUS_ERROR
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_today_unconsumed_ticket_runs_even_after_grace_window():
    core = SchedulerCore(queue.Queue(), CredentialManager("scheduler_data.json"), "scheduler_data.json")
    project = type(
        "ProjectStub",
        (),
        {
            "next_run": "2026-04-24 11:00",
            "last_consumed_ticket": "2026-04-20 11:00",
            "catch_up_missed": False,
        },
    )()
    current_time = time.strptime("2026-04-24 16:30", "%Y-%m-%d %H:%M")
    current_dt = datetime.datetime(*current_time[:5])

    assert core._get_ticket_block_reason(project, current_dt) is None


def test_today_trace_finish_repairs_stale_project_status():
    base = tempfile.mkdtemp(prefix="scheduler_trace_recovery_")
    try:
        data_path = os.path.join(base, "scheduler_data.json")
        today = time.strftime("%Y-%m-%d")
        payload = [
            {
                "name": "trace-recovered",
                "run_time": "00:40",
                "schedule_type": "daily",
                "schedule_value": "",
                "enabled": True,
                "step_mode": "parallel",
                "tasks": [
                    {
                        "task_id": "task-a",
                        "filepath": os.path.join(base, "task.py"),
                        "step": 1,
                    }
                ],
                "status": "Error",
                "last_run": "2026-01-01 00:45",
                "last_consumed_ticket": "2026-01-01 00:40",
                "completed_tasks": 0,
                "total_tasks": 1,
            }
        ]
        atomic_write_json(data_path, payload, indent=2)
        os.makedirs(os.path.join(base, "logs"), exist_ok=True)
        with open(os.path.join(base, "logs", f"schedule_trace_{today}.log"), "w", encoding="utf-8") as handle:
            handle.write(
                f"{today} 00:40:00 | trace-recovered | SCHEDULE_TICKET_CONSUMED | next_run={today} 00:40, execution_id=1\n"
            )
            handle.write(
                f"{today} 00:45:00 | trace-recovered | PROJECT_FINISH | trigger_source=scheduled, status=Done, last_run={today} 00:45, next_run=2099-01-01 00:40\n"
            )

        core = SchedulerCore(queue.Queue(), CredentialManager(data_path), data_path)
        core.load_data()
        project = core.projects[0]

        assert project.status == core.STATUS_COMPLETED
        assert project.last_run == f"{today} 00:45"
        assert project.last_consumed_ticket == f"{today} 00:40"
        assert project.completed_tasks == 1
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_due_onetime_project_keeps_runnable_ticket_until_consumed():
    due_at = (datetime.datetime.now() - datetime.timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M")
    project = Project("one-shot", "00:00", schedule_type="onetime", schedule_value=due_at)

    assert project.next_run == due_at

    project.last_consumed_ticket = due_at
    project.calculate_next_run()

    assert project.next_run == "Expired"


def test_failed_task_preserves_concrete_error_status_after_attempts():
    base = tempfile.mkdtemp(prefix="scheduler_task_status_")
    try:
        script_path = os.path.join(base, "fail.py")
        with open(script_path, "w", encoding="utf-8") as handle:
            handle.write("import sys\nsys.exit(7)\n")

        data_path = os.path.join(base, "scheduler_data.json")
        core = SchedulerCore(queue.Queue(), CredentialManager(data_path), data_path)
        task = ProjectTask(script_path, 1, max_retries=0)
        project = Project("status-project", "09:00", [])
        core._run_single_script(task, project.name, {project.name: project})

        assert task.status == core.TASK_STATUS_ERROR
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_events_endpoint_rejects_bad_query_with_400():
    base = tempfile.mkdtemp(prefix="scheduler_events_validation_")
    port = 18961
    proc = None
    try:
        proc = subprocess.Popen(
            [sys.executable, "scheduler_engine.py", "--base-dir", base, "--port", str(port)],
            cwd=os.getcwd(),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        client = LocalEngineClient(port=port, timeout=0.75)
        for _ in range(30):
            try:
                client.health()
                break
            except Exception:
                if proc.poll() is not None:
                    raise AssertionError(f"engine exited early: {proc.returncode}")
                time.sleep(0.2)
        else:
            raise AssertionError("engine health did not become available")

        try:
            client._request("GET", "/events?limit=bad")
        except urllib.error.HTTPError as exc:
            assert exc.code == 400
        else:
            raise AssertionError("bad events query should be rejected")

        client.shutdown()
        proc.wait(timeout=10)
    finally:
        if proc and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        shutil.rmtree(base, ignore_errors=True)


def test_single_task_launch_honors_distributed_claim_rejection():
    base = tempfile.mkdtemp(prefix="scheduler_single_task_claim_")
    try:
        script_path = os.path.join(base, "task.py")
        with open(script_path, "w", encoding="utf-8") as handle:
            handle.write("print('should not run')\n")
        data_path = os.path.join(base, "scheduler_data.json")
        payload = [
            {
                "name": "claim-blocked",
                "run_time": "09:00",
                "tasks": [{"task_id": "task-a", "filepath": script_path, "step": 1}],
            }
        ]
        atomic_write_json(data_path, payload, indent=2)

        service = EngineService(base)
        service.store.initialize()
        service.core.load_data()

        class DenyingControlPlane:
            def claim_project_run(self, project, trigger_source):
                return type(
                    "Claim",
                    (),
                    {
                        "allowed": False,
                        "claimed": False,
                        "run_key": "blocked",
                        "reason": "test_denied",
                        "lane": "local",
                    },
                )()

            def status(self):
                return {}

        service.control_plane = DenyingControlPlane()
        project = service.core.projects[0]

        accepted = service._run_single_task_only(project, "task-a")

        assert accepted is False
        assert not project.execution_lock.locked()
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_live_legacy_sync_does_not_mutate_current_run_schedule_config():
    base = tempfile.mkdtemp(prefix="scheduler_live_sync_config_")
    try:
        service = EngineService(base)
        target = Project("live", "09:00", [], schedule_type="daily", schedule_value="", enabled=True, step_mode="parallel")
        source = Project("live", "10:00", [], schedule_type="interval", schedule_value="5", enabled=False, step_mode="sequential")

        service._apply_legacy_config_to_live_project(target, source)

        assert target.run_time == "09:00"
        assert target.schedule_type == "daily"
        assert target.schedule_value == ""
        assert target.enabled is True
        assert target.step_mode == "parallel"
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_engine_store_projects_expose_ui_status_labels_for_live_runs():
    base = tempfile.mkdtemp(prefix="scheduler_ui_status_")
    try:
        store = EngineStore(os.path.join(base, "scheduler_engine.db"))
        store.initialize()
        store._upsert_projects_payload(
            [
                {
                    "name": "ui-running",
                    "run_time": "09:00",
                    "schedule_type": "daily",
                    "schedule_value": "",
                    "enabled": True,
                    "step_mode": "parallel",
                    "tasks": [],
                    "status": "Waiting",
                }
            ]
        )
        run_id = store.start_project_run("ui-running", "manual")
        store.heartbeat_run(run_id, status="running", project_name="ui-running")

        item = store.list_projects()[0]

        assert item["status"] == "Running"
        assert item["engine_status"] == "running"
    finally:
        shutil.rmtree(base, ignore_errors=True)


def main():
    test_engine_startup_lock_and_shutdown()
    test_atomic_json_backup_recovery()
    test_store_recovers_interrupted_runtime_state()
    test_scheduler_data_validation_quarantines_bad_entries()
    test_stale_daily_session_status_does_not_block_today_schedule()
    test_today_unconsumed_ticket_runs_even_after_grace_window()
    test_today_trace_finish_repairs_stale_project_status()
    test_due_onetime_project_keeps_runnable_ticket_until_consumed()
    test_failed_task_preserves_concrete_error_status_after_attempts()
    test_events_endpoint_rejects_bad_query_with_400()
    test_single_task_launch_honors_distributed_claim_rejection()
    test_live_legacy_sync_does_not_mutate_current_run_schedule_config()
    test_engine_store_projects_expose_ui_status_labels_for_live_runs()
    print("engine resilience test passed")


if __name__ == "__main__":
    main()
