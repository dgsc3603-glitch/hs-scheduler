import json
import os
import tempfile
import threading
import time

from component.engine import EngineService, LocalApiServer, LocalEngineClient
from component.engine.store import EngineStore


def wait_until(predicate, timeout=15.0, interval=0.25, label="condition"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(interval)
    raise AssertionError(f"Timed out waiting for {label}")


def write_file(path, content):
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(content)


def current_data_bootstrap_check(base_dir):
    temp_dir = tempfile.mkdtemp(prefix="stage2-bootstrap-")
    db_path = os.path.join(temp_dir, "bootstrap.db")
    store = EngineStore(db_path)
    store.initialize()
    result = store.bootstrap_from_legacy_json(os.path.join(base_dir, "scheduler_data.json"))
    counts = store.get_counts()
    assert result["projects_loaded"] > 0, "expected legacy bootstrap to load projects"
    assert counts["projects"] == result["projects_loaded"], "project count mismatch after bootstrap"
    return result, counts


def store_reconcile_check():
    temp_dir = tempfile.mkdtemp(prefix="stage2-store-reconcile-")
    db_path = os.path.join(temp_dir, "store.db")
    store = EngineStore(db_path)
    store.initialize()

    def model_for(payload):
        return type("Model", (), {"to_dict": lambda self, data=payload: data})()

    first = [
        {
            "name": "alpha",
            "run_time": "09:00",
            "schedule_type": "daily",
            "schedule_value": "",
            "dependencies": [],
            "enabled": True,
            "step_mode": "parallel",
            "tasks": [],
            "status": "waiting",
            "total_tasks": 0,
            "completed_tasks": 0,
            "last_run": "-",
            "last_execution_time": None,
            "last_executed_minute": None,
            "stop_requested": False,
            "last_consumed_ticket": None,
            "execution_id": 0,
            "catch_up_missed": False,
            "last_trigger_source": "-",
            "last_manual_run": "-",
            "last_scheduled_run": "-",
        },
        {
            "name": "beta",
            "run_time": "09:30",
            "schedule_type": "daily",
            "schedule_value": "",
            "dependencies": [],
            "enabled": True,
            "step_mode": "parallel",
            "tasks": [],
            "status": "waiting",
            "total_tasks": 0,
            "completed_tasks": 0,
            "last_run": "-",
            "last_execution_time": None,
            "last_executed_minute": None,
            "stop_requested": False,
            "last_consumed_ticket": None,
            "execution_id": 0,
            "catch_up_missed": False,
            "last_trigger_source": "-",
            "last_manual_run": "-",
            "last_scheduled_run": "-",
        },
    ]
    second = [
        {
            "name": "alpha-renamed",
            "run_time": "10:00",
            "schedule_type": "daily",
            "schedule_value": "",
            "dependencies": [],
            "enabled": True,
            "step_mode": "parallel",
            "tasks": [],
            "status": "waiting",
            "total_tasks": 0,
            "completed_tasks": 0,
            "last_run": "-",
            "last_execution_time": None,
            "last_executed_minute": None,
            "stop_requested": False,
            "last_consumed_ticket": None,
            "execution_id": 0,
            "catch_up_missed": False,
            "last_trigger_source": "-",
            "last_manual_run": "-",
            "last_scheduled_run": "-",
        }
    ]

    store.sync_from_models([model_for(item) for item in first])
    store.sync_from_models([model_for(item) for item in second])
    names = [item["project_name"] for item in store.list_projects()]
    assert names == ["alpha-renamed"], f"stale projects should be removed, got {names}"


def isolated_engine_flow_check():
    base_dir = tempfile.mkdtemp(prefix="stage2-engine-")
    quick_script = os.path.join(base_dir, "quick_task.py")
    slow_script = os.path.join(base_dir, "slow_task.py")
    timeout_script = os.path.join(base_dir, "timeout_task.py")
    write_file(quick_script, "print('quick task ok')\n")
    write_file(
        slow_script,
        "import time\nfor _ in range(40):\n    print('slow tick')\n    time.sleep(0.25)\n",
    )
    write_file(
        timeout_script,
        "import time\nprint('timeout task start')\ntime.sleep(10)\n",
    )

    data = [
        {
            "name": "demo-project",
            "run_time": "23:59",
            "schedule_type": "daily",
            "schedule_value": "",
            "dependencies": [],
            "enabled": True,
            "step_mode": "parallel",
            "tasks": [
                {
                    "task_id": "task-quick",
                    "filepath": quick_script,
                    "step": 1,
                    "args": "",
                    "timeout": 10,
                    "max_retries": 0,
                    "order": 0,
                    "status": "Waiting",
                    "checked": True,
                    "condition": {"enabled": False, "type": "file_exists", "value": ""},
                },
                {
                    "task_id": "task-slow",
                    "filepath": slow_script,
                    "step": 1,
                    "args": "",
                    "timeout": 30,
                    "max_retries": 0,
                    "order": 1,
                    "status": "Waiting",
                    "checked": False,
                    "condition": {"enabled": False, "type": "file_exists", "value": ""},
                },
                {
                    "task_id": "task-timeout",
                    "filepath": timeout_script,
                    "step": 1,
                    "args": "",
                    "timeout": 1,
                    "max_retries": 0,
                    "order": 2,
                    "status": "Waiting",
                    "checked": False,
                    "condition": {"enabled": False, "type": "file_exists", "value": ""},
                },
            ],
            "status": "Waiting",
            "total_tasks": 2,
            "completed_tasks": 0,
            "last_run": "-",
            "last_execution_time": None,
            "last_executed_minute": None,
            "stop_requested": False,
            "last_consumed_ticket": None,
            "execution_id": 0,
            "catch_up_missed": False,
            "last_trigger_source": "-",
            "last_manual_run": "-",
            "last_scheduled_run": "-",
        }
    ]
    with open(os.path.join(base_dir, "scheduler_data.json"), "w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=2)

    service = EngineService(base_dir)
    service.start()
    server = LocalApiServer(service, host="127.0.0.1", port=18758)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    time.sleep(1.0)

    client = LocalEngineClient(port=18758)

    health = client.health()
    assert health["mode"] == "runtime", "engine health should report runtime mode"
    assert client.list_projects(), "engine should expose at least one project"
    assert len(client.list_project_tasks("demo-project")) == 3, "task API should expose all tasks"

    missing_command_id = client.run_project("missing-project")["command_id"]
    wait_until(
        lambda: client.get_command(missing_command_id)["status"] == "failed",
        label="missing project command failure",
    )

    client.run_task("demo-project", "task-quick")
    wait_until(
        lambda: client.list_project_tasks("demo-project")[0]["status"] == service.core.TASK_STATUS_COMPLETED,
        label="single task completion",
    )

    client.run_task("demo-project", "task-slow")
    wait_until(
        lambda: client.list_project_tasks("demo-project")[1]["status"].startswith(service.core.TASK_STATUS_RUNNING),
        label="slow task running",
    )
    client.stop_task("demo-project", "task-slow")
    wait_until(
        lambda: client.list_project_tasks("demo-project")[1]["status"] == service.core.TASK_STATUS_STOPPED,
        label="slow task stopped",
    )

    client.run_task("demo-project", "task-timeout")
    wait_until(
        lambda: client.list_project_tasks("demo-project")[2]["status"] == service.core.TASK_STATUS_TIMEOUT,
        label="timeout task status",
    )
    assert not service.core.active_processes, "timed out tasks should be removed from active process tracking"

    time.sleep(1.0)

    client.run_project("demo-project", trigger_source="manual_checked", only_checked=True)
    wait_until(
        lambda: any(
            item["status"] == service.core.STATUS_COMPLETED
            for item in client.list_projects()
            if item["project_name"] == "demo-project"
        ),
        label="checked project run completion",
    )

    server.shutdown()
    service.stop()


def live_sync_merge_check():
    base_dir = tempfile.mkdtemp(prefix="stage2-live-sync-")
    slow_script = os.path.join(base_dir, "slow_task.py")
    write_file(
        slow_script,
        "import time\nfor _ in range(8):\n    print('slow tick')\n    time.sleep(0.25)\n",
    )

    payload = [
        {
            "name": "runner",
            "run_time": "23:59",
            "schedule_type": "daily",
            "schedule_value": "",
            "dependencies": [],
            "enabled": True,
            "step_mode": "parallel",
            "tasks": [
                {
                    "task_id": "task-slow",
                    "filepath": slow_script,
                    "step": 1,
                    "args": "",
                    "timeout": 30,
                    "max_retries": 0,
                    "order": 0,
                    "status": "Waiting",
                    "checked": True,
                    "condition": {"enabled": False, "type": "file_exists", "value": ""},
                }
            ],
            "status": "Waiting",
            "total_tasks": 1,
            "completed_tasks": 0,
            "last_run": "-",
            "last_execution_time": None,
            "last_executed_minute": None,
            "stop_requested": False,
            "last_consumed_ticket": None,
            "execution_id": 0,
            "catch_up_missed": False,
            "last_trigger_source": "-",
            "last_manual_run": "-",
            "last_scheduled_run": "-",
        },
        {
            "name": "editable",
            "run_time": "21:30",
            "schedule_type": "daily",
            "schedule_value": "",
            "dependencies": [],
            "enabled": True,
            "step_mode": "parallel",
            "tasks": [],
            "status": "Waiting",
            "total_tasks": 0,
            "completed_tasks": 0,
            "last_run": "-",
            "last_execution_time": None,
            "last_executed_minute": None,
            "stop_requested": False,
            "last_consumed_ticket": None,
            "execution_id": 0,
            "catch_up_missed": False,
            "last_trigger_source": "-",
            "last_manual_run": "-",
            "last_scheduled_run": "-",
        },
    ]
    json_path = os.path.join(base_dir, "scheduler_data.json")
    with open(json_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)

    service = EngineService(base_dir)
    try:
        service.start()
        project = next(item for item in service.core.projects if item.name == "runner")
        assert service.core.run_project_manual(project), "runner project should start"
        wait_until(service._has_live_runtime, label="live runtime")

        payload[1]["enabled"] = False
        with open(json_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)

        result = service.sync_from_legacy()
        assert result["merged_core"] is True, "live sync should merge into core"

        editable = next(item for item in service.core.projects if item.name == "editable")
        assert editable.enabled is False, "live sync should update non-running project state"
        names = [item["project_name"] for item in service.list_projects()]
        assert "editable" in names and "runner" in names, f"project listing should remain intact: {names}"
    finally:
        service.stop()


def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    bootstrap_result, bootstrap_counts = current_data_bootstrap_check(base_dir)
    store_reconcile_check()
    isolated_engine_flow_check()
    live_sync_merge_check()
    print("bootstrap", bootstrap_result, bootstrap_counts)
    print("stage2 smoke test passed")


if __name__ == "__main__":
    main()
