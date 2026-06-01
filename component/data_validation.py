import copy
import datetime
import os
import uuid


VALID_SCHEDULE_TYPES = {"daily", "weekly", "interval", "onetime"}
VALID_STEP_MODES = {"parallel", "sequential"}
VALID_CONDITION_TYPES = {"always", "file_exists", "prev_success"}


def validate_scheduler_payload(payload):
    diagnostics = {
        "errors": [],
        "warnings": [],
        "repairs": [],
        "quarantined_projects": [],
        "quarantined_tasks": [],
    }
    if not isinstance(payload, list):
        diagnostics["errors"].append(
            {
                "path": "$",
                "code": "invalid_root",
                "message": "scheduler payload root must be a list",
            }
        )
        return [], diagnostics

    projects = []
    seen_project_names = set()
    seen_task_ids = set()
    for project_index, raw_project in enumerate(payload):
        project_path = f"$[{project_index}]"
        project, project_ok = _normalize_project(
            raw_project,
            project_path,
            seen_project_names,
            seen_task_ids,
            diagnostics,
        )
        if project_ok:
            projects.append(project)
        else:
            diagnostics["quarantined_projects"].append(
                {
                    "path": project_path,
                    "value": _safe_snapshot(raw_project),
                }
            )
    return projects, diagnostics


def has_diagnostics(diagnostics):
    return any(diagnostics.get(key) for key in ("errors", "warnings", "repairs", "quarantined_projects", "quarantined_tasks"))


def _normalize_project(raw_project, path, seen_project_names, seen_task_ids, diagnostics):
    if not isinstance(raw_project, dict):
        diagnostics["errors"].append(
            {
                "path": path,
                "code": "invalid_project",
                "message": "project must be an object",
            }
        )
        return None, False

    project = copy.deepcopy(raw_project)
    name = _coerce_text(project.get("name")).strip()
    if not name:
        diagnostics["errors"].append(
            {
                "path": f"{path}.name",
                "code": "missing_project_name",
                "message": "project name is required",
            }
        )
        return None, False

    if name in seen_project_names:
        diagnostics["errors"].append(
            {
                "path": f"{path}.name",
                "code": "duplicate_project_name",
                "message": f"duplicate project name: {name}",
            }
        )
        return None, False
    seen_project_names.add(name)
    project["name"] = name

    run_time = _coerce_text(project.get("run_time", "09:00")).strip() or "09:00"
    if not _valid_hhmm(run_time):
        diagnostics["warnings"].append(
            {
                "path": f"{path}.run_time",
                "code": "invalid_run_time",
                "message": f"invalid run_time repaired to 09:00: {run_time}",
            }
        )
        run_time = "09:00"
    project["run_time"] = run_time

    schedule_type = _coerce_text(project.get("schedule_type", "daily")).strip().lower() or "daily"
    if schedule_type not in VALID_SCHEDULE_TYPES:
        diagnostics["warnings"].append(
            {
                "path": f"{path}.schedule_type",
                "code": "invalid_schedule_type",
                "message": f"invalid schedule_type repaired to daily: {schedule_type}",
            }
        )
        schedule_type = "daily"
    project["schedule_type"] = schedule_type
    project["schedule_value"] = _coerce_text(project.get("schedule_value", ""))
    _validate_schedule_value(project, path, diagnostics)

    step_mode = _coerce_text(project.get("step_mode", "parallel")).strip().lower() or "parallel"
    if step_mode not in VALID_STEP_MODES:
        diagnostics["warnings"].append(
            {
                "path": f"{path}.step_mode",
                "code": "invalid_step_mode",
                "message": f"invalid step_mode repaired to parallel: {step_mode}",
            }
        )
        step_mode = "parallel"
    project["step_mode"] = step_mode

    project["enabled"] = bool(project.get("enabled", True))
    dependencies = project.get("dependencies", [])
    if not isinstance(dependencies, list):
        diagnostics["warnings"].append(
            {
                "path": f"{path}.dependencies",
                "code": "invalid_dependencies",
                "message": "dependencies repaired to an empty list",
            }
        )
        dependencies = []
    project["dependencies"] = [_coerce_text(item).strip() for item in dependencies if _coerce_text(item).strip()]

    tasks = project.get("tasks", [])
    if not isinstance(tasks, list):
        diagnostics["warnings"].append(
            {
                "path": f"{path}.tasks",
                "code": "invalid_tasks",
                "message": "tasks repaired to an empty list",
            }
        )
        tasks = []

    normalized_tasks = []
    for task_index, raw_task in enumerate(tasks):
        task_path = f"{path}.tasks[{task_index}]"
        task, task_ok = _normalize_task(raw_task, task_path, seen_task_ids, diagnostics, default_order=task_index)
        if task_ok:
            normalized_tasks.append(task)
        else:
            diagnostics["quarantined_tasks"].append(
                {
                    "path": task_path,
                    "project_name": name,
                    "value": _safe_snapshot(raw_task),
                }
            )
    project["tasks"] = normalized_tasks
    return project, True


def _normalize_task(raw_task, path, seen_task_ids, diagnostics, default_order):
    if not isinstance(raw_task, dict):
        diagnostics["errors"].append(
            {
                "path": path,
                "code": "invalid_task",
                "message": "task must be an object",
            }
        )
        return None, False

    task = copy.deepcopy(raw_task)
    filepath = _coerce_text(task.get("filepath")).strip()
    if not filepath:
        diagnostics["errors"].append(
            {
                "path": f"{path}.filepath",
                "code": "missing_task_filepath",
                "message": "task filepath is required",
            }
        )
        return None, False
    task["filepath"] = filepath

    task_id = _coerce_text(task.get("task_id")).strip()
    if not task_id or task_id in seen_task_ids:
        original = task_id
        task_id = str(uuid.uuid4())
        diagnostics["repairs"].append(
            {
                "path": f"{path}.task_id",
                "code": "generated_task_id",
                "message": f"generated unique task_id; original={original or '<empty>'}",
            }
        )
    seen_task_ids.add(task_id)
    task["task_id"] = task_id

    task["step"] = _coerce_non_negative_int(task.get("step", 1), default=1, path=f"{path}.step", diagnostics=diagnostics)
    task["order"] = _coerce_non_negative_int(task.get("order", default_order), default=default_order, path=f"{path}.order", diagnostics=diagnostics)
    task["timeout"] = _coerce_non_negative_int(task.get("timeout", 0), default=0, path=f"{path}.timeout", diagnostics=diagnostics)
    task["max_retries"] = _coerce_non_negative_int(task.get("max_retries", 0), default=0, path=f"{path}.max_retries", diagnostics=diagnostics)
    task["args"] = _coerce_text(task.get("args", ""))
    task["checked"] = bool(task.get("checked", False))
    task["status"] = _coerce_text(task.get("status", "대기")) or "대기"
    task["condition"] = _normalize_condition(task.get("condition", {}), f"{path}.condition", diagnostics)
    return task, True


def _normalize_condition(condition, path, diagnostics):
    if not isinstance(condition, dict):
        diagnostics["warnings"].append(
            {
                "path": path,
                "code": "invalid_condition",
                "message": "condition repaired to disabled file_exists condition",
            }
        )
        condition = {}

    enabled = bool(condition.get("enabled", False))
    condition_type = _coerce_text(condition.get("type", "file_exists")).strip() or "file_exists"
    if condition_type not in VALID_CONDITION_TYPES:
        diagnostics["warnings"].append(
            {
                "path": f"{path}.type",
                "code": "invalid_condition_type",
                "message": f"invalid condition type repaired to file_exists: {condition_type}",
            }
        )
        condition_type = "file_exists"
    return {
        "enabled": enabled,
        "type": condition_type,
        "value": _coerce_text(condition.get("value", "")),
    }


def _validate_schedule_value(project, path, diagnostics):
    schedule_type = project["schedule_type"]
    schedule_value = project["schedule_value"]
    if schedule_type == "weekly":
        weekdays = {"mon", "tue", "wed", "thu", "fri", "sat", "sun"}
        values = [item.strip()[:3].lower() for item in schedule_value.split(",") if item.strip()]
        if not values or any(item not in weekdays for item in values):
            diagnostics["warnings"].append(
                {
                    "path": f"{path}.schedule_value",
                    "code": "invalid_weekly_schedule_value",
                    "message": "weekly schedule_value should contain weekday names such as mon,tue",
                }
            )
    elif schedule_type == "interval":
        if not schedule_value.isdigit() or int(schedule_value) <= 0:
            diagnostics["warnings"].append(
                {
                    "path": f"{path}.schedule_value",
                    "code": "invalid_interval_schedule_value",
                    "message": "interval schedule_value repaired to 60",
                }
            )
            project["schedule_value"] = "60"
    elif schedule_type == "onetime":
        try:
            datetime.datetime.strptime(schedule_value, "%Y-%m-%d %H:%M")
        except ValueError:
            diagnostics["warnings"].append(
                {
                    "path": f"{path}.schedule_value",
                    "code": "invalid_onetime_schedule_value",
                    "message": "onetime schedule_value should be YYYY-MM-DD HH:MM",
                }
            )


def _coerce_positive_int(value, default, path, diagnostics):
    repaired = _coerce_int(value, default, path, diagnostics)
    if repaired <= 0:
        diagnostics["warnings"].append(
            {
                "path": path,
                "code": "invalid_positive_integer",
                "message": f"value repaired to {default}: {value}",
            }
        )
        return default
    return repaired


def _coerce_non_negative_int(value, default, path, diagnostics):
    repaired = _coerce_int(value, default, path, diagnostics)
    if repaired < 0:
        diagnostics["warnings"].append(
            {
                "path": path,
                "code": "invalid_non_negative_integer",
                "message": f"value repaired to {default}: {value}",
            }
        )
        return default
    return repaired


def _coerce_int(value, default, path, diagnostics):
    try:
        return int(value)
    except (TypeError, ValueError):
        diagnostics["warnings"].append(
            {
                "path": path,
                "code": "invalid_integer",
                "message": f"value repaired to {default}: {value}",
            }
        )
        return default


def _valid_hhmm(value):
    try:
        datetime.datetime.strptime(value, "%H:%M")
        return True
    except ValueError:
        return False


def _coerce_text(value):
    if value is None:
        return ""
    return str(value)


def _safe_snapshot(value):
    if isinstance(value, (dict, list, str, int, float, bool)) or value is None:
        return copy.deepcopy(value)
    return repr(value)
