import datetime
import json
import os
import sqlite3
import threading
import uuid

from component.data_validation import validate_scheduler_payload
from component.utils import load_json_file


MAX_STORED_EVENTS = 20000
MAX_STORED_COMMANDS = 5000
LIVE_RUN_STATUSES = ("starting", "running", "stopping", "finishing")
UI_STATUS_BY_ENGINE_STATUS = {
    "starting": "실행중",
    "running": "실행중",
    "stopping": "사용자중지",
    "finishing": "실행중",
    "completed": "완료",
    "error": "오류 발생",
    "failed": "오류 발생",
    "stopped": "사용자중지",
}


def _utc_now():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


class EngineStore:
    def __init__(self, db_path):
        self.db_path = db_path
        self._write_lock = threading.Lock()

    def initialize(self):
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        with self._connect() as conn:
            conn.executescript(
                """
                PRAGMA journal_mode=WAL;
                PRAGMA synchronous=NORMAL;
                PRAGMA wal_autocheckpoint=1000;

                CREATE TABLE IF NOT EXISTS projects (
                    project_name TEXT PRIMARY KEY,
                    schedule_type TEXT NOT NULL,
                    run_time TEXT NOT NULL,
                    schedule_value TEXT,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    step_mode TEXT NOT NULL DEFAULT 'parallel',
                    source_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS project_tasks (
                    task_id TEXT PRIMARY KEY,
                    project_name TEXT NOT NULL,
                    filepath TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    step INTEGER NOT NULL,
                    task_order INTEGER NOT NULL,
                    args TEXT NOT NULL DEFAULT '',
                    timeout_seconds INTEGER NOT NULL DEFAULT 0,
                    max_retries INTEGER NOT NULL DEFAULT 0,
                    checked INTEGER NOT NULL DEFAULT 0,
                    condition_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(project_name) REFERENCES projects(project_name)
                );

                CREATE INDEX IF NOT EXISTS idx_project_tasks_project
                ON project_tasks(project_name, step, task_order);

                CREATE TABLE IF NOT EXISTS project_state (
                    project_name TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    active_run_id TEXT,
                    last_run_at TEXT,
                    last_result TEXT,
                    last_heartbeat_at TEXT,
                    last_consumed_ticket TEXT,
                    completed_tasks INTEGER NOT NULL DEFAULT 0,
                    total_tasks INTEGER NOT NULL DEFAULT 0,
                    details_json TEXT NOT NULL,
                    FOREIGN KEY(project_name) REFERENCES projects(project_name)
                );

                CREATE TABLE IF NOT EXISTS project_runs (
                    run_id TEXT PRIMARY KEY,
                    project_name TEXT NOT NULL,
                    trigger_source TEXT NOT NULL,
                    status TEXT NOT NULL,
                    result TEXT,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    heartbeat_at TEXT,
                    message TEXT,
                    metadata_json TEXT NOT NULL,
                    FOREIGN KEY(project_name) REFERENCES projects(project_name)
                );

                CREATE INDEX IF NOT EXISTS idx_project_runs_project
                ON project_runs(project_name, started_at DESC);

                CREATE TABLE IF NOT EXISTS task_runs (
                    task_run_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    project_name TEXT NOT NULL,
                    task_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempt INTEGER NOT NULL DEFAULT 1,
                    pid INTEGER,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    message TEXT,
                    metadata_json TEXT NOT NULL,
                    FOREIGN KEY(run_id) REFERENCES project_runs(run_id)
                );

                CREATE TABLE IF NOT EXISTS scheduler_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    level TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    project_name TEXT,
                    run_id TEXT,
                    message TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_scheduler_events_created
                ON scheduler_events(created_at DESC);

                CREATE TABLE IF NOT EXISTS engine_commands (
                    command_id TEXT PRIMARY KEY,
                    command_type TEXT NOT NULL,
                    project_name TEXT,
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    result_message TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_engine_commands_status
                ON engine_commands(status, created_at);
                """
            )
            self._prune_operational_history(conn)

    def recover_interrupted_runtime(self, message="engine restarted before run completed"):
        now = _utc_now()
        with self._write_lock:
            with self._connect() as conn:
                live_rows = conn.execute(
                    """
                    SELECT run_id, project_name
                    FROM project_runs
                    WHERE status IN ('starting', 'running', 'stopping', 'finishing')
                    """
                ).fetchall()
                running_commands = conn.execute(
                    """
                    SELECT command_id
                    FROM engine_commands
                    WHERE status = 'running'
                    """
                ).fetchall()

                conn.execute(
                    """
                    UPDATE project_runs
                    SET status = 'interrupted',
                        result = 'interrupted',
                        finished_at = COALESCE(finished_at, ?),
                        heartbeat_at = ?,
                        message = COALESCE(message, ?)
                    WHERE status IN ('starting', 'running', 'stopping', 'finishing')
                    """,
                    (now, now, message),
                )
                conn.execute(
                    """
                    UPDATE task_runs
                    SET status = 'interrupted',
                        finished_at = COALESCE(finished_at, ?),
                        message = COALESCE(message, ?)
                    WHERE status IN ('starting', 'running', 'stopping', 'finishing')
                    """,
                    (now, message),
                )
                conn.execute(
                    """
                    UPDATE project_state
                    SET status = 'interrupted',
                        active_run_id = NULL,
                        last_result = 'interrupted',
                        last_heartbeat_at = ?
                    WHERE active_run_id IS NOT NULL
                       OR status IN ('starting', 'running', 'stopping', 'finishing')
                    """,
                    (now,),
                )
                conn.execute(
                    """
                    UPDATE engine_commands
                    SET status = 'failed',
                        finished_at = ?,
                        result_message = COALESCE(result_message, ?)
                    WHERE status = 'running'
                    """,
                    (now, message),
                )

                if live_rows:
                    conn.execute(
                        """
                        INSERT INTO scheduler_events (
                            created_at, level, event_type, project_name, run_id, message, payload_json
                        ) VALUES (?, 'WARNING', 'ENGINE_RECOVERED_INTERRUPTED_RUNS', NULL, NULL, ?, ?)
                        """,
                        (
                            now,
                            message,
                            json.dumps(
                                {
                                    "recovered_runs": len(live_rows),
                                    "recovered_running_commands": len(running_commands),
                                    "project_names": sorted({row["project_name"] for row in live_rows if row["project_name"]}),
                                },
                                ensure_ascii=False,
                            ),
                        ),
                    )

        return {
            "recovered_runs": len(live_rows),
            "recovered_running_commands": len(running_commands),
        }

    def bootstrap_from_legacy_json(self, json_path):
        if not os.path.exists(json_path):
            return {"projects_loaded": 0, "tasks_loaded": 0}

        payload = load_json_file(json_path, default=[])
        projects, _ = validate_scheduler_payload(payload)
        return self._upsert_projects_payload(projects)

    def sync_from_models(self, projects):
        payload = [project.to_dict() for project in projects]
        return self._upsert_projects_payload(payload)

    def _upsert_projects_payload(self, projects):
        now = _utc_now()
        loaded_projects = 0
        loaded_tasks = 0
        project_names = {project["name"] for project in projects}

        with self._write_lock:
            with self._connect() as conn:
                stale_rows = conn.execute("SELECT project_name FROM projects").fetchall()
                stale_names = [
                    row["project_name"]
                    for row in stale_rows
                    if row["project_name"] not in project_names
                ]
                for project_name in stale_names:
                    conn.execute("DELETE FROM project_tasks WHERE project_name = ?", (project_name,))
                    conn.execute("DELETE FROM project_state WHERE project_name = ?", (project_name,))
                    conn.execute("DELETE FROM projects WHERE project_name = ?", (project_name,))

                for project in projects:
                    project_name = project["name"]
                    conn.execute(
                        """
                        INSERT INTO projects (
                            project_name, schedule_type, run_time, schedule_value,
                            enabled, step_mode, source_json, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(project_name) DO UPDATE SET
                            schedule_type=excluded.schedule_type,
                            run_time=excluded.run_time,
                            schedule_value=excluded.schedule_value,
                            enabled=excluded.enabled,
                            step_mode=excluded.step_mode,
                            source_json=excluded.source_json,
                            updated_at=excluded.updated_at
                        """,
                        (
                            project_name,
                            project.get("schedule_type", "daily"),
                            project.get("run_time", "00:00"),
                            project.get("schedule_value", ""),
                            1 if project.get("enabled", True) else 0,
                            project.get("step_mode", "parallel"),
                            json.dumps(project, ensure_ascii=False),
                            now,
                        ),
                    )

                    conn.execute(
                        """
                        INSERT INTO project_state (
                            project_name, status, active_run_id, last_run_at, last_result,
                            last_heartbeat_at, last_consumed_ticket, completed_tasks,
                            total_tasks, details_json
                        ) VALUES (?, ?, NULL, ?, NULL, NULL, ?, ?, ?, ?)
                        ON CONFLICT(project_name) DO UPDATE SET
                            status=excluded.status,
                            last_run_at=excluded.last_run_at,
                            last_consumed_ticket=excluded.last_consumed_ticket,
                            completed_tasks=excluded.completed_tasks,
                            total_tasks=excluded.total_tasks,
                            details_json=excluded.details_json
                        """,
                        (
                            project_name,
                            project.get("status", "waiting"),
                            project.get("last_run"),
                            project.get("last_consumed_ticket"),
                            int(project.get("completed_tasks", 0)),
                            int(project.get("total_tasks", 0)),
                            json.dumps(
                                {
                                    "next_run": project.get("next_run"),
                                    "last_trigger_source": project.get("last_trigger_source"),
                                    "last_manual_run": project.get("last_manual_run"),
                                    "last_scheduled_run": project.get("last_scheduled_run"),
                                },
                                ensure_ascii=False,
                            ),
                        ),
                    )

                    conn.execute(
                        "DELETE FROM project_tasks WHERE project_name = ?",
                        (project_name,),
                    )

                    for index, task in enumerate(project.get("tasks", [])):
                        conn.execute(
                            """
                            INSERT INTO project_tasks (
                                task_id, project_name, filepath, filename, step, task_order,
                                args, timeout_seconds, max_retries, checked, condition_json,
                                updated_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                task.get("task_id", str(uuid.uuid4())),
                                project_name,
                                task["filepath"],
                                os.path.basename(task["filepath"]),
                                int(task.get("step", 1)),
                                int(task.get("order", index)),
                                task.get("args", ""),
                                int(task.get("timeout", 0)),
                                int(task.get("max_retries", 0)),
                                1 if task.get("checked", False) else 0,
                                json.dumps(task.get("condition", {}), ensure_ascii=False),
                                now,
                            ),
                        )
                        loaded_tasks += 1

                    loaded_projects += 1

        return {"projects_loaded": loaded_projects, "tasks_loaded": loaded_tasks}

    def list_projects(self):
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    p.project_name,
                    p.schedule_type,
                    p.run_time,
                    p.schedule_value,
                    p.enabled,
                    p.step_mode,
                    s.status,
                    s.active_run_id,
                    s.last_run_at,
                    s.last_result,
                    s.last_heartbeat_at,
                    s.last_consumed_ticket,
                    s.completed_tasks,
                    s.total_tasks,
                    s.details_json
                FROM projects p
                LEFT JOIN project_state s
                    ON s.project_name = p.project_name
                ORDER BY p.project_name COLLATE NOCASE
                """
            ).fetchall()

        items = []
        for row in rows:
            details = json.loads(row["details_json"]) if row["details_json"] else {}
            engine_status = row["status"]
            items.append(
                {
                    "project_name": row["project_name"],
                    "schedule_type": row["schedule_type"],
                    "run_time": row["run_time"],
                    "schedule_value": row["schedule_value"],
                    "enabled": bool(row["enabled"]),
                    "step_mode": row["step_mode"],
                    "status": UI_STATUS_BY_ENGINE_STATUS.get(engine_status, engine_status),
                    "engine_status": engine_status,
                    "active_run_id": row["active_run_id"],
                    "last_run_at": row["last_run_at"],
                    "last_result": row["last_result"],
                    "last_heartbeat_at": row["last_heartbeat_at"],
                    "last_consumed_ticket": row["last_consumed_ticket"],
                    "completed_tasks": row["completed_tasks"],
                    "total_tasks": row["total_tasks"],
                    "details": details,
                }
            )
        return items

    def get_project_tasks(self, project_name):
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT source_json
                FROM projects
                WHERE project_name = ?
                """,
                (project_name,),
            ).fetchone()

        if not row:
            return []

        payload = json.loads(row["source_json"]) if row["source_json"] else {}
        tasks = payload.get("tasks", []) if isinstance(payload, dict) else []
        normalized = []
        for index, task in enumerate(tasks):
            normalized.append(
                {
                    "task_id": task.get("task_id"),
                    "filepath": task.get("filepath", ""),
                    "filename": os.path.basename(task.get("filepath", "")) or task.get("filename", ""),
                    "step": int(task.get("step", 1)),
                    "order": int(task.get("order", index)),
                    "args": task.get("args", ""),
                    "timeout": int(task.get("timeout", 0)),
                    "max_retries": int(task.get("max_retries", 0)),
                    "checked": bool(task.get("checked", False)),
                    "status": task.get("status", ""),
                    "condition": task.get("condition", {}),
                }
            )

        normalized.sort(key=lambda item: (item["step"], item["order"]))
        return normalized

    def get_counts(self):
        with self._connect() as conn:
            projects = conn.execute("SELECT COUNT(*) AS value FROM projects").fetchone()["value"]
            commands = conn.execute(
                "SELECT COUNT(*) AS value FROM engine_commands WHERE status = 'pending'"
            ).fetchone()["value"]
            live_runs = conn.execute(
                "SELECT COUNT(*) AS value FROM project_runs WHERE status IN ('starting', 'running', 'stopping', 'finishing')"
            ).fetchone()["value"]
        return {
            "projects": projects,
            "pending_commands": commands,
            "live_runs": live_runs,
        }

    def submit_command(self, command_type, project_name=None, payload=None):
        command_id = str(uuid.uuid4())
        with self._write_lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO engine_commands (
                        command_id, command_type, project_name, payload_json,
                        status, created_at, started_at, finished_at, result_message
                    ) VALUES (?, ?, ?, ?, 'pending', ?, NULL, NULL, NULL)
                    """,
                    (
                        command_id,
                        command_type,
                        project_name,
                        json.dumps(payload or {}, ensure_ascii=False),
                        _utc_now(),
                    ),
                )
        return command_id

    def list_pending_commands(self, limit=100):
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT command_id, command_type, project_name, payload_json, created_at
                FROM engine_commands
                WHERE status = 'pending'
                ORDER BY created_at ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        return [
            {
                "command_id": row["command_id"],
                "command_type": row["command_type"],
                "project_name": row["project_name"],
                "payload": json.loads(row["payload_json"]) if row["payload_json"] else {},
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def get_command(self, command_id):
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    command_id,
                    command_type,
                    project_name,
                    payload_json,
                    status,
                    created_at,
                    started_at,
                    finished_at,
                    result_message
                FROM engine_commands
                WHERE command_id = ?
                """,
                (command_id,),
            ).fetchone()

        if not row:
            return None

        return {
            "command_id": row["command_id"],
            "command_type": row["command_type"],
            "project_name": row["project_name"],
            "payload": json.loads(row["payload_json"]) if row["payload_json"] else {},
            "status": row["status"],
            "created_at": row["created_at"],
            "started_at": row["started_at"],
            "finished_at": row["finished_at"],
            "result_message": row["result_message"],
        }

    def mark_command_status(self, command_id, status, result_message=None):
        field_name = "started_at" if status == "running" else "finished_at"
        with self._write_lock:
            with self._connect() as conn:
                conn.execute(
                    f"""
                    UPDATE engine_commands
                    SET status = ?, {field_name} = ?, result_message = COALESCE(?, result_message)
                    WHERE command_id = ?
                    """,
                    (status, _utc_now(), result_message, command_id),
                )

    def record_event(self, level, event_type, message, project_name=None, run_id=None, payload=None):
        with self._write_lock:
            with self._connect() as conn:
                cursor = conn.execute(
                    """
                    INSERT INTO scheduler_events (
                        created_at, level, event_type, project_name, run_id, message, payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        _utc_now(),
                        level,
                        event_type,
                        project_name,
                        run_id,
                        message,
                        json.dumps(payload or {}, ensure_ascii=False),
                    ),
                )
                if cursor.lastrowid and cursor.lastrowid % 100 == 0:
                    self._prune_operational_history(conn)

    def list_events(self, after_event_id=0, limit=200):
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    event_id,
                    created_at,
                    level,
                    event_type,
                    project_name,
                    run_id,
                    message,
                    payload_json
                FROM scheduler_events
                WHERE event_id > ?
                ORDER BY event_id ASC
                LIMIT ?
                """,
                (int(after_event_id or 0), int(limit)),
            ).fetchall()

        return [
            {
                "event_id": row["event_id"],
                "created_at": row["created_at"],
                "level": row["level"],
                "event_type": row["event_type"],
                "project_name": row["project_name"],
                "run_id": row["run_id"],
                "message": row["message"],
                "payload": json.loads(row["payload_json"]) if row["payload_json"] else {},
            }
            for row in rows
        ]

    def latest_event_id(self):
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COALESCE(MAX(event_id), 0) AS latest_event_id
                FROM scheduler_events
                """
            ).fetchone()
        if not row:
            return 0
        return int(row["latest_event_id"] or 0)

    def start_project_run(self, project_name, trigger_source, metadata=None):
        run_id = str(uuid.uuid4())
        now = _utc_now()
        with self._write_lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO project_runs (
                        run_id, project_name, trigger_source, status, result,
                        started_at, finished_at, heartbeat_at, message, metadata_json
                    ) VALUES (?, ?, ?, 'starting', NULL, ?, NULL, ?, NULL, ?)
                    """,
                    (
                        run_id,
                        project_name,
                        trigger_source,
                        now,
                        now,
                        json.dumps(metadata or {}, ensure_ascii=False),
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO project_state (
                        project_name, status, active_run_id, last_run_at, last_result,
                        last_heartbeat_at, last_consumed_ticket, completed_tasks,
                        total_tasks, details_json
                    ) VALUES (?, 'starting', ?, ?, NULL, ?, NULL, 0, 0, '{}')
                    ON CONFLICT(project_name) DO UPDATE SET
                        status='starting',
                        active_run_id=excluded.active_run_id,
                        last_run_at=excluded.last_run_at,
                        last_heartbeat_at=excluded.last_heartbeat_at
                    """,
                    (project_name, run_id, now, now),
                )
        return run_id

    def heartbeat_run(self, run_id, status=None, project_name=None):
        now = _utc_now()
        with self._write_lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    UPDATE project_runs
                    SET heartbeat_at = ?, status = COALESCE(?, status)
                    WHERE run_id = ?
                    """,
                    (now, status, run_id),
                )
                if project_name:
                    if status:
                        conn.execute(
                            """
                            UPDATE project_state
                            SET last_heartbeat_at = ?, status = ?
                            WHERE project_name = ?
                            """,
                            (now, status, project_name),
                        )
                    else:
                        conn.execute(
                            """
                            UPDATE project_state
                            SET last_heartbeat_at = ?
                            WHERE project_name = ?
                            """,
                            (now, project_name),
                        )

    def finish_project_run(self, run_id, project_name, result, message=None):
        now = _utc_now()
        terminal_status = "completed" if result == "success" else result
        with self._write_lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    UPDATE project_runs
                    SET status = ?, result = ?, finished_at = ?, heartbeat_at = ?, message = ?
                    WHERE run_id = ?
                    """,
                    (terminal_status, result, now, now, message, run_id),
                )
                conn.execute(
                    """
                    UPDATE project_state
                    SET status = ?, active_run_id = NULL, last_result = ?,
                        last_heartbeat_at = ?, last_run_at = ?
                    WHERE project_name = ?
                    """,
                    (terminal_status, result, now, now, project_name),
                )

    def _connect(self):
        conn = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA wal_autocheckpoint=1000")
        return conn

    def _prune_operational_history(self, conn):
        conn.execute(
            """
            DELETE FROM scheduler_events
            WHERE event_id <= (
                SELECT COALESCE(MAX(event_id), 0) - ?
                FROM scheduler_events
            )
            """,
            (MAX_STORED_EVENTS,),
        )
        conn.execute(
            """
            DELETE FROM engine_commands
            WHERE status IN ('completed', 'failed')
              AND command_id NOT IN (
                SELECT command_id
                FROM engine_commands
                WHERE status IN ('completed', 'failed')
                ORDER BY created_at DESC
                LIMIT ?
              )
            """,
            (MAX_STORED_COMMANDS,),
        )
