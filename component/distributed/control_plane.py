import datetime
import json
import os
from dataclasses import dataclass

from .archive import ControlPlaneArchiveStore
from .artifacts import ArtifactSpoolManager
from .d1 import CloudflareD1Client
from .policies import should_allow_run, should_enable_project_in_runtime
from component.utils import load_json_file


def _utc_now():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _utc_after(seconds):
    return (
        datetime.datetime.utcnow() + datetime.timedelta(seconds=seconds)
    ).replace(microsecond=0).isoformat() + "Z"


def _utc_days_ago(days):
    return (
        datetime.datetime.utcnow() - datetime.timedelta(days=days)
    ).replace(microsecond=0).isoformat() + "Z"


def _parse_ticket_datetime(ticket):
    try:
        return datetime.datetime.strptime(str(ticket), "%Y-%m-%d %H:%M")
    except Exception:
        return None


@dataclass
class DistributedRunClaim:
    allowed: bool
    claimed: bool
    run_key: str
    reason: str
    lane: str


class DistributedControlPlane:
    def __init__(self, runtime_config, policies, logger=None):
        self.runtime_config = runtime_config
        self.policies = policies
        self.logger = logger
        self.client = CloudflareD1Client(
            account_id=runtime_config.d1_account_id,
            database_id=runtime_config.d1_database_id,
            api_token=runtime_config.d1_api_token,
            timeout_seconds=runtime_config.control_plane_timeout_seconds,
        )
        self.archive_store = ControlPlaneArchiveStore(runtime_config.archive_db_path)
        self.artifacts = ArtifactSpoolManager(runtime_config.artifact_spool_root)
        self._manifest = {}
        self._lease_owner = ""
        self._is_primary = not runtime_config.enabled
        self._lease_owner_priority = None
        self._lease_takeover_reason = ""
        self._last_heartbeat_at = None
        self._last_archive_maintenance = None
        self._load_manifest()

    @property
    def enabled(self):
        return self.runtime_config.enabled

    @property
    def control_plane_enabled(self):
        return self.runtime_config.control_plane_enabled and self.client.enabled

    @property
    def is_primary(self):
        return self._is_primary

    def _log(self, level, message, *args):
        if not self.logger:
            return
        log_method = getattr(self.logger, level, self.logger.info)
        log_method(message, *args)

    def _load_manifest(self):
        path = self.runtime_config.cloud_copy_manifest
        if not path or not os.path.exists(path):
            self._manifest = {}
            return
        try:
            self._manifest = load_json_file(path, default={}) or {}
        except Exception as exc:
            self._manifest = {}
            self._log("warning", "Failed to load cloud copy manifest: %s", exc)

    def initialize(self):
        self.archive_store.initialize()
        if not self.control_plane_enabled:
            self._is_primary = True
            return
        self.ensure_schema()
        self.register_node(status="booting")
        self.refresh_lease()

    def ensure_schema(self):
        statements = [
            """
            CREATE TABLE IF NOT EXISTS scheduler_nodes (
                node_id TEXT PRIMARY KEY,
                node_role TEXT NOT NULL,
                status TEXT NOT NULL,
                last_heartbeat_at TEXT NOT NULL,
                capabilities_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS scheduler_leases (
                lease_name TEXT PRIMARY KEY,
                owner_node_id TEXT NOT NULL,
                lease_expires_at TEXT NOT NULL,
                epoch INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS scheduled_runs (
                run_key TEXT PRIMARY KEY,
                project_name TEXT NOT NULL,
                lane TEXT NOT NULL,
                assigned_node_id TEXT NOT NULL,
                trigger_source TEXT NOT NULL,
                scheduled_for TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT,
                finished_at TEXT,
                heartbeat_at TEXT,
                result TEXT,
                message TEXT,
                artifact_status TEXT NOT NULL DEFAULT 'pending',
                details_json TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS task_runs (
                task_key TEXT PRIMARY KEY,
                run_key TEXT NOT NULL,
                project_name TEXT NOT NULL,
                task_id TEXT NOT NULL,
                status TEXT NOT NULL,
                attempt_count INTEGER NOT NULL DEFAULT 1,
                heartbeat_at TEXT NOT NULL,
                finished_at TEXT,
                message TEXT,
                details_json TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS artifact_transfers (
                run_key TEXT PRIMARY KEY,
                project_name TEXT NOT NULL,
                source_node_id TEXT NOT NULL,
                spool_path TEXT,
                checksum TEXT,
                file_count INTEGER NOT NULL DEFAULT 0,
                byte_size INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                pc_archive_path TEXT,
                verified_at TEXT,
                updated_at TEXT NOT NULL,
                details_json TEXT NOT NULL
            )
            """,
        ]
        for statement in statements:
            self.client.execute(statement)

    def register_node(self, status="online"):
        if not self.control_plane_enabled:
            return
        now = _utc_now()
        capabilities = {
            "node_role": self.runtime_config.node_role,
            "node_priority": self.runtime_config.node_priority,
            "pc_fallback": self.runtime_config.pc_fallback,
        }
        self.client.execute(
            """
            INSERT INTO scheduler_nodes (
                node_id, node_role, status, last_heartbeat_at, capabilities_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(node_id) DO UPDATE SET
                node_role=excluded.node_role,
                status=excluded.status,
                last_heartbeat_at=excluded.last_heartbeat_at,
                capabilities_json=excluded.capabilities_json,
                updated_at=excluded.updated_at
            """,
            [
                self.runtime_config.node_id,
                self.runtime_config.node_role,
                status,
                now,
                json.dumps(capabilities, ensure_ascii=False),
                now,
            ],
        )

    def _read_lease_row(self, lease_name):
        rows = self.client.query(
            """
            SELECT owner_node_id, lease_expires_at
            FROM scheduler_leases
            WHERE lease_name = ?
            """,
            [lease_name],
        )
        return rows[0] if rows else {}

    def _upsert_lease(self, lease_name, owner_node_id, expires_at, now):
        self.client.execute(
            """
            INSERT INTO scheduler_leases (
                lease_name, owner_node_id, lease_expires_at, epoch, updated_at
            ) VALUES (?, ?, ?, 1, ?)
            ON CONFLICT(lease_name) DO UPDATE SET
                owner_node_id = CASE
                    WHEN scheduler_leases.owner_node_id = excluded.owner_node_id
                        OR scheduler_leases.lease_expires_at <= excluded.updated_at
                    THEN excluded.owner_node_id
                    ELSE scheduler_leases.owner_node_id
                END,
                lease_expires_at = CASE
                    WHEN scheduler_leases.owner_node_id = excluded.owner_node_id
                        OR scheduler_leases.lease_expires_at <= excluded.updated_at
                    THEN excluded.lease_expires_at
                    ELSE scheduler_leases.lease_expires_at
                END,
                epoch = CASE
                    WHEN scheduler_leases.owner_node_id = excluded.owner_node_id
                        OR scheduler_leases.lease_expires_at <= excluded.updated_at
                    THEN scheduler_leases.epoch + 1
                    ELSE scheduler_leases.epoch
                END,
                updated_at = excluded.updated_at
            """,
            [lease_name, owner_node_id, expires_at, now],
        )

    def _force_takeover_lease(self, lease_name, current_owner, expires_at, now):
        self.client.execute(
            """
            UPDATE scheduler_leases
            SET owner_node_id = ?,
                lease_expires_at = ?,
                epoch = epoch + 1,
                updated_at = ?
            WHERE lease_name = ? AND owner_node_id = ?
            """,
            [self.runtime_config.node_id, expires_at, now, lease_name, current_owner],
        )

    def _parse_capabilities(self, raw_payload):
        if isinstance(raw_payload, dict):
            return raw_payload
        try:
            return json.loads(raw_payload or "{}")
        except Exception:
            return {}

    def _get_node_priority(self, node_id):
        if not node_id:
            return -1
        if node_id == self.runtime_config.node_id:
            return self.runtime_config.node_priority
        rows = self.client.query(
            """
            SELECT capabilities_json
            FROM scheduler_nodes
            WHERE node_id = ?
            """,
            [node_id],
        )
        if not rows:
            return -1
        capabilities = self._parse_capabilities(rows[0].get("capabilities_json"))
        try:
            return int(capabilities.get("node_priority", -1))
        except Exception:
            return -1

    def _owner_has_active_runs(self, owner_node_id):
        if not owner_node_id:
            return False
        rows = self.client.query(
            """
            SELECT run_key
            FROM scheduled_runs
            WHERE assigned_node_id = ?
              AND status IN ('starting', 'running', 'finishing')
            LIMIT 1
            """,
            [owner_node_id],
        )
        return bool(rows)

    def _is_ticket_within_retention(self, ticket):
        ticket_dt = _parse_ticket_datetime(ticket)
        if not ticket_dt:
            return False
        retention_floor = datetime.datetime.now() - datetime.timedelta(days=max(self.runtime_config.retention_days, 1))
        return ticket_dt >= retention_floor

    def _local_backfill_state(self, project, ticket):
        local_status = str(getattr(project, "status", "") or "").strip()
        if "Error" in local_status:
            return "failed", "failed"
        if "Stopped" in local_status:
            return "cancelled", "stopped"
        if "Done" in local_status:
            return "completed", "success"
        return "completed", "success"

    def backfill_local_scheduled_runs(self, projects):
        if not self.control_plane_enabled:
            return {"backfilled_count": 0}

        backfilled_count = 0
        for project in projects:
            ticket = str(getattr(project, "last_consumed_ticket", "") or "").strip()
            if not ticket or not self._is_ticket_within_retention(ticket):
                continue
            run_key = f"{project.schedule_type}:{project.name}:{ticket}"
            existing = self.client.query(
                """
                SELECT run_key
                FROM scheduled_runs
                WHERE run_key = ?
                """,
                [run_key],
            )
            if existing:
                continue

            policy = self.policy_for(project.name)
            status, result = self._local_backfill_state(project, ticket)
            details = json.dumps(
                {
                    "schedule_type": project.schedule_type,
                    "browser_mode": policy["browser_mode"],
                    "fallback_policy": policy["fallback_policy"],
                    "backfilled_from_local_state": True,
                },
                ensure_ascii=False,
            )
            finished_at = ticket if status in {"completed", "failed", "cancelled"} else None
            self.client.execute(
                """
                INSERT INTO scheduled_runs (
                    run_key, project_name, lane, assigned_node_id, trigger_source,
                    scheduled_for, status, started_at, finished_at, heartbeat_at,
                    result, message, details_json
                ) VALUES (?, ?, ?, ?, 'scheduled', ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_key) DO NOTHING
                """,
                [
                    run_key,
                    project.name,
                    self.lane_for(project.name),
                    self.runtime_config.node_id,
                    ticket,
                    status,
                    ticket,
                    finished_at,
                    _utc_now(),
                    result,
                    "backfilled_from_local_state",
                    details,
                ],
            )
            backfilled_count += 1

        return {"backfilled_count": backfilled_count}

    def reconcile_consumed_tickets(self, projects):
        if not self.control_plane_enabled:
            return {"reconciled_count": 0}

        now = datetime.datetime.now().replace(second=0, microsecond=0)
        reconciled_count = 0
        for project in projects:
            scheduled_for = str(getattr(project, "next_run", "") or "").strip()
            scheduled_dt = _parse_ticket_datetime(scheduled_for)
            if not scheduled_dt or scheduled_dt > now:
                continue
            if scheduled_for == getattr(project, "last_consumed_ticket", None):
                continue

            run_key = f"{project.schedule_type}:{project.name}:{scheduled_for}"
            rows = self.client.query(
                """
                SELECT run_key, status
                FROM scheduled_runs
                WHERE run_key = ?
                """,
                [run_key],
            )
            if not rows:
                continue

            project.last_consumed_ticket = scheduled_for
            project.calculate_next_run()
            reconciled_count += 1
            self._log(
                "info",
                "Reconciled consumed ticket from D1 for %s: %s",
                project.name,
                scheduled_for,
            )

        return {"reconciled_count": reconciled_count}

    def refresh_lease(self):
        if not self.control_plane_enabled:
            self._is_primary = True
            return True

        lease_name = self.runtime_config.lease_name
        now = _utc_now()
        expires_at = _utc_after(self.runtime_config.lease_ttl_seconds)
        takeover_reason = ""

        self._upsert_lease(lease_name, self.runtime_config.node_id, expires_at, now)
        row = self._read_lease_row(lease_name)
        owner = str(row.get("owner_node_id") or "")
        lease_expires_at = str(row.get("lease_expires_at") or "")

        owner_priority = self._get_node_priority(owner)
        if (
            owner
            and owner != self.runtime_config.node_id
            and owner_priority < self.runtime_config.node_priority
            and not self._owner_has_active_runs(owner)
        ):
            self._force_takeover_lease(lease_name, owner, expires_at, now)
            row = self._read_lease_row(lease_name)
            owner = str(row.get("owner_node_id") or "")
            lease_expires_at = str(row.get("lease_expires_at") or "")
            owner_priority = self._get_node_priority(owner)
            if owner == self.runtime_config.node_id:
                takeover_reason = "priority_takeover"

        if not row:
            self._is_primary = False
            self._lease_owner = ""
            self._lease_owner_priority = None
            self._lease_takeover_reason = ""
            return False

        self._lease_owner = owner
        self._lease_owner_priority = owner_priority if owner else None
        self._lease_takeover_reason = takeover_reason
        self._is_primary = owner == self.runtime_config.node_id
        self._last_heartbeat_at = now
        return self._is_primary

    def release_lease(self):
        if not self.control_plane_enabled:
            self._is_primary = True
            return False

        lease_name = self.runtime_config.lease_name
        self.client.execute(
            """
            DELETE FROM scheduler_leases
            WHERE lease_name = ? AND owner_node_id = ?
            """,
            [lease_name, self.runtime_config.node_id],
        )
        self._lease_owner = ""
        self._lease_owner_priority = None
        self._lease_takeover_reason = "released"
        self._is_primary = False
        self._last_heartbeat_at = _utc_now()
        return True

    def heartbeat(self):
        if not self.enabled:
            return
        self.register_node(status="online")
        self.refresh_lease()

    def status(self):
        return {
            "enabled": self.enabled,
            "control_plane_enabled": self.control_plane_enabled,
            "node_id": self.runtime_config.node_id,
            "node_role": self.runtime_config.node_role,
            "node_priority": self.runtime_config.node_priority,
            "is_primary": self._is_primary,
            "lease_owner": self._lease_owner,
            "lease_owner_priority": self._lease_owner_priority,
            "lease_takeover_reason": self._lease_takeover_reason,
            "manifest_loaded": bool(self._manifest),
            "cloud_copy_manifest": self.runtime_config.cloud_copy_manifest,
        }

    def policy_for(self, project_name):
        return self.policies.get(project_name)

    def apply_runtime_overrides(self, projects):
        manifest_projects = (self._manifest or {}).get("projects", {})
        for project in projects:
            policy = self.policy_for(project.name)
            project.enabled = should_enable_project_in_runtime(policy, self.runtime_config, project.enabled)
            if self.runtime_config.is_oracle and policy["oracle_enabled"]:
                manifest_project = manifest_projects.get(project.name, {})
                task_map = manifest_project.get("tasks", {})
                for task in project.tasks:
                    task_entry = task_map.get(task.task_id, {})
                    copy_path = task_entry.get("copy_workspace_path")
                    if not copy_path and task_entry.get("copy_relative_path"):
                        copy_path = os.path.join(
                            self.runtime_config.cloud_copy_root,
                            *task_entry["copy_relative_path"].split("/"),
                        )
                    if copy_path:
                        task.filepath = copy_path
                        task.filename = os.path.basename(copy_path)

    def lane_for(self, project_name):
        policy = self.policy_for(project_name)
        return "cloud" if policy["oracle_enabled"] else "local"

    def build_run_key(self, project, trigger_source):
        if trigger_source == "scheduled":
            scheduled_for = project.next_run or datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
            return f"{project.schedule_type}:{project.name}:{scheduled_for}"
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return f"{trigger_source}:{project.name}:{timestamp}:{project.execution_id}"

    def claim_project_run(self, project, trigger_source):
        policy = self.policy_for(project.name)
        allowed, reason = should_allow_run(policy, self.runtime_config, trigger_source, self._is_primary)
        run_key = self.build_run_key(project, trigger_source)
        lane = self.lane_for(project.name)
        if not allowed:
            return DistributedRunClaim(False, False, run_key, reason, lane)

        if not self.control_plane_enabled:
            return DistributedRunClaim(True, True, run_key, "control_plane_disabled", lane)

        scheduled_for = project.next_run or datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        details = json.dumps(
            {
                "schedule_type": project.schedule_type,
                "browser_mode": policy["browser_mode"],
                "fallback_policy": policy["fallback_policy"],
            },
            ensure_ascii=False,
        )
        self.client.execute(
            """
            INSERT INTO scheduled_runs (
                run_key, project_name, lane, assigned_node_id, trigger_source,
                scheduled_for, status, started_at, heartbeat_at, details_json
            ) VALUES (?, ?, ?, ?, ?, ?, 'starting', ?, ?, ?)
            ON CONFLICT(run_key) DO NOTHING
            """,
            [
                run_key,
                project.name,
                lane,
                self.runtime_config.node_id,
                trigger_source,
                scheduled_for,
                _utc_now(),
                _utc_now(),
                details,
            ],
        )
        rows = self.client.query(
            """
            SELECT run_key, assigned_node_id, status
            FROM scheduled_runs
            WHERE run_key = ?
            """,
            [run_key],
        )
        if not rows:
            return DistributedRunClaim(False, False, run_key, "claim_lookup_failed", lane)

        row = rows[0]
        assigned_node_id = str(row.get("assigned_node_id") or "")
        status = str(row.get("status") or "")
        if assigned_node_id != self.runtime_config.node_id:
            return DistributedRunClaim(False, False, run_key, f"claimed_by:{assigned_node_id}", lane)
        if status in {"completed", "failed", "cancelled"}:
            return DistributedRunClaim(False, False, run_key, f"already_{status}", lane)
        return DistributedRunClaim(True, True, run_key, "claimed", lane)

    def heartbeat_run(self, project_name, run_key, status, message=""):
        if not self.control_plane_enabled or not run_key:
            return
        self.client.execute(
            """
            UPDATE scheduled_runs
            SET status = ?, heartbeat_at = ?, message = COALESCE(NULLIF(?, ''), message)
            WHERE run_key = ?
            """,
            [status, _utc_now(), message, run_key],
        )

    def finish_run(self, project_name, run_key, result, message=""):
        if not self.control_plane_enabled or not run_key:
            return
        status = "completed" if result == "success" else "failed"
        if result == "stopped":
            status = "cancelled"
        self.client.execute(
            """
            UPDATE scheduled_runs
            SET status = ?, result = ?, message = ?, finished_at = ?, heartbeat_at = ?
            WHERE run_key = ?
            """,
            [status, result, message, _utc_now(), _utc_now(), run_key],
        )

    def sync_task_states(self, project_name, run_key, tasks):
        if not self.control_plane_enabled or not run_key:
            return
        now = _utc_now()
        for task in tasks:
            task_key = f"{run_key}:{task.task_id}"
            details = json.dumps(
                {
                    "filepath": task.filepath,
                    "filename": task.filename,
                    "step": task.step,
                    "order": task.order,
                    "checked": bool(task.checked),
                },
                ensure_ascii=False,
            )
            finished_at = now if str(task.status).strip() and "Running" not in str(task.status) else None
            self.client.execute(
                """
                INSERT INTO task_runs (
                    task_key, run_key, project_name, task_id, status,
                    attempt_count, heartbeat_at, finished_at, message, details_json
                ) VALUES (?, ?, ?, ?, ?, 1, ?, ?, '', ?)
                ON CONFLICT(task_key) DO UPDATE SET
                    status=excluded.status,
                    heartbeat_at=excluded.heartbeat_at,
                    finished_at=COALESCE(excluded.finished_at, task_runs.finished_at),
                    details_json=excluded.details_json
                """,
                [
                    task_key,
                    run_key,
                    project_name,
                    task.task_id,
                    task.status,
                    now,
                    finished_at,
                    details,
                ],
            )

    def get_project_artifact_roots(self, project_name):
        policy = self.policy_for(project_name)
        roots = list(policy.get("artifact_roots", []))
        project_manifest = (self._manifest or {}).get("projects", {}).get(project_name, {})
        roots.extend(project_manifest.get("artifact_copy_roots", []) or [])
        if not project_manifest.get("artifact_copy_roots"):
            roots.extend(
                os.path.join(
                    self.runtime_config.cloud_copy_root,
                    *str(item).split("/"),
                )
                for item in (project_manifest.get("artifact_roots", []) or [])
            )
        normalized = []
        seen = set()
        for item in roots:
            normalized_item = str(item).strip()
            if normalized_item and normalized_item not in seen:
                normalized.append(normalized_item)
                seen.add(normalized_item)
        return normalized

    def begin_artifact_capture(self, project_name):
        if not self.runtime_config.artifact_capture_enabled:
            return None
        roots = self.get_project_artifact_roots(project_name)
        return {"roots": roots, "baseline": self.artifacts.capture_baseline(roots)}

    def finalize_artifact_capture(self, project_name, run_key, capture_state):
        if not self.runtime_config.artifact_capture_enabled or not capture_state:
            return {"spool_path": "", "files": [], "checksum": "", "byte_size": 0}
        roots = capture_state.get("roots", [])
        baseline = capture_state.get("baseline", {})
        result = self.artifacts.finalize_run(project_name, run_key, roots, baseline)
        if result.get("spool_path"):
            self.record_artifact_transfer(
                project_name,
                run_key,
                result["spool_path"],
                result["checksum"],
                len(result["files"]),
                result["byte_size"],
            )
        return result

    def record_artifact_transfer(self, project_name, run_key, spool_path, checksum, file_count, byte_size):
        if not self.control_plane_enabled or not run_key:
            return
        self.client.execute(
            """
            INSERT INTO artifact_transfers (
                run_key, project_name, source_node_id, spool_path, checksum,
                file_count, byte_size, status, updated_at, details_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'ready_for_pc', ?, ?)
            ON CONFLICT(run_key) DO UPDATE SET
                project_name=excluded.project_name,
                source_node_id=excluded.source_node_id,
                spool_path=excluded.spool_path,
                checksum=excluded.checksum,
                file_count=excluded.file_count,
                byte_size=excluded.byte_size,
                status=excluded.status,
                updated_at=excluded.updated_at,
                details_json=excluded.details_json
            """,
            [
                run_key,
                project_name,
                self.runtime_config.node_id,
                spool_path,
                checksum,
                int(file_count or 0),
                int(byte_size or 0),
                _utc_now(),
                json.dumps({"project_name": project_name}, ensure_ascii=False),
            ],
        )
        self.client.execute(
            """
            UPDATE scheduled_runs
            SET artifact_status = 'ready_for_pc'
            WHERE run_key = ?
            """,
            [run_key],
        )

    def acknowledge_artifact_archived(self, run_key, pc_archive_path, checksum=""):
        if not self.control_plane_enabled or not run_key or not self.runtime_config.artifact_capture_enabled:
            return False
        self.client.execute(
            """
            UPDATE artifact_transfers
            SET status = 'archived_on_pc',
                pc_archive_path = ?,
                checksum = COALESCE(NULLIF(?, ''), checksum),
                verified_at = ?,
                updated_at = ?
            WHERE run_key = ?
            """,
            [pc_archive_path, checksum, _utc_now(), _utc_now(), run_key],
        )
        self.client.execute(
            """
            UPDATE scheduled_runs
            SET artifact_status = 'archived_on_pc'
            WHERE run_key = ?
            """,
            [run_key],
        )
        return True

    def cleanup_spool_after_archive(self, project_name, run_key):
        if not self.runtime_config.artifact_capture_enabled:
            return False
        return self.artifacts.cleanup_run(project_name, run_key)

    def archive_expired_rows(self):
        if not self.control_plane_enabled or not self.runtime_config.is_pc:
            return {"archived_count": 0}

        now = datetime.datetime.utcnow()
        if self._last_archive_maintenance and (now - self._last_archive_maintenance).total_seconds() < 3600:
            return {"archived_count": 0}
        self._last_archive_maintenance = now

        cutoff = _utc_days_ago(self.runtime_config.retention_days)
        total_archived = 0
        table_specs = [
            ("scheduled_runs", "run_key", "finished_at", "status IN ('completed', 'failed', 'cancelled')"),
            ("task_runs", "task_key", "finished_at", "finished_at IS NOT NULL"),
            ("artifact_transfers", "run_key", "verified_at", "status = 'archived_on_pc'"),
        ]
        for table_name, key_field, time_field, predicate in table_specs:
            rows = self.client.query(
                f"""
                SELECT *
                FROM {table_name}
                WHERE {predicate}
                  AND {time_field} IS NOT NULL
                  AND {time_field} < ?
                """,
                [cutoff],
            )
            if not rows:
                continue
            result = self.archive_store.archive_rows(table_name, key_field, rows)
            archived_count = int(result.get("archived_count", 0))
            if archived_count != len(rows):
                continue
            for row in rows:
                record_key = row.get(key_field)
                if not record_key:
                    continue
                self.client.execute(f"DELETE FROM {table_name} WHERE {key_field} = ?", [record_key])
            total_archived += archived_count
        return {"archived_count": total_archived}
