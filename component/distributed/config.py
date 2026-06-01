import os
import socket
from copy import deepcopy

from component.utils import load_json_file

from .policies import DEFAULT_POLICY, normalize_policy


def _deep_merge(base, override):
    merged = deepcopy(base)
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


class DistributedRuntimeConfig:
    DEFAULTS = {
        "enabled": False,
        "node_id": "",
        "node_role": "pc",
        "node_priority": 100,
        "lease_name": "global_scheduler_primary",
        "lease_ttl_seconds": 45,
        "heartbeat_interval_seconds": 10,
        "retention_days": 7,
        "control_plane": {
            "provider": "cloudflare_d1",
            "account_id": "",
            "database_id": "",
            "api_token": "",
            "timeout_seconds": 8,
        },
        "paths": {
            "cloud_copy_root": "",
            "cloud_copy_manifest": "",
            "artifact_spool_root": "",
            "archive_db_path": "",
        },
        "artifact_capture_enabled": False,
        "pc_fallback": {
            "enabled": True,
            "run_originals": True,
        },
    }

    def __init__(self, base_dir, raw=None):
        defaults = deepcopy(self.DEFAULTS)
        defaults["node_id"] = socket.gethostname().lower()
        defaults["node_role"] = os.getenv("SCHEDULER_NODE_ROLE", defaults["node_role"]).strip().lower() or "pc"
        defaults["paths"]["cloud_copy_root"] = os.path.join(base_dir, "cloud_copies")
        defaults["paths"]["cloud_copy_manifest"] = os.path.join(base_dir, "cloud_copies", "manifest.json")
        defaults["paths"]["artifact_spool_root"] = os.path.join(base_dir, "cloud_spool")
        defaults["paths"]["archive_db_path"] = os.path.join(base_dir, "archive", "control_plane_archive.db")
        merged = _deep_merge(defaults, raw or {})

        self.base_dir = base_dir
        self.enabled = bool(merged.get("enabled", False))
        self.node_id = str(merged.get("node_id") or defaults["node_id"]).strip() or defaults["node_id"]
        self.node_role = str(merged.get("node_role") or "pc").strip().lower() or "pc"
        self.node_priority = int(merged.get("node_priority") or defaults["node_priority"])
        self.lease_name = str(merged.get("lease_name") or defaults["lease_name"]).strip() or defaults["lease_name"]
        self.lease_ttl_seconds = int(merged.get("lease_ttl_seconds") or defaults["lease_ttl_seconds"])
        self.heartbeat_interval_seconds = int(
            merged.get("heartbeat_interval_seconds") or defaults["heartbeat_interval_seconds"]
        )
        self.retention_days = int(merged.get("retention_days") or defaults["retention_days"])
        self.control_plane = merged.get("control_plane", {})
        self.paths = merged.get("paths", {})
        self.pc_fallback = merged.get("pc_fallback", {})

        self.cloud_copy_root = self.paths.get("cloud_copy_root") or defaults["paths"]["cloud_copy_root"]
        self.cloud_copy_manifest = self.paths.get("cloud_copy_manifest") or defaults["paths"]["cloud_copy_manifest"]
        self.artifact_spool_root = self.paths.get("artifact_spool_root") or defaults["paths"]["artifact_spool_root"]
        self.archive_db_path = self.paths.get("archive_db_path") or defaults["paths"]["archive_db_path"]
        self.artifact_capture_enabled = bool(merged.get("artifact_capture_enabled", False))

        self.control_plane_timeout_seconds = int(
            self.control_plane.get("timeout_seconds") or defaults["control_plane"]["timeout_seconds"]
        )
        self.control_plane_provider = str(
            self.control_plane.get("provider") or defaults["control_plane"]["provider"]
        ).strip()
        self.d1_account_id = str(self.control_plane.get("account_id") or "").strip()
        self.d1_database_id = str(self.control_plane.get("database_id") or "").strip()
        self.d1_api_token = str(self.control_plane.get("api_token") or "").strip()

    @classmethod
    def load(cls, base_dir, config_path=None):
        config_path = config_path or os.path.join(base_dir, "config", "distributed_runtime.json")
        raw = {}
        if os.path.exists(config_path):
            raw = load_json_file(config_path, default={}) or {}
        return cls(base_dir, raw=raw)

    @property
    def control_plane_enabled(self):
        return bool(
            self.enabled
            and self.control_plane_provider == "cloudflare_d1"
            and self.d1_account_id
            and self.d1_database_id
            and self.d1_api_token
        )

    @property
    def is_oracle(self):
        return self.node_role == "oracle"

    @property
    def is_pc(self):
        return self.node_role == "pc"

    def to_dict(self):
        return {
            "enabled": self.enabled,
            "node_id": self.node_id,
            "node_role": self.node_role,
            "node_priority": self.node_priority,
            "lease_name": self.lease_name,
            "lease_ttl_seconds": self.lease_ttl_seconds,
            "heartbeat_interval_seconds": self.heartbeat_interval_seconds,
            "retention_days": self.retention_days,
            "control_plane_enabled": self.control_plane_enabled,
            "paths": {
                "cloud_copy_root": self.cloud_copy_root,
                "cloud_copy_manifest": self.cloud_copy_manifest,
                "artifact_spool_root": self.artifact_spool_root,
                "archive_db_path": self.archive_db_path,
            },
            "artifact_capture_enabled": self.artifact_capture_enabled,
            "pc_fallback": self.pc_fallback,
        }


class ProjectPolicyCollection:
    def __init__(self, policies=None):
        self._policies = {}
        for project_name, payload in (policies or {}).items():
            self._policies[project_name] = normalize_policy(project_name, payload)

    @classmethod
    def load(cls, base_dir, policy_path=None):
        policy_path = policy_path or os.path.join(base_dir, "config", "project_policies.json")
        raw = {}
        if os.path.exists(policy_path):
            raw = load_json_file(policy_path, default={}) or {}
        return cls(raw)

    def get(self, project_name):
        return self._policies.get(project_name, normalize_policy(project_name, deepcopy(DEFAULT_POLICY)))

    def to_dict(self):
        return deepcopy(self._policies)
