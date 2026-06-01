from copy import deepcopy


DEFAULT_POLICY = {
    "oracle_enabled": True,
    "browser_mode": "headless",
    "auth_mode": "direct_login",
    "max_browser_concurrency": 5,
    "fallback_policy": "pc_original",
    "retention_days": 7,
    "artifact_roots": [],
    "notes": "",
}


def normalize_policy(project_name, payload):
    merged = deepcopy(DEFAULT_POLICY)
    merged.update(payload or {})
    merged["project_name"] = project_name
    merged["oracle_enabled"] = bool(merged.get("oracle_enabled", True))
    merged["browser_mode"] = str(merged.get("browser_mode") or "headless").strip().lower() or "headless"
    merged["auth_mode"] = str(merged.get("auth_mode") or "direct_login").strip().lower() or "direct_login"
    merged["max_browser_concurrency"] = min(max(int(merged.get("max_browser_concurrency") or 5), 1), 5)
    merged["fallback_policy"] = str(merged.get("fallback_policy") or "pc_original").strip().lower() or "pc_original"
    merged["retention_days"] = max(int(merged.get("retention_days") or 7), 1)
    merged["artifact_roots"] = [str(item).strip() for item in merged.get("artifact_roots", []) if str(item).strip()]
    merged["notes"] = str(merged.get("notes") or "").strip()
    return merged


def should_allow_run(policy, runtime_config, trigger_source, is_primary):
    if runtime_config.is_oracle:
        if not policy["oracle_enabled"]:
            return False, "oracle_disabled"
        if trigger_source == "scheduled" and not is_primary:
            return False, "lease_not_owned"
        return True, "oracle_allowed"

    if runtime_config.is_pc:
        if not is_primary:
            if trigger_source == "scheduled":
                return False, "pc_standby"
            if not policy["oracle_enabled"]:
                return True, "pc_only_manual"
            return True, "pc_standby_manual"
        if not policy["oracle_enabled"]:
            return True, "pc_only_primary"
        return True, "pc_failover_primary"

    return True, "local_default"


def should_enable_project_in_runtime(policy, runtime_config, original_enabled):
    if not original_enabled:
        return False
    if runtime_config.is_oracle and not policy["oracle_enabled"]:
        return False
    return True
