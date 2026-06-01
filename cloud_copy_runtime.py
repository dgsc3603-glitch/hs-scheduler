import os
from pathlib import Path

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None


def _ensure_dir(path):
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def workspace_root():
    return Path(os.getenv("SCHEDULER_WORKSPACE_ROOT") or Path(__file__).resolve().parent)


def cloud_copy_root():
    return Path(os.getenv("SCHEDULER_CLOUD_COPY_ROOT") or (workspace_root() / "cloud_copies"))


def cloud_spool_root():
    return _ensure_dir(os.getenv("SCHEDULER_CLOUD_SPOOL_ROOT") or (workspace_root() / "cloud_spool"))


def find_project_root(script_file=None):
    path = Path(script_file or __file__).resolve()
    candidates = [path.parent, *path.parents]
    for candidate in candidates:
        parent = candidate.parent
        grandparent = parent.parent if parent != parent.parent else None
        if grandparent and grandparent.name == "source_mirror" and len(parent.name) == 1:
            return candidate
    return path.parent


def runtime_root(script_file=None):
    return _ensure_dir(find_project_root(script_file) / ".oracle_runtime")


def runtime_subdir(name, script_file=None):
    return _ensure_dir(runtime_root(script_file) / name)


def env_bool(name, default=False):
    value = str(os.getenv(name, "")).strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "on"}


def env_int(name, default=0):
    value = str(os.getenv(name, "")).strip()
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def browser_mode(default="headless"):
    value = str(os.getenv("SCHEDULER_BROWSER_MODE", "")).strip().lower()
    return value or default


def browser_headless(default=True):
    mode = browser_mode("headless" if default else "headed_virtual")
    return mode == "headless"


def max_browser_concurrency(default=5):
    value = env_int("SCHEDULER_MAX_BROWSER_CONCURRENCY", default)
    return min(max(value, 1), 5)


def load_project_env(script_file=None, extra_candidates=None):
    if load_dotenv is None:
        return []
    project_root = find_project_root(script_file)
    candidates = [
        project_root / ".env",
        project_root / ".env.local",
        workspace_root() / ".env",
    ]
    for item in extra_candidates or []:
        candidates.append(Path(item))
    loaded = []
    for candidate in candidates:
        if candidate.exists():
            load_dotenv(candidate, override=False)
            loaded.append(str(candidate))
    return loaded


def today_path(base_dir, *parts):
    base_path = _ensure_dir(base_dir)
    dated_dir = _ensure_dir(base_path / Path().joinpath(*parts) / Path().joinpath()) if parts else base_path
    return dated_dir


def run_spool_dir(project_name=None, run_key=None):
    project_name = project_name or os.getenv("SCHEDULER_PROJECT_NAME", "unknown_project")
    run_key = run_key or os.getenv("SCHEDULER_RUN_KEY", "manual")
    safe_run_key = str(run_key).replace(":", "_").replace("/", "_")
    return _ensure_dir(cloud_spool_root() / project_name / safe_run_key)
