import argparse
import datetime
import json
import os
import shutil
import stat
from pathlib import PureWindowsPath


IGNORED_DIR_NAMES = {
    "__pycache__",
    ".git",
    ".venv",
    "venv",
    "node_modules",
    "logs",
    "task_logs",
    "build",
    "dist",
    "processed",
    "output",
    "outputs",
    "result",
    "results",
    "downloads",
    "download",
    "tmp",
    "temp",
    "backup",
}

IGNORED_SUFFIXES = {
    ".pyc",
    ".pyo",
    ".log",
    ".tmp",
    ".cache",
    ".sqlite",
    ".sqlite3",
    ".db",
    ".duckdb",
    ".parquet",
    ".feather",
    ".jsonl",
    ".zip",
    ".7z",
    ".tar",
    ".gz",
    ".xlsx",
    ".xls",
    ".csv",
    ".jpg",
    ".jpeg",
    ".png",
    ".webp",
    ".mp4",
    ".mp3",
}


def load_json(path):
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def remove_tree(path):
    def _handle_error(func, target_path, exc_info):
        try:
            os.chmod(target_path, stat.S_IWRITE)
        except OSError:
            pass
        func(target_path)

    shutil.rmtree(path, onerror=_handle_error)


def safe_copytree(src, dst):
    def _ignore(current_dir, entries):
        ignored = []
        for entry in entries:
            full_path = os.path.join(current_dir, entry)
            if os.path.isdir(full_path) and entry in IGNORED_DIR_NAMES:
                ignored.append(entry)
                continue
            if os.path.isdir(full_path) and (
                entry.startswith("chrome_profile")
                or entry.startswith("chrome_debug_profile")
                or entry.endswith("_profile")
            ):
                ignored.append(entry)
                continue
            if os.path.isfile(full_path) and os.path.splitext(entry)[1].lower() in IGNORED_SUFFIXES:
                ignored.append(entry)
        return ignored

    shutil.copytree(src, dst, dirs_exist_ok=True, ignore=_ignore)


def infer_source_root(filepath):
    path = PureWindowsPath(filepath)
    parts = path.parts
    if len(parts) < 2:
        raise ValueError(f"Cannot infer source root from path: {filepath}")
    drive = path.drive.rstrip(":").upper()
    top_level = parts[1]
    source_root = PureWindowsPath(f"{drive}:/{top_level}")
    return drive, top_level, str(source_root).replace("\\", "/")


def build_manifest(scheduler_data, policies, output_root):
    projects_manifest = {}
    copy_jobs = {}
    root_files_added = set()

    for project in scheduler_data:
        project_name = project["name"]
        policy = policies.get(project_name, {})
        oracle_enabled = bool(policy.get("oracle_enabled", True))
        project_entry = {
            "oracle_enabled": oracle_enabled,
            "browser_mode": policy.get("browser_mode", "headless"),
            "fallback_policy": policy.get("fallback_policy", "pc_original"),
            "copied_roots": [],
            "artifact_roots": [],
            "tasks": {},
        }

        if oracle_enabled:
            for task in project.get("tasks", []):
                filepath = task.get("filepath", "")
                if not filepath:
                    continue
                drive, top_level, source_root = infer_source_root(filepath)
                source_root_key = source_root.lower()
                task_dir = str(PureWindowsPath(filepath).parent).replace("\\", "/")
                task_dir_relative = str(PureWindowsPath(task_dir).relative_to(PureWindowsPath(source_root))).replace("\\", "/")
                copy_relative_root = f"source_mirror/{drive}/{top_level}/{task_dir_relative}".rstrip("/")
                copy_jobs[task_dir] = {
                    "source_path": task_dir,
                    "copy_relative_path": copy_relative_root,
                    "kind": "dir",
                }
                task_relative = str(PureWindowsPath(filepath).relative_to(PureWindowsPath(source_root))).replace("\\", "/")
                copy_relative_path = f"source_mirror/{drive}/{top_level}/{task_relative}"
                project_entry["tasks"][task["task_id"]] = {
                    "original_filepath": filepath,
                    "copy_relative_path": copy_relative_path,
                }

                if source_root_key not in root_files_added:
                    source_root_path = str(PureWindowsPath(source_root)).replace("\\", "/")
                    if os.path.exists(source_root_path):
                        for entry in os.listdir(source_root_path):
                            full_path = os.path.join(source_root_path, entry)
                            if not os.path.isfile(full_path):
                                continue
                            if os.path.splitext(entry)[1].lower() not in {
                                ".py",
                                ".json",
                                ".yaml",
                                ".yml",
                                ".ini",
                                ".cfg",
                                ".txt",
                                ".env",
                            }:
                                continue
                            copy_jobs[full_path.replace("\\", "/")] = {
                                "source_path": full_path.replace("\\", "/"),
                                "copy_relative_path": f"source_mirror/{drive}/{top_level}/{entry}",
                                "kind": "file",
                            }
                    root_files_added.add(source_root_key)

            project_entry["copied_roots"] = sorted(
                {
                    os.path.dirname(task_entry["copy_relative_path"]).replace("\\", "/")
                    for task_entry in project_entry["tasks"].values()
                }
            )
            project_entry["artifact_roots"] = list(project_entry["copied_roots"])

        projects_manifest[project_name] = project_entry

    manifest = {
        "generated_at": datetime.datetime.now().isoformat(timespec="seconds"),
        "output_root": output_root.replace("\\", "/"),
        "projects": projects_manifest,
        "copy_jobs": list(sorted(copy_jobs.values(), key=lambda item: item["source_path"])),
    }
    return manifest


def materialize_copies(manifest, output_root):
    for job in manifest.get("copy_jobs", []):
        source_path = job["source_path"]
        if not os.path.exists(source_path):
            print(f"[WARN] missing source path: {source_path}")
            continue
        target_path = os.path.join(output_root, *job["copy_relative_path"].split("/"))
        ensure_dir(os.path.dirname(target_path))
        print(f"[COPY] {source_path} -> {target_path}")
        if job["kind"] == "dir":
            safe_copytree(source_path, target_path)
        else:
            shutil.copy2(source_path, target_path)


def attach_workspace_paths(manifest, output_root):
    for project_entry in manifest.get("projects", {}).values():
        project_entry["artifact_copy_roots"] = [
            os.path.join(output_root, *rel_path.split("/")).replace("\\", "/")
            for rel_path in project_entry.get("artifact_roots", [])
        ]
        for task_entry in project_entry.get("tasks", {}).values():
            task_entry["copy_workspace_path"] = os.path.join(
                output_root,
                *task_entry["copy_relative_path"].split("/"),
            ).replace("\\", "/")


def main():
    parser = argparse.ArgumentParser(description="Build Oracle cloud copy tree without touching originals")
    parser.add_argument(
        "--scheduler-data",
        default="scheduler_data.json",
    )
    parser.add_argument(
        "--policy-file",
        default="config/project_policies.json",
    )
    parser.add_argument(
        "--output-root",
        default="cloud_copies",
    )
    parser.add_argument(
        "--manifest",
        default="cloud_copies/manifest.json",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
    )
    args = parser.parse_args()

    ensure_dir(args.output_root)
    source_mirror_root = os.path.join(args.output_root, "source_mirror")
    if args.clean and os.path.exists(source_mirror_root):
        remove_tree(source_mirror_root)
    scheduler_data = load_json(args.scheduler_data)
    policies = load_json(args.policy_file)
    manifest = build_manifest(scheduler_data, policies, args.output_root)
    materialize_copies(manifest, args.output_root)
    attach_workspace_paths(manifest, args.output_root)

    with open(args.manifest, "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, ensure_ascii=False, indent=2)
    print(f"[DONE] manifest written to {args.manifest}")


if __name__ == "__main__":
    main()
