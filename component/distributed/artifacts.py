import hashlib
import os
import shutil


IGNORED_DIR_NAMES = {"__pycache__", ".git", ".venv", "venv", "node_modules"}
IGNORED_FILE_SUFFIXES = {".pyc", ".pyo", ".tmp", ".log"}


def _walk_snapshot(root_path):
    snapshot = {}
    if not root_path or not os.path.exists(root_path):
        return snapshot

    for current_root, dir_names, file_names in os.walk(root_path):
        dir_names[:] = [name for name in dir_names if name not in IGNORED_DIR_NAMES]
        for file_name in file_names:
            if os.path.splitext(file_name)[1].lower() in IGNORED_FILE_SUFFIXES:
                continue
            abs_path = os.path.join(current_root, file_name)
            try:
                stat = os.stat(abs_path)
            except OSError:
                continue
            rel_path = os.path.relpath(abs_path, root_path).replace("\\", "/")
            snapshot[rel_path] = {
                "size": int(stat.st_size),
                "mtime_ns": int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000))),
            }
    return snapshot


def _sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class ArtifactSpoolManager:
    def __init__(self, spool_root):
        self.spool_root = spool_root

    def capture_baseline(self, roots):
        baselines = {}
        for root in roots:
            baselines[root] = _walk_snapshot(root)
        return baselines

    def finalize_run(self, project_name, run_key, roots, baseline):
        if not roots:
            return {"spool_path": "", "files": [], "checksum": "", "byte_size": 0}

        spool_path = os.path.join(self.spool_root, project_name, run_key.replace(":", "_"))
        os.makedirs(spool_path, exist_ok=True)
        copied_files = []
        total_bytes = 0

        for root in roots:
            before = (baseline or {}).get(root, {})
            after = _walk_snapshot(root)
            changed_rel_paths = [
                rel_path
                for rel_path, meta in after.items()
                if rel_path not in before or before[rel_path] != meta
            ]
            for rel_path in changed_rel_paths:
                source_path = os.path.join(root, rel_path)
                if not os.path.exists(source_path):
                    continue
                target_path = os.path.join(spool_path, os.path.basename(root), rel_path)
                os.makedirs(os.path.dirname(target_path), exist_ok=True)
                shutil.copy2(source_path, target_path)
                file_size = os.path.getsize(source_path)
                copied_files.append(
                    {
                        "root": root,
                        "relative_path": rel_path,
                        "source_path": source_path,
                        "spooled_path": target_path,
                        "size": file_size,
                    }
                )
                total_bytes += file_size

        if not copied_files:
            try:
                shutil.rmtree(spool_path)
            except OSError:
                pass
            return {"spool_path": "", "files": [], "checksum": "", "byte_size": 0}

        checksum_source = "|".join(
            f"{item['relative_path']}:{item['size']}:{_sha256_file(item['spooled_path'])}"
            for item in copied_files
        )
        checksum = hashlib.sha256(checksum_source.encode("utf-8")).hexdigest()
        return {
            "spool_path": spool_path,
            "files": copied_files,
            "checksum": checksum,
            "byte_size": total_bytes,
        }

    def cleanup_run(self, project_name, run_key):
        spool_path = os.path.join(self.spool_root, project_name, run_key.replace(":", "_"))
        if os.path.exists(spool_path):
            shutil.rmtree(spool_path)
            return True
        return False
