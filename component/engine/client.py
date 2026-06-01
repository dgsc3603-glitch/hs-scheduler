import json
import time
import urllib.error
import urllib.request
from urllib.parse import quote, urlencode


class LocalEngineClient:
    def __init__(self, host="127.0.0.1", port=18731, timeout=0.75):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.base_url = f"http://{host}:{port}"

    def health(self):
        return self._request("GET", "/health")

    def list_projects(self):
        data = self._request("GET", "/projects")
        return data.get("items", [])

    def distributed_status(self):
        return self._request("GET", "/control-plane/status")

    def distributed_config(self):
        return self._request("GET", "/control-plane/config")

    def update_distributed_config(self, document):
        return self._request("POST", "/control-plane/config", document)

    def list_project_tasks(self, project_name):
        encoded_name = quote(project_name, safe="")
        data = self._request("GET", f"/projects/{encoded_name}/tasks")
        return data.get("items", [])

    def list_events(self, after_event_id=0, limit=200):
        query = urlencode(
            {
                "after_id": int(after_event_id or 0),
                "limit": int(limit),
            }
        )
        data = self._request("GET", f"/events?{query}")
        return data.get("items", [])

    def latest_event_id(self):
        data = self._request("GET", "/events/latest")
        return int(data.get("latest_event_id", 0) or 0)

    def get_command(self, command_id):
        encoded_id = quote(command_id, safe="")
        return self._request("GET", f"/commands/{encoded_id}")

    def wait_command(self, command_id, timeout=3.0, interval=0.15):
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            last = self.get_command(command_id)
            if last.get("status") in ("completed", "failed"):
                return last
            time.sleep(interval)
        return last or {"command_id": command_id, "status": "unknown"}

    def sync(self):
        return self._request("POST", "/sync", {})

    def shutdown(self):
        return self._request("POST", "/shutdown", {}, timeout=5.0)

    def run_project(self, project_name, trigger_source="manual", only_checked=False):
        return self._request(
            "POST",
            "/commands/run",
            {
                "project_name": project_name,
                "trigger_source": trigger_source,
                "only_checked": bool(only_checked),
            },
        )

    def stop_project(self, project_name):
        return self._request(
            "POST",
            "/commands/stop",
            {"project_name": project_name},
        )

    def stop_task(self, project_name, task_id):
        return self._request(
            "POST",
            "/commands/stop-task",
            {"project_name": project_name, "task_id": task_id},
        )

    def run_task(self, project_name, task_id):
        return self._request(
            "POST",
            "/commands/run-task",
            {"project_name": project_name, "task_id": task_id},
        )

    def acknowledge_artifact_transfer(self, project_name, run_key, pc_archive_path, checksum=""):
        return self._request(
            "POST",
            "/artifacts/ack",
            {
                "project_name": project_name,
                "run_key": run_key,
                "pc_archive_path": pc_archive_path,
                "checksum": checksum,
            },
        )

    def _request(self, method, path, payload=None, timeout=None):
        body = None
        headers = {}
        if payload is not None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json; charset=utf-8"

        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=body,
            method=method,
            headers=headers,
        )
        with urllib.request.urlopen(request, timeout=timeout or self.timeout) as response:
            raw = response.read()
        if not raw:
            return {}
        return json.loads(raw.decode("utf-8"))

    def is_available(self):
        try:
            self.health()
            return True
        except (urllib.error.URLError, TimeoutError, OSError, ValueError):
            return False
