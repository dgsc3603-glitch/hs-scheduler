import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse


class _EngineRequestHandler(BaseHTTPRequestHandler):
    service = None

    def do_GET(self):
        try:
            self._handle_get()
        except Exception as exc:
            self._write_json(500, {"error": "internal_error", "message": str(exc)})

    def do_POST(self):
        try:
            self._handle_post()
        except json.JSONDecodeError:
            self._write_json(400, {"error": "invalid_json"})
        except Exception as exc:
            self._write_json(500, {"error": "internal_error", "message": str(exc)})

    def _handle_get(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        if path == "/health":
            self._write_json(200, self.service.health())
            return
        if path == "/control-plane/status":
            self._write_json(200, self.service.distributed_status())
            return
        if path == "/control-plane/config":
            self._write_json(200, self.service.distributed_config_document())
            return
        if path == "/projects":
            self._write_json(200, {"items": self.service.list_projects()})
            return
        if path == "/events":
            try:
                after_event_id = int(query.get("after_id", ["0"])[0] or 0)
                limit = int(query.get("limit", ["200"])[0] or 200)
            except (TypeError, ValueError):
                self._write_json(400, {"error": "invalid_query"})
                return
            if after_event_id < 0 or limit < 1 or limit > 1000:
                self._write_json(400, {"error": "invalid_query"})
                return
            self._write_json(
                200,
                {"items": self.service.list_events(after_event_id=after_event_id, limit=limit)},
            )
            return
        if path == "/events/latest":
            self._write_json(200, {"latest_event_id": self.service.latest_event_id()})
            return
        if path.startswith("/commands/"):
            command_id = path[len("/commands/"):]
            command_id = self._decode_path_component(command_id)
            command = self.service.get_command(command_id)
            if not command:
                self._write_json(404, {"error": "command_not_found"})
                return
            self._write_json(200, command)
            return
        if path.startswith("/projects/") and path.endswith("/tasks"):
            project_name = path[len("/projects/"):-len("/tasks")]
            project_name = self._decode_path_component(project_name)
            self._write_json(200, {"items": self.service.get_project_tasks(project_name)})
            return
        self._write_json(404, {"error": "not_found"})

    def _handle_post(self):
        if self.path == "/shutdown":
            result = self.service.shutdown()
            self._write_json(200, result)
            threading.Thread(target=self.server.shutdown, daemon=True).start()
            return

        if self.path == "/sync":
            self._write_json(200, self.service.sync_from_legacy())
            return

        if self.path == "/control-plane/config":
            payload = self._read_json()
            self._write_json(200, self.service.update_distributed_config(payload))
            return

        if self.path == "/commands/run":
            payload = self._read_json()
            project_name = payload.get("project_name")
            if not project_name:
                self._write_json(400, {"error": "project_name_required"})
                return
            command_id = self.service.submit_run_command(
                project_name,
                trigger_source=payload.get("trigger_source", "manual"),
                only_checked=bool(payload.get("only_checked", False)),
            )
            self._write_json(202, {"command_id": command_id})
            return

        if self.path == "/commands/stop":
            payload = self._read_json()
            project_name = payload.get("project_name")
            if not project_name:
                self._write_json(400, {"error": "project_name_required"})
                return
            command_id = self.service.submit_stop_command(project_name)
            self._write_json(202, {"command_id": command_id})
            return

        if self.path == "/commands/stop-task":
            payload = self._read_json()
            project_name = payload.get("project_name")
            task_id = payload.get("task_id")
            if not project_name:
                self._write_json(400, {"error": "project_name_required"})
                return
            if not task_id:
                self._write_json(400, {"error": "task_id_required"})
                return
            command_id = self.service.submit_stop_task_command(project_name, task_id)
            self._write_json(202, {"command_id": command_id})
            return

        if self.path == "/commands/run-task":
            payload = self._read_json()
            project_name = payload.get("project_name")
            task_id = payload.get("task_id")
            if not project_name:
                self._write_json(400, {"error": "project_name_required"})
                return
            if not task_id:
                self._write_json(400, {"error": "task_id_required"})
                return
            command_id = self.service.submit_run_task_command(project_name, task_id)
            self._write_json(202, {"command_id": command_id})
            return

        if self.path == "/artifacts/ack":
            payload = self._read_json()
            project_name = payload.get("project_name")
            run_key = payload.get("run_key")
            pc_archive_path = payload.get("pc_archive_path")
            if not project_name:
                self._write_json(400, {"error": "project_name_required"})
                return
            if not run_key:
                self._write_json(400, {"error": "run_key_required"})
                return
            if not pc_archive_path:
                self._write_json(400, {"error": "pc_archive_path_required"})
                return
            self._write_json(
                200,
                self.service.acknowledge_artifact_transfer(
                    project_name,
                    run_key,
                    pc_archive_path,
                    checksum=payload.get("checksum", ""),
                ),
            )
            return

        self._write_json(404, {"error": "not_found"})

    def log_message(self, format_string, *args):
        return

    def _read_json(self):
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        if not raw:
            return {}
        return json.loads(raw.decode("utf-8"))

    def _write_json(self, status_code, payload):
        encoded = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _decode_path_component(self, value):
        from urllib.parse import unquote
        return unquote(value)


class LocalApiServer:
    def __init__(self, service, host="127.0.0.1", port=18731):
        self.service = service
        self.host = host
        self.port = port
        handler_class = type(
            "SchedulerEngineRequestHandler",
            (_EngineRequestHandler,),
            {"service": service},
        )
        self._server = ThreadingHTTPServer((host, port), handler_class)

    def serve_forever(self):
        self._server.serve_forever()

    def shutdown(self):
        self._server.shutdown()
        self._server.server_close()
