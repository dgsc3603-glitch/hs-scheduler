# -*- coding: utf-8 -*-
"""Telegram remote-control bot for HS Scheduler."""

import asyncio
import json
import os
import threading
import time
import urllib.parse
import urllib.request

try:
    import httpx

    HAS_HTTPX = True
except ImportError:
    HAS_HTTPX = False


class SchedulerTelegramBot:
    """Small Telegram bot integrated with the scheduler core."""

    DEFAULT_TOKEN = ""
    DEFAULT_CHAT_ID = ""

    def __init__(self, scheduler_core, credentials, ui_callback=None):
        self.core = scheduler_core
        self.credentials = credentials
        self.ui_callback = ui_callback
        self._token = None
        self._chat_id = None
        self.running = False
        self._thread = None
        self._offset = None
        self._load_credentials()

    def _load_credentials(self):
        try:
            secrets = self.credentials.load()
            self._token = secrets.get("TELEGRAM_BOT_TOKEN") or self.DEFAULT_TOKEN
            self._chat_id = secrets.get("CHAT_ID") or self.DEFAULT_CHAT_ID
        except Exception:
            self._token = self.DEFAULT_TOKEN
            self._chat_id = self.DEFAULT_CHAT_ID

    @property
    def is_configured(self):
        return bool(self._token and self._chat_id)

    def start(self):
        if not self.is_configured:
            self.core.log("Telegram bot is not configured. Check scheduler_secrets.json.")
            return False
        self.running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="TelegramBot")
        self._thread.start()
        self.core.log("Telegram bot started.")
        return True

    def stop(self):
        self.running = False

    def _run_loop(self):
        if HAS_HTTPX:
            asyncio.run(self._async_polling_loop())
        else:
            self._sync_polling_loop()

    async def _async_polling_loop(self):
        async with httpx.AsyncClient(timeout=35) as client:
            try:
                await client.post(
                    f"https://api.telegram.org/bot{self._token}/deleteWebhook",
                    json={"drop_pending_updates": False},
                )
            except Exception:
                pass

            await self._send_message_async(client, "HS Scheduler bot is ready. Type /help.")
            if self.ui_callback:
                try:
                    self.ui_callback(connected=True)
                except Exception:
                    pass

            while self.running:
                try:
                    url = f"https://api.telegram.org/bot{self._token}/getUpdates?timeout=30"
                    if self._offset:
                        url += f"&offset={self._offset + 1}"
                    response = await client.get(url)
                    response.raise_for_status()
                    for update in response.json().get("result", []):
                        self._offset = update["update_id"]
                        callback = update.get("callback_query")
                        if callback:
                            await self._answer_callback_async(client, callback["id"])
                            await self._handle_callback_async(client, callback["data"])
                            continue
                        text = update.get("message", {}).get("text", "").strip()
                        if text:
                            await self._handle_command_async(client, text)
                except httpx.ReadTimeout:
                    pass
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code == 409:
                        self.core.log("Telegram bot conflict detected. Retrying in 15 seconds.")
                        await asyncio.sleep(15)
                    else:
                        self.core.log(f"Telegram polling error: {exc}")
                        await asyncio.sleep(5)
                except Exception as exc:
                    self.core.log(f"Telegram polling error: {exc}")
                    await asyncio.sleep(5)

    async def _send_message_async(self, client, text, reply_markup=None):
        payload = {"chat_id": self._chat_id, "text": text, "parse_mode": "HTML"}
        if reply_markup:
            payload["reply_markup"] = json.dumps(reply_markup)
        try:
            await client.post(f"https://api.telegram.org/bot{self._token}/sendMessage", json=payload, timeout=10)
        except Exception as exc:
            self.core.log(f"Telegram send failed: {exc}")

    async def _answer_callback_async(self, client, callback_id):
        try:
            await client.post(
                f"https://api.telegram.org/bot{self._token}/answerCallbackQuery",
                json={"callback_query_id": callback_id},
                timeout=5,
            )
        except Exception:
            pass

    async def _handle_command_async(self, client, text):
        text_lower = text.lower().strip()
        if text_lower in ["/help", "help", "?"]:
            await self._cmd_help(client)
        elif text_lower in ["/status", "status"]:
            await self._cmd_status(client)
        elif text_lower in ["/run", "run"]:
            await self._cmd_run_menu(client)
        elif text_lower in ["/stop", "stop"]:
            await self._cmd_stop(client)
        elif text_lower in ["/log", "log"]:
            await self._cmd_log(client)
        elif text_lower in ["/next", "next", "schedule"]:
            await self._cmd_next(client)
        elif text_lower in ["/mem", "mem", "memory", "ram"]:
            await self._cmd_memory(client)
        elif text_lower.startswith("/pause ") or text_lower.startswith("pause "):
            await self._cmd_pause(client, text.split(" ", 1)[1].strip())
        elif text_lower.startswith("/resume ") or text_lower.startswith("resume "):
            await self._cmd_resume(client, text.split(" ", 1)[1].strip())
        elif text_lower.startswith("/run ") or text_lower.startswith("run "):
            await self._cmd_run_project(client, text.split(" ", 1)[1].strip())
        else:
            await self._send_message_async(client, "Unknown command. Type /help for available commands.")

    async def _handle_callback_async(self, client, data):
        if data.startswith("run:"):
            await self._cmd_run_project(client, data.split(":", 1)[1])
        elif data.startswith("stop:"):
            await self._cmd_stop_project(client, data.split(":", 1)[1])
        elif data.startswith("pause:"):
            await self._cmd_pause(client, data.split(":", 1)[1])
        elif data.startswith("resume:"):
            await self._cmd_resume(client, data.split(":", 1)[1])
        elif data == "status":
            await self._cmd_status(client)
        elif data == "log":
            await self._cmd_log(client)
        elif data == "mem":
            await self._cmd_memory(client)
        elif data == "next":
            await self._cmd_next(client)

    async def _cmd_help(self, client):
        await self._send_message_async(
            client,
            "<b>HS Scheduler Bot Commands</b>\n\n"
            "<b>Read</b>\n"
            "/status - Show project status\n"
            "/next - Show next run times\n"
            "/log - Show recent log output\n"
            "/mem - Show memory and CPU information\n\n"
            "<b>Control</b>\n"
            "/run - Open project run menu\n"
            "/run [name] - Run a project now\n"
            "/stop - Stop running projects\n"
            "/pause [name] - Disable a project\n"
            "/resume [name] - Enable a project",
            reply_markup=self._quick_keyboard(),
        )

    def _quick_keyboard(self):
        return {
            "inline_keyboard": [
                [{"text": "Status", "callback_data": "status"}, {"text": "Next", "callback_data": "next"}],
                [{"text": "Log", "callback_data": "log"}, {"text": "Memory", "callback_data": "mem"}],
                [{"text": "Run", "callback_data": "run:__menu__"}, {"text": "Stop all", "callback_data": "stop:__all__"}],
            ]
        }

    async def _cmd_status(self, client):
        if not self.core.projects:
            await self._send_message_async(client, "No projects are registered.")
            return
        lines = ["<b>Project Status</b>"]
        for project in self.core.projects:
            progress = ""
            if project.total_tasks:
                progress = f" ({project.completed_tasks}/{project.total_tasks})"
            enabled = "enabled" if project.enabled else "paused"
            lines.append(f"- {project.name}: {project.status}{progress}, next={project.next_run}, {enabled}")
        await self._send_message_async(client, "\n".join(lines))

    async def _cmd_run_menu(self, client):
        if not self.core.projects:
            await self._send_message_async(client, "No projects are registered.")
            return
        rows = [[{"text": project.name[:30], "callback_data": f"run:{project.name}"}] for project in self.core.projects]
        rows.append([{"text": "Status", "callback_data": "status"}, {"text": "Stop all", "callback_data": "stop:__all__"}])
        await self._send_message_async(client, "Choose a project to run:", reply_markup={"inline_keyboard": rows})

    async def _cmd_run_project(self, client, name):
        if name == "__menu__":
            await self._cmd_run_menu(client)
            return
        project = self._find_project(name)
        if not project:
            await self._send_message_async(client, f"Project not found: {name}")
            return
        if project.status == self.core.STATUS_RUNNING:
            await self._send_message_async(client, f"{project.name} is already running.")
            return
        if self.core.run_project(project, only_checked=False, trigger_source="manual"):
            await self._send_message_async(client, f"Started project: {project.name}")
        else:
            await self._send_message_async(client, f"Failed to start project: {project.name}")

    async def _cmd_stop(self, client):
        running = [p for p in self.core.projects if p.status == self.core.STATUS_RUNNING]
        if not running:
            await self._send_message_async(client, "No projects are currently running.")
            return
        keyboard = {"inline_keyboard": [[{"text": f"Stop {p.name[:24]}", "callback_data": f"stop:{p.name}"}] for p in running]}
        keyboard["inline_keyboard"].append([{"text": "Stop all", "callback_data": "stop:__all__"}])
        await self._send_message_async(client, "Choose a project to stop:", reply_markup=keyboard)

    async def _cmd_stop_project(self, client, name):
        if name == "__all__":
            running = [p for p in self.core.projects if p.status == self.core.STATUS_RUNNING]
            for project in running:
                self.core.stop_project(project.name)
            await self._send_message_async(client, f"Stop signal sent to {len(running)} project(s).")
            return
        project = self._find_project(name)
        if not project:
            await self._send_message_async(client, f"Project not found: {name}")
            return
        self.core.stop_project(project.name)
        await self._send_message_async(client, f"Stop signal sent: {project.name}")

    async def _cmd_log(self, client):
        log_path = self._latest_log_file()
        if not log_path:
            await self._send_message_async(client, "No log file is available.")
            return
        try:
            with open(log_path, "r", encoding="utf-8", errors="replace") as handle:
                content = handle.read()[-1500:]
        except Exception as exc:
            await self._send_message_async(client, f"Failed to read log: {exc}")
            return
        await self._send_message_async(client, f"<b>Recent Log</b> ({os.path.basename(log_path)})\n<pre>{content}</pre>")

    async def _cmd_next(self, client):
        if not self.core.projects:
            await self._send_message_async(client, "No projects are registered.")
            return
        lines = ["<b>Next Runs</b>"]
        for project in sorted(self.core.projects, key=lambda item: item.next_run if item.next_run != "-" else "9999"):
            state = "paused" if not project.enabled else "enabled"
            lines.append(f"- {project.name}: {project.next_run} ({state})")
        await self._send_message_async(client, "\n".join(lines))

    async def _cmd_pause(self, client, name):
        project = self._find_project(name)
        if not project:
            await self._send_message_async(client, f"Project not found: {name}")
            return
        project.enabled = False
        project.calculate_next_run()
        self.core.save_data()
        await self._send_message_async(client, f"Paused project: {project.name}")

    async def _cmd_resume(self, client, name):
        project = self._find_project(name)
        if not project:
            await self._send_message_async(client, f"Project not found: {name}")
            return
        project.enabled = True
        project.calculate_next_run()
        self.core.save_data()
        await self._send_message_async(client, f"Resumed project: {project.name}. Next run: {project.next_run}")

    async def _cmd_memory(self, client):
        lines = ["<b>System Resources</b>"]
        try:
            import psutil

            proc = psutil.Process()
            mem = proc.memory_info()
            sys_mem = psutil.virtual_memory()
            lines.append(f"Scheduler RAM: {mem.rss / 1024 / 1024:.1f} MB")
            lines.append(f"Threads: {proc.num_threads()}")
            lines.append(f"System RAM: {sys_mem.used / 1024**3:.1f}/{sys_mem.total / 1024**3:.1f} GB ({sys_mem.percent}%)")
            lines.append(f"CPU: {psutil.cpu_percent(interval=0.3)}%")
        except ImportError:
            lines.append("psutil is not installed.")
        except Exception as exc:
            lines.append(f"Failed to collect resource data: {exc}")
        lines.append(f"Active processes: {len(getattr(self.core, 'active_processes', []))}")
        lines.append(f"Projects: {len(self.core.projects)}")
        await self._send_message_async(client, "\n".join(lines))

    def _find_project(self, name):
        target = str(name or "").strip().lower()
        for project in self.core.projects:
            if project.name.lower() == target:
                return project
        for project in self.core.projects:
            if target and target in project.name.lower():
                return project
        return None

    def _latest_log_file(self):
        log_dir = getattr(self.core, "log_dir", "logs")
        if not os.path.isdir(log_dir):
            return None
        files = [
            os.path.join(log_dir, name)
            for name in os.listdir(log_dir)
            if name.endswith(".log") and os.path.isfile(os.path.join(log_dir, name))
        ]
        return max(files, key=os.path.getmtime) if files else None

    def send_alert(self, message):
        if not self.is_configured:
            return False
        if HAS_HTTPX:
            try:
                asyncio.run(self._send_alert_async(message))
                return True
            except Exception as exc:
                self.core.log(f"Telegram alert failed: {exc}")
                return False
        return self._send_alert_sync(message)

    async def _send_alert_async(self, message):
        async with httpx.AsyncClient(timeout=10) as client:
            await self._send_message_async(client, message)

    def _sync_polling_loop(self):
        self.send_alert("HS Scheduler bot is ready. Type /help.")
        while self.running:
            try:
                url = f"https://api.telegram.org/bot{self._token}/getUpdates?timeout=30"
                if self._offset:
                    url += f"&offset={self._offset + 1}"
                with urllib.request.urlopen(url, timeout=35) as response:
                    updates = json.loads(response.read().decode("utf-8"))
                for update in updates.get("result", []):
                    self._offset = update["update_id"]
                    text = update.get("message", {}).get("text", "").strip()
                    if text:
                        self._handle_command_sync(text)
            except Exception as exc:
                self.core.log(f"Telegram sync polling error: {exc}")
                time.sleep(5)

    def _send_alert_sync(self, message):
        try:
            payload = urllib.parse.urlencode({"chat_id": self._chat_id, "text": message, "parse_mode": "HTML"}).encode()
            url = f"https://api.telegram.org/bot{self._token}/sendMessage"
            urllib.request.urlopen(url, data=payload, timeout=10)
            return True
        except Exception as exc:
            self.core.log(f"Telegram sync send failed: {exc}")
            return False

    def _handle_command_sync(self, text):
        text_lower = text.lower().strip()
        if text_lower in ["/help", "help", "?"]:
            self.send_alert(
                "HS Scheduler Bot Commands\n"
                "/status - Project status\n"
                "/next - Next run times\n"
                "/run [name] - Run a project\n"
                "/stop - Stop running projects\n"
                "/mem - RAM status"
            )
        elif text_lower in ["/status", "status"]:
            lines = ["Project Status"]
            for project in self.core.projects:
                lines.append(f"- {project.name}: {project.status}")
            self.send_alert("\n".join(lines))
        elif text_lower in ["/mem", "mem", "memory", "ram"]:
            try:
                import psutil

                mem = psutil.Process().memory_info()
                self.send_alert(f"RAM: {mem.rss / 1024 / 1024:.0f} MB")
            except Exception as exc:
                self.send_alert(f"Memory check failed: {exc}")
