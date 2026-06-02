import tkinter as tk
from tkinter import messagebox, filedialog, simpledialog
import ttkbootstrap as ttk
import queue
import os
import threading
import time
import subprocess
import sys
from types import SimpleNamespace
from component.ui.main_window import MainWindow
from component.ui.distributed_settings_dialog import DistributedSettingsDialog
from component.core.scheduler_core import SchedulerEvent
from component.core.stabilized_scheduler_core import StabilizedSchedulerCore
from component.core.telegram_bot import SchedulerTelegramBot
from component.engine import LocalEngineClient
from component.utils import (
    CredentialManager,
    atomic_write_json,
    load_json_file,
    send_telegram_alert,
    setup_logging,
)



class HSSchedulerApp:
    def __init__(self, root, base_dir=None):
        self.root = root
        self.event_queue = queue.Queue(maxsize=5000)  # C-2: 메모리 폭증 방지
        self.engine_client = LocalEngineClient()
        self.engine_process = None
        self.engine_process_owned = False
        self.engine_mode = False
        self.engine_projects_cache = []
        self.engine_tasks_cache = {}
        self._last_engine_projects_refresh = 0.0
        self._last_engine_tasks_refresh = {}
        self._engine_projects_refresh_interval = 2.0
        self._engine_tasks_refresh_interval = 1.0
        self._last_engine_reconnect_attempt = 0.0
        self._engine_reconnect_interval = 5.0
        self._last_engine_mismatch_log = 0.0
        self._engine_health_cache = None
        self._engine_health_checked_at = 0.0
        self._engine_health_interval = 0.75
        self._last_distributed_status_refresh = 0.0
        self._distributed_status_refresh_interval = 2.0
        self._distributed_status_cache = {}
        self._distributed_settings_dialog = None
        self._last_engine_event_id = 0
        
        # Paths - 외부에서 base_dir 지정 가능 (런처에서 루트 폴더 전달)
        if base_dir is None:
            base_dir = os.path.dirname(os.path.abspath(__file__))
        self.base_dir = base_dir
        self.data_file = os.path.join(base_dir, "scheduler_data.json")
        
        # Managers
        self.credentials = CredentialManager(self.data_file)
        self.logger = setup_logging(self.data_file, 10*1024*1024, 5)
        
        # Core Engine (Source of Truth)
        self.core = StabilizedSchedulerCore(self.event_queue, self.credentials, self.data_file)
        
        # UI
        self.ui = MainWindow(self.root, self)
        
        # 창 닫기 → 바로 종료 (트레이 아이콘 없음)
        self.root.protocol("WM_DELETE_WINDOW", self.clean_quit)

        # Startup
        self.engine_mode = self._ensure_engine_bridge()
        if self.engine_mode:
            self.core.load_data()
        else:
            self.core.start()
        self._sync_engine_state(force=True)
        self._sync_engine_event_cursor(force=True)
        self._refresh_engine_projects(force=True)
        self._refresh_distributed_status(force=True)
        self.refresh_project_list()
        self.core.log("시스템 초기화 완료.")
        
        # 텔레그램 봇 시작
        if not self.engine_mode:
            self.telegram_bot = SchedulerTelegramBot(
                self.core, 
                self.credentials,
                ui_callback=self.ui.update_bot_status
            )
            self.telegram_bot.start()
        else:
            self.ui.update_bot_status(False)

    @property
    def projects(self):
        """Delegate access to Core projects"""
        return self.core.projects

    def _get_selected_project_name(self):
        if not hasattr(self, "ui") or not hasattr(self.ui, "project_panel"):
            return None
        return self.ui.project_panel.get_selected_name()

    def _get_selected_task_id(self):
        if not hasattr(self, "ui") or not hasattr(self.ui, "task_panel"):
            return None
        return self.ui.task_panel.get_selected_task_id()

    def _show_engine_action_failed(self, action_label):
        messagebox.showwarning(
            "Engine Connection Error",
            f"Cannot connect to the engine for {action_label}.",
        )

    def _engine_process_alive(self):
        return bool(self.engine_process and self.engine_process.poll() is None)

    def _normalize_path(self, path):
        return os.path.normcase(os.path.abspath(path))

    def _log_engine_bridge_issue(self, message):
        now = time.time()
        if (now - self._last_engine_mismatch_log) < 30:
            return
        self._last_engine_mismatch_log = now
        try:
            self.core.log(message)
        except Exception:
            pass

    def _engine_health_matches_base(self, health):
        expected_base = self._normalize_path(self.base_dir)
        expected_data = self._normalize_path(self.data_file)
        expected_db = self._normalize_path(os.path.join(self.base_dir, "scheduler_engine.db"))

        actual_base = health.get("base_dir")
        actual_data = health.get("legacy_data_path")
        actual_db = health.get("db_path")
        if actual_base and self._normalize_path(actual_base) != expected_base:
            return False
        if actual_data and self._normalize_path(actual_data) != expected_data:
            return False
        if actual_db and self._normalize_path(actual_db) != expected_db:
            return False
        return True

    def _get_engine_health(self, force=False):
        now = time.time()
        if not force and (now - self._engine_health_checked_at) < self._engine_health_interval:
            return self._engine_health_cache
        self._engine_health_checked_at = now
        try:
            health = self.engine_client.health()
        except Exception:
            self._engine_health_cache = None
            return False
        self._engine_health_cache = health
        return health

    def _engine_available_for_base(self, force=False):
        health = self._get_engine_health(force=force)
        if not health:
            return False
        if self._engine_health_matches_base(health):
            return True
        self._log_engine_bridge_issue(
            "엔진 포트가 다른 작업 폴더에서 사용 중입니다. 현재 폴더는 내장 코어로 실행합니다."
        )
        return False

    def _stop_owned_engine_process(self):
        if not self.engine_process_owned or not self.engine_process:
            return
        try:
            if self.engine_process.poll() is None:
                self.engine_process.terminate()
                try:
                    self.engine_process.wait(timeout=2.0)
                except subprocess.TimeoutExpired:
                    self.engine_process.kill()
                    self.engine_process.wait(timeout=2.0)
        except Exception:
            pass
        finally:
            self.engine_process = None
            self.engine_process_owned = False

    def _recover_engine_bridge(self, force_restart=False):
        if self._engine_available_for_base(force=True):
            self._sync_engine_event_cursor(force=True)
            return True

        now = time.time()
        if not force_restart and (now - self._last_engine_reconnect_attempt) < self._engine_reconnect_interval:
            return False
        self._last_engine_reconnect_attempt = now

        if force_restart or (self.engine_process_owned and not self._engine_process_alive()):
            self._stop_owned_engine_process()

        recovered = self._ensure_engine_bridge()
        if recovered:
            self._sync_engine_event_cursor(force=True)
        return recovered

    # Project Management Delegates
    def add_project(self):
        name = simpledialog.askstring("New Project", "Project name")
        if not name: return
        self.core.add_project(name)
        self._sync_engine_state(force=True)
        self.refresh_project_list()

    def delete_project(self):
        idx = self.ui.project_panel.get_selected()
        if idx is None: return
        if not messagebox.askyesno("Confirm", "Delete the selected project?"): return
        project_name = self.projects[idx].name
        self.core.remove_project(idx)
        self.engine_tasks_cache.pop(project_name, None)
        self._last_engine_tasks_refresh.pop(project_name, None)
        self._sync_engine_state(force=True)
        self.refresh_project_list()

    def toggle_project_enabled(self):
        idx = self.ui.project_panel.get_selected()
        if idx is None: return
        
        # Direct modification via core could be better, but for now modify object and save
        proj = self.projects[idx]
        proj.enabled = not proj.enabled
        proj.calculate_next_run()
        if not self.save_data():
            self._show_engine_action_failed("Save project settings")
        self.refresh_project_list(selected_project_name=proj.name)

    def stop_project(self):
        idx = self.ui.project_panel.get_selected()
        if idx is not None:
            proj = self.projects[idx]
            proj.stop_requested = True
            if self.engine_mode:
                if not self._submit_engine_stop_command(proj.name):
                    proj.stop_requested = False
                    self._show_engine_action_failed("Stop project")
                    return
            else:
                self.core.stop_project(proj.name)
            self.core.log(f"⏹ [{proj.name}] 중지 요청됨")

    def run_project_now(self):
        idx = self.ui.project_panel.get_selected()
        if idx is not None:
            if self.engine_mode:
                if not self._submit_engine_run_command(self.projects[idx].name, trigger_source="manual", only_checked=False):
                    self._show_engine_action_failed("Run project")
            else:
                self.core.run_project_manual(self.projects[idx])

    def on_project_select(self, event):
        self.refresh_task_list()

    def edit_project(self, event=None):
        """H-3: 프로젝트 편집 다이얼로그"""
        idx = self.ui.project_panel.get_selected()
        if idx is None: return
        proj = self.projects[idx]
        
        dialog = tk.Toplevel(self.root)
        dialog.title(f"Project Settings: {proj.name}")
        dialog.geometry("450x400")
        dialog.transient(self.root)
        dialog.grab_set()
        
        # 프로젝트명
        ttk.Label(dialog, text="Project name:").pack(anchor="w", padx=15, pady=(15,2))
        name_var = tk.StringVar(value=proj.name)
        ttk.Entry(dialog, textvariable=name_var, width=40).pack(padx=15, fill="x")
        
        # 실행 시간
        ttk.Label(dialog, text="Run time (HH:MM):").pack(anchor="w", padx=15, pady=(10,2))
        time_var = tk.StringVar(value=proj.run_time)
        ttk.Entry(dialog, textvariable=time_var, width=40).pack(padx=15, fill="x")
        
        # 스케줄 타입
        ttk.Label(dialog, text="Schedule type:").pack(anchor="w", padx=15, pady=(10,2))
        type_var = tk.StringVar(value=proj.schedule_type)
        type_combo = ttk.Combobox(dialog, textvariable=type_var, values=["daily", "weekly", "interval", "onetime"], state="readonly", width=38)
        type_combo.pack(padx=15, fill="x")
        
        # 스케줄 값
        ttk.Label(dialog, text="Schedule value (weekday/interval/date):").pack(anchor="w", padx=15, pady=(10,2))
        value_var = tk.StringVar(value=proj.schedule_value)
        ttk.Entry(dialog, textvariable=value_var, width=40).pack(padx=15, fill="x")
        
        # Step 모드
        ttk.Label(dialog, text="Step mode:").pack(anchor="w", padx=15, pady=(10,2))
        mode_var = tk.StringVar(value=proj.step_mode)
        mode_combo = ttk.Combobox(dialog, textvariable=mode_var, values=["parallel", "sequential"], state="readonly", width=38)
        mode_combo.pack(padx=15, fill="x")
        
        def save_changes():
            proj.name = name_var.get().strip() or proj.name
            proj.run_time = time_var.get().strip()
            proj.schedule_type = type_var.get()
            proj.schedule_value = value_var.get().strip()
            proj.step_mode = mode_var.get()
            proj.calculate_next_run()
            self.save_data()
            self.refresh_project_list()
            self.core.log(f"✅ [{proj.name}] 프로젝트 설정 저장 완료")
            dialog.destroy()
        
        btn_frame = ttk.Frame(dialog)
        btn_frame.pack(fill="x", padx=15, pady=20)
        ttk.Button(btn_frame, text="Save", command=save_changes, bootstyle="success").pack(side="right", padx=5)
        ttk.Button(btn_frame, text="Cancel", command=dialog.destroy, bootstyle="secondary").pack(side="right")

    # Task Management Delegates
    def add_task_to_project(self):
        idx = self.ui.project_panel.get_selected()
        if idx is None: return
        paths = filedialog.askopenfilenames(filetypes=[("Python Files", "*.py")])
        if not paths: return
        proj = self.projects[idx]
        max_step = max((t.step for t in proj.tasks), default=0)
        for i, p in enumerate(paths):
            proj.add_task(p, max_step + 1 + i)
        self.save_data()
        self.refresh_task_list()

    def on_drop_files(self, event):
        """외부 파일 드래그앤드롭으로 작업 추가"""
        idx = self.ui.project_panel.get_selected()
        if idx is None:
            messagebox.showwarning("No Project Selected", "Select a project first.")
            return
        
        # tkinterdnd2의 드롭 데이터 파싱 (중괄호로 묶인 경로 처리)
        raw = event.data
        files = []
        if '{' in raw:
            import re
            files = re.findall(r'\{([^}]+)\}', raw)
            # 중괄호 밖의 나머지도 처리
            remaining = re.sub(r'\{[^}]+\}', '', raw).strip()
            if remaining:
                files.extend(remaining.split())
        else:
            files = raw.split()
        
        py_files = [f for f in files if f.lower().endswith('.py')]
        if not py_files:
            messagebox.showinfo("Notice", "Only .py files can be added.")
            return
        
        proj = self.projects[idx]
        max_step = max((t.step for t in proj.tasks), default=0)
        for i, p in enumerate(py_files):
            proj.add_task(p, max_step + 1 + i)
        
        self.save_data()
        self.refresh_task_list()
        self.core.log(f"📂 드래그앤드롭으로 {len(py_files)}개 파일 추가됨")

    def delete_task_from_project(self):
        idx = self.ui.project_panel.get_selected()
        if idx is None: return
        sel = self.ui.task_panel.tree.selection()
        if not sel: return
        
        item_id = sel[0]
        vals = self.ui.task_panel.tree.item(item_id, 'values')
        if not vals: return
        
        target_task_id = vals[4] # task_id column
        
        proj = self.projects[idx]
        for i, task in enumerate(proj.tasks):
            if task.task_id == target_task_id:
                filename = task.filename
                del proj.tasks[i]
                self.core.log(f"[{proj.name}] 🗑️ 작업 '{filename}' 삭제 완료.")
                break
        self.save_data()
        self.refresh_task_list()

    def toggle_task_check(self, specific_item=None):
        idx = self.ui.project_panel.get_selected()
        if idx is None: return
        items = [specific_item] if specific_item else self.ui.task_panel.tree.selection()
        toggled_task_ids = []
        for item_id in items:
            vals = self.ui.task_panel.tree.item(item_id, 'values')
            if not vals or len(vals) < 5: continue
            
            target_task_id = vals[4] 
            for t in self.projects[idx].tasks:
                if t.task_id == target_task_id:
                    t.checked = not t.checked
                    if self.engine_mode:
                        for cached in self.engine_tasks_cache.get(self.projects[idx].name, []):
                            if cached.get("task_id") == target_task_id:
                                cached["checked"] = t.checked
                                break
                    toggled_task_ids.append(target_task_id)
                    break
        self.save_data()
        self.refresh_task_list(selected_task_id=toggled_task_ids[0] if len(toggled_task_ids) == 1 else None)

    def run_checked_tasks(self):
        idx = self.ui.project_panel.get_selected()
        if idx is not None:
            if self.engine_mode:
                if not self._submit_engine_run_command(self.projects[idx].name, trigger_source="manual_checked", only_checked=True):
                    self._show_engine_action_failed("Run selected tasks")
            else:
                self.core.run_project_manual(self.projects[idx], only_checked=True)

    def save_data(self):
        self._refresh_core_runtime_from_engine()
        self.core.save_data()
        sync_ok = self._sync_engine_state(force=True)
        self._refresh_engine_projects(force=True)
        idx = self.ui.project_panel.get_selected()
        if idx is not None and 0 <= idx < len(self.projects):
            self._refresh_engine_tasks(self.projects[idx].name, force=True)
        if self.engine_mode and not sync_ok:
            self._show_engine_action_failed("Save settings")
        return sync_ok or not self.engine_mode

    def _refresh_core_runtime_from_engine(self):
        if not self.engine_mode:
            return
        if not self._engine_available_for_base():
            return
        self._refresh_engine_projects(force=True)
        snapshots = {
            item.get("project_name"): item
            for item in self.engine_projects_cache
            if item.get("project_name")
        }
        for project in self.projects:
            snapshot = snapshots.get(project.name)
            if not snapshot:
                continue
            project.status = snapshot.get("status", project.status) or project.status
            project.completed_tasks = int(snapshot.get("completed_tasks", project.completed_tasks) or 0)
            project.total_tasks = int(snapshot.get("total_tasks", project.total_tasks) or 0)
            if snapshot.get("last_run_at"):
                project.last_run = snapshot["last_run_at"]
            if snapshot.get("last_consumed_ticket"):
                project.last_consumed_ticket = snapshot["last_consumed_ticket"]
            details = snapshot.get("details") or {}
            if details.get("last_trigger_source"):
                project.last_trigger_source = details["last_trigger_source"]
            if details.get("last_manual_run"):
                project.last_manual_run = details["last_manual_run"]
            if details.get("last_scheduled_run"):
                project.last_scheduled_run = details["last_scheduled_run"]

            self._refresh_engine_tasks(project.name, force=True)
            task_snapshots = {
                item.get("task_id"): item
                for item in self.engine_tasks_cache.get(project.name, [])
                if item.get("task_id")
            }
            for task in project.tasks:
                task_snapshot = task_snapshots.get(task.task_id)
                if task_snapshot and task_snapshot.get("status"):
                    task.status = task_snapshot["status"]

    def stop_selected_task(self):
        idx = self.ui.project_panel.get_selected()
        if idx is None: return
        sel = self.ui.task_panel.tree.selection()
        if not sel: return
        
        item_id = sel[0]
        vals = self.ui.task_panel.tree.item(item_id, 'values')
        if not vals or len(vals) < 5: return
        
        target_task_id = vals[4]
        
        proj = self.projects[idx]
        task_obj = next((t for t in proj.tasks if t.task_id == target_task_id), None)
        if task_obj:
            if self.engine_mode:
                if not self._submit_engine_stop_task_command(proj.name, task_obj.task_id):
                    self._show_engine_action_failed("Stop task")
            else:
                self.core.stop_task(proj.name, task_obj.task_id)

    # UI Refresh Delegates
    def refresh_task_list(self, selected_task_id=None, selected_project_name=None):
        if selected_task_id is None:
            selected_task_id = self._get_selected_task_id()
        if selected_project_name is None:
            selected_project_name = self._get_selected_project_name()

        idx = self.ui.project_panel.get_selected()
        if idx is None and selected_project_name:
            for project_index, project in enumerate(self.projects):
                if project.name == selected_project_name:
                    idx = project_index
                    break
        # Fix 8: 프로젝트 미선택 시 실행 중인 프로젝트 자동 표시
        if idx is None:
            for i, p in enumerate(self.projects):
                if p.status == self.core.STATUS_RUNNING:
                    idx = i
                    break
        if idx is not None:
            proj = self.projects[idx]
            steps = proj.get_tasks_by_step()
            if self.engine_mode:
                steps = self._get_engine_task_steps(proj)
            self.ui.task_panel.refresh(steps, {
                'TASK_STATUS_COMPLETED': self.core.TASK_STATUS_COMPLETED,
                'TASK_STATUS_ERROR': self.core.TASK_STATUS_ERROR,
                'TASK_STATUS_RUNNING': self.core.TASK_STATUS_RUNNING,
                'TASK_STATUS_TIMEOUT': self.core.TASK_STATUS_TIMEOUT,
                'TASK_STATUS_STOPPED': self.core.TASK_STATUS_STOPPED,
                'TASK_STATUS_FINAL_FAIL': self.core.TASK_STATUS_FINAL_FAIL
            }, selected_task_id=selected_task_id)

    def refresh_project_list(self, selected_project_name=None):
        if selected_project_name is None:
            selected_project_name = self._get_selected_project_name()
        display_projects = self._build_display_projects()
        self.ui.project_panel.refresh(
            display_projects,
            self.core.STATUS_WAITING,
            self.core.STATUS_RUNNING,
            self.core.STATUS_COMPLETED,
            self.core.STATUS_ERROR,
            self.core.STATUS_DEPENDENCY_WAIT,
            selected_project_name=selected_project_name,
        )
        names = ["All Projects"] + [p.name for p in display_projects]
        self.ui.log_panel.update_project_list(names)

    def get_projects_for_ui(self):
        return self._build_display_projects()

    def _build_display_projects(self):
        self._refresh_engine_projects()
        snapshots = {item["project_name"]: item for item in self.engine_projects_cache}
        display_projects = []
        for project in self.projects:
            display_projects.append(self._make_display_project(project, snapshots.get(project.name)))
        return display_projects

    def _make_display_project(self, project, snapshot):
        details = snapshot.get("details", {}) if snapshot else {}
        return SimpleNamespace(
            name=project.name,
            enabled=bool(snapshot["enabled"]) if snapshot and "enabled" in snapshot else project.enabled,
            status=snapshot.get("status", project.status) if snapshot else project.status,
            next_run=details.get("next_run") or getattr(project, "next_run", "-"),
            completed_tasks=snapshot.get("completed_tasks", project.completed_tasks) if snapshot else project.completed_tasks,
            total_tasks=snapshot.get("total_tasks", project.total_tasks) if snapshot else project.total_tasks,
        )

    def _refresh_engine_projects(self, force=False):
        if not self._engine_available_for_base():
            return
        now = time.time()
        if not force and (now - self._last_engine_projects_refresh) < self._engine_projects_refresh_interval:
            return
        try:
            self.engine_projects_cache = self.engine_client.list_projects()
            current_names = {item.get("project_name") for item in self.engine_projects_cache}
            for stale_name in list(self.engine_tasks_cache.keys()):
                if stale_name not in current_names:
                    self.engine_tasks_cache.pop(stale_name, None)
                    self._last_engine_tasks_refresh.pop(stale_name, None)
            self._last_engine_projects_refresh = now
        except Exception:
            pass

    def _refresh_engine_tasks(self, project_name, force=False):
        if not self._engine_available_for_base():
            return
        now = time.time()
        last = self._last_engine_tasks_refresh.get(project_name, 0.0)
        if not force and (now - last) < self._engine_tasks_refresh_interval:
            return
        try:
            self.engine_tasks_cache[project_name] = self.engine_client.list_project_tasks(project_name)
            self._last_engine_tasks_refresh[project_name] = now
        except Exception:
            pass

    def _distributed_config_path(self):
        return os.path.join(self.base_dir, "config", "distributed_runtime.json")

    def _refresh_distributed_status(self, force=False):
        now = time.time()
        if not force and (now - self._last_distributed_status_refresh) < self._distributed_status_refresh_interval:
            return self._distributed_status_cache

        status = {}
        if self._engine_available_for_base():
            try:
                status = self.engine_client.distributed_status()
            except Exception:
                status = {}
        else:
            status = self._load_distributed_config_local().get("resolved", {})

        self._distributed_status_cache = status or {}
        self._last_distributed_status_refresh = now
        return self._distributed_status_cache

    def get_distributed_status(self, force=False):
        return self._refresh_distributed_status(force=force)

    def _sync_engine_event_cursor(self, force=False):
        if not self.engine_mode:
            return False
        if not self._engine_available_for_base() and not self._recover_engine_bridge():
            return False
        if not force and self._last_engine_event_id > 0:
            return True
        try:
            self._last_engine_event_id = self.engine_client.latest_event_id()
            return True
        except Exception:
            return False

    def _load_distributed_config_local(self):
        config_path = self._distributed_config_path()
        raw = {}
        if os.path.exists(config_path):
            raw = load_json_file(config_path, default={}) or {}
        return {
            "document": raw,
            "resolved": raw,
            "status": self._distributed_status_cache,
            "config_path": config_path,
            "policy_path": os.path.join(self.base_dir, "config", "project_policies.json"),
        }

    def load_distributed_config(self):
        if self.engine_mode:
            if not self._engine_available_for_base(force=True) and not self._recover_engine_bridge(force_restart=True):
                raise RuntimeError("엔진에 연결할 수 없습니다.")
            return self.engine_client.distributed_config()
        return self._load_distributed_config_local()

    def save_distributed_config(self, document):
        if self.engine_mode:
            if not self._engine_available_for_base(force=True) and not self._recover_engine_bridge(force_restart=True):
                raise RuntimeError("엔진에 연결할 수 없습니다.")
            result = self.engine_client.update_distributed_config(document)
        else:
            config_path = self._distributed_config_path()
            atomic_write_json(config_path, document, indent=2)
            result = self._load_distributed_config_local()
        self._refresh_distributed_status(force=True)
        return result

    def open_distributed_settings(self):
        if self._distributed_settings_dialog and self._distributed_settings_dialog.is_open():
            self._distributed_settings_dialog.focus()
            return
        self._distributed_settings_dialog = DistributedSettingsDialog(self.root, self)
        self._distributed_settings_dialog.show()

    def _get_engine_task_steps(self, project):
        self._refresh_engine_tasks(project.name)
        snapshots = self.engine_tasks_cache.get(project.name)
        if not snapshots:
            return project.get_tasks_by_step()

        tasks = []
        for task in snapshots:
            tasks.append(
                SimpleNamespace(
                    task_id=task.get("task_id"),
                    filepath=task.get("filepath", ""),
                    filename=task.get("filename", ""),
                    step=int(task.get("step", 1)),
                    order=int(task.get("order", 0)),
                    args=task.get("args", ""),
                    timeout=int(task.get("timeout", 0)),
                    max_retries=int(task.get("max_retries", 0)),
                    checked=bool(task.get("checked", False)),
                    status=task.get("status", self.core.TASK_STATUS_WAITING),
                )
            )

        steps = {}
        for task in tasks:
            steps.setdefault(task.step, []).append(task)
        for step_tasks in steps.values():
            step_tasks.sort(key=lambda item: item.order)
        return dict(sorted(steps.items()))

    def _sync_engine_state(self, force=False):
        if not self._engine_available_for_base() and not self._recover_engine_bridge():
            return False
        try:
            self.engine_client.sync()
            if force:
                self._last_engine_projects_refresh = 0.0
                self._last_engine_tasks_refresh = {}
            return True
        except Exception:
            return False

    def _submit_engine_command(self, submit_func, action_label):
        if not self._engine_available_for_base(force=True) and not self._recover_engine_bridge(force_restart=True):
            return False
        try:
            response = submit_func()
            command_id = response.get("command_id") if isinstance(response, dict) else None
            if command_id:
                self._monitor_engine_command(command_id, action_label)
            return True
        except Exception as exc:
            try:
                self.core.log(f"{action_label} 요청 실패: {exc}")
            except Exception:
                pass
            return False

    def _monitor_engine_command(self, command_id, action_label):
        def _worker():
            try:
                command = self.engine_client.wait_command(command_id, timeout=5.0, interval=0.25)
                if command.get("status") != "failed":
                    return
                message = command.get("result_message") or "engine command failed"
                self._handle_engine_command_failure(action_label, message)
            except Exception as exc:
                try:
                    self.core.log(f"{action_label} 상태 확인 실패: {exc}")
                except Exception:
                    pass

        threading.Thread(
            target=_worker,
            daemon=True,
            name=f"engine-command-monitor:{action_label}:{command_id}",
        ).start()

    def _handle_engine_command_failure(self, action_label, message):
        try:
            self.core.log(f"{action_label} 실패: {message}")
        except Exception:
            pass

        def _show():
            self._show_engine_action_failed(action_label)

        try:
            self.root.after(0, _show)
        except Exception:
            pass

    def _submit_engine_run_command(self, project_name, trigger_source="manual", only_checked=False):
        return self._submit_engine_command(
            lambda: self.engine_client.run_project(
                project_name,
                trigger_source=trigger_source,
                only_checked=only_checked,
            ),
            "프로젝트 실행",
        )

    def _submit_engine_stop_command(self, project_name):
        return self._submit_engine_command(
            lambda: self.engine_client.stop_project(project_name),
            "프로젝트 중지",
        )

    def _submit_engine_stop_task_command(self, project_name, task_id):
        return self._submit_engine_command(
            lambda: self.engine_client.stop_task(project_name, task_id),
            "작업 중지",
        )

    def _submit_engine_run_task_command(self, project_name, task_id):
        return self._submit_engine_command(
            lambda: self.engine_client.run_task(project_name, task_id),
            "단일 작업 실행",
        )

    def _ensure_engine_bridge(self):
        if self._engine_available_for_base(force=True):
            self._sync_engine_event_cursor(force=True)
            return True

        engine_script = os.path.join(self.base_dir, "scheduler_engine.py")
        if not os.path.exists(engine_script):
            return False

        if self._engine_process_alive():
            for _ in range(10):
                if self._engine_available_for_base(force=True):
                    return True
                time.sleep(0.2)
            return False

        creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        os.makedirs(os.path.join(self.base_dir, "logs"), exist_ok=True)
        stdout_path = os.path.join(self.base_dir, "logs", "scheduler_engine.stdout.log")
        stderr_path = os.path.join(self.base_dir, "logs", "scheduler_engine.stderr.log")
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        env["PYTHONIOENCODING"] = "utf-8"
        stdout_handle = None
        stderr_handle = None
        try:
            stdout_handle = open(stdout_path, "a", encoding="utf-8", buffering=1)
            stderr_handle = open(stderr_path, "a", encoding="utf-8", buffering=1)
            self.engine_process = subprocess.Popen(
                [sys.executable, engine_script, "--base-dir", self.base_dir],
                cwd=self.base_dir,
                stdout=stdout_handle,
                stderr=stderr_handle,
                env=env,
                creationflags=creationflags,
            )
            self.engine_process_owned = True
        except Exception:
            self.engine_process = None
            self.engine_process_owned = False
            return False
        finally:
            for handle in (stdout_handle, stderr_handle):
                try:
                    if handle:
                        handle.close()
                except Exception:
                    pass

        for _ in range(30):
            if self._engine_available_for_base(force=True):
                return True
            time.sleep(0.2)
        self._stop_owned_engine_process()
        return False

    def _poll_engine_events(self):
        if not self._engine_available_for_base():
            return
        try:
            events = self.engine_client.list_events(after_event_id=self._last_engine_event_id, limit=200)
        except Exception:
            return

        for event in events:
            event_id = int(event.get("event_id", 0) or 0)
            if event_id > self._last_engine_event_id:
                self._last_engine_event_id = event_id
            self._apply_engine_event(event)

    def _apply_engine_event(self, event):
        event_type = event.get("event_type")
        message = event.get("message", "")
        payload = event.get("payload") or {}

        if event_type == "LOG_SUMMARY":
            self.ui.log_panel.append_summary(message)
            return

        if event_type == "LOG_DETAIL":
            self.ui.log_panel.append_detail(
                text=message,
                proj_name=event.get("project_name") or "",
                task_name=payload.get("task_name", ""),
                log_type=payload.get("log_type", "stdout"),
            )
            return

        if event_type == "CLEAR_LOGS":
            self.ui.log_panel.clear_summary()
            self.ui.log_panel.clear_detail()

    def poll_engine_bridge(self):
        if not self.engine_mode:
            return
        if not self._engine_available_for_base():
            self.ui.update_bot_status(False)
            self._recover_engine_bridge()
            return
        self._poll_engine_events()
        selected_project_name = self._get_selected_project_name()
        selected_task_id = self._get_selected_task_id()
        previous = list(self.engine_projects_cache)
        self._refresh_engine_projects()
        if previous != self.engine_projects_cache:
            self.refresh_project_list(selected_project_name=selected_project_name)
        idx = self.ui.project_panel.get_selected()
        if idx is not None and 0 <= idx < len(self.projects):
            project_name = self.projects[idx].name
            previous_tasks = list(self.engine_tasks_cache.get(project_name, []))
            self._refresh_engine_tasks(project_name)
            if previous_tasks != self.engine_tasks_cache.get(project_name, []):
                self.refresh_task_list(selected_task_id=selected_task_id, selected_project_name=project_name)
        health = self._get_engine_health(force=False) or {}
        self.ui.update_bot_status(
            bool(self._engine_health_matches_base(health) and health.get("telegram_bot_running"))
        )
        self._refresh_distributed_status()

    # Utils Delegates
    def show_notification(self, title, msg):
        def _notify():
            from plyer import notification
            try:
                notification.notify(title=title, message=msg, app_name='HS Scheduler', timeout=5)
            except Exception:
                pass

        threading.Thread(target=_notify, daemon=True).start()

    def send_telegram_alert(self, msg):
        def _send():
            # 내장 봇이 있으면 봇으로 전송, 없으면 기존 유틸 사용
            if hasattr(self, 'telegram_bot') and self.telegram_bot.is_configured:
                self.telegram_bot.send_alert(msg)
            else:
                send_telegram_alert(msg, self.credentials.load(), log_func=self.core.log)

        threading.Thread(target=_send, daemon=True).start()

    # H-4: 우클릭 컨텍스트 메뉴
    def show_context_menu(self, event):
        item = self.ui.task_panel.tree.identify_row(event.y)
        if not item: return
        self.ui.task_panel.tree.selection_set(item)
        
        vals = self.ui.task_panel.tree.item(item, 'values')
        if not vals or len(vals) < 5: return
        
        idx = self.ui.project_panel.get_selected()
        if idx is None: return
        proj = self.projects[idx]
        target_task_id = vals[4]
        task_obj = next((t for t in proj.tasks if t.task_id == target_task_id), None)
        if not task_obj: return
        
        menu = tk.Menu(self.root, tearoff=0)
        menu.add_command(label="Open Folder", command=lambda: os.startfile(os.path.dirname(task_obj.filepath)))
        menu.add_command(label="Edit Script", command=lambda: os.startfile(task_obj.filepath))
        menu.add_separator()
        menu.add_command(label="Run This Task Only", command=lambda: self._run_single_task_only(proj, task_obj))
        menu.add_command(label="Edit Settings", command=lambda: self._edit_task_dialog(proj, task_obj))
        menu.add_separator()
        menu.add_command(label="Delete", command=self.delete_task_from_project)
        
        try:
            menu.tk_popup(event.x_root, event.y_root)
        finally:
            menu.grab_release()

    def _run_single_task_only(self, proj, task_obj):
        """H-4: 단일 작업 단독 실행"""
        if self.engine_mode:
            submitted = self._submit_engine_run_task_command(proj.name, task_obj.task_id)
            if not submitted:
                messagebox.showwarning("Engine Mode", "Failed to send single-task run command.")
            return
        import threading
        proj_dict = {p.name: p for p in self.projects}
        threading.Thread(
            target=self.core._run_single_script, 
            args=(task_obj, proj.name, proj_dict), 
            daemon=True
        ).start()
        self.core.log(f"▶ [{proj.name}] {task_obj.filename} 단독 실행 시작")

    # H-6: 작업 더블클릭 설정 변경
    def on_double_click_task(self, event):
        idx = self.ui.project_panel.get_selected()
        if idx is None: return
        
        item = self.ui.task_panel.tree.identify_row(event.y)
        if not item: return
        
        vals = self.ui.task_panel.tree.item(item, 'values')
        if not vals or len(vals) < 5: return
        if self.ui.task_panel.tree.item(item, "text").startswith("Step"): return
        
        proj = self.projects[idx]
        target_task_id = vals[4]
        task_obj = next((t for t in proj.tasks if t.task_id == target_task_id), None)
        if task_obj:
            self._edit_task_dialog(proj, task_obj)

    def _edit_task_dialog(self, proj, task_obj):
        """H-6: 작업 설정 변경 다이얼로그"""
        dialog = tk.Toplevel(self.root)
        dialog.title(f"Task Settings: {task_obj.filename}")
        dialog.geometry("420x350")
        dialog.transient(self.root)
        dialog.grab_set()
        
        ttk.Label(dialog, text=f"Script: {task_obj.filename}", font=("Segoe UI", 11, "bold")).pack(anchor="w", padx=15, pady=(15,5))
        
        ttk.Label(dialog, text="Arguments:").pack(anchor="w", padx=15, pady=(5,2))
        args_var = tk.StringVar(value=task_obj.args)
        ttk.Entry(dialog, textvariable=args_var, width=40).pack(padx=15, fill="x")
        
        ttk.Label(dialog, text="Timeout (seconds, 0 = unlimited):").pack(anchor="w", padx=15, pady=(10,2))
        timeout_var = tk.StringVar(value=str(task_obj.timeout))
        ttk.Entry(dialog, textvariable=timeout_var, width=40).pack(padx=15, fill="x")
        
        ttk.Label(dialog, text="Retry count:").pack(anchor="w", padx=15, pady=(10,2))
        retry_var = tk.StringVar(value=str(task_obj.max_retries))
        ttk.Entry(dialog, textvariable=retry_var, width=40).pack(padx=15, fill="x")
        
        ttk.Label(dialog, text="Step number:").pack(anchor="w", padx=15, pady=(10,2))
        step_var = tk.StringVar(value=str(task_obj.step))
        ttk.Entry(dialog, textvariable=step_var, width=40).pack(padx=15, fill="x")
        
        def save_task():
            task_obj.args = args_var.get().strip()
            try: task_obj.timeout = int(timeout_var.get())
            except: task_obj.timeout = 0
            try: task_obj.max_retries = int(retry_var.get())
            except: task_obj.max_retries = 0
            try: task_obj.step = int(step_var.get())
            except: pass
            self.save_data()
            self.refresh_task_list()
            self.core.log(f"✅ [{proj.name}] {task_obj.filename} 설정 저장")
            dialog.destroy()
        
        btn_frame = ttk.Frame(dialog)
        btn_frame.pack(fill="x", padx=15, pady=20)
        ttk.Button(btn_frame, text="Save", command=save_task, bootstyle="success").pack(side="right", padx=5)
        ttk.Button(btn_frame, text="Cancel", command=dialog.destroy, bootstyle="secondary").pack(side="right")

    def on_space_task(self, event):
        self.toggle_task_check()
        return "break"

    def on_task_click(self, event):
        region = self.ui.task_panel.tree.identify("region", event.x, event.y)
        item = self.ui.task_panel.tree.identify_row(event.y)
        column = self.ui.task_panel.tree.identify_column(event.x)
        if item:
            self.ui.task_panel.tree.selection_set(item)
            self.ui.task_panel.tree.focus(item)
        if item and region in ("tree", "cell") and column in ("#0", "#1"):
            # Step 그룹 노드가 아니면 체크 토글
            if not self.ui.task_panel.tree.item(item, "text").startswith("Step"):
                self.root.after_idle(lambda target=item: self.toggle_task_check(specific_item=target))
                return "break"
        # 드래그 시작 아이템 기록
        self._drag_start_item = item if item else None
    
    # H-5: 드래그 앤 드롭 (작업 순서 재배치)
    def on_drag_motion(self, event):
        item = self.ui.task_panel.tree.identify_row(event.y)
        if item:
            # Step 그룹 노드는 드래그 대상에서 제외
            if self.ui.task_panel.tree.item(item, "text").startswith("Step"):
                return
            self.ui.task_panel.tree.selection_set(item)
    
    def on_drag_release(self, event):
        """H-5: 드래그 완료 시 작업 순서 교환"""
        idx = self.ui.project_panel.get_selected()
        if idx is None: return
        
        # 드래그 시작 아이템 확인
        src_item = getattr(self, '_drag_start_item', None)
        if not src_item: return
        
        target_item = self.ui.task_panel.tree.identify_row(event.y)
        if not target_item or target_item == src_item: return
        
        # 아이템이 트리에 존재하는지 확인 (갱신 등으로 삭제되었을 수 있음)
        if not self.ui.task_panel.tree.exists(src_item): 
            self._drag_start_item = None
            return
        if not self.ui.task_panel.tree.exists(target_item): return
        
        # Step 그룹 노드는 이동 대상에서 제외
        if self.ui.task_panel.tree.item(src_item, "text").startswith("Step"): return
        if self.ui.task_panel.tree.item(target_item, "text").startswith("Step"): return
        
        src_vals = self.ui.task_panel.tree.item(src_item, 'values')
        dst_vals = self.ui.task_panel.tree.item(target_item, 'values')
        
        if not src_vals or len(src_vals) < 5 or not dst_vals or len(dst_vals) < 5:
            return
        
        proj = self.projects[idx]
        src_task = next((t for t in proj.tasks if t.task_id == src_vals[4]), None)
        dst_task = next((t for t in proj.tasks if t.task_id == dst_vals[4]), None)
        
        if src_task and dst_task:
            src_task.order, dst_task.order = dst_task.order, src_task.order
            proj.tasks.sort(key=lambda x: (x.step, x.order))
            self.save_data()
            self.refresh_task_list()
        
        self._drag_start_item = None

    def clean_quit(self):
        """창 닫기 시 바로 종료 (트레이 아이콘 없음)"""
        try:
            if hasattr(self, 'telegram_bot'):
                self.telegram_bot.stop()
            if self.engine_mode and self._engine_available_for_base(force=True):
                try:
                    self.engine_client.shutdown()
                except Exception:
                    pass
            self.core.running = False
            try:
                self.core.shutdown()
            except Exception:
                pass
            self._stop_owned_engine_process()
        except:
            pass
        self.root.destroy()
        sys.exit()

if __name__ == "__main__":
    import ttkbootstrap as ttk
    root = ttk.Window(title="HS Scheduler", themename="superhero")
    root.geometry("1400x950")
    app = HSSchedulerApp(root)
    root.mainloop()


