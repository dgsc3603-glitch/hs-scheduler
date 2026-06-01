import datetime
import tkinter as tk

import ttkbootstrap as ttk

from component.ui.log_panel import LogPanel
from component.ui.project_panel import ProjectPanel
from component.ui.task_panel import TaskPanel
from component.ui.theme import COLORS, FONT, MONO_FONT, action_button, configure_root, status_pill


STATUS_LABELS = {
    "시스템 대기중": "System idle",
    "대기중": "Waiting",
    "실행중": "Running",
    "완료": "Done",
    "오류": "Error",
    "에러": "Error",
}


def display_status(text):
    value = str(text or "")
    for source, target in STATUS_LABELS.items():
        value = value.replace(source, target)
    return value


class MainWindow:
    def __init__(self, root, scheduler_app):
        self.root = root
        self.app = scheduler_app
        self.root.title("HS Scheduler")
        self.root.geometry("1500x860")
        self.root.minsize(1180, 720)
        configure_root(self.root)

        self.style = ttk.Style(theme="litera")
        self._configure_styles()

        self._setup_header()
        self._setup_main_area()
        self._setup_footer()

        self.root.after(100, self.process_events)
        self._update_clock()

    def _configure_styles(self):
        s = self.style
        s.configure(".", font=(FONT, 10), background=COLORS["bg_main"], foreground=COLORS["text"])
        s.configure(
            "TButton",
            font=(FONT, 8, "bold"),
            borderwidth=1,
            focusthickness=0,
            focuscolor=COLORS["surface"],
        )
        s.configure(
            "Custom.Treeview",
            rowheight=27,
            font=(FONT, 9),
            background=COLORS["surface"],
            foreground=COLORS["text"],
            fieldbackground=COLORS["surface"],
            borderwidth=0,
        )
        s.configure(
            "Custom.Treeview.Heading",
            font=(FONT, 8, "bold"),
            background=COLORS["surface_subtle"],
            foreground=COLORS["text_muted"],
            borderwidth=0,
            relief="flat",
        )
        s.map(
            "Custom.Treeview",
            background=[("selected", COLORS["accent_soft"])],
            foreground=[("selected", COLORS["text"])],
        )
        s.configure(
            "Custom.Horizontal.TProgressbar",
            troughcolor=COLORS["border"],
            background=COLORS["accent"],
            thickness=5,
        )
        s.configure("Custom.TNotebook", background=COLORS["surface"], borderwidth=0)
        s.configure(
            "Custom.TNotebook.Tab",
            font=(FONT, 9, "bold"),
            padding=(12, 6),
            background=COLORS["surface"],
            foreground=COLORS["text_muted"],
        )
        s.map(
            "Custom.TNotebook.Tab",
            background=[("selected", COLORS["surface_tint"])],
            foreground=[("selected", COLORS["accent_dark"])],
        )

    def _setup_header(self):
        header = tk.Frame(self.root, bg=COLORS["bg_header"], highlightbackground=COLORS["border"], highlightthickness=1)
        header.pack(fill="x")

        nav = tk.Frame(header, bg=COLORS["bg_header"], height=52)
        nav.pack(fill="x", padx=18, pady=(10, 6))
        nav.pack_propagate(False)

        brand = tk.Frame(nav, bg=COLORS["bg_header"])
        brand.pack(side="left", fill="y")
        tk.Label(brand, text="⚡", bg=COLORS["bg_header"], fg=COLORS["accent"], font=("Segoe UI", 17)).pack(side="left")
        brand_text = tk.Frame(brand, bg=COLORS["bg_header"])
        brand_text.pack(side="left", padx=(8, 0))
        tk.Label(brand_text, text="HS Scheduler", bg=COLORS["bg_header"], fg=COLORS["text"], font=(FONT, 14, "bold")).pack(anchor="w")
        tk.Label(brand_text, text="Automation Console", bg=COLORS["bg_header"], fg=COLORS["text_muted"], font=(FONT, 8)).pack(anchor="w")

        center = tk.Frame(nav, bg=COLORS["bg_header"])
        center.pack(side="left", padx=24)
        self.dash_cards = {}
        for key, label, color in (
            ("running", "Running", COLORS["success"]),
            ("waiting", "Waiting", COLORS["warning"]),
            ("completed", "Done", COLORS["accent"]),
            ("error", "Errors", COLORS["error"]),
        ):
            self.dash_cards[key] = status_pill(center, label, "0", color)

        right = tk.Frame(nav, bg=COLORS["bg_header"])
        right.pack(side="right", fill="y")
        self.clock_label = tk.Label(right, text="00:00:00", font=(MONO_FONT, 13, "bold"), bg=COLORS["bg_header"], fg=COLORS["text"])
        self.clock_label.pack(anchor="e")
        self.status_label = tk.Label(right, text="System idle", font=(FONT, 8, "bold"), bg=COLORS["bg_header"], fg=COLORS["success"])
        self.status_label.pack(anchor="e", pady=(2, 0))

        toolbar = tk.Frame(header, bg=COLORS["toolbar"], highlightbackground=COLORS["border"], highlightthickness=1)
        toolbar.pack(fill="x", padx=18, pady=(0, 9))
        left_tools = tk.Frame(toolbar, bg=COLORS["toolbar"])
        left_tools.pack(side="left", padx=10, pady=7)
        action_button(left_tools, "Add Project", self.app.add_project, "primary", width=12)
        action_button(left_tools, "Add File", self.app.add_task_to_project, "secondary", width=10)
        action_button(left_tools, "Run Selected", self.app.run_checked_tasks, "success", width=12)
        action_button(left_tools, "Run Project", self.app.run_project_now, "info", width=12)
        action_button(left_tools, "Stop", self.app.stop_project, "danger", width=8)
        right_tools = tk.Frame(toolbar, bg=COLORS["toolbar"])
        right_tools.pack(side="right", padx=10, pady=7)
        action_button(right_tools, "Save", self.app.save_data, "primary", width=8)
        action_button(right_tools, "Main/Sub Settings", self.app.open_distributed_settings, "secondary", width=16)

        progress_wrap = tk.Frame(header, bg=COLORS["bg_header"])
        progress_wrap.pack(fill="x", padx=14, pady=(0, 7))
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(
            progress_wrap,
            variable=self.progress_var,
            maximum=100,
            style="Custom.Horizontal.TProgressbar",
        )
        self.progress_bar.pack(fill="x")

    def _setup_main_area(self):
        main_frame = tk.Frame(self.root, bg=COLORS["bg_main"])
        main_frame.pack(fill="both", expand=True, padx=10, pady=8)

        self.paned_main = ttk.Panedwindow(main_frame, orient="horizontal")
        self.paned_main.pack(fill="both", expand=True)

        self.project_panel = ProjectPanel(self.paned_main, self.app)
        self.paned_main.add(self.project_panel.frame, weight=1)

        self.task_panel = TaskPanel(self.paned_main, self.app)
        self.paned_main.add(self.task_panel.frame, weight=2)

        self.log_panel = LogPanel(self.paned_main, self.app)
        self.paned_main.add(self.log_panel.frame, weight=1)
        self.root.after_idle(self._set_initial_panes)

    def _set_initial_panes(self):
        try:
            width = self.root.winfo_width()
            self.paned_main.sashpos(0, max(280, int(width * 0.25)))
            self.paned_main.sashpos(1, max(760, int(width * 0.66)))
        except Exception:
            pass

    def _setup_footer(self):
        footer = tk.Frame(self.root, bg=COLORS["bg_header"], highlightbackground=COLORS["border"], highlightthickness=1, height=34)
        footer.pack(fill="x", side="bottom")
        footer.pack_propagate(False)

        self.bot_status_label = tk.Label(footer, text="Telegram: checking", font=(FONT, 9), bg=COLORS["bg_header"], fg=COLORS["text_muted"])
        self.bot_status_label.pack(side="left", padx=14)

        self.distributed_status_label = tk.Label(footer, text="Distributed: checking", font=(FONT, 9), bg=COLORS["bg_header"], fg=COLORS["text_muted"])
        self.distributed_status_label.pack(side="left", padx=(0, 14))

        self.mem_label = tk.Label(footer, text="", font=(MONO_FONT, 9), bg=COLORS["bg_header"], fg=COLORS["text_muted"])
        self.mem_label.pack(side="left", expand=True)
        self._update_memory_display()

        tk.Label(footer, text="HS Scheduler", font=(FONT, 9), bg=COLORS["bg_header"], fg=COLORS["text_muted"]).pack(side="right", padx=14)

    def _update_clock(self):
        now = datetime.datetime.now()
        self.clock_label.config(text=now.strftime("%H:%M:%S"))
        self.root.after(1000, self._update_clock)

    def _update_memory_display(self):
        try:
            import psutil
            mem_mb = psutil.Process().memory_info().rss / 1024 / 1024
            self.mem_label.config(text=f"RAM {mem_mb:.0f} MB")
        except ImportError:
            self.mem_label.config(text="RAM psutil missing")
        except Exception:
            pass
        self.root.after(5000, self._update_memory_display)

    def update_dashboard_counts(self):
        try:
            projects = self.app.get_projects_for_ui()
            running = sum(1 for p in projects if str(p.status).startswith("실행중"))
            waiting = sum(1 for p in projects if p.status == "대기중" and p.enabled)
            completed = sum(1 for p in projects if p.status in ["완료", "실행 완료"])
            error = sum(1 for p in projects if "오류" in str(p.status) or "에러" in str(p.status))
            self.dash_cards["running"].config(text=str(running))
            self.dash_cards["waiting"].config(text=str(waiting))
            self.dash_cards["completed"].config(text=str(completed))
            self.dash_cards["error"].config(text=str(error))
        except Exception:
            pass

    def process_events(self):
        try:
            for _ in range(50):
                if self.app.event_queue.empty():
                    break
                event = self.app.event_queue.get_nowait()
                self._handle_event(event)
            self.app.poll_engine_bridge()
            self.update_dashboard_counts()
            self.update_distributed_status()
        except Exception as e:
            print(f"Event processing error: {e}")
        finally:
            self.root.after(100, self.process_events)

    def _handle_event(self, event):
        from component.core.scheduler_core import SchedulerEvent
        if event.type == SchedulerEvent.LOG_SUMMARY:
            self.log_panel.append_summary(event.data)
        elif event.type == SchedulerEvent.LOG_DETAIL:
            data = event.data
            self.log_panel.append_detail(
                text=data["message"],
                proj_name=data["proj_name"],
                task_name=data["task_name"],
                log_type=data["log_type"],
            )
        elif event.type == SchedulerEvent.STATUS_UPDATE:
            self.update_status(event.data)
        elif event.type == SchedulerEvent.PROGRESS_UPDATE:
            self.set_progress(event.data)
        elif event.type == SchedulerEvent.TASK_REFRESH:
            self.app.refresh_task_list()
        elif event.type == SchedulerEvent.PROJECT_REFRESH:
            self.app.refresh_project_list()
            self.update_dashboard_counts()
        elif event.type == SchedulerEvent.NOTIFICATION:
            self.app.show_notification("Notification", event.data)
        elif event.type == SchedulerEvent.TELEGRAM:
            self.app.send_telegram_alert(event.data)
        elif event.type == SchedulerEvent.SAVE_DATA:
            self.app.save_data()
        elif event.type == SchedulerEvent.CLEAR_LOGS:
            self.log_panel.clear_summary()
            self.log_panel.clear_detail()

    def update_status(self, text, bootstyle=None):
        color = COLORS["success"] if "대기" in text else COLORS["accent"]
        if "오류" in text or "에러" in text:
            color = COLORS["error"]
        self.status_label.config(text=display_status(text), fg=color)

    def set_progress(self, value):
        self.progress_var.set(value)

    def update_bot_status(self, connected=False):
        if connected:
            self.bot_status_label.config(text="Telegram: connected", fg=COLORS["success"])
        else:
            self.bot_status_label.config(text="Telegram: disconnected", fg=COLORS["error"])

    def update_distributed_status(self):
        status = self.app.get_distributed_status()
        if not status:
            self.distributed_status_label.config(text="Distributed: not configured", fg=COLORS["text_muted"])
            return
        if not status.get("enabled"):
            self.distributed_status_label.config(text="Distributed: off", fg=COLORS["text_muted"])
            return
        if not status.get("control_plane_enabled"):
            self.distributed_status_label.config(text="Distributed: D1 disconnected", fg=COLORS["error"])
            return
        if status.get("is_primary"):
            text = f"Distributed: Main owns lease ({status.get('node_id', '-')})"
            color = COLORS["success"]
        else:
            text = f"Distributed: Sub standby / owner={status.get('lease_owner', '-')}"
            color = COLORS["warning"]
        self.distributed_status_label.config(text=text, fg=color)
