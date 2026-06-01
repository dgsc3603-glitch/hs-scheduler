import re
import tkinter as tk

from tkinter import ttk

from component.ui.theme import COLORS, action_button, panel, panel_header, section


STATUS_LABELS = {
    "대기중": "Waiting",
    "실행중": "Running",
    "실행중..": "Running",
    "완료": "Done",
    "실행 완료": "Done",
    "오류": "Error",
    "오류 발생": "Error",
    "대기(종속)": "Dependency Wait",
    "사용자중지": "Stopped",
    "일시중지": "Paused",
}


def display_status(status):
    value = str(status or "")
    for source, target in STATUS_LABELS.items():
        value = value.replace(source, target)
    return value


class ProjectPanel:
    def __init__(self, parent, scheduler_app):
        self.parent = parent
        self.app = scheduler_app

        self.frame = panel(parent)
        header = panel_header(self.frame, "Projects")
        action_button(header, "Add", self.app.add_project, "primary", side="right")
        action_button(header, "Delete", self.app.delete_project, "danger", side="right")

        table_wrap = tk.Frame(self.frame, bg=COLORS["surface"])
        table_wrap.pack(fill="both", expand=True, padx=10, pady=(0, 8))

        self.tree = ttk.Treeview(
            table_wrap,
            columns=("next_run", "status"),
            show="tree headings",
            selectmode="browse",
            style="Custom.Treeview",
        )
        self.tree.heading("#0", text="Project")
        self.tree.heading("next_run", text="Next Run")
        self.tree.heading("status", text="Status")
        self.tree.column("#0", width=145, minwidth=110)
        self.tree.column("next_run", width=92, anchor="center")
        self.tree.column("status", width=118, anchor="center")

        scroll_y = ttk.Scrollbar(table_wrap, orient="vertical", command=self.tree.yview)
        scroll_y.pack(side="right", fill="y")
        self.tree.configure(yscrollcommand=scroll_y.set)
        self.tree.pack(fill="both", expand=True)

        self.tree.bind("<<TreeviewSelect>>", self.app.on_project_select)
        self.tree.bind("<Double-1>", self.app.edit_project)

        actions = section(self.frame)
        action_button(actions, "Run", self.app.run_project_now, "primary", expand=True)
        action_button(actions, "Pause/Resume", self.app.toggle_project_enabled, "secondary", expand=True)
        action_button(actions, "Stop", self.app.stop_project, "danger", expand=True)

    def refresh(self, projects, STATUS_WAITING, STATUS_RUNNING, STATUS_COMPLETED, STATUS_ERROR, STATUS_DEPENDENCY_WAIT, selected_project_name=None):
        self.tree.delete(*self.tree.get_children())
        selected_item = None
        for idx, proj in enumerate(projects):
            status_icon = "○"
            tag = "even" if idx % 2 == 0 else "odd"

            if proj.status == STATUS_WAITING:
                status_icon = "○"
            elif proj.status == STATUS_RUNNING:
                status_icon = "●"
                tag = "running"
            elif proj.status == STATUS_COMPLETED:
                status_icon = "●"
                tag = "completed"
            elif proj.status == STATUS_ERROR or proj.status == "오류":
                status_icon = "●"
                tag = "error"
            elif proj.status == STATUS_DEPENDENCY_WAIT:
                status_icon = "…"
            elif "사용자중지" in proj.status:
                status_icon = "■"

            if not proj.enabled and proj.status == STATUS_WAITING:
                status_display = "Paused"
                tag = "disabled"
            else:
                progress_text = ""
                if proj.total_tasks > 0:
                    percent = int((proj.completed_tasks / proj.total_tasks) * 100)
                    progress_text = f" {percent}%"
                status_display = f"{status_icon} {display_status(proj.status)}{progress_text}"

            next_display = proj.next_run
            match = re.match(r"\d{4}-(\d{2}-\d{2}\s+\d{2}:\d{2})", str(next_display))
            if match:
                next_display = match.group(1)

            item_id = self.tree.insert("", "end", text=proj.name, values=(next_display, status_display), tags=(tag,))
            if selected_project_name and proj.name == selected_project_name:
                selected_item = item_id

        self.tree.tag_configure("even", background=COLORS["row_even"], foreground=COLORS["text"])
        self.tree.tag_configure("odd", background=COLORS["row_odd"], foreground=COLORS["text"])
        self.tree.tag_configure("running", background=COLORS["success_soft"], foreground=COLORS["text"])
        self.tree.tag_configure("completed", background=COLORS["row_odd"], foreground=COLORS["text"])
        self.tree.tag_configure("error", background=COLORS["error_soft"], foreground=COLORS["error"])
        self.tree.tag_configure("disabled", background=COLORS["surface_subtle"], foreground=COLORS["text_muted"])
        if selected_item:
            self.tree.selection_set(selected_item)
            self.tree.focus(selected_item)
            self.tree.see(selected_item)

    def get_selected(self):
        selection = self.tree.selection()
        if not selection:
            return None
        return self.tree.index(selection[0])

    def get_selected_name(self):
        selection = self.tree.selection()
        if not selection:
            return None
        return self.tree.item(selection[0], "text")
