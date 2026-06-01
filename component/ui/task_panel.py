import tkinter as tk
from tkinter import ttk

from component.ui.theme import COLORS, action_button, panel, panel_header, section


STATUS_LABELS = {
    "대기중": "Waiting",
    "실행중": "Running",
    "완료": "Done",
    "오류": "Error",
    "시간초과": "Timeout",
    "사용자중지": "Stopped",
    "최종실패": "Final Failed",
    "건너뜀": "Skipped",
}


def display_status(status):
    value = str(status or "")
    for source, target in STATUS_LABELS.items():
        value = value.replace(source, target)
    return value


class TaskPanel:
    def __init__(self, parent, scheduler_app):
        self.parent = parent
        self.app = scheduler_app

        self.frame = panel(parent)
        header = panel_header(self.frame, "Task Configuration")
        action_button(header, "Add File", self.app.add_task_to_project, "primary", side="right")
        action_button(header, "Delete", self.app.delete_task_from_project, "danger", side="right")
        action_button(header, "Save", self.app.save_data, "success", side="right")

        quick = section(self.frame, bg=COLORS["toolbar"])
        quick.configure(highlightbackground=COLORS["border"], highlightthickness=1)
        action_button(quick, "Run Selected", self.app.run_checked_tasks, "success")
        action_button(quick, "Invert Check", self.app.toggle_task_check, "secondary")
        action_button(quick, "Stop Task", self.app.stop_selected_task, "warning")

        table_wrap = tk.Frame(self.frame, bg=COLORS["surface"])
        table_wrap.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        self.tree = ttk.Treeview(
            table_wrap,
            columns=("filename", "args", "timeout", "status", "task_id"),
            show="tree headings",
            selectmode="browse",
            style="Custom.Treeview",
        )
        self.tree.heading("#0", text="Step")
        self.tree.heading("filename", text="Script")
        self.tree.heading("args", text="Args")
        self.tree.heading("timeout", text="Timeout")
        self.tree.heading("status", text="Status")
        self.tree.heading("task_id", text="ID")

        self.tree.column("#0", width=58, anchor="center")
        self.tree.column("filename", width=260, minwidth=180)
        self.tree.column("args", width=130)
        self.tree.column("timeout", width=72, anchor="center")
        self.tree.column("status", width=112, anchor="center")
        self.tree.column("task_id", width=0, stretch=False)
        self.tree["displaycolumns"] = ("filename", "args", "timeout", "status")

        scroll_y = ttk.Scrollbar(table_wrap, orient="vertical", command=self.tree.yview)
        scroll_y.pack(side="right", fill="y")
        scroll_x = ttk.Scrollbar(table_wrap, orient="horizontal", command=self.tree.xview)
        scroll_x.pack(side="bottom", fill="x")
        self.tree.configure(xscrollcommand=scroll_x.set, yscrollcommand=scroll_y.set)
        self.tree.pack(fill="both", expand=True)

        self.tree.tag_configure("success", foreground=COLORS["success"], background=COLORS["success_soft"])
        self.tree.tag_configure("error", foreground=COLORS["error"], background=COLORS["error_soft"])
        self.tree.tag_configure("running", foreground=COLORS["accent"], background=COLORS["accent_soft"])
        self.tree.tag_configure("wait_even", foreground=COLORS["text"], background=COLORS["row_even"])
        self.tree.tag_configure("wait_odd", foreground=COLORS["text"], background=COLORS["row_odd"])
        self.tree.tag_configure("stopped", foreground=COLORS["text_muted"], background=COLORS["surface_subtle"])

        self.tree.bind("<Button-3>", self.app.show_context_menu)
        self.tree.bind("<Double-1>", self.app.on_double_click_task)
        self.tree.bind("<space>", self.app.on_space_task)
        self.tree.bind("<Button-1>", self.app.on_task_click)
        self.tree.bind("<B1-Motion>", self.app.on_drag_motion)
        self.tree.bind("<ButtonRelease-1>", self.app.on_drag_release)

        try:
            self.tree.drop_target_register("DND_Files")
            self.tree.dnd_bind("<<Drop>>", self.app.on_drop_files)
        except Exception:
            pass

    def refresh(self, steps, status_constants, selected_task_id=None):
        self.tree.delete(*self.tree.get_children())
        task_idx = 0
        selected_item = None
        for step_num, tasks in steps.items():
            step_node = self.tree.insert("", "end", text=f"Step {step_num}", values=("Group", "", "", ""), open=True)
            for task in tasks:
                tag = "wait_even" if task_idx % 2 == 0 else "wait_odd"
                icon = "○"
                if task.status == status_constants["TASK_STATUS_COMPLETED"]:
                    tag = "success"
                    icon = "Done"
                elif task.status == status_constants["TASK_STATUS_ERROR"]:
                    tag = "error"
                    icon = "Error"
                elif task.status.startswith(status_constants["TASK_STATUS_RUNNING"]):
                    tag = "running"
                    icon = "Run"
                elif task.status == status_constants["TASK_STATUS_TIMEOUT"]:
                    tag = "error"
                    icon = "Timeout"
                elif task.status == status_constants["TASK_STATUS_STOPPED"]:
                    tag = "stopped"
                    icon = "Stopped"
                elif task.status == status_constants["TASK_STATUS_FINAL_FAIL"]:
                    tag = "error"
                    icon = "Failed"
                elif "건너뜀" in task.status:
                    tag = "stopped"
                    icon = "Skipped"

                check_mark = "☑" if task.checked else "☐"
                display_filename = f"{check_mark} {task.filename}"
                item_id = self.tree.insert(
                    step_node,
                    "end",
                    text="",
                    values=(display_filename, task.args, f"{task.timeout}s" if task.timeout else "-", f"{icon} {display_status(task.status)}", task.task_id),
                    tags=(tag,),
                )
                if selected_task_id and task.task_id == selected_task_id:
                    selected_item = item_id
                task_idx += 1
        if selected_item:
            self.tree.selection_set(selected_item)
            self.tree.focus(selected_item)
            self.tree.see(selected_item)

    def get_selected_task_id(self):
        selection = self.tree.selection()
        if not selection:
            return None
        values = self.tree.item(selection[0], "values")
        if not values or len(values) < 5:
            return None
        return values[4]
