import datetime
import os
import re
import tkinter as tk
from tkinter import filedialog, messagebox

import ttkbootstrap as ttk

from component.ui.theme import COLORS, FONT, MONO_FONT, action_button, panel, panel_header


ALL_PROJECTS = "All Projects"


class LogPanel:
    def __init__(self, parent, scheduler_app):
        self.parent = parent
        self.app = scheduler_app

        self.frame = panel(parent)
        panel_header(self.frame, "Execution Logs", "Filter live output or load the latest saved project log.")
        self.notebook = ttk.Notebook(self.frame, style="Custom.TNotebook")
        self.notebook.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        self._current_log_file = None
        self._loaded_detail_project = None
        self._summary_lines = []
        self._detail_lines = []

        self._setup_summary_tab()
        self._setup_detail_tab()

    def _setup_summary_tab(self):
        self.summary_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.summary_tab, text="Live Summary")

        ctrl_frame = self._filter_bar(self.summary_tab)
        self.log_proj_var = tk.StringVar(value=ALL_PROJECTS)
        self.combo_log_proj = self._project_combo(ctrl_frame, self.log_proj_var, self._render_summary)
        self.log_filter_var = tk.StringVar()
        self.log_filter_var.trace_add("write", lambda *_: self._render_summary())
        self._keyword_entry(ctrl_frame, self.log_filter_var)
        self.log_regex_var = tk.BooleanVar(value=False)
        self._regex_toggle(ctrl_frame, self.log_regex_var, self._render_summary)
        action_button(ctrl_frame, "Clear", self.clear_summary, "secondary", side="right", width=7)
        action_button(ctrl_frame, "Export", self.export_summary, "secondary", side="right", width=9)

        self.summary_status = self._status_line(self.summary_tab)
        self.summary_hint = self._hint_line(self.summary_tab, "Project and keyword filters apply to the live summary shown below.")
        self.log_text = self._log_text(self.summary_tab, height=15)

    def _setup_detail_tab(self):
        self.detail_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.detail_tab, text="Project Details")

        ctrl_frame = self._filter_bar(self.detail_tab)
        self.detail_proj_var = tk.StringVar(value=ALL_PROJECTS)
        self.combo_detail_proj = self._project_combo(ctrl_frame, self.detail_proj_var, self.on_detail_project_selected)
        self.detail_filter_var = tk.StringVar()
        self.detail_filter_var.trace_add("write", lambda *_: self._render_detail())
        self._keyword_entry(ctrl_frame, self.detail_filter_var)
        self.detail_regex_var = tk.BooleanVar(value=False)
        self._regex_toggle(ctrl_frame, self.detail_regex_var, self._render_detail)
        self.auto_scroll_var = tk.BooleanVar(value=True)
        tk.Checkbutton(
            ctrl_frame,
            text="Auto",
            variable=self.auto_scroll_var,
            command=self._render_detail,
            bg=COLORS["toolbar"],
            fg=COLORS["text_muted"],
            activebackground=COLORS["toolbar"],
            activeforeground=COLORS["text"],
            selectcolor=COLORS["surface"],
            font=(FONT, 8),
            bd=0,
        ).pack(side="right", padx=5)
        action_button(ctrl_frame, "Clear", self.clear_detail, "secondary", side="right", width=7)
        action_button(ctrl_frame, "Open File", self.open_current_log, "secondary", side="right", width=9)
        action_button(ctrl_frame, "Export", self.export_detail, "secondary", side="right", width=9)

        self.detail_status = self._status_line(self.detail_tab)
        self.detail_hint = self._hint_line(
            self.detail_tab,
            "Choose a project to load its latest saved task log. Keyword and Regex filter the loaded content.",
        )
        self.detail_text = self._log_text(self.detail_tab, height=15)

    def _filter_bar(self, parent):
        frame = tk.Frame(parent, bg=COLORS["toolbar"], highlightbackground=COLORS["border"], highlightthickness=1)
        frame.pack(fill="x", pady=(0, 4))
        return frame

    def _project_combo(self, parent, variable, command):
        tk.Label(parent, text="Project", bg=COLORS["toolbar"], fg=COLORS["text_muted"], font=(FONT, 8)).pack(side="left", padx=(8, 4))
        combo = ttk.Combobox(parent, textvariable=variable, state="readonly", width=15)
        combo.pack(side="left", padx=(0, 8), pady=5)
        combo.bind("<<ComboboxSelected>>", lambda event: command())
        return combo

    def _keyword_entry(self, parent, variable):
        tk.Label(parent, text="Keyword", bg=COLORS["toolbar"], fg=COLORS["text_muted"], font=(FONT, 8)).pack(side="left", padx=(0, 4))
        entry = ttk.Entry(parent, textvariable=variable, width=16)
        entry.pack(side="left", fill="x", expand=True, padx=(0, 6), pady=5)
        entry.bind("<Return>", lambda event: "break")
        return entry

    def _regex_toggle(self, parent, variable, command):
        tk.Checkbutton(
            parent,
            text="Regex",
            variable=variable,
            command=command,
            bg=COLORS["toolbar"],
            fg=COLORS["text_muted"],
            activebackground=COLORS["toolbar"],
            activeforeground=COLORS["text"],
            selectcolor=COLORS["surface"],
            font=(FONT, 8),
            bd=0,
        ).pack(side="left", padx=(0, 6))

    def _status_line(self, parent):
        label = tk.Label(parent, text="", bg=COLORS["surface"], fg=COLORS["text_muted"], font=(FONT, 8))
        label.pack(fill="x", anchor="w", pady=(0, 4))
        return label

    def _hint_line(self, parent, text):
        label = tk.Label(parent, text=text, bg=COLORS["surface"], fg=COLORS["text_muted"], font=(FONT, 8))
        label.pack(fill="x", anchor="w", pady=(0, 5))
        return label

    def _log_text(self, parent, height):
        container = tk.Frame(parent, bg=COLORS["surface"])
        container.pack(fill="both", expand=True)
        text = tk.Text(
            container,
            state="disabled",
            bg=COLORS["log_bg"],
            fg=COLORS["text"],
            insertbackground=COLORS["text"],
            font=(MONO_FONT, 9),
            bd=0,
            height=height,
            wrap="none",
            padx=8,
            pady=6,
        )
        scroll_y = ttk.Scrollbar(container, orient="vertical", command=text.yview)
        scroll_y.pack(side="right", fill="y")
        scroll_x = ttk.Scrollbar(container, orient="horizontal", command=text.xview)
        scroll_x.pack(side="bottom", fill="x")
        text.configure(xscrollcommand=scroll_x.set, yscrollcommand=scroll_y.set)
        text.pack(fill="both", expand=True)
        text.tag_config("highlight", background="#fff7b2", foreground=COLORS["text"])
        text.tag_config("stderr", foreground=COLORS["error"])
        text.tag_config("info", foreground=COLORS["text_muted"])
        text.tag_config("task_header", foreground=COLORS["accent_dark"], font=(MONO_FONT, 9, "bold"))
        return text

    def append_summary(self, message):
        for line in self._split_lines(message):
            self._summary_lines.append((line, self._extract_project(line)))
        if len(self._summary_lines) > 1000:
            self._summary_lines = self._summary_lines[-1000:]
        self._render_summary()

    def append_detail(self, text, proj_name, task_name, log_type):
        timestamp = datetime.datetime.now().strftime("[%H:%M:%S]")
        project = proj_name or ""
        if log_type == "task_header":
            lines = ["", "=" * 50, f"{timestamp} [{project}] {task_name}", "=" * 50]
            tag = "task_header"
        elif log_type == "stderr":
            prefix = f"{timestamp} [{project}] {task_name} ! " if project else f"{timestamp} "
            lines = [f"{prefix}{line}" for line in self._split_lines(text)]
            tag = "stderr"
        else:
            prefix = f"{timestamp} [{project}] {task_name} | " if project else f"{timestamp} "
            lines = [f"{prefix}{line}" for line in self._split_lines(text)]
            tag = "stdout"

        for line in lines:
            self._detail_lines.append((line, project, tag))
        if len(self._detail_lines) > 2000:
            self._detail_lines = self._detail_lines[-2000:]
        self._render_detail()

    def _render_summary(self):
        project = self.log_proj_var.get()
        search = self.log_filter_var.get().strip()
        rows = []
        for line, line_project in self._summary_lines:
            if not self._matches_project(project, line_project):
                continue
            if not self._matches_search(line, search, self.log_regex_var.get()):
                continue
            rows.append((line, "stdout"))
        self._replace_text(self.log_text, rows, search, self.log_regex_var.get(), autoscroll=True)
        self._set_status(self.summary_status, len(rows), len(self._summary_lines), project, search)

    def _render_detail(self):
        project = self.detail_proj_var.get()
        search = self.detail_filter_var.get().strip()
        rows = []
        for line, line_project, tag in self._detail_lines:
            if not self._matches_project(project, line_project):
                continue
            if not self._matches_search(line, search, self.detail_regex_var.get()):
                continue
            rows.append((line, tag))
        self._replace_text(self.detail_text, rows, search, self.detail_regex_var.get(), self.auto_scroll_var.get())
        self._set_status(self.detail_status, len(rows), len(self._detail_lines), project, search)

    def _replace_text(self, widget, rows, search, use_regex, autoscroll):
        widget.config(state="normal")
        widget.delete("1.0", "end")
        for line, tag in rows:
            start = widget.index("end-1c")
            widget.insert("end", f"{line}\n", tag if tag != "stdout" else None)
            if search:
                self._highlight_last_line(widget, start, line, search, use_regex)
        if autoscroll:
            widget.see("end")
        widget.config(state="disabled")

    def _highlight_last_line(self, widget, line_start, line, search, use_regex):
        try:
            matches = (
                re.finditer(search, line, re.IGNORECASE)
                if use_regex
                else re.finditer(re.escape(search), line, re.IGNORECASE)
            )
            for match in matches:
                widget.tag_add("highlight", f"{line_start}+{match.start()}c", f"{line_start}+{match.end()}c")
        except re.error:
            return

    def _set_status(self, label, shown, total, project, search):
        scope = "All projects" if not project or project == ALL_PROJECTS else project
        suffix = f"   keyword '{search}'" if search else ""
        label.config(text=f"Showing {shown}/{total} lines in {scope}{suffix}")

    def filter_logs(self):
        self._render_summary()

    def filter_detail_logs(self):
        self._render_detail()

    def clear_summary(self):
        self._summary_lines.clear()
        self._render_summary()

    def clear_detail(self):
        self._detail_lines.clear()
        self._loaded_detail_project = None
        self._current_log_file = None
        self._render_detail()

    def on_detail_project_selected(self, event=None):
        if self.load_latest_detail_log_for_selected_project():
            return
        self._render_detail()

    def load_latest_detail_log_for_selected_project(self):
        project_name = self.detail_proj_var.get()
        if not project_name or project_name == ALL_PROJECTS:
            self._loaded_detail_project = None
            self._current_log_file = None
            return False

        log_file = self._find_latest_task_log(project_name)
        if not log_file:
            self._loaded_detail_project = None
            self._current_log_file = None
            self._detail_lines = [(f"No saved task log found for {project_name}. Run the project first.", project_name, "info")]
            self._render_detail()
            return True

        try:
            with open(log_file, "r", encoding="utf-8", errors="replace") as handle:
                content = handle.read()
        except Exception as exc:
            self._detail_lines = [(f"Failed to load latest detail log: {exc}", project_name, "stderr")]
            self._render_detail()
            return True

        self._current_log_file = log_file
        self._loaded_detail_project = project_name
        display_path = os.path.relpath(log_file, getattr(self.app, "base_dir", os.getcwd()))
        self._detail_lines = [
            (f"[Loaded latest saved log] {display_path}", project_name, "task_header"),
            ("", project_name, "stdout"),
        ]
        self._detail_lines.extend((line, project_name, "stdout") for line in self._split_lines(content))
        self._render_detail()
        return True

    def _find_latest_task_log(self, project_name):
        base_dir = getattr(self.app, "base_dir", None)
        if not base_dir:
            return None
        project_dir = os.path.join(base_dir, "task_logs", project_name)
        if not os.path.isdir(project_dir):
            return None

        date_dirs = [
            os.path.join(project_dir, name)
            for name in os.listdir(project_dir)
            if os.path.isdir(os.path.join(project_dir, name))
        ]
        if not date_dirs:
            return None
        latest_date_dir = max(date_dirs, key=lambda item: os.path.basename(item))
        text_files = [
            os.path.join(latest_date_dir, name)
            for name in os.listdir(latest_date_dir)
            if name.lower().endswith(".txt") and os.path.isfile(os.path.join(latest_date_dir, name))
        ]
        if not text_files:
            return None
        return max(text_files, key=lambda item: os.path.getmtime(item))

    def export_summary(self):
        self._export_text(self.log_text, "Export Summary Log")

    def export_detail(self):
        self._export_text(self.detail_text, "Export Detail Log")

    def _export_text(self, widget, title):
        content = widget.get("1.0", "end").strip()
        if not content:
            messagebox.showinfo("Notice", "There is no log content to export.")
            return
        filepath = filedialog.asksaveasfilename(
            defaultextension=".txt",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
            title=title,
        )
        if filepath:
            try:
                with open(filepath, "w", encoding="utf-8") as handle:
                    handle.write(content)
            except Exception as exc:
                messagebox.showerror("Error", f"Save failed: {exc}")

    def open_current_log(self):
        if self._current_log_file and os.path.exists(self._current_log_file):
            os.startfile(self._current_log_file)
        else:
            messagebox.showinfo("Notice", "No recent log file is available.")

    def update_project_list(self, project_names):
        self.combo_log_proj["values"] = project_names
        self.combo_detail_proj["values"] = project_names
        if self.log_proj_var.get() not in project_names:
            self.log_proj_var.set(ALL_PROJECTS)
        if self.detail_proj_var.get() not in project_names:
            self.detail_proj_var.set(ALL_PROJECTS)

    def _matches_project(self, selected_project, line_project):
        if not selected_project or selected_project == ALL_PROJECTS:
            return True
        return line_project == selected_project

    def _matches_search(self, line, search, use_regex):
        if not search:
            return True
        try:
            pattern = search if use_regex else re.escape(search)
            return re.search(pattern, line, re.IGNORECASE) is not None
        except re.error:
            return True

    def _extract_project(self, line):
        matches = re.findall(r"\[([^\]]+)\]", line)
        if len(matches) >= 2:
            return matches[1]
        if len(matches) == 1 and not re.match(r"\d{2}:\d{2}:\d{2}", matches[0]):
            return matches[0]
        return ""

    def _split_lines(self, text):
        lines = str(text).splitlines()
        return lines or [""]
