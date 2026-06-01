import tkinter as tk
from tkinter import messagebox

import ttkbootstrap as ttk

from component.ui.theme import COLORS, FONT, action_button


class DistributedSettingsDialog:
    def __init__(self, parent, app):
        self.parent = parent
        self.app = app
        self.window = None

        self.enabled_var = tk.BooleanVar(value=False)
        self.node_id_var = tk.StringVar()
        self.node_role_var = tk.StringVar(value="pc")
        self.pc_role_var = tk.StringVar(value="Sub PC")
        self.node_priority_var = tk.StringVar(value="100")
        self.lease_ttl_var = tk.StringVar(value="45")
        self.heartbeat_interval_var = tk.StringVar(value="10")
        self.retention_days_var = tk.StringVar(value="7")
        self.account_id_var = tk.StringVar()
        self.database_id_var = tk.StringVar()
        self.api_token_var = tk.StringVar()
        self.cloud_copy_root_var = tk.StringVar()
        self.cloud_copy_manifest_var = tk.StringVar()
        self.artifact_spool_root_var = tk.StringVar()
        self.archive_db_path_var = tk.StringVar()
        self.pc_fallback_enabled_var = tk.BooleanVar(value=True)
        self.run_originals_var = tk.BooleanVar(value=True)

        self.status_summary_var = tk.StringVar(value="Loading distributed status...")
        self.status_detail_var = tk.StringVar(value="")

    def is_open(self):
        return bool(self.window and self.window.winfo_exists())

    def focus(self):
        if not self.is_open():
            return
        self.window.deiconify()
        self.window.lift()
        self.window.focus_force()

    def show(self):
        if self.is_open():
            self.focus()
            return

        self.window = tk.Toplevel(self.parent)
        self.window.title("Main/Sub Failover Settings")
        self.window.geometry("780x800")
        self.window.minsize(720, 680)
        self.window.configure(bg=COLORS["bg_main"])
        self.window.transient(self.parent)
        self.window.grab_set()
        self.window.protocol("WM_DELETE_WINDOW", self._close)

        container = tk.Frame(self.window, bg=COLORS["bg_main"])
        container.pack(fill="both", expand=True)

        header = tk.Frame(container, bg=COLORS["bg_header"], highlightbackground=COLORS["border"], highlightthickness=1)
        header.pack(fill="x", padx=14, pady=(14, 10))
        title = tk.Label(header, text="Main/Sub Settings", bg=COLORS["bg_header"], fg=COLORS["text"], font=(FONT, 17, "bold"))
        title.pack(anchor="w", padx=18, pady=(14, 2))

        subtitle = tk.Label(
            header,
            text="Main and Sub PCs share the same D1 control plane. Only the lease owner runs scheduled jobs.",
            bg=COLORS["bg_header"],
            fg=COLORS["text_muted"],
            font=(FONT, 10),
        )
        subtitle.pack(anchor="w", padx=18, pady=(0, 14))

        status_frame = self._card(container, "Current Status")
        tk.Label(status_frame, textvariable=self.status_summary_var, bg=COLORS["surface"], fg=COLORS["text"], font=(FONT, 11, "bold")).pack(anchor="w")
        tk.Label(status_frame, textvariable=self.status_detail_var, bg=COLORS["surface"], fg=COLORS["text_muted"], font=(FONT, 9)).pack(anchor="w", pady=(4, 0))

        runtime_frame = self._card(container, "Runtime")
        self._add_checkbox(runtime_frame, "Enable distributed mode", self.enabled_var, 0, 0)
        self._add_entry(runtime_frame, "Node ID", self.node_id_var, 1)
        role_combo = self._add_combo(runtime_frame, "This PC role", self.pc_role_var, ["Main PC", "Sub PC"], 2)
        role_combo.bind("<<ComboboxSelected>>", self._on_pc_role_changed)
        self._add_entry(runtime_frame, "Node Priority (auto)", self.node_priority_var, 3)
        self._add_entry(runtime_frame, "Lease TTL (sec)", self.lease_ttl_var, 4)
        self._add_entry(runtime_frame, "Heartbeat Interval (sec)", self.heartbeat_interval_var, 5)
        self._add_entry(runtime_frame, "Retention Days", self.retention_days_var, 6)

        d1_frame = self._card(container, "Cloudflare D1")
        self._add_entry(d1_frame, "Account ID", self.account_id_var, 0)
        self._add_entry(d1_frame, "Database ID", self.database_id_var, 1)
        self._add_entry(d1_frame, "API Token", self.api_token_var, 2, show="*")

        path_frame = self._card(container, "Paths")
        self._add_entry(path_frame, "Cloud Copy Root", self.cloud_copy_root_var, 0)
        self._add_entry(path_frame, "Cloud Copy Manifest", self.cloud_copy_manifest_var, 1)
        self._add_entry(path_frame, "Artifact Spool Root", self.artifact_spool_root_var, 2)
        self._add_entry(path_frame, "Archive DB Path", self.archive_db_path_var, 3)

        fallback_frame = self._card(container, "Fallback")
        self._add_checkbox(fallback_frame, "Enable fallback", self.pc_fallback_enabled_var, 0, 0)
        self._add_checkbox(fallback_frame, "Run original .py files during fallback", self.run_originals_var, 1, 0)

        help_text = (
            "Recommended setup\n"
            "- Choose Main PC for the machine that is normally online\n"
            "- Choose Sub PC for the standby machine\n"
            "- Use a unique node_id on each PC\n"
            "- Keep D1 values identical on both PCs\n"
            "- Keep the Sub PC awake while the engine is running"
        )
        help_frame = self._card(container, "Operations Notes")
        tk.Label(help_frame, text=help_text, justify="left", bg=COLORS["surface"], fg=COLORS["text_muted"], font=(FONT, 9)).pack(anchor="w")

        button_row = tk.Frame(container, bg=COLORS["bg_main"])
        button_row.pack(fill="x", padx=14, pady=(0, 14))
        action_button(button_row, "Refresh", self._load, "secondary")
        action_button(button_row, "Save", self._save, "primary", side="right")
        action_button(button_row, "Close", self._close, "secondary", side="right")

        self._load()

    def _card(self, parent, title):
        outer = tk.Frame(parent, bg=COLORS["surface"], highlightbackground=COLORS["border"], highlightthickness=1, bd=0)
        outer.pack(fill="x", padx=14, pady=(0, 10))
        tk.Label(outer, text=title, bg=COLORS["surface"], fg=COLORS["text"], font=(FONT, 11, "bold")).pack(anchor="w", padx=14, pady=(12, 4))
        body = tk.Frame(outer, bg=COLORS["surface"])
        body.pack(fill="x", padx=14, pady=(0, 12))
        return body

    def _add_entry(self, parent, label, variable, row, show=None):
        ttk.Label(parent, text=label).grid(row=row, column=0, sticky="w", padx=(0, 10), pady=5)
        entry = ttk.Entry(parent, textvariable=variable, show=show)
        entry.grid(row=row, column=1, sticky="ew", pady=5)
        parent.grid_columnconfigure(1, weight=1)
        return entry

    def _add_checkbox(self, parent, label, variable, row, column):
        check = ttk.Checkbutton(parent, text=label, variable=variable, bootstyle="round-toggle")
        check.grid(row=row, column=column, sticky="w", pady=6)
        return check

    def _add_combo(self, parent, label, variable, values, row):
        ttk.Label(parent, text=label).grid(row=row, column=0, sticky="w", padx=(0, 10), pady=6)
        combo = ttk.Combobox(parent, textvariable=variable, values=values, state="readonly")
        combo.grid(row=row, column=1, sticky="ew", pady=6)
        parent.grid_columnconfigure(1, weight=1)
        return combo

    def _on_pc_role_changed(self, event=None):
        self.node_priority_var.set("10" if self.pc_role_var.get() == "Main PC" else "100")

    def _load(self):
        try:
            payload = self.app.load_distributed_config()
        except Exception as exc:
            messagebox.showerror("Distributed Settings", f"Failed to load settings.\n\n{exc}")
            return

        document = payload.get("document", {}) or {}
        resolved = payload.get("resolved", {}) or {}
        status = payload.get("status", {}) or {}

        merged = self._merge_document(document, resolved)

        self.enabled_var.set(bool(merged.get("enabled", False)))
        self.node_id_var.set(str(merged.get("node_id", "")))
        self.node_role_var.set(str(merged.get("node_role", "pc")))
        priority = int(merged.get("node_priority", 100) or 100)
        self.node_priority_var.set(str(priority))
        self.pc_role_var.set("Main PC" if priority <= 50 else "Sub PC")
        self.lease_ttl_var.set(str(merged.get("lease_ttl_seconds", 45)))
        self.heartbeat_interval_var.set(str(merged.get("heartbeat_interval_seconds", 10)))
        self.retention_days_var.set(str(merged.get("retention_days", 7)))

        control_plane = merged.get("control_plane", {})
        self.account_id_var.set(str(control_plane.get("account_id", "")))
        self.database_id_var.set(str(control_plane.get("database_id", "")))
        self.api_token_var.set(str(control_plane.get("api_token", "")))

        paths = merged.get("paths", {})
        self.cloud_copy_root_var.set(str(paths.get("cloud_copy_root", "")))
        self.cloud_copy_manifest_var.set(str(paths.get("cloud_copy_manifest", "")))
        self.artifact_spool_root_var.set(str(paths.get("artifact_spool_root", "")))
        self.archive_db_path_var.set(str(paths.get("archive_db_path", "")))

        pc_fallback = merged.get("pc_fallback", {})
        self.pc_fallback_enabled_var.set(bool(pc_fallback.get("enabled", True)))
        self.run_originals_var.set(bool(pc_fallback.get("run_originals", True)))

        self._render_status(status)

    def _merge_document(self, document, resolved):
        merged = {
            "enabled": resolved.get("enabled", False),
            "node_id": resolved.get("node_id", ""),
            "node_role": resolved.get("node_role", "pc"),
            "node_priority": resolved.get("node_priority", 100),
            "lease_ttl_seconds": resolved.get("lease_ttl_seconds", 45),
            "heartbeat_interval_seconds": resolved.get("heartbeat_interval_seconds", 10),
            "retention_days": resolved.get("retention_days", 7),
            "control_plane": {
                "provider": "cloudflare_d1",
                "account_id": "",
                "database_id": "",
                "api_token": "",
                "timeout_seconds": 8,
            },
            "paths": {
                "cloud_copy_root": resolved.get("paths", {}).get("cloud_copy_root", ""),
                "cloud_copy_manifest": resolved.get("paths", {}).get("cloud_copy_manifest", ""),
                "artifact_spool_root": resolved.get("paths", {}).get("artifact_spool_root", ""),
                "archive_db_path": resolved.get("paths", {}).get("archive_db_path", ""),
            },
            "pc_fallback": {
                "enabled": True,
                "run_originals": True,
            },
        }
        for key, value in (document or {}).items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key].update(value)
            else:
                merged[key] = value
        return merged

    def _render_status(self, status):
        if not status:
            self.status_summary_var.set("Distributed status is not available yet.")
            self.status_detail_var.set("The engine may be stopped or the configuration may be empty.")
            return

        enabled = "ON" if status.get("enabled") else "OFF"
        mode = "Lease owner" if status.get("is_primary") else "Standby"
        if not status.get("enabled"):
            mode = "LOCAL"
        summary = f"Distributed mode {enabled} / current state {mode}"
        detail = (
            f"node_id={status.get('node_id', '-')}, "
            f"priority={status.get('node_priority', '-')}, "
            f"lease_owner={status.get('lease_owner', '-')}, "
            f"d1={'OK' if status.get('control_plane_enabled') else 'OFF'}"
        )
        self.status_summary_var.set(summary)
        self.status_detail_var.set(detail)

    def _collect_document(self):
        try:
            lease_ttl = int(self.lease_ttl_var.get().strip() or "45")
            heartbeat_interval = int(self.heartbeat_interval_var.get().strip() or "10")
            retention_days = int(self.retention_days_var.get().strip() or "7")
        except ValueError as exc:
            raise ValueError("Numeric fields must be integers.") from exc

        node_id = self.node_id_var.get().strip()
        if not node_id:
            raise ValueError("Node ID cannot be empty.")

        pc_role = self.pc_role_var.get().strip()
        node_priority = 10 if pc_role == "Main PC" else 100
        self.node_priority_var.set(str(node_priority))

        return {
            "enabled": bool(self.enabled_var.get()),
            "node_id": node_id,
            "node_role": "pc",
            "node_priority": node_priority,
            "lease_name": "global_scheduler_primary",
            "lease_ttl_seconds": lease_ttl,
            "heartbeat_interval_seconds": heartbeat_interval,
            "retention_days": retention_days,
            "control_plane": {
                "provider": "cloudflare_d1",
                "account_id": self.account_id_var.get().strip(),
                "database_id": self.database_id_var.get().strip(),
                "api_token": self.api_token_var.get().strip(),
                "timeout_seconds": 8,
            },
            "paths": {
                "cloud_copy_root": self.cloud_copy_root_var.get().strip(),
                "cloud_copy_manifest": self.cloud_copy_manifest_var.get().strip(),
                "artifact_spool_root": self.artifact_spool_root_var.get().strip(),
                "archive_db_path": self.archive_db_path_var.get().strip(),
            },
            "pc_fallback": {
                "enabled": bool(self.pc_fallback_enabled_var.get()),
                "run_originals": bool(self.run_originals_var.get()),
            },
        }

    def _save(self):
        try:
            document = self._collect_document()
            result = self.app.save_distributed_config(document)
        except Exception as exc:
            messagebox.showerror("Distributed Settings", f"Failed to save settings.\n\n{exc}")
            return

        reload_error = result.get("reload_error", "")
        self._render_status(result.get("status", {}))
        if reload_error:
            messagebox.showwarning(
                "Distributed Settings",
                f"Settings were saved, but engine reload failed.\n\n{reload_error}",
            )
            return
        messagebox.showinfo("Distributed Settings", "Settings were saved and applied to the engine.")

    def _close(self):
        if self.window and self.window.winfo_exists():
            self.window.destroy()
        self.window = None
