import tkinter as tk

import ttkbootstrap as ttk


COLORS = {
    "bg_main": "#f8f5fb",
    "bg_header": "#ffffff",
    "surface": "#ffffff",
    "surface_tint": "#f4edff",
    "surface_subtle": "#f7f7f8",
    "border": "#e3e5ea",
    "border_strong": "#d9dce3",
    "text": "#24282d",
    "text_muted": "#79797f",
    "accent": "#7d33f6",
    "accent_dark": "#5f22d9",
    "accent_soft": "#f4edff",
    "success": "#00b67a",
    "success_soft": "#daf7db",
    "warning": "#d97706",
    "warning_soft": "#fff7ed",
    "error": "#dc2626",
    "error_soft": "#fee2e2",
    "selected": "#eadcff",
    "row_even": "#ffffff",
    "row_odd": "#faf9fc",
    "log_bg": "#fbfbfd",
    "toolbar": "#fbf9fd",
}

FONT = "Segoe UI"
MONO_FONT = "Consolas"
_STYLE_READY = False


def ensure_style():
    global _STYLE_READY
    if _STYLE_READY:
        return
    try:
        ttk.Style(theme="litera")
    except Exception:
        pass
    _STYLE_READY = True


def configure_root(root):
    ensure_style()
    root.configure(bg=COLORS["bg_main"])


def panel(parent):
    ensure_style()
    frame = tk.Frame(
        parent,
        bg=COLORS["surface"],
        highlightbackground=COLORS["border"],
        highlightthickness=1,
        bd=0,
    )
    return frame


def panel_header(parent, title, subtitle=None):
    header = tk.Frame(parent, bg=COLORS["surface"])
    header.pack(fill="x", padx=12, pady=(10, 6))

    text_frame = tk.Frame(header, bg=COLORS["surface"])
    text_frame.pack(side="left", fill="x", expand=True)
    tk.Label(
        text_frame,
        text=title,
        bg=COLORS["surface"],
        fg=COLORS["text"],
        font=(FONT, 11, "bold"),
    ).pack(anchor="w")
    if subtitle:
        tk.Label(
            text_frame,
            text=subtitle,
            bg=COLORS["surface"],
            fg=COLORS["text_muted"],
            font=(FONT, 9),
        ).pack(anchor="w", pady=(2, 0))
    return header


def section(parent, bg=None):
    frame = tk.Frame(parent, bg=bg or COLORS["surface"])
    frame.pack(fill="x", padx=12, pady=(0, 8))
    return frame


def action_button(parent, text, command, variant="secondary", side="left", expand=False, width=None):
    palette = {
        "primary": (COLORS["accent"], "#ffffff", COLORS["accent_dark"]),
        "success": (COLORS["success"], "#ffffff", COLORS["success"]),
        "danger": ("#ffffff", COLORS["error"], COLORS["error"]),
        "warning": ("#ffffff", COLORS["warning"], COLORS["warning"]),
        "info": ("#ffffff", COLORS["accent"], COLORS["accent"]),
        "secondary": (COLORS["surface_tint"], COLORS["text"], COLORS["border"]),
    }
    bg, fg, border = palette.get(variant, palette["secondary"])
    button = tk.Frame(
        parent,
        bg=bg,
        bd=0,
        highlightthickness=1,
        highlightbackground=border,
        highlightcolor=border,
        cursor="hand2",
    )
    label = tk.Label(
        button,
        text=text,
        bg=bg,
        fg=fg,
        font=(FONT, 9, "bold"),
        padx=10,
        pady=5,
        width=width,
        cursor="hand2",
    )
    label.pack(fill="both", expand=True)

    normal_bg = bg
    hover_bg = COLORS["accent_dark"] if bg == COLORS["accent"] else "#eadcff"

    def run(_event=None):
        if command:
            command()

    def enter(_event=None):
        button.configure(bg=hover_bg)
        label.configure(bg=hover_bg)

    def leave(_event=None):
        button.configure(bg=normal_bg)
        label.configure(bg=normal_bg)

    for widget in (button, label):
        widget.bind("<Button-1>", run)
        widget.bind("<Return>", run)
        widget.bind("<Enter>", enter)
        widget.bind("<Leave>", leave)
    button.pack(side=side, padx=3, pady=2, fill="x" if expand else None, expand=expand)
    return button


def status_pill(parent, label, value, color):
    frame = tk.Frame(
        parent,
        bg=COLORS["surface"],
        highlightbackground=COLORS["border"],
        highlightthickness=1,
        bd=0,
    )
    frame.pack(side="left", padx=3, pady=0)
    value_label = tk.Label(
        frame,
        text=value,
        bg=COLORS["surface"],
        fg=color,
        font=(MONO_FONT, 12, "bold"),
        width=3,
    )
    value_label.pack(side="left", padx=(9, 3), pady=5)
    tk.Label(
        frame,
        text=label,
        bg=COLORS["surface"],
        fg=COLORS["text_muted"],
        font=(FONT, 8),
    ).pack(side="left", padx=(0, 9), pady=5)
    return value_label
