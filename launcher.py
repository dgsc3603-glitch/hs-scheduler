# -*- coding: utf-8 -*-
"""
HS Scheduler launcher.
- Starts the modular app from the component package.
- Uses scheduler_data.json in the project root.
- Writes startup and fatal errors to logs/scheduler_launcher.log.
"""

import faulthandler
import logging
import os
import sys
import tkinter as tk
from logging.handlers import RotatingFileHandler


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(ROOT_DIR, "logs")
FAULT_LOG_FILE = None


def configure_launcher_logging():
    global FAULT_LOG_FILE

    os.makedirs(LOG_DIR, exist_ok=True)
    logger = logging.getLogger("SchedulerLauncher")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    handler = RotatingFileHandler(
        os.path.join(LOG_DIR, "scheduler_launcher.log"),
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    try:
        if FAULT_LOG_FILE is None:
            FAULT_LOG_FILE = open(
                os.path.join(LOG_DIR, "scheduler_launcher.fault.log"),
                "a",
                encoding="utf-8",
            )
        faulthandler.enable(file=FAULT_LOG_FILE)
    except Exception:
        logger.exception("Failed to enable launcher faulthandler")

    return logger


def show_startup_error(title, message):
    try:
        root = tk.Tk()
        root.withdraw()
        from tkinter import messagebox

        messagebox.showerror(title, message)
        root.destroy()
    except Exception:
        pass


def create_root_window():
    try:
        from tkinterdnd2 import TkinterDnD

        return TkinterDnD.Tk()
    except ImportError:
        return tk.Tk()


def main():
    logger = configure_launcher_logging()
    sys.path.insert(0, ROOT_DIR)

    def _excepthook(exc_type, exc, tb):
        logger.exception("Unhandled launcher exception", exc_info=(exc_type, exc, tb))

    sys.excepthook = _excepthook

    try:
        from component.app import HSSchedulerApp
    except Exception as exc:
        logger.exception("Application import failed")
        show_startup_error(
            "HS Scheduler Startup Failed",
            f"Failed to load the application module.\n\n{exc}\n\nSee logs/scheduler_launcher.log for details.",
        )
        return 1

    root = None
    try:
        root = create_root_window()
        HSSchedulerApp(root, base_dir=ROOT_DIR)
        root.mainloop()
        return 0
    except Exception as exc:
        logger.exception("Application crashed")
        show_startup_error(
            "HS Scheduler Error",
            f"The application crashed while running.\n\n{exc}\n\nSee logs/scheduler_launcher.log for details.",
        )
        return 1
    finally:
        if root is not None:
            try:
                root.destroy()
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
