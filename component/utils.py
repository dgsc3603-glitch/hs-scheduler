import os
import json
import logging
import datetime
import shutil
import tempfile
from logging.handlers import RotatingFileHandler

def setup_logging(data_file, max_size, backup_count):
    logger = logging.getLogger("Scheduler")
    logger.setLevel(logging.INFO)
    
    log_dir = os.path.join(os.path.dirname(data_file), "logs")
    if not os.path.exists(log_dir): 
        os.makedirs(log_dir)
    
    log_file = os.path.join(log_dir, "scheduler_app.log")
    handler = RotatingFileHandler(
        log_file, 
        maxBytes=max_size, 
        backupCount=backup_count, 
        encoding="utf-8"
    )
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    handler.setFormatter(formatter)
    
    if not logger.handlers:
        logger.addHandler(handler)
    return logger

def atomic_write_json(path, data, indent=2, backup=True):
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    fd, temp_path = tempfile.mkstemp(
        prefix=f".{os.path.basename(path)}.",
        suffix=".tmp",
        dir=directory,
        text=True,
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(data, handle, ensure_ascii=False, indent=indent)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        if backup and os.path.exists(path):
            shutil.copy2(path, f"{path}.bak")
        os.replace(temp_path, path)
    except Exception:
        try:
            os.remove(temp_path)
        except OSError:
            pass
        raise

def load_json_file(path, default=None):
    if not os.path.exists(path):
        return default

    candidates = [path, f"{path}.bak"]
    last_error = None
    for candidate in candidates:
        if not os.path.exists(candidate):
            continue
        try:
            with open(candidate, "r", encoding="utf-8") as handle:
                return json.load(handle)
        except Exception as exc:
            last_error = exc
    if last_error:
        raise last_error
    return default

class CredentialManager:
    def __init__(self, data_file):
        self.path = os.path.join(os.path.dirname(data_file), "scheduler_secrets.json")

    def load(self):
        if not os.path.exists(self.path): 
            return {}
        try:
            return load_json_file(self.path, default={}) or {}
        except Exception:
            return {}

    def save(self, secrets):
        try:
            atomic_write_json(self.path, secrets, indent=4)
            return True
        except Exception:
            return False

    def inject_to_args(self, args_str):
        secrets = self.load()
        for k, v in secrets.items():
            placeholder = f"%{k}%"
            if placeholder in args_str:
                args_str = args_str.replace(placeholder, v)
        return args_str

def send_telegram_alert(message, secrets, log_func=None):
    token = secrets.get("TELEGRAM_BOT_TOKEN")
    chat_id = secrets.get("CHAT_ID")
    
    if not token or not chat_id:
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    import urllib.request
    import urllib.parse
    
    data = urllib.parse.urlencode({"chat_id": chat_id, "text": message}).encode()
    req = urllib.request.Request(url, data=data)
    
    try:
        with urllib.request.urlopen(req) as response:
            if log_func:
                log_func("             Done")
            return True
    except Exception as e:
        if log_func:
            log_func(f"           Failed: {e}")
        return False
