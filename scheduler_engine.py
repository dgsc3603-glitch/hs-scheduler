import argparse
import logging
import os
import sys
import time
import faulthandler
from logging.handlers import RotatingFileHandler

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT_DIR)

from component.engine import EngineService, LocalApiServer


_FAULT_LOG_FILE = None


def configure_logging(base_dir):
    global _FAULT_LOG_FILE
    log_dir = os.path.join(base_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger("SchedulerEngine")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    file_handler = RotatingFileHandler(
        os.path.join(log_dir, "scheduler_engine.log"),
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    try:
        if _FAULT_LOG_FILE is None:
            _FAULT_LOG_FILE = open(os.path.join(log_dir, "scheduler_engine.fault.log"), "a", encoding="utf-8")
        faulthandler.enable(file=_FAULT_LOG_FILE)
    except Exception:
        logger.exception("Failed to enable faulthandler")

    return logger


def acquire_engine_lock(base_dir, logger):
    lock_path = os.path.join(base_dir, "scheduler_engine.lock")
    lock_file = open(lock_path, "a+b")
    try:
        lock_file.seek(0, os.SEEK_END)
        if lock_file.tell() == 0:
            lock_file.write(b"\0")
            lock_file.flush()
        lock_file.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        lock_file.close()
        logger.error("Another scheduler engine is already running for %s", base_dir)
        return None

    pid_path = os.path.join(base_dir, "scheduler_engine.pid")
    try:
        with open(pid_path, "w", encoding="utf-8") as pid_file:
            pid_file.write(str(os.getpid()))
            pid_file.write("\n")
            pid_file.flush()
            os.fsync(pid_file.fileno())
    except OSError:
        logger.warning("Failed to write engine pid file", exc_info=True)

    return lock_file


def release_engine_lock(base_dir, lock_file, logger):
    try:
        lock_file.close()
    finally:
        pid_path = os.path.join(base_dir, "scheduler_engine.pid")
        try:
            if os.path.exists(pid_path):
                os.remove(pid_path)
        except OSError:
            logger.warning("Failed to remove engine pid file", exc_info=True)


def main():
    parser = argparse.ArgumentParser(description="Local scheduler engine service")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18731)
    parser.add_argument("--base-dir", default=ROOT_DIR)
    parser.add_argument("--runtime-config", default=None)
    parser.add_argument("--policy-config", default=None)
    args = parser.parse_args()

    args.base_dir = os.path.abspath(args.base_dir)
    logger = configure_logging(args.base_dir)
    lock_file = acquire_engine_lock(args.base_dir, logger)
    if lock_file is None:
        return 2

    service = None
    api_server = None
    try:
        service = EngineService(
            args.base_dir,
            logger=logger,
            runtime_config_path=args.runtime_config,
            policy_path=args.policy_config,
        )
        service.start()

        api_server = LocalApiServer(service, host=args.host, port=args.port)
        logger.info("Engine API listening on http://%s:%s", args.host, args.port)

        api_server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Engine shutdown requested")
    finally:
        if api_server:
            api_server.shutdown()
        if service:
            service.stop()
        release_engine_lock(args.base_dir, lock_file, logger)
        time.sleep(0.1)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception:
        logger = configure_logging(ROOT_DIR)
        logger.exception("Engine crashed during startup")
        raise
