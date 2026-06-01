import json
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from component.engine.service import EngineService


def main():
    service = EngineService(str(BASE_DIR))
    service.control_plane.initialize()
    service.core.load_data()
    service.control_plane.apply_runtime_overrides(service.core.projects)

    if hasattr(service, "_reconcile_control_plane_tickets"):
        result = service._reconcile_control_plane_tickets(force=True)
        mode = "engine_service"
    else:
        backfill_result = service.control_plane.backfill_local_scheduled_runs(service.core.projects)
        reconcile_result = service.control_plane.reconcile_consumed_tickets(service.core.projects)
        result = {
            "backfilled_count": int(backfill_result.get("backfilled_count", 0)),
            "reconciled_count": int(reconcile_result.get("reconciled_count", 0)),
        }
        if hasattr(service.core, "save_data"):
            service.core.save_data()
        if hasattr(service, "_sync_store_from_core"):
            service._sync_store_from_core()
        mode = "control_plane_fallback"

    payload = {
        "result": result,
        "mode": mode,
        "status": service.control_plane.status(),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
