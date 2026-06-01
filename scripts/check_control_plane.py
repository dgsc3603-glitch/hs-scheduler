import json
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from component.distributed import DistributedControlPlane, DistributedRuntimeConfig, ProjectPolicyCollection


def main():
    runtime_config = DistributedRuntimeConfig.load(str(BASE_DIR))
    policies = ProjectPolicyCollection.load(str(BASE_DIR))
    control_plane = DistributedControlPlane(runtime_config, policies)
    control_plane.initialize()

    status = control_plane.status()
    print(json.dumps({"status": status}, ensure_ascii=False, indent=2))

    if not control_plane.control_plane_enabled:
        print("control_plane_enabled=false")
        return

    tables = control_plane.client.query(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name",
        [],
    )
    print(json.dumps({"tables": tables}, ensure_ascii=False, indent=2))

    leases = control_plane.client.query(
        "SELECT lease_name, owner_node_id, lease_expires_at, epoch FROM scheduler_leases ORDER BY lease_name",
        [],
    )
    print(json.dumps({"leases": leases}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
