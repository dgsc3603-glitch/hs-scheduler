import argparse
import json
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_DIR = BASE_DIR / "config"
DEFAULT_OUTPUT = CONFIG_DIR / "distributed_runtime.json"
PROFILE_MAP = {
    "main-pc": CONFIG_DIR / "distributed_runtime.main_pc.json",
    "sub-laptop": CONFIG_DIR / "distributed_runtime.sub_laptop.json",
}


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description="Apply a distributed runtime profile to distributed_runtime.json")
    parser.add_argument(
        "--profile",
        required=True,
        choices=sorted(PROFILE_MAP.keys()),
        help="Which runtime profile to apply",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help="Path to write the applied runtime config",
    )
    parser.add_argument(
        "--node-id",
        default="",
        help="Optional node_id override for this machine",
    )
    args = parser.parse_args()

    source_path = PROFILE_MAP[args.profile]
    output_path = Path(args.output).resolve()
    payload = load_json(source_path)

    if args.node_id.strip():
        payload["node_id"] = args.node_id.strip()

    write_json(output_path, payload)

    print(f"Applied profile: {args.profile}")
    print(f"Source: {source_path.as_posix()}")
    print(f"Output: {output_path.as_posix()}")
    print(f"node_id: {payload.get('node_id', '')}")
    print(f"enabled: {payload.get('enabled', False)}")
    control_plane = payload.get("control_plane", {})
    print(f"account_id: {control_plane.get('account_id', '')}")
    print(f"database_id: {control_plane.get('database_id', '')}")
    print(f"api_token_present: {bool(control_plane.get('api_token', '').strip())}")


if __name__ == "__main__":
    main()
