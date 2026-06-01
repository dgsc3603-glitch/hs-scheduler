import json
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from component.engine import LocalEngineClient


def main():
    client = LocalEngineClient()
    health = client.health()
    distributed = client.distributed_status()
    print(json.dumps({"health": health, "distributed": distributed}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
