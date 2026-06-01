import datetime
import hashlib
import json
import os
import sqlite3


def _utc_now():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


class ControlPlaneArchiveStore:
    def __init__(self, db_path):
        self.db_path = db_path

    def initialize(self):
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS archived_records (
                    table_name TEXT NOT NULL,
                    record_key TEXT NOT NULL,
                    archived_at TEXT NOT NULL,
                    payload_hash TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    PRIMARY KEY(table_name, record_key)
                )
                """
            )

    def archive_rows(self, table_name, key_field, rows):
        if not rows:
            return {"archived_count": 0}

        self.initialize()
        archived_count = 0
        with sqlite3.connect(self.db_path) as conn:
            for row in rows:
                record_key = str(row.get(key_field) or "")
                if not record_key:
                    continue
                payload_json = json.dumps(row, ensure_ascii=False, sort_keys=True)
                payload_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
                conn.execute(
                    """
                    INSERT INTO archived_records (
                        table_name, record_key, archived_at, payload_hash, payload_json
                    ) VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(table_name, record_key) DO UPDATE SET
                        archived_at=excluded.archived_at,
                        payload_hash=excluded.payload_hash,
                        payload_json=excluded.payload_json
                    """,
                    (table_name, record_key, _utc_now(), payload_hash, payload_json),
                )
                archived_count += 1
        return {"archived_count": archived_count}
