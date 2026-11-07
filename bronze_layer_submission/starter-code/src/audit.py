"""Ingestion audit / evidence logging (Task 5).

Appends one JSON line per batch/entity run to config.AUDIT_LOG_PATH.
Provides an audit trail of:
- rows_read, rows_written, duplicates_skipped, quarantined
- drift_events (schema changes)
- timestamp, batch_id, entity, and source_file
"""
import json
import time
from pathlib import Path
from typing import Any

from .config import AUDIT_LOG_PATH


def write_audit_record(
    batch_id: str,
    entity: str,
    source_file: str,
    rows_read: int,
    rows_written: int,
    duplicates_skipped: int,
    drift_events: list | None = None,
    quarantined: int = 0,
    status: str = "SUCCESS",
) -> dict[str, Any]:
    """Append one audit record. Keep this append-only and idempotent-safe
    (one record per actual run - do not silently overwrite history).
    """
    record = {
        "batch_id": batch_id,
        "entity": entity,
        "source_file": source_file,
        "rows_read": int(rows_read),
        "rows_written": int(rows_written),
        "duplicates_skipped": int(duplicates_skipped),
        "quarantined": int(quarantined),
        "drift_events": drift_events if drift_events is not None else [],
        "status": status,
        "logged_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

    log_file = Path(AUDIT_LOG_PATH)
    log_file.parent.mkdir(parents=True, exist_ok=True)

    with open(log_file, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, default=str) + "\n")

    return record


def read_audit_records(
    batch_id: str | None = None,
    entity: str | None = None,
) -> list[dict[str, Any]]:
    """Read audit log records, optionally filtered by batch_id and/or entity."""
    log_file = Path(AUDIT_LOG_PATH)
    if not log_file.exists():
        return []

    records = []
    with open(log_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            if batch_id and rec.get("batch_id") != batch_id:
                continue
            if entity and rec.get("entity") != entity:
                continue
            records.append(rec)
    return records
