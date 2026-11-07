"""Skill-driven evidence log and per-batch audit trail.

Writes an append-only markdown log under ``outputs/artifacts/pipeline_log.md``
recording, per batch: entity, batch id, row counts, quarantine reasons, and the
skill gap identified/closed for that batch. This file is the machine-generated
half of the skill-driven evidence log; the narrative half lives in
``outputs/artifacts/skill_driven_evidence_log.md``.
"""

import datetime
from typing import Mapping

from . import config

LOG_FILE = config.ARTIFACTS_DIR / "pipeline_log.md"

HEADER = """# Pipeline run log

Append-only log written by `src/audit.py` during a run of
`python -m src.pipeline`. One row per audited batch.

| Timestamp | Level | Batch | Entity | Detail |
| --- | --- | --- | --- | --- |
"""


def _escape(text: str) -> str:
    """Keep free text inside a single markdown table cell."""
    return text.replace("|", "\\|").replace("\n", " ")


def log_event(entity: str, batch_id: str, message: str, level: str = "INFO") -> str:
    """Append one table row to the markdown log and echo it to stdout."""
    config.ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as fh:
        fh.write(f"| {ts} | {level} | {batch_id} | {entity} | {_escape(message)} |\n")
    printable = f"{ts}\t{level}\t[{batch_id}]\t{entity}\t{message}"
    print(printable)
    return printable


def log_batch(entity: str, batch_id: str, counts: Mapping[str, int], extra: str = "") -> str:
    """Log a batch's outcome counts in one row."""
    rendered = ", ".join(f"{k}={v}" for k, v in counts.items())
    message = f"counts: {rendered}" + (f" | {extra}" if extra else "")
    return log_event(entity, batch_id, message)


def reset_log() -> None:
    """Recreate the log so a fresh pipeline run starts from a clean slate."""
    config.ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    LOG_FILE.write_text(HEADER, encoding="utf-8")
