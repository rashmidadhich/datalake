"""Skill-driven evidence log and per-batch audit trail.

Writes a single append-only log under ``outputs/artifacts/pipeline_log.txt``
recording, per batch: entity, batch id, row counts, quarantine reasons, and any
skill gap identified/closed. This file is the machine-generated half of the
skill-driven evidence log; the narrative half lives in
``outputs/artifacts/skill_driven_evidence_log.md``.
"""

import datetime
from typing import Mapping

from . import config

LOG_FILE = config.ARTIFACTS_DIR / "pipeline_log.txt"


def log_event(entity: str, batch_id: str, message: str, level: str = "INFO") -> str:
    """Append one line to the pipeline log and echo it to stdout."""
    config.ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts}\t{level}\t[{batch_id}]\t{entity}\t{message}"
    with open(LOG_FILE, "a", encoding="utf-8") as fh:
        fh.write(line + "\n")
    print(line)
    return line


def log_batch(entity: str, batch_id: str, counts: Mapping[str, int], extra: str = "") -> str:
    """Log a batch's outcome counts in one line."""
    rendered = ", ".join(f"{k}={v}" for k, v in counts.items())
    message = f"counts: {rendered}" + (f" | {extra}" if extra else "")
    return log_event(entity, batch_id, message)


def reset_log() -> None:
    """Truncate the log so a fresh pipeline run starts from a clean slate."""
    config.ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    LOG_FILE.write_text("", encoding="utf-8")
