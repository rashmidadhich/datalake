"""Idempotent bronze write logic and duplicate checking.

Read .claude/skills/Idempotency/SKILL.md before changing this logic (Task 3).
Features:
- Duplicate key detection against existing Bronze data (cross-batch idempotency).
- Intra-batch duplicate key identification.
- Atomic append and replacement writes to protect Bronze table integrity.
"""
import shutil
import uuid
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def read_existing_keys(
    spark: SparkSession,
    bronze_path: str,
    business_key: str,
) -> DataFrame | None:
    """Return distinct stored business keys, or None before the first write."""
    if not Path(bronze_path).exists():
        return None

    return spark.read.parquet(bronze_path).select(business_key).distinct()


def dedup_against_existing(
    incoming_df: DataFrame,
    existing_keys: DataFrame | None,
    business_key: str,
) -> DataFrame:
    """Keep incoming rows whose business key is not already in Bronze."""
    if existing_keys is None:
        return incoming_df

    return incoming_df.join(
        existing_keys.select(business_key).distinct(),
        on=business_key,
        how="left_anti",
    )


def check_idempotency(
    incoming_df: DataFrame,
    existing_keys: DataFrame | None,
    business_key: str,
) -> dict[str, Any]:
    """Analyze incoming batch against existing keys for idempotency & duplicate checks.

    Returns diagnostic metrics:
    - total_incoming_rows
    - distinct_incoming_keys
    - intra_batch_duplicates
    - existing_keys_found
    - new_keys_to_write
    - is_full_redelivery (True if all incoming keys already exist)
    """
    total_incoming = incoming_df.count()
    if total_incoming == 0:
        return {
            "total_incoming_rows": 0,
            "distinct_incoming_keys": 0,
            "intra_batch_duplicates": 0,
            "existing_keys_found": 0,
            "new_keys_to_write": 0,
            "is_full_redelivery": False,
        }

    distinct_keys_count = incoming_df.select(business_key).distinct().count()
    intra_batch_dups = total_incoming - distinct_keys_count

    if existing_keys is None:
        return {
            "total_incoming_rows": total_incoming,
            "distinct_incoming_keys": distinct_keys_count,
            "intra_batch_duplicates": intra_batch_dups,
            "existing_keys_found": 0,
            "new_keys_to_write": total_incoming,
            "is_full_redelivery": False,
        }

    # Intersect with existing keys
    already_seen = (
        incoming_df.select(business_key)
        .distinct()
        .join(existing_keys.select(business_key).distinct(), on=business_key, how="inner")
        .count()
    )
    new_distinct = distinct_keys_count - already_seen

    return {
        "total_incoming_rows": total_incoming,
        "distinct_incoming_keys": distinct_keys_count,
        "intra_batch_duplicates": intra_batch_dups,
        "existing_keys_found": already_seen,
        "new_keys_to_write": new_distinct,
        "is_full_redelivery": (already_seen > 0 and new_distinct == 0),
    }


def write_bronze(df: DataFrame, bronze_path: str) -> None:
    """Append new rows without replacing previously landed Bronze data."""
    df.write.mode("append").parquet(bronze_path)


def replace_bronze(df: DataFrame, bronze_path: str) -> None:
    """Safely replace a local Parquet dataset after schema evolution.

    Materialize the combined data beside the current target before moving the
    old directory, so Spark never reads from and overwrites the same path.
    """
    target = Path(bronze_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    token = uuid.uuid4().hex
    staging = target.with_name(f".{target.name}.{token}.staging")
    backup = target.with_name(f".{target.name}.{token}.backup")
    moved_existing = False

    try:
        df.write.mode("error").parquet(str(staging))
        if target.exists():
            target.rename(backup)
            moved_existing = True

        try:
            staging.rename(target)
        except Exception:
            if moved_existing:
                backup.rename(target)
                moved_existing = False
            raise

        if moved_existing:
            shutil.rmtree(backup)
    finally:
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)
