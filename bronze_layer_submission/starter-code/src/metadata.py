"""Ingestion metadata tagging.

Scaffolding only - read .claude/skills/ingestion_metadata/SKILL.md before filling
this in (Task 2).
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


METADATA_COLUMNS = ("batch_id", "source_file", "ingestion_timestamp")


def add_ingestion_metadata(
    df: DataFrame,
    batch_id: str,
    source_file: str,
) -> DataFrame:
    """Attach batch_id, source_file and ingestion_timestamp to every row,
    preserving all original columns unchanged.

    Source columns that collide with metadata names are rejected rather than
    silently overwritten.
    """
    if not batch_id or not batch_id.strip():
        raise ValueError("batch_id must be a non-empty string")
    if not source_file or not source_file.strip():
        raise ValueError("source_file must be a non-empty string")

    collisions = set(METADATA_COLUMNS).intersection(df.columns)
    if collisions:
        names = ", ".join(sorted(collisions))
        raise ValueError(f"Source data already contains metadata column(s): {names}")

    return (
        df.withColumn("batch_id", F.lit(batch_id))
        .withColumn("source_file", F.lit(source_file))
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )
