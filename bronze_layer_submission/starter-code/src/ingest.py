"""Batch ingestion entry point for a single entity/file (Task 1-2).

Read a source CSV and attach its ingestion metadata.
"""
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from .metadata import add_ingestion_metadata


def read_source_csv(spark: SparkSession, path: str) -> DataFrame:
    """Read one headered CSV while preserving source values as strings."""
    return (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .csv(path)
    )


def ingest_entity(
    spark: SparkSession,
    entity: str,
    source_path: str,
    batch_id: str,
) -> DataFrame:
    """Read one entity's CSV and tag it immediately with ingestion metadata."""
    raw_df = read_source_csv(spark, source_path)
    return add_ingestion_metadata(raw_df, batch_id, Path(source_path).name)
