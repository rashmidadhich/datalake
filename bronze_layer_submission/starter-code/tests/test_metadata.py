"""Tests for src/metadata.py (Task 2)."""
import pytest
from pyspark.sql.types import TimestampType

from src.metadata import add_ingestion_metadata


def test_metadata_columns_present_and_populated(spark):
    source = spark.createDataFrame(
        [("C-001", "regular"), ("C-002", "premium")],
        ["customer_id", "segment"],
    )

    tagged = add_ingestion_metadata(source, "batch_1", "customers.csv")

    assert tagged.columns == [
        "customer_id",
        "segment",
        "batch_id",
        "source_file",
        "ingestion_timestamp",
    ]
    assert tagged.select("customer_id", "segment").collect() == source.collect()
    assert tagged.filter(
        "batch_id IS NULL OR source_file IS NULL OR ingestion_timestamp IS NULL"
    ).count() == 0
    assert {
        row.batch_id for row in tagged.select("batch_id").distinct().collect()
    } == {"batch_1"}
    assert {
        row.source_file for row in tagged.select("source_file").distinct().collect()
    } == {"customers.csv"}
    assert isinstance(tagged.schema["ingestion_timestamp"].dataType, TimestampType)


def test_metadata_never_overwrites_a_source_column(spark):
    source = spark.createDataFrame([("source-value",)], ["batch_id"])

    with pytest.raises(ValueError, match="already contains metadata column.*batch_id"):
        add_ingestion_metadata(source, "batch_1", "customers.csv")
