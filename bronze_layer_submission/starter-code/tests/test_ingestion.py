"""Tests for src/ingest.py (Task 1-2, Batch 1 baseline)."""
from pathlib import Path

from src.config import BATCHES
from src.pipeline import run_batch


def test_batch_1_loads_all_five_entities(spark, tmp_path):
    batch_id = "batch_1"
    batch = next(item for item in BATCHES if item["batch_id"] == batch_id)
    source_columns = {}
    expected_counts = {}
    for entity, source_path in batch["entities"].items():
        source_df = spark.read.option("header", True).csv(str(source_path))
        source_columns[entity] = source_df.columns
        expected_counts[entity] = source_df.count()

    output_dir = tmp_path / "bronze"

    assert run_batch(spark, batch_id, bronze_dir=output_dir) == expected_counts

    for entity, expected_count in expected_counts.items():
        output_path = output_dir / f"bronze_{entity}"
        assert output_path.is_dir()

        bronze_df = spark.read.parquet(str(output_path))
        assert bronze_df.count() == expected_count
        assert bronze_df.columns == source_columns[entity] + [
            "batch_id",
            "source_file",
            "ingestion_timestamp",
        ]
        assert bronze_df.filter(
            "batch_id IS NULL OR source_file IS NULL OR ingestion_timestamp IS NULL"
        ).count() == 0
        assert {
            row.batch_id for row in bronze_df.select("batch_id").distinct().collect()
        } == {batch_id}
        assert {
            row.source_file
            for row in bronze_df.select("source_file").distinct().collect()
        } == {Path(batch["entities"][entity]).name}
