"""Tests for src/idempotent_writer.py (Task 3)."""
from pathlib import Path

from src.config import BATCHES, ENTITY_BUSINESS_KEYS
from src.pipeline import run_batch


def test_batch_1_rerun_leaves_row_count_unchanged(spark, tmp_path):
    batch_id = "batch_1"
    batch = next(item for item in BATCHES if item["batch_id"] == batch_id)
    bronze_dir = tmp_path / "bronze"
    expected_counts = {
        entity: spark.read.option("header", True).csv(str(source_path)).count()
        for entity, source_path in batch["entities"].items()
    }

    assert run_batch(spark, batch_id, bronze_dir=bronze_dir) == expected_counts
    counts_after_first_run = {
        entity: spark.read.parquet(str(bronze_dir / f"bronze_{entity}")).count()
        for entity in batch["entities"]
    }

    assert run_batch(spark, batch_id, bronze_dir=bronze_dir) == {
        entity: 0 for entity in batch["entities"]
    }

    for entity in batch["entities"]:
        bronze_df = spark.read.parquet(str(bronze_dir / f"bronze_{entity}"))
        assert bronze_df.count() == counts_after_first_run[entity]
        business_key = ENTITY_BUSINESS_KEYS[entity]
        assert (
            bronze_df.groupBy(business_key)
            .count()
            .filter("count > 1")
            .limit(1)
            .count()
            == 0
        )


def test_batch_2_redelivered_rows_are_not_duplicated(spark, tmp_path):
    batch_1 = next(item for item in BATCHES if item["batch_id"] == "batch_1")
    batch_2 = next(item for item in BATCHES if item["batch_id"] == "batch_2")
    bronze_dir = tmp_path / "bronze"

    run_batch(spark, batch_1["batch_id"], bronze_dir=bronze_dir)
    sales_path = bronze_dir / "bronze_sales_transactions"
    sales_before = spark.read.parquet(str(sales_path))
    rows_before = sales_before.count()
    business_key = ENTITY_BUSINESS_KEYS["sales_transactions"]
    existing_keys = {
        row[business_key]
        for row in sales_before.select(business_key).distinct().collect()
    }
    redelivered_path = batch_2["entities"]["sales_transactions"]
    redelivered_keys = {
        row[business_key]
        for row in spark.read.option("header", True)
        .csv(str(redelivered_path))
        .select(business_key)
        .distinct()
        .collect()
    }
    assert redelivered_keys
    assert redelivered_keys.issubset(existing_keys)

    assert run_batch(spark, batch_2["batch_id"], bronze_dir=bronze_dir) == {
        "sales_transactions": 0
    }

    sales_after = spark.read.parquet(str(sales_path))
    assert sales_after.count() == rows_before
    assert (
        sales_after.groupBy(business_key)
        .count()
        .filter("count > 1")
        .limit(1)
        .count()
        == 0
    )
