"""Tests for src/schema_utils.py (Task 4 and Task 6)."""
import pytest
from pyspark.sql import functions as F

from src.config import BATCHES
from src.pipeline import run_batch


def test_batch_3_loyalty_tier_detected_and_backfilled(spark, tmp_path, capsys):
    batch_1 = next(batch for batch in BATCHES if batch["batch_id"] == "batch_1")
    batch_3 = next(batch for batch in BATCHES if batch["batch_id"] == "batch_3")
    bronze_dir = tmp_path / "bronze"
    batch_1_source = batch_1["entities"]["customers"]
    batch_3_source = batch_3["entities"]["customers"]
    batch_1_count = spark.read.option("header", True).csv(
        str(batch_1_source)
    ).count()
    batch_3_df = spark.read.option("header", True).csv(str(batch_3_source))
    batch_3_values = {
        (row.customer_id, row.loyalty_tier)
        for row in batch_3_df.select("customer_id", "loyalty_tier").collect()
    }

    run_batch(spark, batch_1["batch_id"], bronze_dir=bronze_dir)
    bronze_path = bronze_dir / "bronze_customers"
    baseline = spark.read.parquet(str(bronze_path))
    assert baseline.count() == batch_1_count
    assert "loyalty_tier" not in baseline.columns

    assert run_batch(spark, batch_3["batch_id"], bronze_dir=bronze_dir) == {
        "customers": len(batch_3_values)
    }

    customers = spark.read.parquet(str(bronze_path))
    assert "loyalty_tier" in customers.columns
    assert customers.count() == batch_1_count + len(batch_3_values)

    old_rows = customers.filter(F.col("batch_id") == batch_1["batch_id"])
    new_rows = customers.filter(F.col("batch_id") == batch_3["batch_id"])
    assert old_rows.count() == batch_1_count
    assert old_rows.filter(F.col("loyalty_tier").isNotNull()).count() == 0
    assert new_rows.count() == len(batch_3_values)
    assert new_rows.filter(F.col("loyalty_tier").isNull()).count() == 0
    assert {
        (row.customer_id, row.loyalty_tier)
        for row in new_rows.select("customer_id", "loyalty_tier").collect()
    } == batch_3_values
    assert (
        customers.groupBy("customer_id")
        .count()
        .filter("count > 1")
        .limit(1)
        .count()
        == 0
    )

    drift_log = capsys.readouterr().out
    assert "schema drift detected" in drift_log
    assert batch_3["batch_id"] in drift_log
    assert "loyalty_tier" in drift_log


def test_batch_4_second_drift_preserves_loyalty_tier(spark, tmp_path, capsys):
    batch_1 = next(batch for batch in BATCHES if batch["batch_id"] == "batch_1")
    batch_3 = next(batch for batch in BATCHES if batch["batch_id"] == "batch_3")
    batch_4 = next(batch for batch in BATCHES if batch["batch_id"] == "batch_4")
    bronze_dir = tmp_path / "bronze"

    batch_1_count = spark.read.option("header", True).csv(
        str(batch_1["entities"]["customers"])
    ).count()
    batch_3_df = spark.read.option("header", True).csv(
        str(batch_3["entities"]["customers"])
    )
    batch_4_df = spark.read.option("header", True).csv(
        str(batch_4["entities"]["customers"])
    )
    batch_3_values = {
        (row.customer_id, row.loyalty_tier)
        for row in batch_3_df.select("customer_id", "loyalty_tier").collect()
    }
    batch_4_values = {
        (row.customer_id, row.loyalty_tier, row.marketing_opt_in)
        for row in batch_4_df.select(
            "customer_id", "loyalty_tier", "marketing_opt_in"
        ).collect()
    }

    run_batch(spark, batch_1["batch_id"], bronze_dir=bronze_dir)
    run_batch(spark, batch_3["batch_id"], bronze_dir=bronze_dir)
    bronze_path = bronze_dir / "bronze_customers"
    after_batch_3 = spark.read.parquet(str(bronze_path))
    assert "loyalty_tier" in after_batch_3.columns
    assert "marketing_opt_in" not in after_batch_3.columns

    assert run_batch(spark, batch_4["batch_id"], bronze_dir=bronze_dir) == {
        "customers": len(batch_4_values)
    }

    customers = spark.read.parquet(str(bronze_path))
    assert "loyalty_tier" in customers.columns
    assert "marketing_opt_in" in customers.columns
    assert customers.count() == (
        batch_1_count + len(batch_3_values) + len(batch_4_values)
    )

    batch_1_rows = customers.filter(F.col("batch_id") == batch_1["batch_id"])
    batch_3_rows = customers.filter(F.col("batch_id") == batch_3["batch_id"])
    batch_4_rows = customers.filter(F.col("batch_id") == batch_4["batch_id"])

    assert batch_1_rows.filter(
        F.col("loyalty_tier").isNotNull()
        | F.col("marketing_opt_in").isNotNull()
    ).count() == 0
    assert batch_3_rows.filter(
        F.col("loyalty_tier").isNull()
        | F.col("marketing_opt_in").isNotNull()
    ).count() == 0
    assert {
        (row.customer_id, row.loyalty_tier)
        for row in batch_3_rows.select("customer_id", "loyalty_tier").collect()
    } == batch_3_values
    assert batch_4_rows.filter(
        F.col("loyalty_tier").isNull() | F.col("marketing_opt_in").isNull()
    ).count() == 0
    assert {
        (row.customer_id, row.loyalty_tier, row.marketing_opt_in)
        for row in batch_4_rows.select(
            "customer_id", "loyalty_tier", "marketing_opt_in"
        ).collect()
    } == batch_4_values
    assert (
        customers.groupBy("customer_id")
        .count()
        .filter("count > 1")
        .limit(1)
        .count()
        == 0
    )

    drift_log = capsys.readouterr().out
    assert "schema drift detected" in drift_log
    assert batch_4["batch_id"] in drift_log
    assert "marketing_opt_in" in drift_log
