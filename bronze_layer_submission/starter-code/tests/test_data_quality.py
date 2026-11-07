"""Tests for src/data_quality.py (Task 7 - data quality skill)."""
from pyspark.sql import functions as F

from src.config import BATCHES
from src.ingest import ingest_entity
from src.pipeline import run_batch


def _customer_key_counts(spark, source_path):
    source = spark.read.option("header", True).csv(str(source_path))
    key = F.trim(F.col("customer_id").cast("string"))
    invalid = F.col("customer_id").isNull() | (key == "")
    return source, source.filter(~invalid).count(), source.filter(invalid).count()


def test_batch_5_null_customer_id_is_quarantined_not_dropped(spark, tmp_path):
    batch_1 = next(batch for batch in BATCHES if batch["batch_id"] == "batch_1")
    batch_5 = next(batch for batch in BATCHES if batch["batch_id"] == "batch_5")
    bronze_dir = tmp_path / "bronze"
    quarantine_dir = tmp_path / "quarantine"
    source_path = batch_5["entities"]["customers"]
    source, valid_source_count, invalid_source_count = _customer_key_counts(
        spark, source_path
    )
    assert invalid_source_count > 0
    assert valid_source_count > 0

    run_batch(spark, batch_1["batch_id"], bronze_dir=bronze_dir)
    customers_path = bronze_dir / "bronze_customers"
    rows_before = spark.read.parquet(str(customers_path)).count()

    assert run_batch(
        spark,
        batch_5["batch_id"],
        bronze_dir=bronze_dir,
        quarantine_dir=quarantine_dir,
    ) == {"customers": valid_source_count}

    customers = spark.read.parquet(str(customers_path))
    quarantine_path = quarantine_dir / "customers" / batch_5["batch_id"]
    quarantined = spark.read.parquet(str(quarantine_path))
    assert customers.count() == rows_before + valid_source_count
    assert quarantined.count() == invalid_source_count
    assert "rejection_reason" in quarantined.columns
    assert quarantined.filter(
        F.col("rejection_reason").isNull()
        | (F.trim(F.col("rejection_reason")) == "")
    ).count() == 0
    assert quarantined.filter(
        F.col("rejection_reason") == "customer_id is null or blank"
    ).count() == invalid_source_count
    assert quarantined.filter(
        F.col("batch_id").isNull()
        | F.col("source_file").isNull()
        | F.col("ingestion_timestamp").isNull()
    ).count() == 0
    assert customers.filter(
        F.col("customer_id").isNull()
        | (F.trim(F.col("customer_id")) == "")
    ).count() == 0

    original_columns = source.columns
    expected_quarantine_rows = {
        tuple(row)
        for row in source.filter(
            F.col("customer_id").isNull()
            | (F.trim(F.col("customer_id")) == "")
        ).select(*original_columns).collect()
    }
    actual_quarantine_rows = {
        tuple(row) for row in quarantined.select(*original_columns).collect()
    }
    assert actual_quarantine_rows == expected_quarantine_rows


def test_batch_5_does_not_fail_the_whole_job(spark, tmp_path):
    batch_1 = next(batch for batch in BATCHES if batch["batch_id"] == "batch_1")
    batch_5 = next(batch for batch in BATCHES if batch["batch_id"] == "batch_5")
    bronze_dir = tmp_path / "bronze"
    quarantine_dir = tmp_path / "quarantine"
    customer_target = bronze_dir / "bronze_customers"

    existing = ingest_entity(
        spark,
        "customers",
        str(batch_1["entities"]["customers"]),
        batch_1["batch_id"],
    )
    existing = (
        existing.withColumn(
            "loyalty_tier",
            F.when(F.col("customer_id") == "C-0006", F.lit("Silver")),
        )
        .withColumn(
            "marketing_opt_in",
            F.when(F.col("customer_id") == "C-0007", F.lit("Yes")),
        )
    )
    existing.write.mode("overwrite").parquet(str(customer_target))
    rows_before = existing.count()

    _, valid_source_count, invalid_source_count = _customer_key_counts(
        spark, batch_5["entities"]["customers"]
    )
    assert run_batch(
        spark,
        batch_5["batch_id"],
        bronze_dir=bronze_dir,
        quarantine_dir=quarantine_dir,
    ) == {"customers": valid_source_count}

    customers = spark.read.parquet(str(customer_target))
    assert customers.count() == rows_before + valid_source_count
    assert "loyalty_tier" in customers.columns
    assert "marketing_opt_in" in customers.columns
    assert customers.filter(
        (F.col("customer_id") == "C-0006")
        & (F.col("loyalty_tier") == "Silver")
    ).count() == 1
    assert customers.filter(
        (F.col("customer_id") == "C-0007")
        & (F.col("marketing_opt_in") == "Yes")
    ).count() == 1
    batch_5_rows = customers.filter(F.col("batch_id") == batch_5["batch_id"])
    assert batch_5_rows.filter(
        F.col("loyalty_tier").isNotNull()
        | F.col("marketing_opt_in").isNotNull()
    ).count() == 0
    assert spark.read.parquet(
        str(quarantine_dir / "customers" / batch_5["batch_id"])
    ).count() == invalid_source_count


def test_intra_batch_duplicate_detection(spark):
    from src.data_quality import validate_intra_batch_duplicates

    df = spark.createDataFrame(
        [
            ("C-001", "Alice"),
            ("C-002", "Bob"),
            ("C-001", "Alice Duplicate"),
        ],
        ["customer_id", "name"],
    )

    valid_df, quarantined_df = validate_intra_batch_duplicates(df, "customer_id")
    assert valid_df.count() == 2
    assert quarantined_df.count() == 1
    assert "rejection_reason" in quarantined_df.columns
    assert quarantined_df.first().rejection_reason == "duplicate customer_id in incoming batch"
    assert quarantined_df.first().name == "Alice Duplicate"
