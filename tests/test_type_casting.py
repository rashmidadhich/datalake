"""Task 9: tests for Task 3 (Type Casting).

Unit tests build small in-memory batches; the integration tests assert the real
Batch 3 outcome, including the format-drift fallback used by Batch 5.
"""

from datetime import date

from pyspark.sql import functions as F

from src import config, ingest, type_casting


def _raw_accounts(spark, rows):
    return spark.createDataFrame(rows, ingest.ACCOUNT_COLS + [ingest.CORRUPT_COL])


def test_mm_dd_yyyy_is_cast_and_plan_id_is_normalized(spark):
    raw = _raw_accounts(spark, [
        ("ACC-9001", "Sara Haddad", "s***@t.com", "01/20/2026", "platform_v2", "plan-gold", "active", None),
    ])
    casted, quarantine = type_casting.cast_billing_accounts(raw)

    assert quarantine.count() == 0
    row = casted.collect()[0]
    assert row["signup_date"] == date(2026, 1, 20)
    assert row["plan_id"] == "PLAN-GOLD"


def test_format_drift_uses_the_matching_known_pattern(spark):
    """A platform_v2 row written in legacy format must still cast (Batch 5)."""
    raw = _raw_accounts(spark, [
        ("ACC-9002", "Priya Okoye", "p***@t.com", "2026-07-23", "platform_v2", "PLAN-STUDENT", "active", None),
    ])
    casted, quarantine = type_casting.cast_billing_accounts(raw)

    assert quarantine.count() == 0
    assert casted.collect()[0]["signup_date"] == date(2026, 7, 23)


def test_unrecognized_format_is_quarantined_not_guessed(spark):
    raw = _raw_accounts(spark, [
        ("ACC-9003", "Nina Costa", "n***@t.com", "December 11, 2025", "platform_v2", "PLAN-GOLD", "active", None),
    ])
    casted, quarantine = type_casting.cast_billing_accounts(raw)

    assert casted.count() == 0
    rows = quarantine.collect()
    assert len(rows) == 1
    assert rows[0]["account_id"] == "ACC-9003"
    assert rows[0]["reason"] == config.QUARANTINE_REASONS["UNRECOGNIZED_FORMAT"]


def test_batch_3_gains_three_rows_and_quarantines_one(spark, pipeline_run, silver):
    batch = pipeline_run["batch_3"]
    assert batch["inserted"] == 3
    assert batch["quarantined"] == 1

    q = silver["accounts_quarantine"].filter(F.col("batch_id") == "batch_3_accounts")
    assert q.count() == 1
    assert q.collect()[0]["account_id"] == "ACC-0104"
    assert q.collect()[0]["reason"] == config.QUARANTINE_REASONS["UNRECOGNIZED_FORMAT"]

    # The three accepted rows are present with normalized casing.
    batch_accounts = silver["accounts"].filter(F.col("account_id").isin(["ACC-0101", "ACC-0102", "ACC-0103"]))
    assert batch_accounts.count() == 3
    plans = {r["plan_id"] for r in batch_accounts.collect()}
    assert plans == {"PLAN-GOLD", "PLAN-STUDENT", "PLAN-PLATINUM"}
