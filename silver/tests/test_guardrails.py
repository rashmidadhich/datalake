"""Task 9: tests for Task 8 (Pipeline guardrails).

Covers all five invariants: row-count conservation, controlled quarantine
reasons, masked-field integrity, idempotent re-runs, and non-destructive
writes. Each assertion-based guardrail is shown to be able to fail.
"""

import pytest
from pyspark.sql import functions as F

from src import config, guardrails, ingest, pipeline


# --------------------------------------------------------------------------- #
# 1. Row-count conservation
# --------------------------------------------------------------------------- #
def test_conservation_passes_when_every_row_is_accounted_for():
    assert guardrails.assert_row_count_conservation(6, silver_count=0, quarantine_count=4, true_dup_count=2)
    assert guardrails.assert_row_count_conservation(6, silver_count=2, quarantine_count=4, true_dup_count=0)


def test_conservation_fails_when_a_row_vanishes():
    with pytest.raises(AssertionError, match="conservation"):
        guardrails.assert_row_count_conservation(6, silver_count=0, quarantine_count=4, true_dup_count=1)


# --------------------------------------------------------------------------- #
# 2. Controlled quarantine reason vocabulary
# --------------------------------------------------------------------------- #
def test_every_quarantine_reason_is_from_the_vocabulary(silver):
    guardrails.assert_valid_quarantine_reason(silver["accounts_quarantine"])
    guardrails.assert_valid_quarantine_reason(silver["invoices_quarantine"])
    observed = {r["reason"] for r in silver["accounts_quarantine"].collect()} | {
        r["reason"] for r in silver["invoices_quarantine"].collect()
    }
    assert observed <= set(config.QUARANTINE_REASONS.values())


def test_vocabulary_check_can_fail(spark):
    bad = spark.createDataFrame([("made_up_reason",), (None,)], "reason string")
    with pytest.raises(AssertionError, match="vocabulary"):
        guardrails.assert_valid_quarantine_reason(bad)


# --------------------------------------------------------------------------- #
# 3. Masked-field passthrough integrity
# --------------------------------------------------------------------------- #
def test_bronze_masked_emails_are_unchanged_in_silver(spark, pipeline_run, silver):
    bronze = ingest.read_accounts_raw(spark, config.BATCH_FILES["batch_1_billing_accounts"]).select(
        "account_id", "email_masked")
    original_silver = silver["accounts"].filter(
        F.col("entity_id") == F.col("account_id")).select("account_id", "email_masked")
    assert guardrails.assert_masked_field_unchanged(bronze, original_silver, "account_id")


def test_masked_integrity_check_can_fail(spark):
    before = spark.createDataFrame([("ACC-1", "a***@x.com")], "account_id string, email_masked string")
    after = spark.createDataFrame([("ACC-1", "a@x.com")], "account_id string, email_masked string")
    with pytest.raises(AssertionError, match="masked-field integrity"):
        guardrails.assert_masked_field_unchanged(before, after, "account_id")


# --------------------------------------------------------------------------- #
# 4. Idempotent re-runs
# --------------------------------------------------------------------------- #
def test_replaying_batches_2_to_5_changes_nothing(spark, pipeline_run, snapshot):
    before = snapshot()

    pipeline.run_batch_2(spark)
    pipeline.run_batch_3(spark)
    pipeline.run_batch_4(spark)
    pipeline.run_batch_5(spark)

    assert snapshot() == before


# --------------------------------------------------------------------------- #
# 5. Non-destructive writes
# --------------------------------------------------------------------------- #
def test_earlier_batches_are_not_destroyed(spark, pipeline_run, silver):
    # Batch 1 baseline is fully preserved.
    assert silver["invoices"].count() == 480
    assert silver["plans"].count() == 6
    assert silver["accounts"].count() >= 100

    original_ids = {r["account_id"] for r in
                    ingest.read_accounts_raw(spark, config.BATCH_FILES["batch_1_billing_accounts"]).collect()}
    present_ids = {r["account_id"] for r in silver["accounts"].collect()}
    assert original_ids <= present_ids
