"""Task 9: tests for Task 2 (Deduplication).

Unit tests exercise all three outcomes (retain / overwrite / quarantine) on
small in-memory frames. The integration test asserts the *actual* Batch 2
outcome, and documents the discrepancy with the assignment narrative.
"""

from datetime import date
from decimal import Decimal

from pyspark.sql import functions as F

from src import config, dedup

INVOICE_SCHEMA = (
    "invoice_id string, account_id string, invoice_date date, "
    "amount_due decimal(10,2), currency string, payment_status string"
)


def _frame(spark, rows):
    return spark.createDataFrame(rows, INVOICE_SCHEMA)


def _existing(spark):
    return _frame(spark, [
        ("INV-A", "ACC-1", date(2026, 1, 1), Decimal("10.00"), "USD", "Paid"),
        ("INV-B", "ACC-1", date(2026, 1, 2), Decimal("20.00"), "USD", "Paid"),
        ("INV-C", "ACC-1", date(2026, 1, 3), Decimal("30.00"), "USD", "Paid"),
    ])


def _incoming(spark):
    return _frame(spark, [
        ("INV-A", "ACC-1", date(2026, 1, 1), Decimal("10.00"), "USD", "Paid"),        # true duplicate
        ("INV-B", "ACC-1", date(2026, 1, 2), Decimal("25.00"), "USD", "Pending"),     # mutable-only -> overwrite
        ("INV-C", "ACC-2", date(2026, 1, 3), Decimal("30.00"), "USD", "Paid"),        # identity change -> quarantine
        ("INV-D", "ACC-3", date(2026, 1, 4), Decimal("40.00"), "USD", "Paid"),        # new key
    ])


def test_true_duplicate_is_skipped_not_inserted(spark):
    res = dedup.dedup_invoices(_incoming(spark), _existing(spark))
    assert res.counts["retained_unchanged"] == 1
    assert res.counts["new"] == 1  # only INV-D
    keys_inserted = {r["invoice_id"] for r in res.new.collect()}
    assert "INV-A" not in keys_inserted


def test_mutable_only_change_is_an_overwrite_not_a_duplicate_or_conflict(spark):
    res = dedup.dedup_invoices(_incoming(spark), _existing(spark))
    assert res.counts["overwritten"] == 1
    overwritten = {r["invoice_id"] for r in res.overwrite.collect()}
    assert overwritten == {"INV-B"}
    # A mutable-only change must NOT be quarantined.
    assert {r["invoice_id"] for r in res.quarantine.collect()} == {"INV-C"}


def test_conflicting_key_is_quarantined_not_inserted_or_overwritten(spark):
    res = dedup.dedup_invoices(_incoming(spark), _existing(spark))
    assert res.counts["quarantined"] == 1
    rows = res.quarantine.collect()
    assert rows[0]["invoice_id"] == "INV-C"


def test_conflicting_key_quarantine_reason_is_correct(spark):
    res = dedup.dedup_invoices(_incoming(spark), _existing(spark))
    reasons = {r["reason"] for r in res.quarantine.collect()}
    assert reasons == {config.QUARANTINE_REASONS["CONFLICTING_KEY"]}


def test_batch2_actual_outcome(spark, pipeline_run, silver):
    """The provided Batch 2 CSV contains 2 true duplicates and 4 rows that
    disagree with the baseline on an identity-defining column (account_id and
    invoice_date) — i.e. 0 legitimate updates. This differs from the assignment
    narrative (which describes 4 duplicates + 1 update + 1 conflict), so the
    test asserts what the data actually contains rather than the narrative.
    """
    res = pipeline_run["batch_2"]
    assert res["retained_unchanged"] == 2
    assert res["overwritten"] == 0
    assert res["quarantined"] == 4

    # Silver invoices are unchanged from Batch 1 (no insert, no update).
    assert silver["invoices"].count() == 480

    q = silver["invoices_quarantine"]
    assert q.count() == 4
    assert {r["reason"] for r in q.collect()} == {config.QUARANTINE_REASONS["CONFLICTING_KEY"]}
    assert {r["invoice_id"] for r in q.collect()} == {
        "INV-00113", "INV-00220", "INV-00301", "INV-00378"
    }
    # A retained duplicate must never appear in quarantine.
    assert q.filter(F.col("invoice_id") == "INV-00449").count() == 0
