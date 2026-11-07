"""Orchestrates Batch 1 through Batch 5 end to end.

Per the assignment's Required Architecture:

    Read bronze batch -> cast/normalize types -> resolve entity identity ->
    deduplicate against existing silver -> apply SCD2 for tracked changes ->
    write silver / quarantine

Pipeline guardrails are checked after every batch, and a per-batch row is
written to ``outputs/artifacts/pipeline_log.md``.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import (
    audit,
    config,
    dedup,
    entity_resolution,
    guardrails,
    ingest,
    scd2_writer,
    type_casting,
)
from .spark_session import get_spark


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def add_history_columns(df: DataFrame) -> DataFrame:
    """Attach the SCD2 columns. ``entity_id`` must already be present."""
    return (
        df.withColumn("valid_from", F.col("signup_date"))
        .withColumn("valid_to", F.lit(None).cast("date"))
        .withColumn("is_current", F.lit(True))
    )


def _apply_invoice_overwrites(spark: SparkSession, overwrites: DataFrame) -> None:
    """Replace mutable columns on matched invoice rows in place (no history)."""
    if overwrites.limit(1).count() == 0:
        return
    existing = ingest.read_table(spark, config.SILVER_INVOICES)
    prefixed = overwrites.select(
        F.col(config.INVOICE_KEY),
        *[F.col(c).alias("o_" + c) for c in config.INVOICE_MUTABLE_COLUMNS],
    )
    joined = existing.join(prefixed, on=config.INVOICE_KEY, how="left")
    updated = joined.select(
        F.col("invoice_id"),
        F.col("account_id"),
        F.col("invoice_date"),
        F.coalesce("o_amount_due", F.col("amount_due")).alias("amount_due"),
        F.col("currency"),
        F.coalesce("o_payment_status", F.col("payment_status")).alias("payment_status"),
    )
    ingest.write_table(updated, config.SILVER_INVOICES, checkpoint=True)


def _insert_new_accounts(spark: SparkSession, casted_accounts: DataFrame, batch_id: str):
    """Resolve identity, then insert only accounts not already in silver.

    Returns ``(inserted, retained_unchanged, er_counts)``.
    """
    existing = ingest.read_table(spark, config.SILVER_BILLING_ACCOUNTS)
    er = entity_resolution.resolve_entities(casted_accounts, existing, batch_id)

    existing_ids = existing.select(config.ACCOUNT_HISTORY_GRAIN).distinct()
    fresh = (
        er.resolved
        .join(existing_ids, on=config.ACCOUNT_HISTORY_GRAIN, how="left_anti")
        .drop("identity_status")
    )
    fresh = add_history_columns(fresh)
    fresh = ingest.materialize(fresh)

    inserted = fresh.count()
    retained_unchanged = er.resolved.count() - inserted

    ingest.merge_append(spark, fresh, config.SILVER_BILLING_ACCOUNTS)

    silver_after = ingest.read_table(spark, config.SILVER_BILLING_ACCOUNTS)
    guardrails.assert_masked_field_unchanged(fresh, silver_after, config.ACCOUNT_HISTORY_GRAIN)
    return inserted, retained_unchanged, er.counts


# --------------------------------------------------------------------------- #
# Batches
# --------------------------------------------------------------------------- #
def load_batch_1(spark: SparkSession) -> dict:
    accounts = (
        ingest.read_baseline_accounts(spark)
        .withColumn(config.ACCOUNT_HISTORY_GRAIN, F.col(config.ACCOUNT_KEY))
    )
    accounts = add_history_columns(accounts)
    plans = ingest.read_plan_catalog(spark)
    invoices = ingest.read_baseline_invoices(spark)

    ingest.write_table(accounts, config.SILVER_BILLING_ACCOUNTS)
    ingest.write_table(plans, config.SILVER_PLAN_CATALOG)
    ingest.write_table(invoices, config.SILVER_INVOICES)

    counts = {
        "billing_accounts": accounts.count(),
        "plan_catalog": plans.count(),
        "invoices": invoices.count(),
    }
    audit.log_batch("baseline", "batch_1", counts,
                    "initial full load (valid_from=signup_date, is_current=true)")
    assert counts == {"billing_accounts": 100, "plan_catalog": 6, "invoices": 480}, counts
    return counts


def run_batch_2(spark: SparkSession):
    raw = ingest.read_invoices_raw(spark, config.BATCH_FILES["batch_2_invoices"])
    typed = type_casting.cast_invoices(raw)
    existing = ingest.read_table(spark, config.SILVER_INVOICES)

    res = dedup.dedup_invoices(typed, existing, "batch_2_invoices")
    _apply_invoice_overwrites(spark, res.overwrite)
    ingest.merge_append(spark, res.new, config.SILVER_INVOICES)
    ingest.append_quarantine(spark, res.quarantine, config.QUARANTINE_INVOICES)

    guardrails.assert_row_count_conservation(
        raw.count(), res.counts["new"], res.counts["quarantined"],
        res.counts["retained_unchanged"], res.counts["overwritten"], "batch_2_invoices")
    guardrails.assert_valid_quarantine_reason(res.quarantine)

    silver = ingest.read_table(spark, config.SILVER_INVOICES).count()
    audit.log_batch("invoices", "batch_2_invoices", res.counts,
                    f"silver_invoices={silver}")
    return res.counts, silver


def run_batch_3(spark: SparkSession):
    raw = ingest.read_accounts_raw(spark, config.BATCH_FILES["batch_3_accounts"])
    casted, quarantine = type_casting.cast_billing_accounts(raw, "batch_3_accounts")
    inserted, unchanged, er_counts = _insert_new_accounts(spark, casted, "batch_3_accounts")
    ingest.append_quarantine(spark, quarantine, config.QUARANTINE_BILLING_ACCOUNTS)

    guardrails.assert_row_count_conservation(
        raw.count(), inserted, quarantine.count(), unchanged, 0, "batch_3_accounts")
    guardrails.assert_valid_quarantine_reason(quarantine)

    silver = ingest.read_table(spark, config.SILVER_BILLING_ACCOUNTS).count()
    audit.log_batch("billing_accounts", "batch_3_accounts",
                    {"inserted": inserted, "quarantined": quarantine.count(),
                     "retained_unchanged": unchanged},
                    f"format-aware cast + plan_id casing; silver_billing_accounts={silver}")
    return {"inserted": inserted, "quarantined": quarantine.count(),
            "retained_unchanged": unchanged}, silver


def run_batch_4(spark: SparkSession):
    events = ingest.read_plan_change_batch(spark, config.BATCH_FILES["batch_4_plan_change"])
    existing = ingest.read_table(spark, config.SILVER_BILLING_ACCOUNTS)

    res = scd2_writer.apply_plan_change(events, existing, "batch_4_plan_change")
    ingest.write_table(res.updated_silver, config.SILVER_BILLING_ACCOUNTS, checkpoint=True)
    ingest.append_quarantine(spark, res.quarantine, config.QUARANTINE_BILLING_ACCOUNTS)

    guardrails.assert_row_count_conservation(
        events.count(), res.counts["applied"], res.counts["quarantined"],
        res.counts["already_applied"], 0, "batch_4_plan_change")
    guardrails.assert_valid_quarantine_reason(res.quarantine)

    silver_after = ingest.read_table(spark, config.SILVER_BILLING_ACCOUNTS)
    multi_open = (
        silver_after.filter(F.col("is_current"))
        .groupBy(config.ACCOUNT_HISTORY_GRAIN).count()
        .filter(F.col("count") > 1).count()
    )
    if multi_open:
        raise AssertionError(f"SCD2 invariant violated: {multi_open} identities have more than one open row")

    audit.log_batch("billing_accounts", "batch_4_plan_change", res.counts,
                    f"close-and-append; silver_billing_accounts={silver_after.count()}")
    return res.counts, silver_after.count()


def run_batch_5(spark: SparkSession):
    raw = ingest.read_accounts_raw(spark, config.BATCH_FILES["batch_5_reused_accounts"])
    casted, quarantine = type_casting.cast_billing_accounts(raw, "batch_5_reused_accounts")
    inserted, unchanged, er_counts = _insert_new_accounts(spark, casted, "batch_5_reused_accounts")
    ingest.append_quarantine(spark, quarantine, config.QUARANTINE_BILLING_ACCOUNTS)

    guardrails.assert_row_count_conservation(
        raw.count(), inserted, quarantine.count(), unchanged, 0, "batch_5_reused_accounts")
    guardrails.assert_valid_quarantine_reason(quarantine)

    silver = ingest.read_table(spark, config.SILVER_BILLING_ACCOUNTS).count()
    audit.log_batch("billing_accounts", "batch_5_reused_accounts",
                    {"inserted": inserted, "quarantined": quarantine.count(),
                     "retained_unchanged": unchanged, **er_counts},
                    f"entity resolution: reused keys -> new identities; silver_billing_accounts={silver}")
    return {"inserted": inserted, "quarantined": quarantine.count(),
            "retained_unchanged": unchanged, **er_counts}, silver


# --------------------------------------------------------------------------- #
# Entry points
# --------------------------------------------------------------------------- #
def run_all(spark: SparkSession) -> dict:
    """Run Batch 1 through Batch 5 in order and return a summary."""
    summary = {
        "batch_1": load_batch_1(spark),
        "batch_2": run_batch_2(spark)[0],
        "batch_3": run_batch_3(spark)[0],
        "batch_4": run_batch_4(spark)[0],
        "batch_5": run_batch_5(spark)[0],
    }
    summary["final"] = {
        "silver_billing_accounts": ingest.read_table(spark, config.SILVER_BILLING_ACCOUNTS).count(),
        "silver_plan_catalog": ingest.read_table(spark, config.SILVER_PLAN_CATALOG).count(),
        "silver_invoices": ingest.read_table(spark, config.SILVER_INVOICES).count(),
        "billing_accounts_quarantine": ingest.read_table(spark, config.QUARANTINE_BILLING_ACCOUNTS).count(),
        "invoices_quarantine": ingest.read_table(spark, config.QUARANTINE_INVOICES).count(),
    }
    audit.log_batch("pipeline", "summary", summary["final"], "final table counts")
    return summary


def main() -> None:
    spark = get_spark()
    audit.reset_log()
    try:
        run_all(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
