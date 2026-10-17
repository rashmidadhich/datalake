"""Task 5 / Task 6: SCD2 History Tracking - Plan Change.

Implements the modified ``.claude/skills/scd2_history/SKILL.md``. A plan change
closes the account's current open row (``valid_to = effective_date``,
``is_current = false``) and appends a new current row
(``valid_from = effective_date``, ``valid_to = null``, ``is_current = true``)
via read-close-append-write, not a MERGE.

Binding on the SCD2 grain (``entity_id``) rather than the raw ``account_id`` is
what lets a reused key stay a separate identity. The three guard cases from the
skill are all handled:

  * already applied (re-run): skip, so the batch is idempotent;
  * ``effective_date`` earlier than the current ``valid_from``: quarantine
    (``out_of_order_effective_date``) rather than write an inverted interval;
  * a stale stated prior value, or no open row: quarantine too.
"""

from typing import NamedTuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from . import config, ingest

_CARRY_COLS = ["customer_name", "email_masked", "signup_date", "source_system", "status"]


class Scd2Result(NamedTuple):
    updated_silver: DataFrame
    quarantine: DataFrame
    counts: dict


def apply_plan_change(change_events_df: DataFrame, current_silver_df: DataFrame,
                      batch_id: str = "batch_4_plan_change") -> Scd2Result:
    """Return ``(updated_silver_df, quarantine_df, counts)``."""
    grain = config.ACCOUNT_HISTORY_GRAIN
    tracked = config.ACCOUNT_TRACKED_ATTRIBUTE

    current_open = (
        current_silver_df
        .filter(F.col("is_current"))
        .select(
            F.col(grain).alias("cur_entity_id"),
            F.col("account_id").alias("cur_account_id"),
            F.col(tracked).alias("cur_plan_id"),
            F.col("valid_from").alias("cur_valid_from"),
            *[F.col(c) for c in _CARRY_COLS],
        )
    )

    events = change_events_df.select(
        "account_id", "old_plan_id", "new_plan_id", "effective_date"
    )
    ev = events.join(current_open, events["account_id"] == current_open["cur_account_id"], how="left")

    already_applied = (
        F.col("cur_plan_id").eqNullSafe(F.col("new_plan_id"))
        & (F.col("cur_valid_from") == F.col("effective_date"))
    )
    out_of_order = F.col("effective_date") < F.col("cur_valid_from")
    stale = ~F.col("old_plan_id").eqNullSafe(F.col("cur_plan_id"))
    no_open_row = F.col("cur_entity_id").isNull()
    quarantine_flag = ~already_applied & (out_of_order | stale | no_open_row)

    normal = ev.filter(~already_applied & ~quarantine_flag)
    quarantined = ev.filter(~already_applied & quarantine_flag)

    # Close the current open row of each normally-applied change.
    closes = normal.select(F.col("cur_entity_id").alias(grain), F.col("effective_date").alias("_eff"))
    closed = current_silver_df.join(closes, on=grain, how="left")
    closed = (
        closed
        .withColumn("valid_to",
                    F.when(F.col("_eff").isNotNull() & F.col("is_current"), F.col("_eff"))
                     .otherwise(F.col("valid_to")))
        .withColumn("is_current",
                    F.when(F.col("_eff").isNotNull() & F.col("is_current"), F.lit(False))
                     .otherwise(F.col("is_current")))
        .drop("_eff")
    )

    # Append the new current row for each normally-applied change.
    new_current = normal.select(
        F.col("cur_entity_id").alias(grain),
        F.col("cur_account_id").alias("account_id"),
        F.col("customer_name"),
        F.col("email_masked"),
        F.col("signup_date"),
        F.col("source_system"),
        F.col("new_plan_id").alias("plan_id"),
        F.col("status"),
        F.col("effective_date").alias("valid_from"),
        F.lit(None).cast("date").alias("valid_to"),
        F.lit(True).alias("is_current"),
    )

    updated = closed.unionByName(new_current)

    q_rows = quarantined.select(
        F.col("account_id"),
        F.col("customer_name"),
        F.col("email_masked"),
        F.col("signup_date"),
        F.col("source_system"),
        F.col("new_plan_id").alias("plan_id"),
        F.col("status"),
    )
    quarantine_df = ingest.quarantine_accounts(
        q_rows, config.QUARANTINE_REASONS["OUT_OF_ORDER_EFFECTIVE_DATE"], batch_id
    )

    updated = ingest.materialize(updated)
    quarantine_df = ingest.materialize(quarantine_df)

    counts = {
        "applied": normal.count(),
        "quarantined": quarantine_df.count(),
        "already_applied": ev.filter(already_applied).count(),
    }
    return Scd2Result(updated, quarantine_df, counts)
