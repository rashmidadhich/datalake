"""Task 3: Type Casting - Inconsistent Formats.

Implements the guidance in ``.claude/skills/type_casting/SKILL.md``: choose the
known pattern for a row's source system, cast explicitly, and route any value
that matches no known pattern to quarantine instead of guessing at it.

The skill allows the *format's own shape* as a signal when the source flag is
not enough. That matters here: one batch declares ``platform_v2`` but writes
dates in the legacy ``yyyy-MM-dd`` form. So a value is accepted when either

  * its ``source_system`` pattern matches, or
  * exactly one known pattern matches its shape unambiguously (a logged
    format-drift fallback);

and is quarantined only when **no** known pattern matches. Two kinds of
unrecognized input are handled:

  * a value whose ``signup_date`` matches no known pattern;
  * a row Spark reports as ``_corrupt_record`` - how the long-form textual date
    (``December 11, 2025``) arrives, since its embedded comma is unquoted and
    shifts every later column.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from . import config, ingest

_FMT_LEGACY = config.SIGNUP_DATE_FORMATS["legacy_v1"]
_FMT_PLATFORM = config.SIGNUP_DATE_FORMATS["platform_v2"]


def cast_billing_accounts(incoming_df: DataFrame, batch_id: str = "batch_3_accounts"):
    """Return ``(casted_df, quarantine_df)`` for a raw accounts batch."""
    raw_date = F.trim(F.col("signup_date"))
    as_legacy = F.to_date(raw_date, _FMT_LEGACY)
    as_platform = F.to_date(raw_date, _FMT_PLATFORM)

    src = F.trim(F.col("source_system"))
    by_source = (
        F.when(src == F.lit("legacy_v1"), as_legacy)
        .when(src == F.lit("platform_v2"), as_platform)
        .otherwise(F.lit(None).cast("date"))
    )
    # Fall back to a known pattern that matches the value's shape.
    resolved = F.coalesce(by_source, as_legacy, as_platform)

    is_corrupt = F.col(ingest.CORRUPT_COL).isNotNull()
    unmatched_date = F.col("signup_date").isNotNull() & resolved.isNull()
    unrecognized = is_corrupt | unmatched_date

    with_cast = incoming_df.withColumn("_signup_date_cast", resolved)
    bad = with_cast.filter(unrecognized)
    good = with_cast.filter(~unrecognized)

    casted_df = good.select(
        F.trim("account_id").alias("account_id"),
        F.col("customer_name"),
        F.col("email_masked"),
        F.col("_signup_date_cast").alias("signup_date"),
        F.trim("source_system").alias("source_system"),
        F.upper(F.trim("plan_id")).alias("plan_id"),
        F.trim("status").alias("status"),
    )
    quarantine_df = ingest.quarantine_accounts(
        bad, config.QUARANTINE_REASONS["UNRECOGNIZED_FORMAT"], batch_id
    )
    return casted_df, quarantine_df


def cast_invoices(incoming_df: DataFrame) -> DataFrame:
    """Normalize an invoice batch to canonical invoice types.

    Invoices only ever arrive as ``yyyy-MM-dd``, but the same explicit-cast
    discipline is applied so types stay consistent with Batch 1.
    """
    return ingest.typed_invoice_columns(incoming_df)
