"""Task 2: Deduplication - Retried Load.

Implements the modified ``.claude/skills/deduplication/SKILL.md``. Every
incoming invoice row whose ``invoice_id`` already exists in silver is routed to
exactly one of three outcomes, decided by *which* columns differ:

  * any identity-defining column differs (``account_id``, ``invoice_date``,
    ``currency``)  -> **quarantine** with reason ``conflicting_key``;
  * only mutable columns differ (``amount_due``, ``payment_status``)
    -> **overwrite** the existing row in place (no history);
  * nothing differs -> **retain unchanged** (skip; not quarantined).

Rows whose key is new are passed through for insertion.
"""

import operator
from functools import reduce
from typing import NamedTuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from . import config, ingest


class DedupResult(NamedTuple):
    new: DataFrame
    overwrite: DataFrame
    quarantine: DataFrame
    unchanged_count: int
    counts: dict


def _differs(prefix: str, cols) -> "F.Column":
    return reduce(
        operator.or_,
        [~F.col(f"{prefix}{c}").eqNullSafe(F.col(f"ex_{c}")) for c in cols],
    )


def dedup_invoices(incoming_df: DataFrame, existing_silver_df: DataFrame,
                   batch_id: str = "batch_2_invoices") -> DedupResult:
    """Classify an incoming invoice batch against the existing silver table.

    ``incoming_df`` and ``existing_silver_df`` must already be typed (date /
    decimal), so comparisons are on logical values rather than string forms.
    """
    key = config.INVOICE_KEY
    identity = config.INVOICE_IDENTITY_COLUMNS
    mutable = config.INVOICE_MUTABLE_COLUMNS
    non_key = identity + mutable

    existing = existing_silver_df.select(
        F.col(key),
        *[F.col(c).alias(f"ex_{c}") for c in non_key],
    )
    incoming = incoming_df.select(
        F.col(key),
        *[F.col(c).alias(f"in_{c}") for c in non_key],
    )
    joined = incoming.join(existing, on=key, how="inner")

    identity_differs = _differs("in_", identity)
    mutable_differs = _differs("in_", mutable)

    def restore(df: DataFrame) -> DataFrame:
        return df.select(F.col(key), *[F.col(f"in_{c}").alias(c) for c in non_key])

    quarantine = ingest.quarantine_invoices(
        restore(joined.filter(identity_differs)),
        config.QUARANTINE_REASONS["CONFLICTING_KEY"],
        batch_id,
    )
    overwrite = restore(joined.filter(~identity_differs & mutable_differs))
    unchanged = joined.filter(~identity_differs & ~mutable_differs)

    # Materialize now, while the source silver table still exists: these
    # outputs are still lazy and the caller overwrites silver_invoices before
    # consuming the quarantine frame.
    new = ingest.materialize(incoming_df.join(existing_silver_df.select(key), on=key, how="left_anti"))
    overwrite = ingest.materialize(overwrite)
    quarantine = ingest.materialize(quarantine)
    unchanged = ingest.materialize(unchanged)

    counts = {
        "new": new.count(),
        "overwritten": overwrite.count(),
        "quarantined": quarantine.count(),
        "retained_unchanged": unchanged.count(),
    }
    return DedupResult(new, overwrite, quarantine, counts["retained_unchanged"], counts)
