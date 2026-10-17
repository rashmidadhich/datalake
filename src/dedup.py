"""Task 2: Deduplication - Retried Load.

Before implementing, read skills/deduplication/SKILL.md, identify its gap for
a repeated business key whose non-key values differ, and MODIFY the skill
file first. Then implement against your modified skill.

TODO: implement dedup_invoices() so that:
  - rows whose invoice_id already exists in silver AND are identical on every
    non-key column are skipped (true duplicates)
  - rows whose invoice_id already exists in silver but differ on a non-key
    column are routed to quarantine with reason CONFLICTING_KEY
    (see config.QUARANTINE_REASONS)
  - rows whose invoice_id is new are passed through for insertion
"""

from pyspark.sql import DataFrame


def dedup_invoices(incoming_df: DataFrame, existing_silver_df: DataFrame):
    """TODO: return (rows_to_insert_df, rows_to_quarantine_df)."""
    raise NotImplementedError
