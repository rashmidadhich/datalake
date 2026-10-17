"""Task 5/Task 6: SCD2 History Tracking - Plan Change.

Before implementing, read skills/scd2_history/SKILL.md, identify its gap for
an out-of-order effective_date, and MODIFY the skill file first (Task 6).
Then implement against your modified skill (Task 5).

TODO: implement apply_plan_change() so that:
  - for a normally-ordered effective_date, the current silver row is closed
    (valid_to = effective_date, is_current = False) and a new current row is
    appended (valid_from = effective_date, valid_to = None, is_current = True)
  - for an effective_date earlier than the account's current valid_from, apply
    whatever rule your modified skill defines - or, if unresolved, route the
    row to quarantine with reason OUT_OF_ORDER_EFFECTIVE_DATE
    (see config.QUARANTINE_REASONS)
  - at most one is_current = True row exists per account_id at any time
"""

from pyspark.sql import DataFrame


def apply_plan_change(change_events_df: DataFrame, current_silver_df: DataFrame):
    """TODO: return (updated_silver_df, rows_to_quarantine_df)."""
    raise NotImplementedError
