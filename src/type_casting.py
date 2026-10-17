"""Task 3: Type Casting - Inconsistent Formats.

Read skills/type_casting/SKILL.md (used as-is, no modification needed).

TODO: implement cast_billing_accounts() so that:
  - signup_date is cast to a consistent date type across legacy_v1
    (YYYY-MM-DD) and platform_v2 (MM/DD/YYYY) rows
  - plan_id casing is normalized to one canonical representation
  - a row whose signup_date matches neither known pattern is NOT cast and is
    routed to quarantine with reason UNRECOGNIZED_FORMAT
    (see config.QUARANTINE_REASONS)
"""

from pyspark.sql import DataFrame


def cast_billing_accounts(incoming_df: DataFrame):
    """TODO: return (casted_df, rows_to_quarantine_df)."""
    raise NotImplementedError
