"""Task 7: Entity Resolution - Reused Account IDs.

Author skills/entity_resolution/SKILL.md from scratch before implementing
this module (no stub SKILL.md is provided).

TODO: implement resolve_entities() so that:
  - an incoming row whose account_id already exists in silver, but whose
    non-key attributes (customer_name, signup_date, source_system) are
    inconsistent with the existing identity, is assigned a new surrogate
    identity rather than merged into the existing account's history
  - the original legacy_v1 account's existing rows and SCD2 history are left
    untouched
  - email_masked for the new identity is carried through unchanged and is
    never derived from or cross-referenced against the original account
"""

from pyspark.sql import DataFrame


def resolve_entities(incoming_df: DataFrame, existing_silver_df: DataFrame) -> DataFrame:
    """TODO: return a DataFrame where reused-but-conflicting account_id rows
    have been assigned a new surrogate identity (e.g. a new synthetic
    account_id or a separate identity key column)."""
    raise NotImplementedError
