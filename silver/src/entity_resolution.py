"""Task 7: Entity Resolution - Reused / Reassigned Account IDs.

Implements ``.claude/skills/entity_resolution/SKILL.md`` (authored from
scratch). Before any key-based logic runs, this module decides whether a
repeated ``account_id`` is the *same* customer as the one already in silver, or
a *different* customer that reused a reassigned key.

Identity is decided on the stable fingerprint (``customer_name``,
``signup_date``). A change of ``source_system`` alone is deliberately *not*
decisive, because the same customer can legitimately migrate to the newer
platform.

A reused key is given a distinct, deterministic surrogate identity
(``<account_id>:R``) which becomes the SCD2 grain, so the newcomer never merges
into or overwrites the original customer's rows or history.
"""

import operator
from functools import reduce
from typing import NamedTuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from . import config

REUSED_SUFFIX = ":R"
STATUS_NEW = "new"
STATUS_SAME = "same_entity"
STATUS_REUSED = "reused_key_new_identity"


class EntityResolutionResult(NamedTuple):
    resolved: DataFrame
    counts: dict


def resolve_entities(incoming_df: DataFrame, existing_silver_df: DataFrame,
                     batch_id: str = "batch_5_reused_accounts") -> EntityResolutionResult:
    """Return incoming accounts annotated with ``entity_id`` and ``identity_status``."""
    key = config.ACCOUNT_KEY
    fingerprint = config.ACCOUNT_IDENTITY_FINGERPRINT

    # Original identities = rows of the original (legacy) owner of each key.
    existing_orig = (
        existing_silver_df
        .filter(F.col(config.ACCOUNT_HISTORY_GRAIN) == F.col(key))
        .select(F.col(key), *[F.col(c).alias(f"ex_{c}") for c in fingerprint])
        .distinct()
    )

    incoming = incoming_df.select(
        F.col(key),
        *[F.col(c).alias(f"in_{c}") for c in fingerprint],
    )
    joined = incoming.join(existing_orig, on=key, how="inner")
    consistent = reduce(
        operator.and_,
        [F.col(f"in_{c}").eqNullSafe(F.col(f"ex_{c}")) for c in fingerprint],
    )
    reused_keys = joined.filter(~consistent).select(key).distinct()
    known_keys = existing_orig.select(key).distinct()

    reused = (
        incoming_df.join(reused_keys, on=key, how="left_semi")
        .withColumn("entity_id", F.concat(F.col(key), F.lit(REUSED_SUFFIX)))
        .withColumn("identity_status", F.lit(STATUS_REUSED))
    )
    same = (
        incoming_df.join(reused_keys, on=key, how="left_anti")
        .join(known_keys, on=key, how="left_semi")
        .withColumn("entity_id", F.col(key))
        .withColumn("identity_status", F.lit(STATUS_SAME))
    )
    new = (
        incoming_df.join(known_keys, on=key, how="left_anti")
        .withColumn("entity_id", F.col(key))
        .withColumn("identity_status", F.lit(STATUS_NEW))
    )

    resolved = reused.unionByName(same).unionByName(new)
    counts = {
        "reused_new_identity": reused.count(),
        "same_entity": same.count(),
        "new": new.count(),
    }
    return EntityResolutionResult(resolved, counts)
