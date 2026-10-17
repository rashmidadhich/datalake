"""Task 8: Pipeline Guardrails.

Implements ``.claude/skills/pipeline_guardrails/SKILL.md``. These are the
cross-cutting invariants checked after every batch:

  1. row-count conservation;
  2. controlled quarantine reason codes;
  3. masked-field passthrough integrity;
  4. idempotent re-runs (asserted in the test suite by replaying a batch);
  5. non-destructive writes (guaranteed by the read-modify-write writers).

Each check raises ``AssertionError`` with enough context to see what broke.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from . import config


def count_rows(df: DataFrame) -> int:
    return df.count()


def assert_row_count_conservation(input_count: int, silver_count: int,
                                  quarantine_count: int, true_dup_count: int,
                                  overwritten_count: int = 0,
                                  context: str = "") -> bool:
    """Assert every input row landed in exactly one destination.

    ``silver_count`` counts rows *added* to silver (inserts, and for SCD2 the
    new current rows it appends). ``overwritten_count`` counts rows changed in
    place. ``true_dup_count`` counts rows deliberately skipped.
    """
    accounted = silver_count + overwritten_count + quarantine_count + true_dup_count
    if input_count != accounted:
        raise AssertionError(
            f"row-count conservation failed{f' [{context}]' if context else ''}: "
            f"input={input_count} != silver({silver_count}) + overwritten({overwritten_count}) "
            f"+ quarantine({quarantine_count}) + skipped({true_dup_count}) = {accounted}"
        )
    return True


def assert_masked_field_unchanged(before_df: DataFrame, after_df: DataFrame,
                                  key_col: str,
                                  masked_col: str = config.MASKED_COLUMN) -> bool:
    """Assert a masked column is byte-identical for every shared key."""
    before = before_df.select(F.col(key_col).alias("_k"), F.col(masked_col).alias("_before"))
    after = after_df.select(F.col(key_col).alias("_k"), F.col(masked_col).alias("_after"))
    mismatches = (
        before.join(after, on="_k", how="inner")
        .filter(~F.col("_before").eqNullSafe(F.col("_after")))
    )
    n = mismatches.count()
    if n:
        raise AssertionError(
            f"masked-field integrity failed: {n} row(s) changed {masked_col!r} "
            f"between source and silver (keyed by {key_col!r})"
        )
    return True


def assert_valid_quarantine_reason(quarantine_df: DataFrame,
                                   reason_col: str = "reason") -> bool:
    """Assert every quarantine reason is from the controlled vocabulary."""
    allowed = list(config.QUARANTINE_REASONS.values())
    bad = (
        quarantine_df.select(reason_col)
        .distinct()
        .filter(F.col(reason_col).isNull() | ~F.col(reason_col).isin(allowed))
        .collect()
    )
    if bad:
        raise AssertionError(
            f"quarantine reason vocabulary violated: found {[r[reason_col] for r in bad]!r}, "
            f"allowed {allowed!r}"
        )
    return True
