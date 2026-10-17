"""Task 8: Pipeline Guardrails.

Author skills/pipeline_guardrails/SKILL.md from scratch before implementing
this module (no stub SKILL.md is provided). At minimum your skill - and this
module - must address:
  - row-count conservation (input = silver + quarantine + true dups skipped)
  - controlled quarantine reason codes (see config.QUARANTINE_REASONS)
  - masked-field passthrough integrity (email_masked never altered)
  - idempotent re-runs (running a batch twice produces identical output)
  - non-destructive writes (append-only / versioned, never blind overwrite)

TODO: implement these as reusable checks called from pipeline.py after each
batch, and referenced by tests/test_guardrails.py.
"""

from pyspark.sql import DataFrame


def assert_row_count_conservation(input_count: int, silver_count: int, quarantine_count: int, true_dup_count: int):
    """TODO: raise an assertion error if input_count != silver_count +
    quarantine_count + true_dup_count."""
    raise NotImplementedError


def assert_masked_field_unchanged(before_df: DataFrame, after_df: DataFrame, key_col: str, masked_col: str = "email_masked"):
    """TODO: raise an assertion error if masked_col differs for any row
    between before_df and after_df, joined on key_col."""
    raise NotImplementedError


def assert_valid_quarantine_reason(quarantine_df: DataFrame, reason_col: str = "reason"):
    """TODO: raise an assertion error if any value in reason_col is not in
    config.QUARANTINE_REASONS.values()."""
    raise NotImplementedError
