"""Task 9: tests for Task 8 (Pipeline Guardrails).

At least one test per guardrail rule must be capable of FAILING if that rule
were violated - not just pass on the current implementation. Consider
deliberately breaking a rule in a local copy of your code to confirm each
test actually catches it, then revert.
"""

import pytest


def test_row_count_conservation_holds_for_every_batch():
    """TODO: for each of Batch 2-5, assert input_rows == silver_rows_written
    + quarantine_rows_written + true_duplicates_skipped."""
    raise NotImplementedError


def test_quarantine_reason_is_always_from_controlled_vocabulary():
    """TODO: assert every row in quarantine/ has a reason value in
    config.QUARANTINE_REASONS.values(), never free text or null."""
    raise NotImplementedError


def test_masked_field_byte_identical_before_and_after_pipeline():
    """TODO: assert email_masked for every account is unchanged between the
    bronze input and the final silver output."""
    raise NotImplementedError


def test_pipeline_is_idempotent_on_rerun():
    """TODO: run the full batch sequence twice; assert silver and quarantine
    row counts and content are identical after each run."""
    raise NotImplementedError


def test_writes_are_non_destructive():
    """TODO: assert that writing a later batch does not reduce the row count
    of, or overwrite existing rows in, an earlier batch's already-written
    silver/quarantine output."""
    raise NotImplementedError
