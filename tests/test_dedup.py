"""Task 9: tests for Task 2 (Deduplication).

Fill in each test body. Use small in-memory DataFrames built from
data/invoices.csv and data/sample_batches/batch_2_duplicate_retry/ - you do
not need to run the full pipeline to test this module in isolation.
"""

import pytest


def test_true_duplicate_is_skipped_not_inserted():
    """TODO: a byte-identical redelivered invoice row must not increase the
    silver invoices row count."""
    raise NotImplementedError


def test_conflicting_key_is_quarantined_not_inserted_or_overwritten():
    """TODO: a row sharing an existing invoice_id but with different
    amount_due/payment_status must land in quarantine, not silver."""
    raise NotImplementedError


def test_conflicting_key_quarantine_reason_is_correct():
    """TODO: assert the quarantine reason for a conflicting-key row is
    exactly config.QUARANTINE_REASONS['CONFLICTING_KEY']."""
    raise NotImplementedError


def test_batch2_expected_row_counts():
    """TODO: assert silver invoices count is unchanged from Batch 1, and
    quarantine count is exactly 2 after Batch 2."""
    raise NotImplementedError
