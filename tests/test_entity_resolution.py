"""Task 9: tests for Task 7 (Entity Resolution)."""

import pytest


def test_reused_account_id_gets_new_distinct_identity():
    """TODO: for each of the 3 reused account_ids in Batch 5, assert the
    resulting silver row is a new, distinct identity rather than being merged
    into the original legacy_v1 account's row."""
    raise NotImplementedError


def test_original_legacy_account_history_untouched():
    """TODO: assert the original legacy_v1 account's existing silver rows and
    SCD2 history are unchanged after Batch 5 is processed."""
    raise NotImplementedError


def test_masked_email_never_exposed_via_join():
    """TODO: assert the new identity's email_masked cannot be joined back to
    or derived from the original legacy_v1 account's email_masked - e.g. by
    asserting a join on email_masked between the two produces zero matches
    unless they were coincidentally identical placeholders."""
    raise NotImplementedError
