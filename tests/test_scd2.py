"""Task 9: tests for Task 5/Task 6 (SCD2 History Tracking)."""

import pytest


def test_normal_plan_change_closes_prior_row_and_opens_new_row():
    """TODO: for a normally-ordered effective_date, assert the prior row is
    closed (valid_to set, is_current False) and exactly one new row is
    is_current True."""
    raise NotImplementedError


def test_exactly_one_current_row_per_account_after_batch4():
    """TODO: assert every account_id in silver billing_accounts has exactly
    one is_current = True row after processing Batch 4."""
    raise NotImplementedError


def test_history_queryable_by_date():
    """TODO: assert querying an account's plan_id as of a date before its
    change returns the old plan, and as of a date on/after returns the new
    plan."""
    raise NotImplementedError


def test_out_of_order_effective_date_is_handled_or_quarantined():
    """TODO: assert the out-of-order row from Batch 4 either produces
    correct, non-overlapping history per your modified skill, or is
    quarantined with reason
    config.QUARANTINE_REASONS['OUT_OF_ORDER_EFFECTIVE_DATE'] - and in either
    case does not corrupt (e.g. create two open rows for) the account."""
    raise NotImplementedError
