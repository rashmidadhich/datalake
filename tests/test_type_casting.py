"""Task 9: tests for Task 3 (Type Casting)."""

import pytest


def test_legacy_v1_date_format_casts_correctly():
    """TODO: a YYYY-MM-DD signup_date casts to the correct date value."""
    raise NotImplementedError


def test_platform_v2_date_format_casts_correctly():
    """TODO: a MM/DD/YYYY signup_date casts to the correct date value,
    equal to the legacy_v1 format's result for the same logical date."""
    raise NotImplementedError


def test_plan_id_casing_normalized():
    """TODO: 'gold' and 'GOLD' (or similar casing variants) both normalize to
    the same canonical plan_id."""
    raise NotImplementedError


def test_unrecognized_format_is_quarantined_not_guessed():
    """TODO: the long-form textual date row from Batch 3 is not cast to any
    date value and is routed to quarantine with reason
    config.QUARANTINE_REASONS['UNRECOGNIZED_FORMAT']."""
    raise NotImplementedError


def test_batch3_expected_row_counts():
    """TODO: assert silver billing_accounts gains exactly 3 rows (not 4) from
    Batch 3, and quarantine gains exactly 1."""
    raise NotImplementedError
