"""Partial-unique numeric/boolean filters use the write path, not float/yes.

IEEE float(2**53+1) == float(2**53) invented a unique-scope match.
Informal yes is not a write-path boolean. Locale money still matches
a dest-canonical literal. Auto 1,234 does not invent 1234.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.type_system import row_matches_unique_filter  # noqa: E402


def test_locale_money_matches_dest_canonical_literal():
    assert row_matches_unique_filter({"amount": "$1,234.56"}, "amount = 1234.56")
    assert row_matches_unique_filter({"amount": "€2.000,00"}, "amount = 2000.00")
    assert not row_matches_unique_filter({"amount": "$10.00"}, "amount = 1234.56")


def test_mantissa_beyond_float_does_not_collide():
    assert row_matches_unique_filter(
        {"id": "9007199254740993"}, "id = 9007199254740993"
    )
    assert not row_matches_unique_filter(
        {"id": "9007199254740993"}, "id = 9007199254740992"
    )


def test_auto_grouping_does_not_invent_unique_match():
    assert not row_matches_unique_filter({"amount": "1,234"}, "amount = 1234")
    assert not row_matches_unique_filter({"amount": "1.234"}, "amount = 1234")


def test_dest_canonical_fraction_still_matches():
    """Stored 1.234 vs predicate 1.234 — Decimal(text) identity, not Auto refuse."""
    assert row_matches_unique_filter({"amount": "1.234"}, "amount = 1.234")


def test_informal_yes_is_not_boolean_true():
    assert row_matches_unique_filter({"paid": "true"}, "paid = true")
    assert row_matches_unique_filter({"paid": True}, "paid = true")
    assert not row_matches_unique_filter({"paid": "yes"}, "paid = true")
    assert not row_matches_unique_filter({"paid": "on"}, "paid = true")
