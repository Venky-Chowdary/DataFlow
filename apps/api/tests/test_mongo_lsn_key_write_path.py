"""Mongo LSN $in keys use write-path variants, not isdigit().

$1,234 the write path stores must still match. Auto 1,234 stays text.
IEEE-lossy 2**53+1 adds no collapsed float.
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.target_sample import mongo_query_key_variants  # noqa: E402


def test_plain_int_still_widens():
    variants = mongo_query_key_variants("42")
    assert "42" in variants
    assert 42 in variants


def test_locale_money_binds():
    variants = mongo_query_key_variants("$1,234")
    assert "$1,234" in variants
    assert 1234 in variants
    assert Decimal("1234") in variants


def test_auto_grouping_stays_text():
    assert mongo_query_key_variants("1,234") == {"1,234"}


def test_ieee_lossy_mantissa_does_not_add_collapsed_float():
    token = "9007199254740993"
    variants = mongo_query_key_variants(token)
    assert token in variants
    assert 9007199254740993 in variants
    assert 9007199254740992.0 not in variants
    assert float(2**53) not in variants
