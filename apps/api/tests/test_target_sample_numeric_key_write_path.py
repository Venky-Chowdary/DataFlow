"""Gate-8 sample key widen uses write-path numbers, not float(k).

float(k) invented Auto 1.234 and collapsed 2**53+1 onto 2**53.
$1,234 the write path stores must still match. Auto 1,234 stays text.
Bool is not a magnitude.
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.target_sample import numeric_sample_key_variants  # noqa: E402


def test_plain_int_still_widens():
    variants = numeric_sample_key_variants("42")
    assert "42" in variants
    assert 42 in variants


def test_locale_money_binds():
    variants = numeric_sample_key_variants("$1,234")
    assert "$1,234" in variants
    assert 1234 in variants
    assert Decimal("1234") in variants


def test_auto_grouping_stays_text():
    assert numeric_sample_key_variants("1,234") == {"1,234"}
    one_000 = numeric_sample_key_variants("1.000")
    assert "1.000" in one_000
    assert 1000 not in one_000
    # Dest-canonical 1.000 stays identity (Decimal / IEEE 1.0), not thousands 1000.


def test_ieee_lossy_mantissa_does_not_add_collapsed_float():
    token = "9007199254740993"
    variants = numeric_sample_key_variants(token)
    assert token in variants
    assert 9007199254740993 in variants
    assert 9007199254740992.0 not in variants
    assert float(2**53) not in variants


def test_dest_canonical_1_234_stays_identity():
    variants = numeric_sample_key_variants("1.234")
    assert "1.234" in variants
    assert Decimal("1.234") in variants
    assert 1234 not in variants


def test_bool_is_not_a_magnitude():
    # True == 1 in Python; the set must be the bool itself, not 1 / 1.0.
    assert numeric_sample_key_variants(True) == {True}
    assert all(v is True for v in numeric_sample_key_variants(True))
    assert numeric_sample_key_variants(False) == {False}
    assert all(v is False for v in numeric_sample_key_variants(False))
