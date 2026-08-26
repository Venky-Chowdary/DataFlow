"""Integrity parse is Decimal-only — no float() invent sink.

_to_float used float(Decimal) and collapsed 2**53+1 onto 2**53.
Locale money stays dest-canonical. Auto 1,234 refuses.
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import services.data_quality as data_quality  # noqa: E402
from services.data_quality import _to_decimal  # noqa: E402


def test_ieee_sink_removed():
    assert not hasattr(data_quality, "_to_float")


def test_locale_money_stays_decimal():
    assert _to_decimal("$1,234.56") == Decimal("1234.56")
    assert isinstance(_to_decimal("$1,234.56"), Decimal)


def test_auto_grouping_refuses():
    assert _to_decimal("1,234") is None
    assert _to_decimal("1.000") is None


def test_mantissa_beyond_float_stays_exact():
    assert _to_decimal("9007199254740993") == Decimal("9007199254740993")
    assert _to_decimal("9007199254740993") != Decimal("9007199254740992")


def test_boolean_is_not_a_magnitude():
    assert _to_decimal(True) is None
    assert _to_decimal(False) is None
    assert _to_decimal("true") is None
    assert _to_decimal("True") is None
    assert _to_decimal("t") is None
    assert _to_decimal("false") is None


def test_decimal_scientific_stays_identity():
    assert _to_decimal(Decimal("1E+2")) == Decimal("1E+2")
    assert _to_decimal(Decimal("100")) == Decimal("100")
