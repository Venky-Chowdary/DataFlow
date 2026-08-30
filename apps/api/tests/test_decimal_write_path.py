"""DECIMAL bind uses decimal_wire_value — no Decimal(text) invent.

Auto 1.234 used to land as 1.234. Locale money the write path stores must
still bind. Scale-preserving 1.2300 and PG airport pads still land.
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.sql_bind import coerce_decimal_wire, normalize_sql_bind_value  # noqa: E402


def test_auto_three_digit_group_refuses():
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_decimal_wire("1.234")
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_decimal_wire("1,234")
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_decimal_wire("1.005")
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_decimal_wire("12.345")


def test_locale_money_and_bindable_scale_land():
    assert coerce_decimal_wire("$1,234.56") == Decimal("1234.56")
    assert coerce_decimal_wire("€1.234,56") == Decimal("1234.56")
    assert coerce_decimal_wire("$1,234") == Decimal("1234")
    assert coerce_decimal_wire("1.2345") == Decimal("1.2345")
    assert coerce_decimal_wire("1.2300") == Decimal("1.2300")


def test_normalize_sql_bind_decimal_carriers():
    assert normalize_sql_bind_value("$1,234.56", "DECIMAL(10,2)") == Decimal("1234.56")
    assert normalize_sql_bind_value("€1.234,56", "NUMERIC(10,2)") == Decimal("1234.56")
    with pytest.raises(ValueError, match="refuse invent"):
        normalize_sql_bind_value("1.234", "DECIMAL(10,2)")
