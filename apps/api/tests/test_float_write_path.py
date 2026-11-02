"""FLOAT/REAL/DOUBLE bind uses decimal_wire_value — no float(token) invent.

Auto 1.234 used to land as 1.234. Locale money the write path stores must
still bind. Explicit NaN / Infinity stay IEEE wire.
"""

from __future__ import annotations

import math
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.sql_bind import coerce_float_wire, normalize_sql_bind_value  # noqa: E402


def test_auto_three_digit_group_refuses():
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_float_wire("1.234")
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_float_wire("1,234")
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_float_wire("1.005")


def test_locale_money_and_bindable_scale_land():
    assert coerce_float_wire("$1,234.56") == pytest.approx(1234.56)
    assert coerce_float_wire("€1.234,56") == pytest.approx(1234.56)
    assert coerce_float_wire("$1,234") == pytest.approx(1234.0)
    assert coerce_float_wire("1.2345") == pytest.approx(1.2345)
    assert coerce_float_wire("1.5") == 1.5
    assert coerce_float_wire("1e-3") == pytest.approx(0.001)


def test_ieee_nonfinite_and_bool_empty_still_hold():
    assert math.isnan(coerce_float_wire("nan"))
    assert math.isinf(coerce_float_wire("Infinity"))
    assert math.isinf(coerce_float_wire("-inf"))
    with pytest.raises(ValueError, match="boolean token"):
        coerce_float_wire("true")
    with pytest.raises(ValueError, match="empty string"):
        coerce_float_wire("")
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_float_wire("not-a-number")


def test_ieee_lossy_mantissa_refuses():
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_float_wire(9007199254740993)
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_float_wire("9007199254740993")
    from decimal import Decimal

    with pytest.raises(ValueError, match="refuse invent"):
        coerce_float_wire(Decimal("9007199254740993"))
    assert coerce_float_wire(2) == 2.0
    assert coerce_float_wire(Decimal("10.00")) == 10.0


def test_normalize_sql_bind_float_carriers():
    assert normalize_sql_bind_value("$1,234.56", "FLOAT", engine="postgresql") == pytest.approx(
        1234.56
    )
    with pytest.raises(ValueError, match="refuse invent"):
        normalize_sql_bind_value("1.234", "DOUBLE PRECISION", engine="postgresql")
