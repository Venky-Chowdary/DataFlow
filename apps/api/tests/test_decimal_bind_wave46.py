"""Wave 46: DECIMAL/NUMERIC bind — exact Decimal, fail-closed on (p,s) overflow."""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_coerce_decimal_exact_from_str_and_int():
    from connectors.sql_bind import coerce_decimal_wire, normalize_sql_bind_value

    assert coerce_decimal_wire("1.2300") == Decimal("1.2300")
    assert coerce_decimal_wire(42) == Decimal(42)
    # float via str — not binary float expansion path for common decimals
    assert coerce_decimal_wire(0.5) == Decimal("0.5")
    assert normalize_sql_bind_value("9.99", "DECIMAL(10,2)") == Decimal("9.99")


def test_coerce_decimal_overflow_refuse_quantize():
    from connectors.sql_bind import coerce_decimal_wire

    with pytest.raises(ValueError, match="refuse silent quantize"):
        coerce_decimal_wire("12.345", ddl_type="DECIMAL(4,2)")
    with pytest.raises(ValueError, match="refuse silent quantize"):
        coerce_decimal_wire("1000", ddl_type="NUMBER(3,0)")


def test_coerce_decimal_refuse_bool_nan_empty():
    from connectors.sql_bind import coerce_decimal_wire

    with pytest.raises(ValueError, match="refuse invent"):
        coerce_decimal_wire(True)
    with pytest.raises(ValueError, match="NaN|non-finite|parse failed|refuse"):
        coerce_decimal_wire(float("nan"))
    with pytest.raises(ValueError, match="refuse silent NULL invent|empty string cannot coerce"):
        coerce_decimal_wire("  ")
    assert coerce_decimal_wire(None) is None


def test_normalize_routes_money_and_bignumeric():
    from connectors.sql_bind import normalize_sql_bind_value

    assert normalize_sql_bind_value("1.25", "MONEY") == Decimal("1.25")
    assert normalize_sql_bind_value("1", "BIGNUMERIC(10,0)") == Decimal(1)
