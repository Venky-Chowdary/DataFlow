"""Leading-zero 0.025 is a dest-canonical fraction, not Auto thousands.

1.234 stays Auto-ambiguous. 0.025 cannot be EU thousands (leading-zero
integer). FLOAT samples 1500.0 / 0.025 must not widen to VARCHAR.
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.sql_bind import coerce_float_wire, coerce_decimal_wire  # noqa: E402
from services.schema_inference import safe_ddl_logical_type  # noqa: E402
from services.transform_engine import decimal_wire_value  # noqa: E402


def test_leading_zero_fraction_binds():
    assert decimal_wire_value("0.025") == Decimal("0.025")
    assert decimal_wire_value("0.000") == Decimal("0.000")
    assert decimal_wire_value("-0.025") == Decimal("-0.025")
    assert decimal_wire_value("0,025") == Decimal("0.025")
    assert coerce_float_wire("0.025") == pytest.approx(0.025)
    assert coerce_decimal_wire("0.025") == Decimal("0.025")


def test_auto_nonzero_three_digit_group_still_refuses():
    assert decimal_wire_value("1.234") is None
    assert decimal_wire_value("1,234") is None
    assert decimal_wire_value("1.000") is None
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_float_wire("1.234")


def test_locale_money_still_binds():
    assert decimal_wire_value("$1,234.56") == Decimal("1234.56")
    assert decimal_wire_value("€0,025") == Decimal("0.025")


def test_float_safe_ddl_keeps_ieee_residue_samples():
    assert (
        safe_ddl_logical_type("FLOAT", ["1500.0", "0.025"], field_name="amt_float")
        == "FLOAT"
    )
    assert safe_ddl_logical_type("float", ["1.5e3"], field_name="amt_float") == "FLOAT"
    # Auto grouping samples still do not invent FLOAT.
    assert (
        safe_ddl_logical_type("FLOAT", ["1,234", "1.234"], field_name="amt_float")
        != "FLOAT"
    )
