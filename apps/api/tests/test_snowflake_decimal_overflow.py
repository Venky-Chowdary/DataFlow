"""Snowflake NUMBER sizing + decimal overflow quarantine (no live warehouse)."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.snowflake_writer import (  # noqa: E402
    _fits_snowflake_number,
    _format_write_error,
    _quarantine_unfit_decimals,
    _snowflake_decimal_type,
)


def test_number_type_preserves_large_integer_digits():
    # Old algorithm forced scale=10 → NUMBER(38,10) only fits 28 int digits.
    huge = "1" + "0" * 30  # 31 integer digits
    typ = _snowflake_decimal_type(0, [(huge,)])
    assert typ.startswith("NUMBER(")
    p, s = typ[7:-1].split(",")
    precision, scale = int(p), int(s)
    assert precision <= 38
    assert precision - scale >= 31
    assert _fits_snowflake_number(huge, precision, scale)


def test_quarantine_unfit_decimal_nulls_cell():
    # NUMBER(10,2) cannot hold a 20-digit integer.
    rows = [("99999999999999999999", "ok")]
    details: list[dict] = []
    out = _quarantine_unfit_decimals(
        rows,
        ["amount", "label"],
        ["NUMBER(10,2)", "VARCHAR"],
        details,
        policy="quarantine",
    )
    assert out[0][0] is None
    assert out[0][1] == "ok"
    assert details and "does not fit" in details[0]["reason"]


def test_format_overflow_error_is_readable():
    from decimal import Overflow

    msg = _format_write_error(Overflow())
    assert "decimal.Overflow" in msg
    assert "[<class" not in msg
