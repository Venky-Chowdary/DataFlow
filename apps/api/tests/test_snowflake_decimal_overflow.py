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


def test_quarantine_unfit_decimal_holds_out_row():
    # NUMBER(10,2) cannot hold a 20-digit integer — quarantine omits the row.
    rows = [("99999999999999999999", "ok"), ("1.50", "fine")]
    details: list[dict] = []
    out = _quarantine_unfit_decimals(
        rows,
        ["amount", "label"],
        ["NUMBER(10,2)", "VARCHAR"],
        details,
        policy="quarantine",
    )
    assert out == [("1.50", "fine")]
    assert details and "does not fit" in details[0]["reason"]


def test_create_path_honors_map_decimal_stamp():
    """CREATE DDL must match approved Map NUMBER/DECIMAL(p,s) — never batch-invent."""
    from connectors.snowflake_writer import resolve_snowflake_create_types
    from connectors.writer_common import parse_decimal_precision_scale

    logical_types = ["DECIMAL(10,2)", "NUMBER(18,4)", "VARCHAR", "DECIMAL"]
    mapped_rows = [("1.5", "2.5", "x", "3.1"), ("9.99", "1.0", "y", "4.2")]
    types = resolve_snowflake_create_types(logical_types, mapped_rows)
    assert parse_decimal_precision_scale(types[0]) == (10, 2)
    assert parse_decimal_precision_scale(types[1]) == (18, 4)
    assert types[2].upper().startswith("VARCHAR")
    # Bare DECIMAL may still size from batch — but Map-stamped (p,s) stays fixed.
    assert types[0] in {"DECIMAL(10,2)", "NUMBER(10,2)"}
    assert types[1] in {"NUMBER(18,4)", "DECIMAL(18,4)"}


def test_coerce_null_unfit_decimal_nulls_cell():
    rows = [("99999999999999999999", "ok")]
    details: list[dict] = []
    out = _quarantine_unfit_decimals(
        rows,
        ["amount", "label"],
        ["NUMBER(10,2)", "VARCHAR"],
        details,
        policy="coerce_null",
    )
    assert out[0][0] is not None  # DF_MISSING omit sentinel, not SQL NULL wipe
    from services.value_serializer import is_missing_sentinel

    assert is_missing_sentinel(out[0][0])
    assert out[0][1] == "ok"
    assert details


def test_format_overflow_error_is_readable():
    from decimal import Overflow

    msg = _format_write_error(Overflow())
    assert "decimal.Overflow" in msg
    assert "[<class" not in msg
