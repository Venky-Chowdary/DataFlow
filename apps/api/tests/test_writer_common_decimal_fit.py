"""Shared DECIMAL/NUMBER(p,s) fit + quarantine (MySQL/PG/generic/Snowflake path)."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.writer_common import (  # noqa: E402
    fits_decimal,
    parse_decimal_precision_scale,
    quarantine_unfit_decimals,
)


def test_parse_decimal_precision_scale_variants():
    assert parse_decimal_precision_scale("DECIMAL(10,2)") == (10, 2)
    assert parse_decimal_precision_scale("NUMERIC(18,4)") == (18, 4)
    assert parse_decimal_precision_scale("NUMBER(38,10)") == (38, 10)
    assert parse_decimal_precision_scale("decimal(8)") == (8, 0)
    assert parse_decimal_precision_scale("BIGNUMERIC(20,6)") == (20, 6)
    assert parse_decimal_precision_scale("decimal(12,2)") == (12, 2)  # Iceberg DDL
    assert parse_decimal_precision_scale("NUMERIC") == (38, 9)  # BigQuery default
    assert parse_decimal_precision_scale("BIGNUMERIC") == (76, 38)
    assert parse_decimal_precision_scale("DECIMAL") is None  # ambiguous bare
    assert parse_decimal_precision_scale("VARCHAR(10)") is None
    assert parse_decimal_precision_scale("DECIMAL(2,5)") is None  # scale > precision


def test_fits_decimal_integer_and_scale_overflow():
    assert fits_decimal("1.50", 10, 2) is True
    assert fits_decimal("99999999999999999999", 10, 2) is False  # int digits
    assert fits_decimal("1.234", 10, 2) is False  # scale overflow — no silent round
    assert fits_decimal(None, 10, 2) is True


def test_quarantine_holds_out_unfit_row():
    rows = [("99999999999999999999", "ok"), ("1.50", "fine"), ("1.234", "scale")]
    details: list[dict] = []
    out = quarantine_unfit_decimals(
        rows,
        ["amount", "label"],
        ["DECIMAL(10,2)", "VARCHAR"],
        details,
        policy="quarantine",
        dialect_label="MySQL DECIMAL",
    )
    assert out == [("1.50", "fine")]
    assert len(details) == 2
    assert all("does not fit MySQL DECIMAL(10,2)" in d["reason"] for d in details)


def test_coerce_null_nulls_cell_keeps_row():
    rows = [("1.234", "keep")]
    details: list[dict] = []
    out = quarantine_unfit_decimals(
        rows,
        ["amount", "label"],
        ["NUMERIC(10,2)", "TEXT"],
        details,
        policy="coerce_null",
        dialect_label="PostgreSQL NUMERIC",
    )
    assert out == [(None, "keep")]
    assert details and "PostgreSQL NUMERIC(10,2)" in details[0]["reason"]


def test_fail_policy_stamps_and_holds_out_unfit_decimals():
    """Strict/fail must stamp unfit cells so reject_on_strict_policy can abort.

    Leaving rows unchanged used to rely on soft SQL drivers — silent truncate.
    """
    rows = [("99999999999999999999", "ok")]
    details: list[dict] = []
    out = quarantine_unfit_decimals(
        rows,
        ["amount", "label"],
        ["NUMBER(10,2)", "VARCHAR"],
        details,
        policy="fail",
    )
    assert out == []
    assert details
    assert "would truncate/overflow" in details[0]["reason"]
    assert details[0]["policy"] == "write_quarantine"
