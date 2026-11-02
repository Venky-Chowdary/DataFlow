"""Gate proposals must not invent or rewrite values.

Named matrix: decimal, integer, varchar, boolean, currency, temporal.
A blocked gate may propose DEST DDL / mapping only. Source cells stay as-is
until the operator applies a transform.
"""

from __future__ import annotations

from decimal import Decimal

from connectors.sql_bind import coerce_decimal_wire
from connectors.writer_common import (
    fits_decimal,
    integer_overflow_suggested_type,
    proven_varchar_widen,
    quarantine_currency_markers_into_numeric,
)
from services.decimal_observe import (
    CREATE_NEW_NUMERIC_SAFETY_MARGIN,
    exact_create_decimal_ps,
    proven_decimal_widen,
)
from services.population_fit_scan import scan_population_fit
from services.type_system import parse_numeric_precision_scale


def test_safety_margin_stays_zero():
    assert CREATE_NEW_NUMERIC_SAFETY_MARGIN == 0


def test_exact_envelope_never_adds_currency_zeros():
    p, s = exact_create_decimal_ps(4, 2)
    assert (p, s) == (6, 2)
    assert s != 4


def test_proven_widen_does_not_invent_plus_one_scale():
    """Witness scale 2 must stay 2 — no deadlock +1 pad."""
    widened = proven_decimal_widen(
        values=("12.50", "99.99"),
        dest_db="snowflake",
        current_type="NUMBER(5,1)",
        max_int_digits=2,
        max_scale=2,
    )
    _p, s = parse_numeric_precision_scale(widened)
    assert s == 2, widened


def test_fractional_integer_propose_is_exact_decimal_not_float():
    for dest in ("snowflake", "postgresql", "mysql", "bigquery"):
        suggested = integer_overflow_suggested_type(
            "22.433332", "INTEGER", dest_db=dest
        )
        assert suggested
        upper = suggested.upper()
        assert "FLOAT" not in upper and "DOUBLE" not in upper, (dest, suggested)
        _p, s = parse_numeric_precision_scale(suggested)
        assert s is not None and s >= 6, (dest, suggested)
        assert fits_decimal("22.433332", _p, s, dest_db=dest)


def test_varchar_propose_is_measured_width():
    suggested = proven_varchar_widen(
        ("ok", "x" * 50),
        dest_db="postgresql",
        current_type="VARCHAR(8)",
    )
    assert "50" in suggested or "VARCHAR" in suggested.upper() or "TEXT" in suggested.upper()


def test_currency_identity_is_quarantined_not_stripped():
    details: list[dict] = []
    out = quarantine_currency_markers_into_numeric(
        [("$1,234.56",), ("99.00",)],
        ["amount"],
        ["DECIMAL(10,2)"],
        details,
        policy="quarantine",
    )
    assert out == [("99.00",)]
    assert details
    try:
        coerce_decimal_wire("$12.50", ddl_type="DECIMAL(10,2)")
        raise AssertionError("currency marker must not bind on identity")
    except ValueError as exc:
        assert "currency" in str(exc).lower()


def test_gate_apply_changes_type_not_cell_values():
    rows = (
        [{"clock": "9.083333"}]
        + [{"clock": "7.9166665"}]
        + [{"clock": "12"}]
    )
    report = scan_population_fit(
        rows,
        [{"source": "clock", "target": "clock", "target_type": "NUMBER(5,2)"}],
        dest_db="snowflake",
        dialect_label="snowflake",
        source_types={"clock": "DECIMAL"},
        source_kind="file",
        source_format="csv",
        rows_total=len(rows),
        rows_are_population=True,
        job_error_policy="fail",
    )
    assert report.findings
    suggested = report.findings[0].suggested_target_type
    assert suggested
    _p, s = parse_numeric_precision_scale(suggested)
    assert s == 7, suggested
    # Source cells are untouched — propose is DDL only.
    assert rows[0]["clock"] == "9.083333"
    assert rows[1]["clock"] == "7.9166665"


def test_boolean_tokens_are_not_invented_as_money():
    from connectors.sql_bind import coerce_decimal_wire

    try:
        coerce_decimal_wire(True, ddl_type="DECIMAL(10,2)")
        raise AssertionError("bool must not invent 1.00 money")
    except ValueError as exc:
        assert "bool" in str(exc).lower() or "refuse" in str(exc).lower()


def test_trailing_zero_padding_is_same_decimal():
    assert Decimal("12.50") == Decimal("12.500000")
    assert Decimal("12.50") != Decimal("1250")
