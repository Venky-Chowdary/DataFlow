"""Regressions for four defects that refused or misreported correct loads.

1. A delimited export dropped a numeric column's declared scale (``10.50`` →
   ``10.5``) because the CSV cell went through the JSON number parser.
2. The pre-drop spelling probe raised on destinations whose optional SQLAlchemy
   dialect is not installed (Snowflake writes natively), failing the transfer.
3. Native warehouse writers and object-store writers did not report the
   reader's population count, so Gate-8 refused conservation on correct loads.
4. The shared write-quarantine matrix named ``DECIMAL`` on engines whose catalog
   spells the carrier ``NUMBER``.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from connectors.writer_common import (
    decimal_reason_label,
    quarantine_unfit_decimals,
    to_delimited_value,
    to_json_value,
    writer_meta_with_source_rows,
)
from src.transfer.adapters import _writer_diagnostics, carry_dest_spelling_across_drop


def test_delimited_cell_keeps_declared_decimal_scale():
    types = {"amount": "DECIMAL(7,4)"}
    assert to_delimited_value("10.50", "amount", types) == "10.50"
    # JSON exports keep native numbers — that contract does not change.
    assert to_json_value("10.50", "amount", types) == pytest.approx(10.5)


def test_delimited_cell_still_refuses_empty_string_into_a_numeric_column():
    with pytest.raises(ValueError):
        to_delimited_value("", "amount", {"amount": "DECIMAL(7,4)"})


def test_delimited_cell_normalizes_temporal_like_json():
    types = {"ordered_at": "DATE"}
    assert to_delimited_value("2024-01-05", "ordered_at", types) == to_json_value(
        "2024-01-05", "ordered_at", types
    )


def test_delimited_cell_passes_through_non_text_values():
    assert to_delimited_value(Decimal("10.50"), "amount", {"amount": "DECIMAL(7,4)"}) == (
        Decimal("10.50")
    )
    assert to_delimited_value(None, "amount", {"amount": "DECIMAL(7,4)"}) is None


class _Endpoint:
    def __init__(self) -> None:
        self.extra: dict[str, Any] = {}


def test_pre_drop_spelling_probe_never_fails_the_run(monkeypatch):
    """A missing optional dialect leaves no prior spelling — it is not an error."""

    def _explode(*_a: Any, **_kw: Any) -> str:
        raise RuntimeError(
            "SQLAlchemy dialect/driver for 'snowflake' is not available "
            "(tried 'snowflake'). Install/enable snowflake-sqlalchemy."
        )

    monkeypatch.setattr("connectors.generic_sql.physical_table_spelling", _explode)
    dest = _Endpoint()
    carry_dest_spelling_across_drop(dest, "snowflake", {"type": "snowflake"}, "t", None)
    assert dest.extra == {}


def test_pre_drop_spelling_probe_records_the_stored_spelling(monkeypatch):
    monkeypatch.setattr(
        "connectors.generic_sql.physical_table_spelling", lambda *_a, **_kw: "payments"
    )
    dest = _Endpoint()
    carry_dest_spelling_across_drop(dest, "oracle", {"type": "oracle"}, "PAYMENTS", None)
    assert dest.extra["dest_table_prior_spelling"] == "payments"


def test_writer_meta_carries_the_reader_population_to_gate8():
    assert writer_meta_with_source_rows(None, 7) == {"source_row_count": 7}
    assert writer_meta_with_source_rows({"schema_fidelity": {}}, 0) == {
        "schema_fidelity": {},
        "source_row_count": 0,
    }
    # Unknown stays unknown — Gate-8 must fail closed, not read a zero.
    assert writer_meta_with_source_rows({"a": 1}, None) == {"a": 1}


class _Result:
    def __init__(self, meta: dict[str, Any]) -> None:
        self.meta = meta
        self.rejected_rows = 0
        self.coerced_null_rows = 0
        self.rows_skipped = 0
        self.warnings: list[str] = []
        self.rejected_details: list[dict[str, Any]] = []


def test_writer_diagnostics_promotes_source_row_count_into_the_summary():
    out = _writer_diagnostics(_Result(writer_meta_with_source_rows(None, 5)))
    assert out["source_row_count"] == 5


@pytest.mark.parametrize(
    ("dest_db", "declared", "expected"),
    [
        ("snowflake", "DECIMAL", "Snowflake NUMBER"),
        ("oracle", "NUMBER(38,10)", "Oracle NUMBER"),
        ("postgresql", "NUMERIC(10,2)", "PostgreSQL NUMERIC"),
        ("mysql", "DECIMAL(10,2)", "MySQL DECIMAL"),
    ],
)
def test_decimal_quarantine_names_the_destination_carrier(dest_db, declared, expected):
    label = {
        "snowflake": "Snowflake DECIMAL",
        "oracle": "Oracle DECIMAL",
        "postgresql": "PostgreSQL DECIMAL",
        "mysql": "MySQL DECIMAL",
    }[dest_db]
    assert decimal_reason_label(label, dest_db, declared) == expected


def test_snowflake_decimal_quarantine_reason_names_number():
    rejected: list[dict[str, Any]] = []
    kept = quarantine_unfit_decimals(
        [("9" * 31,)],
        ["amount"],
        ["DECIMAL(38,10)"],
        rejected,
        "quarantine",
        dialect_label="Snowflake DECIMAL",
        dest_db="snowflake",
    )
    assert kept == []
    assert "NUMBER(38,10)" in rejected[0]["reason"]
