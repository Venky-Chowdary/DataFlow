"""Oracle '' → NULL write bind (VARCHAR2 / HVR write-location coercion)."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.sql_bind import normalize_sql_bind_value  # noqa: E402


def test_oracle_bind_empty_string_becomes_null():
    assert normalize_sql_bind_value("", "VARCHAR2(100)", engine="oracle") is None
    assert normalize_sql_bind_value("", "NVARCHAR2(50)", engine="oracledb") is None
    assert normalize_sql_bind_value("", "CHAR(1)", engine="oracle") is None


def test_postgres_bind_keeps_empty_string():
    assert normalize_sql_bind_value("", "VARCHAR(100)", engine="postgresql") == ""
    assert normalize_sql_bind_value("", "TEXT", engine="mysql") == ""


def test_oracle_bind_nonempty_passthrough():
    assert normalize_sql_bind_value("hi", "VARCHAR2(10)", engine="oracle") == "hi"


def test_numeric_bind_refuses_empty_null_invent():
    """INT/FLOAT/DECIMAL empty must raise — never invent SQL NULL (upsert wipe)."""
    import pytest
    from connectors.sql_bind import (
        coerce_decimal_wire,
        coerce_float_wire,
        coerce_integer_wire,
    )

    for ddl in ("INTEGER", "INT", "BIGINT", "SMALLINT"):
        with pytest.raises(ValueError, match="refuse silent NULL invent"):
            normalize_sql_bind_value("", ddl, engine="postgresql")
        with pytest.raises(ValueError, match="refuse silent NULL invent"):
            normalize_sql_bind_value("  ", ddl, engine="mysql")
        with pytest.raises(ValueError, match="refuse silent NULL invent"):
            coerce_integer_wire("", ddl_type=ddl)

    for ddl in ("FLOAT", "DOUBLE", "REAL", "DOUBLE PRECISION"):
        with pytest.raises(ValueError, match="refuse silent NULL invent"):
            normalize_sql_bind_value("", ddl, engine="postgresql")
        with pytest.raises(ValueError, match="refuse silent NULL invent"):
            coerce_float_wire("", ddl_type=ddl)

    for ddl in ("DECIMAL", "DECIMAL(10,2)", "NUMERIC(8,2)"):
        with pytest.raises(ValueError, match="refuse silent NULL invent"):
            normalize_sql_bind_value("", ddl, engine="mysql")
        with pytest.raises(ValueError, match="refuse silent NULL invent"):
            coerce_decimal_wire("", ddl_type=ddl)


def test_bind_sql_mapped_rows_quarantines_empty_integer():
    from connectors.writer_common import bind_sql_mapped_rows_with_quarantine

    details: list[dict] = []
    out = bind_sql_mapped_rows_with_quarantine(
        [("", "ok"), ("30", "ok")],
        ["age", "name"],
        ["INTEGER", "VARCHAR"],
        details,
        "quarantine",
        engine="postgresql",
        dialect_label="PostgreSQL",
    )
    assert out == [(30, "ok")]
    assert details
    assert any("refuse silent NULL invent" in str(d.get("reason") or "") for d in details)
