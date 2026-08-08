"""generic_sql SA type mapping — DECIMAL(p,s) must not collapse to TEXT."""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

sa = pytest.importorskip("sqlalchemy")

from connectors.generic_sql import _sa_type_for_logical, _to_sa_value  # noqa: E402


@pytest.mark.parametrize(
    "carrier,db_type,dialect",
    [
        ("DECIMAL(12,4)", "sqlserver", "mssql"),
        ("NUMERIC(12,4)", "oracle", "oracle"),
        ("decimal(12,4)", "duckdb", "duckdb"),
        ("NUMBER(12,4)", "oracle", "oracle"),
    ],
)
def test_sa_type_preserves_decimal_scale(carrier: str, db_type: str, dialect: str):
    sa_t = _sa_type_for_logical(carrier, dialect, db_type)
    assert isinstance(sa_t, (sa.Numeric, sa.DECIMAL)), type(sa_t)
    assert int(sa_t.precision) == 12
    assert int(sa_t.scale) == 4


def test_sa_type_float_stays_ieee_not_numeric():
    sa_t = _sa_type_for_logical("FLOAT", "mssql", "sqlserver")
    bare_dec = _sa_type_for_logical("decimal", "mssql", "sqlserver")
    name = type(sa_t).__name__.lower()
    assert any(tok in name for tok in ("double", "float", "real")), type(sa_t)
    # SQL Server bare DECIMAL SSOT is DECIMAL(38,10) — FLOAT must not share that.
    assert getattr(sa_t, "scale", None) != 10 or "double" in name or "float" in name
    assert int(getattr(bare_dec, "precision", 0) or 0) == 38
    assert int(getattr(bare_dec, "scale", 0) or 0) == 10


@pytest.mark.parametrize(
    "db_type,dialect,expect_p,expect_s",
    [
        ("sqlserver", "mssql", 38, 10),
        ("oracle", "oracle", 38, 10),
        ("databricks", "duckdb", 38, 10),
        ("duckdb", "duckdb", 38, 15),
        ("mysql", "mysql", 38, 15),
        ("synapse", "mssql", 38, 10),
    ],
)
def test_bare_decimal_sa_matches_ddl_type_ssot(
    db_type: str, dialect: str, expect_p: int, expect_s: int
):
    """Map≡CREATE: bare DECIMAL must not invent a different (p,s) than ddl_type."""
    from services.type_system import ddl_type, parse_numeric_precision_scale

    wire = ddl_type(db_type, "DECIMAL")
    wp, ws = parse_numeric_precision_scale(wire)
    assert (wp, ws) == (expect_p, expect_s), wire

    sa_t = _sa_type_for_logical("DECIMAL", dialect, db_type)
    assert isinstance(sa_t, (sa.Numeric, sa.DECIMAL)), type(sa_t)
    assert int(sa_t.precision) == expect_p
    assert int(sa_t.scale) == expect_s


def test_bare_decimal_postgresql_stays_unbounded_numeric():
    sa_t = _sa_type_for_logical("DECIMAL", "postgresql", "postgresql")
    assert isinstance(sa_t, sa.Numeric)
    assert sa_t.precision is None
    assert sa_t.scale is None


def test_to_sa_value_coerces_decimal_carrier_and_iso_z():
    from datetime import datetime, timezone

    assert _to_sa_value("10.5000", "DECIMAL(12,4)") == Decimal("10.5000")
    # SQL Server DATETIME2 → naive UTC
    dt_mssql = _to_sa_value(
        "2026-07-04T06:57:37Z",
        "datetime",
        db_type="sqlserver",
        dialect_name="mssql",
    )
    assert isinstance(dt_mssql, datetime)
    assert dt_mssql.tzinfo is None
    assert dt_mssql == datetime(2026, 7, 4, 6, 57, 37)
    # Oracle NTZ datetime → naive civil digits (same as SQL Server DATETIME2).
    dt_ora_ntz = _to_sa_value("2026-07-04T06:57:37Z", "datetime", db_type="oracle")
    assert dt_ora_ntz.tzinfo is None
    assert dt_ora_ntz == datetime(2026, 7, 4, 6, 57, 37)
    # Oracle TIMESTAMPTZ → aware UTC
    dt_ora = _to_sa_value("2026-07-04T06:57:37Z", "TIMESTAMPTZ", db_type="oracle")
    assert dt_ora == datetime(2026, 7, 4, 6, 57, 37, tzinfo=timezone.utc)
