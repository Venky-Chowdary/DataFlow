"""Map≡CREATE — generic_sql SA types must honor materialize/Map stamps.

Headline invents: Oracle/DuckDB NTZ TIMESTAMP → DateTime(timezone=True);
Databricks FLOAT → sa.Double (mantissa widen); INTEGER → BigInteger.
"""

from __future__ import annotations

from typing import Any

import pytest
import sqlalchemy as sa

from connectors.generic_sql import _sa_type_for_logical
from services.type_system import materialize_dest_ddl

_DIALECT = {
    "databricks": "databricks",
    "sqlserver": "mssql",
    "oracle": "oracle",
    "duckdb": "duckdb",
    "postgresql": "postgresql",
}


def _sa(dest: str, stamp: str) -> tuple[str, Any]:
    wire = materialize_dest_ddl(dest, stamp)
    sa_t = _sa_type_for_logical(wire, _DIALECT.get(dest, dest), dest)
    return wire, sa_t


@pytest.mark.parametrize(
    "dest,stamp",
    [
        ("oracle", "TIMESTAMP"),
        ("oracle", "TIMESTAMP_NTZ"),
        ("oracle", "DATETIME2"),
        ("duckdb", "TIMESTAMP"),
        ("duckdb", "TIMESTAMP_NTZ"),
        ("duckdb", "DATETIME"),
        ("databricks", "TIMESTAMP"),
        ("databricks", "TIMESTAMP_NTZ"),
        ("databricks", "DATETIME2"),
        ("sqlserver", "TIMESTAMP"),
        ("sqlserver", "DATETIME2"),
    ],
)
def test_sa_ntz_stamps_are_timezone_naive(dest: str, stamp: str):
    wire, sa_t = _sa(dest, stamp)
    assert isinstance(sa_t, sa.DateTime), (dest, stamp, wire, sa_t)
    assert sa_t.timezone is False, (
        f"{dest} {stamp} wire={wire!r} invented timezone=True"
    )


@pytest.mark.parametrize(
    "dest,stamp",
    [
        ("oracle", "TIMESTAMPTZ"),
        ("oracle", "TIMESTAMP WITH TIME ZONE"),
        ("duckdb", "TIMESTAMPTZ"),
        ("sqlserver", "TIMESTAMPTZ"),
        ("sqlserver", "DATETIMEOFFSET"),
        ("databricks", "TIMESTAMPTZ"),
        ("postgresql", "TIMESTAMPTZ"),
    ],
)
def test_sa_tz_stamps_remain_timezone_aware(dest: str, stamp: str):
    wire, sa_t = _sa(dest, stamp)
    assert isinstance(sa_t, sa.DateTime), (dest, stamp, wire, sa_t)
    assert sa_t.timezone is True, (
        f"{dest} {stamp} wire={wire!r} lost timezone polarity"
    )


def test_databricks_float_stamp_not_double_invent():
    wire, sa_t = _sa("databricks", "REAL")
    assert wire.upper() == "FLOAT"
    nested = getattr(sa_t, "nested_type", None) or sa_t
    assert isinstance(nested, sa.Float), type(nested)
    assert not isinstance(nested, sa.Double), type(nested)


def test_databricks_float4_materialize_float_not_double():
    wire, sa_t = _sa("databricks", "FLOAT4")
    assert wire.upper() == "FLOAT"
    nested = getattr(sa_t, "nested_type", None) or sa_t
    assert isinstance(nested, sa.Float)
    assert not isinstance(nested, sa.Double)


def test_sqlserver_integer_not_bigint_invent():
    wire, sa_t = _sa("sqlserver", "INTEGER")
    assert wire.upper() in {"INTEGER", "INT"}
    nested = getattr(sa_t, "nested_type", None) or sa_t
    assert isinstance(nested, sa.Integer)
    assert not isinstance(nested, sa.BigInteger)


def test_sqlserver_bigint_keeps_bigint():
    _wire, sa_t = _sa("sqlserver", "BIGINT")
    nested = getattr(sa_t, "nested_type", None) or sa_t
    assert isinstance(nested, sa.BigInteger)


def test_oracle_timestamp_wire_stays_ntz_in_sa():
    """Oracle bare TIMESTAMP is NTZ — SA must not emit WITH TIME ZONE."""
    wire = materialize_dest_ddl("oracle", "TIMESTAMP")
    assert wire.upper() == "TIMESTAMP"
    sa_t = _sa_type_for_logical(wire, "oracle", "oracle")
    assert sa_t.timezone is False
