"""Wave 66: SQL Server ROWVERSION≠datetime + UNIQUEIDENTIFIER native UUID bind."""

from __future__ import annotations

import sys
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_sqlserver_timestamp_introspects_as_rowversion():
    from services.schema_introspect import _sqlserver_to_logical
    from services.type_system import ddl_type, rowversion_would_collapse_to_temporal

    assert _sqlserver_to_logical("timestamp") == "ROWVERSION"
    assert _sqlserver_to_logical("rowversion") == "ROWVERSION"
    assert _sqlserver_to_logical("binary") == "BINARY"

    assert ddl_type("sqlserver", "ROWVERSION") == "ROWVERSION"
    assert ddl_type("postgresql", "ROWVERSION") == "BYTEA"
    assert ddl_type("mysql", "ROWVERSION") == "BINARY(8)"
    assert ddl_type("oracle", "ROWVERSION") == "RAW(8)"

    assert rowversion_would_collapse_to_temporal("ROWVERSION", "TIMESTAMP") is True
    assert rowversion_would_collapse_to_temporal("ROWVERSION", "TIMESTAMPTZ") is True
    assert rowversion_would_collapse_to_temporal("ROWVERSION", "BYTEA") is False
    assert rowversion_would_collapse_to_temporal("ROWVERSION", "BINARY(8)") is False


def test_coerce_rowversion_hex_and_refuse_datetime():
    from connectors.sql_bind import coerce_rowversion_wire, normalize_sql_bind_value

    expected = bytes.fromhex("0000000000000001")
    assert coerce_rowversion_wire("0x0000000000000001") == expected
    assert coerce_rowversion_wire(expected) == expected
    assert coerce_rowversion_wire(1) == expected
    assert normalize_sql_bind_value(
        "00000000000000FF", "ROWVERSION", engine="sqlserver"
    ) == bytes.fromhex("00000000000000FF")

    with pytest.raises(ValueError, match="datetime"):
        coerce_rowversion_wire("2024-01-01T12:00:00Z")
    with pytest.raises(ValueError, match="8 bytes"):
        coerce_rowversion_wire(b"\x01\x02")


def test_mssql_uuid_bind_returns_native_uuid():
    from connectors.sql_bind import normalize_sql_bind_value
    from connectors.generic_sql import _to_sa_value

    raw = "67E616B4-7DBC-4D14-B0BA-0F7DE2F94AEE"
    out = normalize_sql_bind_value(raw, "UNIQUEIDENTIFIER", engine="sqlserver")
    assert isinstance(out, uuid.UUID)
    assert str(out) == raw.lower()

    # Non-MSSQL keeps canonical string (psycopg / MySQL CHAR(36)).
    pg = normalize_sql_bind_value(raw, "UUID", engine="postgresql")
    assert pg == raw.lower()
    assert isinstance(pg, str)

    sa = _to_sa_value(raw, "UNIQUEIDENTIFIER", db_type="azure_sql")
    assert isinstance(sa, uuid.UUID)


def test_rowversion_precision_collapse_surfaces_temporal():
    from services.type_system import (
        is_lossy_coercion,
        is_precision_collapse_coercion,
        specialty_carrier_would_collapse,
    )

    assert is_precision_collapse_coercion("ROWVERSION", "TIMESTAMP") is True
    assert is_lossy_coercion("ROWVERSION", "DATETIME2") is True
    assert specialty_carrier_would_collapse("ROWVERSION", "VARCHAR(32)") is True
    assert specialty_carrier_would_collapse("ROWVERSION", "BYTEA") is False
