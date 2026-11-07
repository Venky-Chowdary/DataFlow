"""SQL Server -> SQL Server lands every cell, run 1 and run 2 (QA round 11).

Found while proving the engine-family resolver (claude/r11-engine-family): on
9c75ada4 a same-engine SQL Server transfer failed even for the plain
``sqlserver`` tile. The source introspect dialed without the operator's TLS
trust (``trust_server_certificate`` / ``encrypt`` saved on the connector), so
ODBC Driver 18 refused the self-signed certificate; with no source schema the
plan created every column NOT NULL and the first NULL cell failed the INSERT.

QA's route: the streamable MCP endpoint ``tools/call start_transfer`` ->
``/api/v1/copilot/confirm`` -> job, on the real SQL Server at :1433. Cells
compared one by one: NULL, '', whitespace, unicode NVARCHAR, DATETIME2(7) and
DATETIMEOFFSET, plus a second run into the table run 1 created. One leg uses
the plain tile, one the Azure SQL Database tile at both ends.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pytest

from tests import r4_route_harness as h


def _mssql():
    conn = h.mssql_conn()
    conn.add_output_converter(-155, _datetimeoffset)
    return conn


def _datetimeoffset(raw: bytes):
    import struct

    y, mo, d, hh, mi, sec, ns, oh, om = struct.unpack("<6hI2h", raw)
    return datetime(y, mo, d, hh, mi, sec, ns // 1000, timezone(timedelta(hours=oh, minutes=om)))


def _sql(statement: str, *params: Any) -> list[tuple]:
    conn = _mssql()
    try:
        cur = conn.cursor()
        cur.execute(statement, *params)
        return [tuple(r) for r in cur.fetchall()] if cur.description else []
    finally:
        conn.close()


def _connector(kind: str, role: str) -> str:
    """Saved the way the Connectors form saves it: TLS trust under ``extra``."""
    from services.connector_store import create_connector

    _mssql().close()  # skips when SQL Server is not configured
    return str(create_connector({
        "name": f"r11q-{kind}-{role}-{uuid.uuid4().hex[:6]}", "type": kind, "role": role,
        "workspace_id": "", "ssl": False, "host": "127.0.0.1", "port": 1433,
        "database": "dataflow", "schema": "dbo",
        "username": os.environ.get("DATAFLOW_TEST_MSSQL_USER", "sa"),
        "password": os.environ.get("DATAFLOW_TEST_MSSQL_PASSWORD", ""),
        "extra": {"trust_server_certificate": True, "encrypt": "yes"},
    }).id)


_TZ = timezone(timedelta(hours=5, minutes=30))
_ROWS = [
    (1, "Zoë ✓ 日本語 🚀", "plain", datetime(2026, 3, 29, 1, 30, 15, 123456),
     datetime(2026, 3, 29, 1, 30, 15, 123456, tzinfo=timezone.utc), Decimal("12.50")),
    (2, None, "", datetime(1753, 1, 1), datetime(2026, 10, 25, 23, 59, 59, 999999, tzinfo=_TZ), None),
    (3, "", None, None, None, Decimal("-0.01")),
    (4, "  ", "  padded  ", datetime(9999, 12, 31, 23, 59, 59, 999999), None, Decimal("99999999.99")),
    (5, "ünïcødé — ‘quotes’", "x", datetime(2000, 2, 29), None, Decimal("0.00")),
]
_COLS = "id, name, note, dt, ts, amt"


def _seed(table: str) -> None:
    _sql(f"CREATE TABLE dbo.[{table}] (id BIGINT PRIMARY KEY, name NVARCHAR(MAX) NULL, "
         "note NVARCHAR(40) NULL, dt DATETIME2(7) NULL, ts DATETIMEOFFSET(6) NULL, "
         "amt DECIMAL(10,2) NULL)")
    for row in _ROWS:
        _sql(f"INSERT INTO dbo.[{table}] ({_COLS}) VALUES (?, ?, ?, ?, ?, ?)", *row)


def _edit(table: str) -> None:
    _sql(f"UPDATE dbo.[{table}] SET name = NULL, note = N'changed' WHERE id = 1")
    _sql(f"UPDATE dbo.[{table}] SET name = N'was null', dt = '2026-01-01T00:00:00.1234567' WHERE id = 2")
    _sql(f"UPDATE dbo.[{table}] SET name = N'東京 é' WHERE id = 3")
    _sql(f"INSERT INTO dbo.[{table}] ({_COLS}) VALUES (?, ?, ?, ?, ?, ?)",
         6, "nouveau ✓", "  ", datetime(2026, 6, 1, 12, 0, 0, 1), None, Decimal("1.00"))


def _cells(table: str) -> list[tuple]:
    out = []
    for row in _sql(f"SELECT {_COLS} FROM dbo.[{table}] ORDER BY id"):
        ts = row[4].astimezone(timezone.utc) if isinstance(row[4], datetime) else row[4]
        out.append((int(row[0]), row[1], row[2], row[3], ts, row[5]))
    return out


def _nullable(table: str) -> dict[str, str]:
    return {
        str(name).lower(): str(nullable)
        for name, nullable in _sql(
            "SELECT COLUMN_NAME, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = ?", table,
        )
    }


def _drop(*tables: str) -> None:
    for t in tables:
        try:
            _sql(f"DROP TABLE IF EXISTS dbo.[{t}]")
        except Exception:
            pass


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_TRAINING", "off")
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _run(client, src: str, src_t: str, dst: str, dst_t: str) -> dict:
    return h.stage_and_run(client, "start_transfer", {
        "source_connector_id": src, "source_table": src_t,
        "dest_connector_id": dst, "dest_table": dst_t,
        "sync_mode": "full_refresh_overwrite", "primary_key": "id",
        "validation_mode": "strict",
    })


@pytest.mark.parametrize("src_kind,dst_kind", [
    ("sqlserver", "sqlserver"),
    ("azure_sql_database", "azure_sql_database"),
], ids=["plain_tile", "azure_sql_alias"])
def test_sqlserver_to_sqlserver_every_cell_run1_and_run2(client, src_kind, dst_kind):
    token = uuid.uuid4().hex[:8]
    src_t, dst_t = f"r11qs_{token}", f"r11qd_{token}"
    try:
        _seed(src_t)
        src, dst = _connector(src_kind, "source"), _connector(dst_kind, "destination")
        _run(client, src, src_t, dst, dst_t)
        assert _cells(dst_t) == _cells(src_t)
        # The source columns are nullable; the created ones must be too.
        nullable = _nullable(dst_t)
        assert nullable["id"] == "NO"
        assert all(nullable[c] == "YES" for c in ("name", "note", "dt", "ts", "amt")), nullable

        _edit(src_t)
        _run(client, src, src_t, dst, dst_t)  # run 2 into the table run 1 created
        got = _cells(dst_t)
        assert got == _cells(src_t)
        assert len(got) == 6
    finally:
        _drop(src_t, dst_t)


def test_sqlserver_source_introspect_keeps_the_saved_tls_trust(client):
    """The source schema the plan is built from comes from the live catalog."""
    from services.connector_probe import endpoint_from_saved_connector
    from src.transfer.endpoint_intelligence import introspect_endpoint

    token = uuid.uuid4().hex[:8]
    src_t = f"r11qs_{token}"
    try:
        _seed(src_t)
        src = _connector("sqlserver", "source")
        info = introspect_endpoint(endpoint_from_saved_connector(src, table=src_t))
        assert info.get("table_exists") is True, info.get("message")
        nulls = {str(k).lower(): v for k, v in (info.get("schema_nullability") or {}).items()}
        assert nulls.get("name") is True and nulls.get("id") is False, nulls
    finally:
        _drop(src_t)
