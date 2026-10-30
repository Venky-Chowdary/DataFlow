"""Provisional D203 — SQL Server native CDC wrote SQL NULL as ''.

Live SQL Server 2022 with native CDC (``sys.sp_cdc_enable_table``, SQL Server
Agent capture job) through ``UniversalTransferEngine().execute_tracked`` into
the live PostgreSQL and MySQL.

``connectors/sqlserver_cdc_native._sqlserver_native_wire_value`` turned every
``None`` into ``""`` and every other value into ``str(value)`` for the
snapshot, the incremental-snapshot chunks and every ``cdc.fn_cdc_get_*``
change row. The shared CDC matrix builder then saw a present empty string, so
a source NULL landed as ``''`` in every text column (and failed every typed
column). The Change Tracking reader beside it encodes with ``wire_cell``.

The test covers a snapshot carrying NULL, '', whitespace, unicode and a literal
sentinel spelling; then UPDATE to NULL, INSERTs with each of those, a DELETE,
and the ``cdc_incremental`` follow-up run, compared cell by cell.
"""

from __future__ import annotations

import os
import socket
import time
import uuid
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest

_MS = {
    "host": os.getenv("DATAFLOW_TEST_MSSQL_HOST", ""),
    "port": int(os.getenv("DATAFLOW_TEST_MSSQL_PORT", "1433")),
    "database": os.getenv("DATAFLOW_TEST_MSSQL_DB", "dataflow"),
    "username": os.getenv("DATAFLOW_TEST_MSSQL_USER", ""),
    "password": os.getenv("DATAFLOW_TEST_MSSQL_PASSWORD", ""),
}
_MY = {
    "host": os.getenv("DATAFLOW_TEST_MYSQL_HOST", ""),
    "port": int(os.getenv("DATAFLOW_TEST_MYSQL_PORT", "3306")),
    "database": os.getenv("DATAFLOW_TEST_MYSQL_DB", "dataflow"),
    "username": os.getenv("DATAFLOW_TEST_MYSQL_USER", ""),
    "password": os.getenv("DATAFLOW_TEST_MYSQL_PASSWORD", ""),
}
_PG = {
    "host": os.getenv("PGHOST", ""),
    "port": int(os.getenv("PGPORT", "5432")),
    "database": os.getenv("PGDATABASE", ""),
    "username": os.getenv("PGUSER", ""),
    "password": os.getenv("PGPASSWORD", ""),
}
_DESTS = {"postgresql": _PG, "mysql": _MY}


def _up(cfg: dict) -> bool:
    if not (cfg["host"] and cfg["password"]):
        return False
    try:
        socket.create_connection((cfg["host"], int(cfg["port"])), timeout=3).close()
        return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(not _up(_MS), reason="live SQL Server not reachable")


def _ms():
    import pyodbc

    return pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={_MS['host']},{_MS['port']};DATABASE={_MS['database']};"
        f"UID={_MS['username']};PWD={_MS['password']};TrustServerCertificate=yes",
        timeout=10,
        autocommit=True,
    )


def _dest_conn(fmt: str):
    if fmt == "postgresql":
        import psycopg2

        conn = psycopg2.connect(
            host=_PG["host"], port=_PG["port"], user=_PG["username"],
            password=_PG["password"], dbname=_PG["database"],
        )
        conn.autocommit = True
        return conn
    import pymysql

    return pymysql.connect(
        host=_MY["host"], port=_MY["port"], user=_MY["username"],
        password=_MY["password"], database=_MY["database"], autocommit=True,
        charset="utf8mb4",
    )


def _exec(conn, sql: str, rows=None) -> None:
    cur = conn.cursor()
    if rows is None:
        cur.execute(sql)
    else:
        cur.executemany(sql, rows)


def _fetch(conn, sql: str, params=None) -> list[tuple]:
    cur = conn.cursor()
    if params is None:
        cur.execute(sql)
    else:
        cur.execute(sql, params)
    return [tuple(r) for r in cur.fetchall()]


def _agent_running() -> bool:
    conn = _ms()
    try:
        rows = _fetch(
            conn,
            "SELECT status_desc FROM sys.dm_server_services "
            "WHERE servicename LIKE 'SQL Server Agent%'",
        )
    finally:
        conn.close()
    return bool(rows) and str(rows[0][0]).lower() == "running"


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    import services.sync_cursor as sc
    import src.transfer.cdc_transfer as cdc_mod
    import src.transfer.engine as engine_mod
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    class _Store(_FakeMongo):
        def update_job_fields(self, job_id, fields):
            self.jobs.setdefault(job_id, {}).update(fields)
            return True

    class _NoChangeTracking:
        """The run must read native CDC; a CT fallback would hide the defect."""

        def __init__(self, *a, **k):
            raise AssertionError("fell back to SQL Server Change Tracking")

    fake = _Store()
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(cdc_mod, "SqlServerChangeTrackingCdc", _NoChangeTracking)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


_COLS = ["id", "name", "note", "amt", "ts"]

# NULL, '', whitespace, unicode (BMP + astral), a literal sentinel spelling.
_SNAPSHOT = [
    (1, "alpha", "a", "1.50", "2026-01-02 03:04:05.123456"),
    (2, None, None, None, None),
    (3, "", "", "0", "2026-01-02 03:04:05"),
    (4, "naïve 中文 ✓ 😀", "ß", "-1.5", "1999-12-31 23:59:59"),
    (5, "  ", " ", "0.0001", "2026-01-01 00:00:00"),
    (6, "__DF_SQL_NULL__", "x", "99.9", "2026-06-30 12:00:00.5"),
]


def _create_source(table: str) -> str:
    conn = _ms()
    try:
        _exec(
            conn,
            "IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = DB_NAME() "
            "AND is_cdc_enabled = 1) EXEC sys.sp_cdc_enable_db",
        )
        _exec(
            conn,
            f"CREATE TABLE dbo.[{table}] (id INT NOT NULL PRIMARY KEY, "
            "name NVARCHAR(80) NULL, note VARCHAR(40) NULL, amt DECIMAL(18,4) NULL, "
            "ts DATETIME2(6) NULL)",
        )
        _exec(
            conn,
            f"INSERT INTO dbo.[{table}] (id, name, note, amt, ts) "  # nosec B608
            "VALUES (?, ?, ?, ?, ?)",
            _SNAPSHOT,
        )
        _exec(
            conn,
            "EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', "
            f"@source_name = N'{table}', @role_name = NULL, "
            "@supports_net_changes = 0",
        )
        rows = _fetch(
            conn,
            "SELECT ct.capture_instance FROM cdc.change_tables ct "
            "WHERE ct.source_object_id = OBJECT_ID(?)",
            (f"dbo.{table}",),
        )
        return str(rows[0][0])
    finally:
        conn.close()


def _drop(table: str, fmt: str) -> None:
    conn = _ms()
    try:
        try:
            _exec(
                conn,
                "EXEC sys.sp_cdc_disable_table @source_schema = N'dbo', "
                f"@source_name = N'{table}', @capture_instance = N'all'",
            )
        except Exception:
            pass
        _exec(conn, f"DROP TABLE IF EXISTS dbo.[{table}]")
    finally:
        conn.close()
    dconn = _dest_conn(fmt)
    try:
        q = f'"{table}"' if fmt == "postgresql" else f"`{table}`"
        _exec(dconn, f"DROP TABLE IF EXISTS {q}")
    finally:
        dconn.close()


def _wait_captured(capture_instance: str, expected: set[tuple[int, int]]) -> None:
    """Block until the Agent capture job has harvested every (operation, id)."""
    deadline = time.monotonic() + 60
    seen: set[tuple[int, int]] = set()
    while time.monotonic() < deadline:
        conn = _ms()
        try:
            seen = {
                (int(op), int(i))
                for op, i in _fetch(
                    conn,
                    f"SELECT __$operation, id FROM cdc.[{capture_instance}_CT]",  # nosec B608
                )
            }
        finally:
            conn.close()
        if expected <= seen:
            return
        time.sleep(0.5)
    raise AssertionError(f"capture job did not harvest {expected - seen}")


def _request(table: str, fmt: str, sync: str):
    from src.transfer.models import EndpointConfig, TransferRequest

    return TransferRequest(
        source=EndpointConfig(
            kind="database", format="sqlserver", schema="dbo", table=table,
            extra={"trust_server_certificate": True}, **_MS,
        ),
        destination=EndpointConfig(kind="database", format=fmt, table=table, **_DESTS[fmt]),
        sync_mode=sync,
        stream_contracts=[
            {"name": table, "selected": True, "primary_key": ["id"], "sync_mode": sync,
             "cursor_semantics": "cdc_position"}
        ],
        skip_preflight=True,
        validation_mode="strict",
    )


def _run(fake, table: str, fmt: str, sync: str):
    from src.transfer.engine import UniversalTransferEngine

    job_id = "d203" + uuid.uuid4().hex[:12]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(_request(table, fmt, sync), job_id)


def _canon(column: str, value):
    if value is None:
        return None
    if column == "amt":
        return Decimal(str(value)).normalize()
    if column == "ts":
        assert isinstance(value, datetime), (column, value)
        return value.replace(tzinfo=None)
    if column == "id":
        return int(value)
    return value


def _image(conn, table: str, fmt: str) -> dict[int, tuple]:
    if fmt == "sqlserver":
        sql = f"SELECT {', '.join(_COLS)} FROM dbo.[{table}]"  # nosec B608
    elif fmt == "postgresql":
        sql = f'SELECT {", ".join(_COLS)} FROM "{table}"'  # nosec B608
    else:
        sql = f"SELECT {', '.join(_COLS)} FROM `{table}`"  # nosec B608
    out: dict[int, tuple] = {}
    for row in _fetch(conn, sql):
        canon = tuple(_canon(c, v) for c, v in zip(_COLS, row))
        out[canon[0]] = canon
    return out


def _assert_cells_match(table: str, fmt: str) -> dict[int, tuple]:
    src_conn, dst_conn = _ms(), _dest_conn(fmt)
    try:
        src = _image(src_conn, table, "sqlserver")
        dst = _image(dst_conn, table, fmt)
    finally:
        src_conn.close()
        dst_conn.close()
    assert set(dst) == set(src), sorted(set(src) ^ set(dst))
    for key, row in src.items():
        assert dst[key] == row, (key, row, dst[key])
    return dst


@pytest.mark.parametrize("fmt", ["postgresql", "mysql"])
def test_native_cdc_null_empty_whitespace_unicode_land_cell_by_cell(isolated, fmt):
    if not _up(_DESTS[fmt]):
        pytest.skip(f"live {fmt} not reachable")
    if not _agent_running():
        pytest.skip("SQL Server Agent is not running (native CDC capture job)")
    table = "d203_" + uuid.uuid4().hex[:10]
    ci = _create_source(table)
    try:
        first = _run(isolated, table, fmt, "cdc")
        assert first.success, (first.error, first.reconciliation)
        dst = _assert_cells_match(table, fmt)
        assert dst[2][1:] == (None,) * 4, "SQL NULL lands as NULL in every column"
        assert dst[3][1] == "" and dst[3][2] == "", "'' stays ''"
        assert dst[5][1] == "  " and dst[5][2] == " ", "whitespace kept"
        assert dst[4][1] == "naïve 中文 ✓ 😀"
        assert dst[6][1] == "__DF_SQL_NULL__", "a literal sentinel spelling is text"

        conn = _ms()
        try:
            _exec(conn, f"UPDATE dbo.[{table}] SET name = NULL, note = NULL, amt = NULL, "
                        "ts = NULL WHERE id = 1")
            _exec(conn, f"UPDATE dbo.[{table}] SET name = N'', note = '' WHERE id = 2")
            _exec(conn, f"INSERT INTO dbo.[{table}] (id, name, note, amt, ts) VALUES "
                        "(7, NULL, NULL, NULL, NULL), (8, N'', '', 0, '2026-02-02'), "
                        "(9, N'   ', '  ', 1, '2026-02-03'), "
                        "(10, N'Grüße 中文 😀', 'é', 2.5, '2026-02-04 01:02:03.5')")
            _exec(conn, f"DELETE FROM dbo.[{table}] WHERE id = 3")
        finally:
            conn.close()
        _wait_captured(ci, {(4, 1), (4, 2), (2, 7), (2, 8), (2, 9), (2, 10), (1, 3)})

        second = _run(isolated, table, fmt, "cdc_incremental")
        assert second.success, (second.error, second.reconciliation)
        dst = _assert_cells_match(table, fmt)
        assert 3 not in dst, "native CDC delete applied"
        assert dst[1][1:] == (None,) * 4, "UPDATE to NULL lands as NULL"
        assert dst[2][1] == "" and dst[2][2] == ""
        assert dst[7][1:] == (None,) * 4
        assert dst[8][1] == "" and dst[9][1] == "   " and dst[9][2] == "  "
        assert dst[10][1] == "Grüße 中文 😀"

        quiet = _run(isolated, table, fmt, "cdc_incremental")
        assert quiet.success, (quiet.error, quiet.reconciliation)
        _assert_cells_match(table, fmt)
    finally:
        _drop(table, fmt)
