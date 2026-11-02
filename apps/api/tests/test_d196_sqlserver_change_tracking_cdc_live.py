"""D196 — SQL Server change tracking CDC fails on a checksum mismatch and applies nothing.

Live SQL Server 2022 with change tracking on, through
``UniversalTransferEngine().execute_tracked`` into the live PostgreSQL and MySQL.
On the round-4 base three faults sat in one family:

* Every SQL Server change-tracking token is a JSON *string*. The exactly-once
  apply read its phase with text matching only (``phase=snapshot``), so every
  snapshot chunk was taken for streaming. The snapshot -> stream handoff never
  advanced the destination's resume blob, and the next run re-read the snapshot
  tail under the committed CT version: "exactly_once redelivery checksum
  mismatch", nothing applied.
* All chunks of the initial snapshot carry the one handoff version, so the
  second chunk of any table larger than one batch was compared with the first
  chunk's payload and refused the same way.
* A ``uniqueidentifier`` column maps with the ``uuid`` transform, which the CDC
  value proof declined. Gate-8 fell back to the COUNT diagnostic, which never
  passes: SQL Server spells a GUID upper-case, the destination lower-case.

The fix makes both sides fingerprint the same canonical value; it does not
loosen the check. A real difference still fails and names the row, and a
same-position redelivery with a different payload is still refused.
"""

from __future__ import annotations

import os
import socket
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


_DESTS = {"postgresql": _PG, "mysql": _MY}


def _exec(conn, sql: str, rows=None) -> None:
    cur = conn.cursor()
    if rows is None:
        cur.execute(sql)
    else:
        cur.executemany(sql, rows)


def _fetch(conn, sql: str) -> list[tuple]:
    cur = conn.cursor()
    cur.execute(sql)
    return [tuple(r) for r in cur.fetchall()]


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    import services.sync_cursor as sc
    import src.transfer.engine as engine_mod
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    class _Store(_FakeMongo):
        def update_job_fields(self, job_id, fields):
            self.jobs.setdefault(job_id, {}).update(fields)
            return True

    fake = _Store()
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


_COLS = ["id", "name", "ts", "amt", "m", "g", "b"]

# NULL, empty, whitespace, unicode, a literal sentinel spelling, 100 ns
# datetime2, 38-digit decimal, money extremes, GUIDs with letters, bit.
_SPECIAL = [
    (1, "alpha", "2026-01-02 03:04:05.1234567", "12345678901234567890.0123456789",
     "12.3456", "6F9619FF-8B86-D011-B42D-00C04FC964FF", 1),
    (2, None, None, None, None, None, None),
    (3, "", "2026-01-02 03:04:05.0000000", "0", "0", "00000000-0000-0000-0000-000000000000", 0),
    (4, "naïve 中文 ✓", "1999-12-31 23:59:59.9999999", "-1.5",
     "-922337203685477.5808", "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11", 1),
    (5, "  ", "2026-01-01 00:00:00.0000001", "0.0000000001", "922337203685477.5807",
     "FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF", 0),
    (6, "__DF_SQL_NULL__", "2026-06-30 12:00:00.5000000", "99.9", "0.0001",
     "12345678-ABCD-EF01-2345-6789ABCDEF01", 1),
]
# More rows than one CDC batch (CHUNK_SIZE=1000): the snapshot is two chunks
# under the same change-tracking version.
_FILLER = 1100


def _create_source(table: str) -> None:
    conn = _ms()
    try:
        _exec(
            conn,
            "IF NOT EXISTS (SELECT 1 FROM sys.change_tracking_databases "
            "WHERE database_id = DB_ID()) ALTER DATABASE CURRENT SET CHANGE_TRACKING = ON "
            "(CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)",
        )
        _exec(
            conn,
            f"CREATE TABLE dbo.[{table}] (id INT NOT NULL PRIMARY KEY, "
            "name NVARCHAR(80) NULL, ts DATETIME2(7) NULL, amt DECIMAL(38,10) NULL, "
            "m MONEY NULL, g UNIQUEIDENTIFIER NULL, b BIT NULL)",
        )
        _exec(
            conn,
            f"ALTER TABLE dbo.[{table}] ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON)",
        )
        _exec(
            conn,
            f"INSERT INTO dbo.[{table}] (id, name, ts, amt, m, g, b) "  # nosec B608
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            _SPECIAL,
        )
        _exec(
            conn,
            f"""WITH n AS (SELECT TOP ({_FILLER}) ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS i
                 FROM sys.all_objects a CROSS JOIN sys.all_objects b)
               INSERT INTO dbo.[{table}] (id, name, ts, amt, m, g, b)
               SELECT 1000 + i, CONCAT(N'row ', i), DATEADD(SECOND, i, '2026-02-01'),
                      i * 1.5, i, NEWID(), i % 2 FROM n""",  # nosec B608
        )
    finally:
        conn.close()


def _drop(table: str, fmt: str) -> None:
    conn = _ms()
    try:
        _exec(conn, f"DROP TABLE IF EXISTS dbo.[{table}]")
    finally:
        conn.close()
    dconn = _dest_conn(fmt)
    try:
        q = f'"{table}"' if fmt == "postgresql" else f"`{table}`"
        _exec(dconn, f"DROP TABLE IF EXISTS {q}")
    finally:
        dconn.close()


def _request(table: str, fmt: str, sync: str = "cdc"):
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


def _run(fake, table: str, fmt: str, sync: str = "cdc"):
    from src.transfer.engine import UniversalTransferEngine

    job_id = "d196" + uuid.uuid4().hex[:12]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(_request(table, fmt, sync), job_id)


def _canon(column: str, value):
    """One spelling per value so the source and destination compare cell by cell."""
    if value is None:
        return None
    if column == "g":
        return str(value).lower()
    if column == "b":
        return bool(int(value)) if not isinstance(value, bool) else value
    if column in {"amt", "m"}:
        return Decimal(str(value)).normalize()
    if column == "ts":
        # The destination keeps microseconds (timestamp(6) / DATETIME(6)); the
        # source driver reads datetime2(7) at the same precision.
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
    assert set(dst) == set(src), (sorted(set(src) ^ set(dst))[:10])
    for key, row in src.items():
        assert dst[key] == row, (key, row, dst[key])
    return dst


def _dest_ok(fmt: str) -> None:
    if not _up(_DESTS[fmt]):
        pytest.skip(f"live {fmt} not reachable")


@pytest.mark.parametrize("fmt", ["postgresql", "mysql"])
def test_change_tracking_snapshot_then_changes_land_cell_by_cell(isolated, fmt):
    _dest_ok(fmt)
    table = "d196_" + uuid.uuid4().hex[:10]
    _create_source(table)
    try:
        first = _run(isolated, table, fmt)
        assert first.success, (first.error, first.reconciliation)
        dst = _assert_cells_match(table, fmt)
        assert dst[2][1:] == (None,) * 6, "SQL NULL lands as NULL in every type"
        assert dst[3][1] == "" and dst[5][1] == "  "
        assert dst[6][1] == "__DF_SQL_NULL__", "a literal sentinel spelling is text"

        conn = _ms()
        try:
            _exec(conn, f"UPDATE dbo.[{table}] SET name = N'upd ✓', amt = 99.5, m = 1.25, "
                        "ts = '2026-05-05 05:05:05.7654321', b = 0, "
                        "g = 'B0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A22' WHERE id = 1")
            _exec(conn, f"UPDATE dbo.[{table}] SET name = N'' , amt = NULL WHERE id = 2")
            _exec(conn, f"UPDATE dbo.[{table}] SET name = NULL WHERE id = 4")
            _exec(conn, f"INSERT INTO dbo.[{table}] (id, name, ts, amt, m, g, b) VALUES "
                        "(7, N'new ✓', '2026-07-07 07:07:07.0000007', 7.0000000007, 7, "
                        "'C0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A33', 1)")
            _exec(conn, f"DELETE FROM dbo.[{table}] WHERE id IN (3, 1500)")
        finally:
            conn.close()

        # QA's repro: the follow-up run is cdc_incremental.
        second = _run(isolated, table, fmt, "cdc_incremental")
        assert second.success, (second.error, second.reconciliation)
        dst = _assert_cells_match(table, fmt)
        assert 3 not in dst and 1500 not in dst, "CT deletes applied"
        assert dst[2][1] == "" and dst[2][3] is None
        assert dst[4][1] is None
        assert dst[7][1] == "new ✓"

        quiet = _run(isolated, table, fmt, "cdc_incremental")
        assert quiet.success, (quiet.error, quiet.reconciliation)
        _assert_cells_match(table, fmt)
    finally:
        _drop(table, fmt)


@pytest.mark.parametrize("fmt", ["postgresql"])
def test_a_real_difference_still_fails_and_names_the_row(isolated, monkeypatch, fmt):
    """Canonical fingerprints must not hide a destination cell that is wrong."""
    import src.transfer.engine as engine_mod

    _dest_ok(fmt)
    table = "d196_" + uuid.uuid4().hex[:10]
    _create_source(table)
    try:
        first = _run(isolated, table, fmt)
        assert first.success, (first.error, first.reconciliation)
        conn = _ms()
        try:
            _exec(conn, f"UPDATE dbo.[{table}] SET name = N'moved' WHERE id = 1")
        finally:
            conn.close()
        real = engine_mod.run_reconciliation

        def _corrupt_then_verify(*args, **kwargs):
            # The run applied every change; a GUID on another row is wrong when
            # Gate-8 looks.
            dconn = _dest_conn(fmt)
            try:
                _exec(dconn, f"UPDATE \"{table}\" SET g = "
                             "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12' WHERE id = 4")
            finally:
                dconn.close()
            return real(*args, **kwargs)

        monkeypatch.setattr(engine_mod, "run_reconciliation", _corrupt_then_verify)
        second = _run(isolated, table, fmt)
        assert not second.success, second.reconciliation
        assert "4" in str(second.error), second.error
    finally:
        _drop(table, fmt)
