"""D183 — PostgreSQL TIMESTAMP(6) / TIMESTAMPTZ -> SQL Server must keep microseconds.

QA saw milliseconds land with the wrong rows left committed. The production
image has no Microsoft ODBC driver, so SQL Server runs on pymssql, which
renders a ``datetime`` parameter with three fractional digits and no offset;
the DATETIME2 carrier already bound text, DATETIMEOFFSET did not. A run that
merges into a populated table is verified on a 50-key sample, which can miss
sub-millisecond rows: only 1 row in 97 here carries microseconds.

Live: PG -> SQL Server through ``execute_tracked`` with the default preflight,
forced onto the pymssql wire (skipped when pymssql is not installed).
"""

from __future__ import annotations

import os
import socket
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pytest

_PG = {
    "host": os.getenv("PGHOST", ""),
    "port": int(os.getenv("PGPORT", "5432")),
    "database": os.getenv("PGDATABASE", ""),
    "username": os.getenv("PGUSER", ""),
    "password": os.getenv("PGPASSWORD", ""),
}
_MS = {
    "host": os.getenv("DATAFLOW_TEST_MSSQL_HOST", ""),
    "port": int(os.getenv("DATAFLOW_TEST_MSSQL_PORT", "1433")),
    "database": os.getenv("DATAFLOW_TEST_MSSQL_DB", "dataflow"),
    "username": os.getenv("DATAFLOW_TEST_MSSQL_USER", ""),
    "password": os.getenv("DATAFLOW_TEST_MSSQL_PASSWORD", ""),
}


def _reachable(host: str, port: int) -> bool:
    try:
        socket.create_connection((host, int(port)), timeout=3).close()
        return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(
    not (
        _PG["host"]
        and _MS["host"]
        and _reachable(_PG["host"], _PG["port"])
        and _reachable(_MS["host"], _MS["port"])
    ),
    reason="live PG + SQL Server: set PG* and DATAFLOW_TEST_MSSQL_* (reachable)",
)


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    pytest.importorskip("pymssql")
    import connectors.generic_sql as gs

    # The production image ships unixodbc but no Microsoft ODBC driver, so the
    # engine falls back to pymssql (FreeTDS) there: reproduce that wire.
    monkeypatch.setattr(gs, "_mssql_odbc_driver", lambda: None)
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


def _mssql():
    import pyodbc

    return pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={_MS['host']},{_MS['port']};DATABASE={_MS['database']};"
        f"UID={_MS['username']};PWD={_MS['password']};TrustServerCertificate=yes",
        autocommit=True,
        timeout=10,
    )


def _run(fake, table: str, sync_mode: str):
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    request = TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", table=table, **_PG),
        destination=EndpointConfig(
            kind="database", format="mssql", table=table,
            extra={"trust_server_certificate": True}, **_MS,
        ),
        sync_mode=sync_mode,
        validation_mode="strict",
    )
    job_id = "d183" + uuid.uuid4().hex[:12]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _dest_text(table: str, column: str) -> dict[int, str | None]:
    conn = _mssql()
    try:
        # CONVERT style 121 renders every stored digit: the read-back is not
        # itself a driver datetime that could truncate.
        rows = conn.execute(
            f"SELECT id, CONVERT(VARCHAR(40), {column}, 121) FROM [{table}] ORDER BY id"  # nosec B608
        ).fetchall()
        return {int(r[0]): r[1] for r in rows}
    finally:
        conn.close()


@pytest.fixture()
def pg_table(request):
    import psycopg2

    column = request.node.callspec.params["column"]
    cols = "ts TIMESTAMP(6)" if column == "ts" else "tstz TIMESTAMPTZ"
    table = "d183_" + uuid.uuid4().hex[:10]
    pg = psycopg2.connect(
        host=_PG["host"], port=_PG["port"], user=_PG["username"],
        password=_PG["password"], dbname=_PG["database"],
    )
    cur = pg.cursor()
    cur.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, {cols})")  # nosec B608
    exprs = ", ".join(
        "TIMESTAMP '2025-01-02 03:04:05' + id * INTERVAL '1 second' "
        "+ (CASE WHEN id % 97 = 0 THEN id * INTERVAL '1 microsecond' ELSE INTERVAL '0' END)"
        for _ in cols.split(",")
    )
    cur.execute(f"INSERT INTO {table} SELECT id, {exprs} FROM generate_series(1, 2000) id")  # nosec B608
    pg.commit()
    yield table
    pg.rollback()
    cur.execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
    pg.commit()
    pg.close()
    conn = _mssql()
    try:
        for name in [table]:
            conn.execute(f"DROP TABLE IF EXISTS [{name}]")  # nosec B608
    finally:
        conn.close()


def _expected(i: int) -> str:
    clock = datetime(2025, 1, 2, 3, 4, 5) + timedelta(seconds=i)
    if i % 97 == 0:
        clock += timedelta(microseconds=i)
    return clock.strftime("%Y-%m-%d %H:%M:%S.%f")


def _wrong_rows(table: str, column: str) -> list[tuple[int, str, str]]:
    """Committed rows whose digits differ from the source (zero padded to 7)."""
    out = []
    for key, text in _dest_text(table, column).items():
        want = _expected(key)
        got = (text or "").replace(" +00:00", "")
        if got.ljust(27, "0") != want.ljust(27, "0"):
            out.append((key, want, got))
    return out


def _table_exists(table: str) -> bool:
    conn = _mssql()
    try:
        return conn.execute("SELECT OBJECT_ID(?)", table).fetchone()[0] is not None
    finally:
        conn.close()


@pytest.mark.parametrize("column", ["ts", "tstz"])
@pytest.mark.parametrize("pre", ["none", "existing"])
def test_microseconds_survive_or_nothing_is_committed(isolated, pg_table, column, pre):
    table = pg_table
    if pre == "existing":
        ddl = "DATETIME2(7)" if column == "ts" else "DATETIMEOFFSET(7)"
        conn = _mssql()
        try:
            conn.execute(f"CREATE TABLE [{table}] (id BIGINT NOT NULL PRIMARY KEY, {column} {ddl})")  # nosec B608
        finally:
            conn.close()
    result = _run(isolated, table, "full_refresh_append")
    wrong = _wrong_rows(table, column) if _table_exists(table) else []
    assert not wrong, f"{len(wrong)} committed rows lost digits, e.g. {wrong[:2]}"
    assert result.success, result.error


@pytest.mark.parametrize("column", ["tstz"])
@pytest.mark.parametrize("mode", ["incremental_upsert"])
def test_upsert_into_populated_table_keeps_sub_millisecond_rows(isolated, pg_table, column, mode):
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    table = pg_table
    conn = _mssql()
    try:
        conn.execute(f"CREATE TABLE [{table}] (id BIGINT NOT NULL PRIMARY KEY, {column} DATETIMEOFFSET(7))")  # nosec B608
        conn.execute(  # nosec B608
            f"INSERT INTO [{table}] SELECT TOP 1000 ROW_NUMBER() OVER (ORDER BY (SELECT 1)), SYSDATETIMEOFFSET() FROM sys.all_objects a CROSS JOIN sys.all_objects b"
        )
        before = conn.execute(f"SELECT COUNT(*) FROM [{table}]").fetchone()[0]  # nosec B608
    finally:
        conn.close()
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", table=table, **_PG),
        destination=EndpointConfig(kind="database", format="mssql", table=table,
            extra={"trust_server_certificate": True}, **_MS),
        sync_mode=mode,
        stream_contracts=[{"name": table, "cursor_field": "id", "cursor_semantics": "monotonic_sequence",
            "sync_mode": mode, "primary_key": ["id"], "selected": True}],
    )
    job_id = "d183" + uuid.uuid4().hex[:12]
    isolated.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    conn = _mssql()
    try:
        after = conn.execute(f"SELECT COUNT(*) FROM [{table}]").fetchone()[0]  # nosec B608
    finally:
        conn.close()
    wrong = _wrong_rows(table, column)
    assert before == 1000
    assert result.success, result.error
    assert not wrong, f"{len(wrong)} committed rows lost digits, e.g. {wrong[:2]}"
