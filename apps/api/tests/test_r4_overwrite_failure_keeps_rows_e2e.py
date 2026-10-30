"""A failed overwrite must not leave the destination empty (generic SQL path).

``full_refresh_overwrite`` into an existing DuckDB / SQL Server table empties
the live table (``empty_existing_for_overwrite``) on its own autocommit
connection, then the writer loads the new rows in separate batches. When the
load fails part-way, nothing put the old rows back: the run failed and the
operator was left with an empty table (or a partial one).

Each case seeds the destination with rows the operator owns, then runs an
overwrite whose write the destination rejects mid-load - a value a DuckDB
CHECK refuses (Validate does not see DuckDB CHECKs) and a SQL Server trigger
(no gate can see a trigger). The run must fail, and the pre-run rows must be
exactly what they were, with no backup table left behind.

Live: PostgreSQL (source) from ``PG*``; SQL Server on :1433 with
``MSSQL_SA_PASSWORD`` / ``DATAFLOW_TEST_MSSQL_PASSWORD``. Skipped when absent.
"""

from __future__ import annotations

import os
import socket
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

psycopg2 = pytest.importorskip("psycopg2")

PG = {
    "host": os.environ.get("PGHOST") or "127.0.0.1",
    "port": int(os.environ.get("PGPORT") or 5432),
    "user": os.environ.get("PGUSER") or "dataflow",
    "password": os.environ.get("PGPASSWORD") or "dataflow",
    "dbname": os.environ.get("PGDATABASE") or "dataflow",
}
MSSQL_PASSWORD = os.environ.get("DATAFLOW_TEST_MSSQL_PASSWORD") or os.environ.get(
    "MSSQL_SA_PASSWORD", ""
)
PRE_RUN = [(1000, 7, "kept"), (1001, 8, None), (1002, 9, "")]


def _up(port: int) -> bool:
    try:
        socket.create_connection(("127.0.0.1", port), timeout=1).close()
        return True
    except OSError:
        return False


@pytest.fixture()
def fake_jobs(tmp_path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


@pytest.fixture()
def pg_source():
    if not _up(PG["port"]):
        pytest.skip("PostgreSQL not reachable")
    try:
        conn = psycopg2.connect(**PG)
    except Exception as exc:  # environmental: rejected credentials
        pytest.skip(f"PostgreSQL login failed: {exc}")
    conn.autocommit = True
    table = f"r4ow_{uuid.uuid4().hex[:8]}"
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, v INT, note TEXT)")
    # 400 good rows, then one the destination rejects: the load fails after
    # the destination was emptied and earlier batches may have landed.
    rows = [(i, i, f"n{i}") for i in range(1, 401)] + [(401, -1, "bad")]
    cur.executemany(f"INSERT INTO {table} VALUES (%s, %s, %s)", rows)
    try:
        yield table
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        conn.close()


def _source(table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database", format="postgresql", host=PG["host"], port=PG["port"],
        database=PG["dbname"], username=PG["user"], password=PG["password"],
        schema="public", table=table,
    )


def _run(fake, source: EndpointConfig, destination: EndpointConfig):
    request = TransferRequest(
        source=source,
        destination=destination,
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
    )
    job_id = "r4ow" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def test_duckdb_failed_overwrite_keeps_pre_run_rows(fake_jobs, pg_source, tmp_path):
    duckdb = pytest.importorskip("duckdb")
    path = str(tmp_path / "dest.duckdb")
    con = duckdb.connect(path)
    con.execute(
        f"CREATE TABLE {pg_source} (id BIGINT PRIMARY KEY, v BIGINT CHECK (v > 0), note VARCHAR)"
    )
    con.executemany(f"INSERT INTO {pg_source} VALUES (?, ?, ?)", PRE_RUN)
    con.close()

    result = _run(
        fake_jobs,
        _source(pg_source),
        EndpointConfig(kind="database", format="duckdb", database=path, table=pg_source),
    )

    assert not result.success
    con = duckdb.connect(path, read_only=True)
    try:
        rows = con.execute(f"SELECT id, v, note FROM {pg_source} ORDER BY id").fetchall()
        tables = {r[0] for r in con.execute("SELECT table_name FROM information_schema.tables").fetchall()}
        check = con.execute(
            "SELECT count(*) FROM duckdb_constraints() "
            f"WHERE table_name = '{pg_source}' AND constraint_type = 'CHECK'"
        ).fetchone()[0]
    finally:
        con.close()
    assert rows == PRE_RUN
    assert tables == {pg_source}, "no backup table may be left behind"
    assert check == 1, "the live table object (and its CHECK) is the one kept"


def test_sqlserver_failed_overwrite_keeps_pre_run_rows(fake_jobs, pg_source):
    pymssql = pytest.importorskip("pymssql")
    if not MSSQL_PASSWORD or not _up(1433):
        pytest.skip("SQL Server not configured")
    conn = pymssql.connect(server="127.0.0.1", port=1433, user="sa", password=MSSQL_PASSWORD,
                           database="dataflow", autocommit=True)
    cur = conn.cursor()
    table = pg_source
    cur.execute(f"CREATE TABLE dbo.{table} (id BIGINT PRIMARY KEY, v BIGINT, note NVARCHAR(50))")
    cur.executemany(f"INSERT INTO dbo.{table} VALUES (%d, %d, %s)", PRE_RUN)
    # The destination rejects the bad row at write time; no gate can see this.
    cur.execute(
        f"CREATE TRIGGER dbo.trg_{table} ON dbo.{table} AFTER INSERT AS BEGIN "
        "IF EXISTS (SELECT 1 FROM inserted WHERE v < 0) "
        "THROW 50001, 'negative v rejected', 1; END"
    )
    try:
        result = _run(
            fake_jobs,
            _source(table),
            EndpointConfig(
                kind="database", format="sqlserver", host="127.0.0.1", port=1433,
                database="dataflow", username="sa", password=MSSQL_PASSWORD,
                schema="dbo", table=table, extra={"trust_server_certificate": True},
            ),
        )

        assert not result.success
        cur.execute(f"SELECT id, v, note FROM dbo.{table} ORDER BY id")
        assert [tuple(r) for r in cur.fetchall()] == PRE_RUN
        cur.execute(f"SELECT name FROM sys.tables WHERE name LIKE '{table}%'")
        assert [r[0] for r in cur.fetchall()] == [table], "no backup table may be left behind"
        cur.execute(f"SELECT count(*) FROM sys.triggers WHERE name = 'trg_{table}'")
        assert cur.fetchone()[0] == 1, "the live table object (and its trigger) is the one kept"
    finally:
        cur.execute(
            f"SELECT name FROM sys.tables WHERE name LIKE '{table}%'"
        )
        for (name,) in cur.fetchall():
            cur.execute(f"DROP TABLE dbo.[{name}]")
        conn.close()
