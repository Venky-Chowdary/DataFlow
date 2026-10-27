"""D3 / D10 — PG → DuckDB type fidelity across create-new and reruns.

QA logged (on c6c16128) that a PG ``NUMERIC(12,2)`` landed in DuckDB as
``VARCHAR`` (D3) and that PG ``TIMESTAMP`` → DuckDB was blocked as a fidelity
collapse (D10); neither could be retested on production (DuckDB loopback
refused). Both pass on this code base with an in-process DuckDB file; this
locks the behaviour: the created table keeps exact DECIMAL/TIMESTAMP/TIMESTAMPTZ
carriers, NULLs land as NULL, and reruns into the table DataFlow created
(overwrite and append) pass preflight instead of reporting a collapse.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from tests.helpers.live_env import pg_creds, pg_up  # noqa: E402

pytestmark = pytest.mark.skipif(not pg_up(), reason="PostgreSQL not reachable")


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    return fake


@pytest.fixture()
def pg_table():
    import psycopg2

    creds = pg_creds()
    conn = psycopg2.connect(
        host=creds["host"], port=creds["port"], user=creds["username"],
        password=creds["password"], dbname=creds["database"],
    )
    conn.autocommit = True
    table = "d3_d10_" + uuid.uuid4().hex[:8]
    with conn.cursor() as cur:
        cur.execute(
            f"CREATE TABLE {table} (id int PRIMARY KEY, amt numeric(12,2), "
            "ts timestamp, tz timestamptz)"
        )
        cur.execute(
            f"INSERT INTO {table} VALUES "
            "(1, 12.34, '2024-01-02 03:04:05', '2024-01-02 03:04:05+00'), "
            "(2, NULL, NULL, NULL)"
        )
    try:
        yield creds, table
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
        conn.close()


def _run(fake, creds, table: str, dst: Path, sync_mode: str):
    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="postgresql", host=creds["host"], port=creds["port"],
            username=creds["username"], password=creds["password"],
            database=creds["database"], table=table,
        ),
        destination=EndpointConfig(kind="database", format="duckdb", database=str(dst), table=table),
        sync_mode=sync_mode,
        validation_mode="strict",
    )
    job_id = "d3d10" + uuid.uuid4().hex[:12]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def test_pg_numeric_and_timestamps_keep_their_duckdb_carriers(tmp_path, isolated, pg_table):
    duckdb = pytest.importorskip("duckdb")
    creds, table = pg_table
    dst = tmp_path / "dst.duckdb"

    for mode in ("full_refresh_overwrite", "full_refresh_append", "full_refresh_overwrite"):
        result = _run(isolated, creds, table, dst, mode)
        assert result.success, f"{mode}: {result.error}"

    conn = duckdb.connect(str(dst))
    try:
        types = {name: dtype for name, dtype, *_ in conn.execute(f"DESCRIBE {table}").fetchall()}
        rows = conn.execute(f"SELECT id, amt, ts, tz FROM {table} ORDER BY id").fetchall()
    finally:
        conn.close()
    assert types["amt"] == "DECIMAL(12,2)"
    assert types["ts"] == "TIMESTAMP"
    assert types["tz"] == "TIMESTAMP WITH TIME ZONE"
    assert len(rows) == 2
    assert str(rows[0][1]) == "12.34"
    assert rows[1][1:] == (None, None, None)
