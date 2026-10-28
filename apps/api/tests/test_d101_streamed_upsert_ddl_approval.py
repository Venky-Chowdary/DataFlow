"""D101 (runtime) — an approved unique index was refused again at Execute.

Validate discloses the ``CREATE UNIQUE INDEX`` an upsert into an existing
table needs and the operator approves it (``ddl_changes_acknowledged``). The
streamed (DB→DB) path dropped that approval twice: Execute's preflight was
never handed the flag, so it re-blocked the run Validate had cleared, and the
streamed writers never got ``upsert_ddl_approved``, so even past preflight
they refused the DDL with the "must be approved at Validate" error.

Live PostgreSQL destination, SQLite source, preflight ON.
"""

from __future__ import annotations

import os
import socket
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


def _pg():
    psycopg2 = pytest.importorskip("psycopg2")
    try:
        with socket.create_connection(("127.0.0.1", 5432), timeout=1):
            pass
        conn = psycopg2.connect(
            host="127.0.0.1", dbname="dataflow", user="dataflow",
            password=os.environ.get("PGPASSWORD", "dataflow"),
        )
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"PostgreSQL not available: {exc}")
    conn.autocommit = True
    return conn


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _request(src: Path, table: str, *, approved: bool) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(
            kind="database", format="postgresql", host="127.0.0.1", port=5432,
            database="dataflow", username="dataflow",
            password=os.environ.get("PGPASSWORD", "dataflow"),
            schema="public", table=table,
        ),
        sync_mode="upsert",
        validation_mode="strict",
        duplicate_policy="upsert",
        stream_contracts=[
            {"name": "t", "selected": True, "sync_mode": "upsert", "primary_key": "code"}
        ],
        ddl_changes_acknowledged=approved,
        acknowledgment_actor="tester",
        acknowledgment_reason="approved the disclosed unique index",
    )


def _run(fake, request):
    job_id = "d101" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


@pytest.fixture()
def keyless_dest(tmp_path):
    src = tmp_path / "src.sqlite"
    conn = sqlite3.connect(str(src))
    conn.execute("CREATE TABLE t (code TEXT, name TEXT)")
    conn.executemany("INSERT INTO t VALUES (?, ?)", [("a", "x"), ("b", "y")])
    conn.commit()
    conn.close()
    pg = _pg()
    table = "d101_" + uuid.uuid4().hex[:8]
    cur = pg.cursor()
    cur.execute(f"CREATE TABLE {table} (code TEXT, name TEXT)")
    try:
        yield src, table, cur
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        pg.close()


def test_approved_unique_index_is_created_and_the_upsert_lands(isolated, keyless_dest):
    src, table, cur = keyless_dest
    result = _run(isolated, _request(src, table, approved=True))
    assert result.success, result.error
    cur.execute(f"SELECT count(*) FROM {table}")
    assert cur.fetchone()[0] == 2
    cur.execute(
        "SELECT 1 FROM pg_indexes WHERE tablename = %s AND indexdef ILIKE '%%UNIQUE%%'",
        (table,),
    )
    assert cur.fetchone() is not None


def test_unapproved_upsert_still_refuses_and_writes_nothing(isolated, keyless_dest):
    src, table, cur = keyless_dest
    result = _run(isolated, _request(src, table, approved=False))
    assert not result.success
    assert "unique" in str(result.error).lower()
    cur.execute(f"SELECT count(*) FROM {table}")
    assert cur.fetchone()[0] == 0
