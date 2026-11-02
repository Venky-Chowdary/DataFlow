"""X-29: an API-created CDC schedule must run against a keyed PG source.

The defect: POST /api/v1/schedules with ``sync_mode=cdc`` and a saved PG
source stores no ``stream_contracts``; ``build_schedule_request`` synthesizes
one with ``primary_key: ""``. The preflight G9 (sync-contract) gate minted by
the Kernel only saw the *contract-declared* key — never the live source
catalog that Validate (preflight_router), the Execute policy pass
(``_catalog_pk_for_policy``) and the CDC transfer itself
(``_catalog_cdc_primary_key``) already fall back to. The run refused with a
"Sync contract incomplete" root and moved 0 rows.

This test drives the real store + runner + engine path: saved connectors,
``create_schedule`` (no primary_key — the bare-API shape), ``_dispatch_transfer``,
and a real PG ``wal_level=logical`` slot. It asserts the snapshot lands and
that a subsequent UPDATE/INSERT is captured by the resumed stream.

Skips cleanly when localhost:5432 lacks a logical-decoding PostgreSQL.
"""

from __future__ import annotations

import socket
import sys
import time
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import psycopg2  # noqa: E402

from connectors.postgresql_conn import get_connection  # noqa: E402

_TABLE = f"x29_orders_{uuid.uuid4().hex[:8]}"
_COLS = ["id", "sku", "qty", "updated_at"]


def _connect(db: str = "dataflow"):
    return get_connection(
        host="localhost",
        port=5432,
        database=db,
        username="dataflow",
        password="dataflow",
        connection_string="",
        ssl=False,
    )


def _exec(sql: str, db: str = "dataflow") -> None:
    conn = _connect(db)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql)
    finally:
        conn.close()


def _rows(db: str, table: str) -> list[tuple]:
    conn = _connect(db)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f"SELECT id, sku, qty FROM {table} ORDER BY id")
            return list(cur.fetchall())
    finally:
        conn.close()


def _logical_decoding_ready() -> bool:
    try:
        with socket.create_connection(("localhost", 5432), timeout=1):
            pass
    except OSError:
        return False
    try:
        conn = _connect()
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SHOW wal_level")
                row = cur.fetchone()
                return bool(row) and row[0] == "logical"
        finally:
            conn.close()
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _logical_decoding_ready(),
    reason="PostgreSQL with wal_level=logical not reachable on localhost:5432",
)


@pytest.fixture
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    # Loopback resolves to both families — without ::1 the guard refuses the
    # saved localhost connector on boxes where localhost -> ::1 (loop run).
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    import services.schedule_store as schedule_store

    monkeypatch.setattr(schedule_store, "_mongo_backend", lambda: None)
    return tmp_path


def _drop_slot_artifacts() -> None:
    """Leave no replication slot / publication / tables behind."""
    conn = _connect()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE %s",
                (f"df_dataflow_{_TABLE}%",),
            )
            for (slot,) in cur.fetchall():
                try:
                    cur.execute("SELECT pg_drop_replication_slot(%s)", (slot,))
                except Exception:
                    pass
            cur.execute("SELECT pubname FROM pg_publication WHERE pubname LIKE %s", (f"%{_TABLE}%",))
            for (pub,) in cur.fetchall():
                cur.execute(f"DROP PUBLICATION IF EXISTS {pub}")
    finally:
        conn.close()


def _wait_job(mongo, job_id: str, timeout: float = 240.0) -> dict:
    deadline = time.time() + timeout
    job: dict = {}
    while time.time() < deadline:
        job = mongo.get_job(job_id) or {}
        if job.get("status") in {"completed", "failed", "cancelled"}:
            return job
        time.sleep(1)
    return job


def test_api_created_cdc_schedule_moves_snapshot_and_updates(isolated):
    """The bare-API CDC schedule: no stream_contracts, no primary_key."""
    _exec(f"DROP TABLE IF EXISTS public.{_TABLE} CASCADE")
    _exec(
        f"CREATE TABLE public.{_TABLE} ("
        "id serial PRIMARY KEY, sku text, qty int, "
        "updated_at timestamptz default now())"
    )
    _exec(f"INSERT INTO public.{_TABLE} (sku, qty) VALUES ('a',1),('b',2)")

    from services.connector_store import create_connector
    from services.schedule_runner import _dispatch_transfer
    from services.schedule_store import create_schedule
    from src.services.mongodb_service import get_mongodb_service

    src = create_connector(
        {
            "name": "x29-pg-src",
            "type": "postgresql",
            "role": "source",
            "host": "localhost",
            "port": 5432,
            "database": "dataflow",
            "username": "dataflow",
            "password": "dataflow",
            "ssl": False,
        }
    )
    dst = create_connector(
        {
            "name": "x29-pg-dst",
            "type": "postgresql",
            "role": "destination",
            "host": "localhost",
            "port": 5432,
            "database": "dataflow",
            "username": "dataflow",
            "password": "dataflow",
            "ssl": False,
        }
    )
    sched = create_schedule(
        {
            "name": "x29-cdc",
            "source_connector_id": src.id,
            "source_table": _TABLE,
            "dest_connector_id": dst.id,
            "dest_table": f"{_TABLE}_dst",
            "interval": "daily",
            "sync_mode": "cdc",
            "snapshot_mode": "initial",
            "schema_policy": "manual_review",
            "validation_mode": "strict",
            "mappings": [{"source": c, "target": c, "confidence": 1.0} for c in _COLS],
        }
    )
    assert sched.enabled is True, sched.enabled_note

    mongo = get_mongodb_service()
    try:
        # Run 1: snapshot lands both rows.
        job1 = _wait_job(mongo, _dispatch_transfer(sched.id, allow_paused=True))
        assert job1.get("status") == "completed", (
            f"X-29 refusal still minted: {job1.get('error')}"
        )
        assert _rows("dataflow", f"{_TABLE}_dst") == [(1, "a", 1), (2, "b", 2)]

        # Run 2: the resumed stream captures UPDATE + INSERT from the WAL.
        _exec(f"UPDATE public.{_TABLE} SET qty=99 WHERE id=1")
        _exec(f"INSERT INTO public.{_TABLE} (sku, qty) VALUES ('c',3)")
        job2 = _wait_job(mongo, _dispatch_transfer(sched.id, allow_paused=True))
        assert job2.get("status") == "completed", job2.get("error")
        assert _rows("dataflow", f"{_TABLE}_dst") == [
            (1, "a", 99),
            (2, "b", 2),
            (3, "c", 3),
        ]
    finally:
        _drop_slot_artifacts()
        _exec(f"DROP TABLE IF EXISTS public.{_TABLE} CASCADE")
        _exec(f"DROP TABLE IF EXISTS public.{_TABLE}_dst CASCADE")


def test_api_created_cdc_schedule_on_keyless_table_still_refuses_named(isolated):
    """Fail-closed is kept: a table with no PK still refuses, naming the element."""
    table = f"x29_nopk_{uuid.uuid4().hex[:8]}"
    _exec(f"DROP TABLE IF EXISTS public.{table} CASCADE")
    _exec(f"CREATE TABLE public.{table} (sku text, qty int)")
    _exec(f"INSERT INTO public.{table} (sku, qty) VALUES ('a',1)")

    from services.connector_store import create_connector
    from services.schedule_runner import _dispatch_transfer
    from services.schedule_store import create_schedule
    from src.services.mongodb_service import get_mongodb_service

    src = create_connector(
        {
            "name": "x29-pg-src-nopk",
            "type": "postgresql",
            "role": "source",
            "host": "localhost",
            "port": 5432,
            "database": "dataflow",
            "username": "dataflow",
            "password": "dataflow",
            "ssl": False,
        }
    )
    sched = create_schedule(
        {
            "name": "x29-cdc-nopk",
            "source_connector_id": src.id,
            "source_table": table,
            "dest_connector_id": src.id,
            "dest_table": f"{table}_dst",
            "interval": "daily",
            "sync_mode": "cdc",
            "mappings": [{"source": "sku", "target": "sku", "confidence": 1.0}],
        }
    )
    mongo = get_mongodb_service()
    try:
        job = _wait_job(mongo, _dispatch_transfer(sched.id, allow_paused=True))
        assert job.get("status") == "failed"
        error = str(job.get("error") or "")
        assert "primary key" in error.lower(), error
        assert table in error, error
    finally:
        _exec(f"DROP TABLE IF EXISTS public.{table} CASCADE")
        _exec(f"DROP TABLE IF EXISTS public.{table}_dst CASCADE")
