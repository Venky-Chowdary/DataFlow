"""C5 — a resumed multi-table run re-reads only the tables it had not finished.

Real engine path, PostgreSQL to PostgreSQL, three streams. The first pass is
cancelled once ``people`` and ``orders`` have committed (a cancel keeps the
committed tables, D102/D134). The resume (``execute_tracked(..., resume=True)``
on the same job) used to read every table again except the one the single job
checkpoint named: ``people`` was re-read and re-appended. Each finished table
is now recorded on the job (``stream_progress``), and the resume reads only
``items``.
"""

from __future__ import annotations

import os
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

TABLES = {"people": 6, "orders": 3, "items": 5}


@pytest.fixture()
def store(monkeypatch, tmp_path):
    import services.mongodb_service as mongo_mod
    import src.transfer.engine as engine_mod
    from services import audit_log
    from services.mongodb_service import MemoryMongoDBService

    mem = MemoryMongoDBService()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: mem)
    monkeypatch.setattr(mongo_mod, "get_mongodb_service", lambda: mem)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    return mem


@pytest.fixture()
def pg():
    psycopg2 = pytest.importorskip("psycopg2")
    from tests.helpers.live_env import pg_creds, pg_up

    if not pg_up():
        pytest.skip("live PostgreSQL not reachable")
    creds = pg_creds()
    conn = psycopg2.connect(
        host=creds["host"], port=creds["port"], dbname=creds["database"],
        user=creds["username"], password=creds["password"],
    )
    conn.autocommit = True
    tok = uuid.uuid4().hex[:8]
    src, dst = f"c5_s_{tok}", f"c5_d_{tok}"
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{src}"')
            cur.execute(f'CREATE SCHEMA "{dst}"')
            for name, n in TABLES.items():
                cur.execute(f'CREATE TABLE "{src}"."{name}" (id BIGINT PRIMARY KEY, label TEXT)')
                cur.execute(
                    f'INSERT INTO "{src}"."{name}" '  # noqa: S608 — fixed test names
                    f"SELECT g, 'v-' || g FROM generate_series(1, {n}) g"
                )
                cur.execute(f'CREATE TABLE "{dst}"."{name}" (id BIGINT PRIMARY KEY, label TEXT)')
        ep = {k: creds[k] for k in ("host", "port", "database", "username", "password")}
        yield conn, src, dst, ep
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{src}" CASCADE')
            cur.execute(f'DROP SCHEMA IF EXISTS "{dst}" CASCADE')
        conn.close()


def _counts(conn, schema: str) -> dict[str, int]:
    out = {}
    with conn.cursor() as cur:
        for name in TABLES:
            cur.execute(f'SELECT count(*), count(DISTINCT id) FROM "{schema}"."{name}"')  # noqa: S608
            total, distinct = cur.fetchone()
            assert total == distinct, f"{name}: {total - distinct} duplicate row(s)"
            out[name] = total
    return out


def test_resume_reads_only_the_tables_the_cancelled_pass_had_not_finished(monkeypatch, store, pg):
    import src.transfer.stream as stream_mod
    import src.transfer.stream_multi as stream_multi
    from src.transfer.engine import UniversalTransferEngine

    conn, src, dst, ep = pg
    job_id = "c5" + uuid.uuid4().hex[:12]
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", schema=src, table="people", **ep),
        destination=EndpointConfig(kind="database", format="postgresql", schema=dst, table="people", **ep),
        sync_mode="full_refresh_append",
        stream_contracts=[{"name": n, "selected": True, "primary_key": ["id"]} for n in TABLES],
        skip_preflight=True,
        validation_mode="strict",
    )

    real_publish = stream_multi._publish_stream_written

    def cancel_after_orders(jid, name, rows):
        real_publish(jid, name, rows)
        if name == "orders":
            store.request_job_cancel(job_id)
            store.update_job_status(job_id, "cancelled", operator_command=True)

    with monkeypatch.context() as m:
        m.setattr(stream_multi, "_publish_stream_written", cancel_after_orders)
        store.update_job_status(job_id, "pending", transfer_request={})
        first = UniversalTransferEngine().execute_tracked(request, job_id)
    assert not first.success and "cancel" in str(first.error).lower(), first.error
    assert _counts(conn, dst) == {"people": 6, "orders": 3, "items": 0}
    progress = (store.get_job(job_id) or {}).get("stream_progress") or {}
    assert sorted(progress) == ["orders", "people"], progress

    read: list[str] = []
    real_stream = stream_mod.stream_database_transfer

    def spy(source, *args, **kwargs):
        read.append(str(source.table))
        return real_stream(source, *args, **kwargs)

    monkeypatch.setattr(stream_mod, "stream_database_transfer", spy)
    store.update_job_status(job_id, "pending", message="resume", allow_terminal_exit=True)
    resumed = UniversalTransferEngine().execute_tracked(request, job_id, resume=True)

    assert read == ["items"], read
    assert resumed.success, resumed.error
    assert _counts(conn, dst) == TABLES
    streams = {s.get("name"): s for s in (store.get_job(job_id) or {}).get("streams") or []}
    for name in ("people", "orders"):
        assert "not read again on resume" in str(streams.get(name) or {}), streams.get(name)
