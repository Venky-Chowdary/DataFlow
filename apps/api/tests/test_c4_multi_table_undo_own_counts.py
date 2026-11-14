"""C4 — batch undo of a failed multi-table run uses each table's own pre-run state.

Real engine path (``UniversalTransferEngine.execute_tracked``), PostgreSQL to
PostgreSQL, three streams with deliberately different pre-run states so one
cannot pass for another:

* ``people`` — absent before the run (the run creates it);
* ``orders`` — occupied: 7 rows, one of which the source changes;
* ``items`` — exists and is empty; the run fails on it.

Before C4 the undo saw one object: ``people`` kept the six rows the run wrote
into a table it created, ``items`` kept this run's rows after a verification
failure, and the strict backup (armed once per run, for ``orders``) was reported
as "the destination table was restored to its pre-run content". Now every
table returns to exactly its pre-run state.
"""

from __future__ import annotations

import os
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

ORDERS_BEFORE = [(1000 + i, "old") for i in range(1, 8)]


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
    src, dst = f"c4_s_{tok}", f"c4_d_{tok}"
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{src}"')
            cur.execute(f'CREATE SCHEMA "{dst}"')
            for name, n in (("people", 6), ("orders", 3), ("items", 5)):
                cur.execute(f'CREATE TABLE "{src}"."{name}" (id BIGINT PRIMARY KEY, label TEXT)')
                cur.execute(
                    f'INSERT INTO "{src}"."{name}" '  # noqa: S608 — fixed test names
                    f"SELECT g, 'new-' || g FROM generate_series(1, {n}) g"
                )
            # The source changes one of the destination's pre-run rows.
            cur.execute(f'INSERT INTO "{src}"."orders" VALUES (1001, \'changed\')')
            cur.execute(f'CREATE TABLE "{dst}"."orders" (id BIGINT PRIMARY KEY, label TEXT)')
            cur.executemany(f'INSERT INTO "{dst}"."orders" VALUES (%s, %s)', ORDERS_BEFORE)
            cur.execute(f'CREATE TABLE "{dst}"."items" (id BIGINT PRIMARY KEY, label TEXT)')
        ep = {k: creds[k] for k in ("host", "port", "database", "username", "password")}
        yield conn, src, dst, ep
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{src}" CASCADE')
            cur.execute(f'DROP SCHEMA IF EXISTS "{dst}" CASCADE')
        conn.close()


def _state(conn, schema: str) -> dict:
    out: dict = {}
    with conn.cursor() as cur:
        for name in ("people", "orders", "items"):
            cur.execute("SELECT to_regclass(%s)", (f'"{schema}"."{name}"',))
            if cur.fetchone()[0] is None:
                out[name] = None
                continue
            cur.execute(f'SELECT id, label FROM "{schema}"."{name}" ORDER BY id')  # noqa: S608
            out[name] = [tuple(r) for r in cur.fetchall()]
    return out


PRE_RUN = {"people": None, "orders": ORDERS_BEFORE, "items": []}


def _run(store, pg, sync_mode: str):
    from src.transfer.engine import UniversalTransferEngine

    conn, src, dst, ep = pg
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", schema=src, table="people", **ep),
        destination=EndpointConfig(kind="database", format="postgresql", schema=dst, table="people", **ep),
        sync_mode=sync_mode,
        stream_contracts=[
            {"name": n, "selected": True, "primary_key": ["id"]} for n in ("people", "orders", "items")
        ],
        skip_preflight=True,
        validation_mode="strict",
    )
    job_id = "c4" + uuid.uuid4().hex[:12]
    store.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    return result, store.get_job(job_id) or {}


def _break_items_values(conn, dst: str) -> None:
    """The destination rewrites what lands in items: verification of the run fails."""
    with conn.cursor() as cur:
        cur.execute(
            f'CREATE FUNCTION "{dst}".up() RETURNS trigger AS $$ '
            "BEGIN NEW.label := upper(NEW.label); RETURN NEW; END $$ LANGUAGE plpgsql"
        )
        cur.execute(
            f'CREATE TRIGGER t BEFORE INSERT OR UPDATE ON "{dst}"."items" '
            f'FOR EACH ROW EXECUTE FUNCTION "{dst}".up()'
        )


def _break_items_write(conn, dst: str) -> None:
    """The destination rejects a row of items: the write itself fails."""
    with conn.cursor() as cur:
        cur.execute(f'ALTER TABLE "{dst}"."items" ADD CONSTRAINT lt4 CHECK (id < 4)')


def test_failed_verification_restores_every_table_to_its_own_pre_run_state(store, pg):
    conn, _src, dst, _ep = pg
    _break_items_values(conn, dst)
    result, job = _run(store, pg, "upsert")
    assert not result.success
    assert _state(conn, dst) == PRE_RUN
    message = str(job.get("message") or result.error)
    assert "people: dropped (this run created it)" in message, message
    assert "orders: restored from its pre-run backup" in message, message
    assert "items: emptied (it was empty before)" in message, message


def test_failed_write_restores_every_table_to_its_own_pre_run_state(store, pg):
    conn, _src, dst, _ep = pg
    _break_items_write(conn, dst)
    result, job = _run(store, pg, "upsert")
    assert not result.success
    assert _state(conn, dst) == PRE_RUN
    message = str(job.get("message") or "")
    assert "people: dropped (this run created it)" in message, message
    assert "orders: restored from its pre-run backup" in message, message


def test_failed_append_names_each_table_with_its_own_count(store, pg):
    """An occupied append table has no backup: it is kept, named with its own count."""
    conn, src, dst, _ep = pg
    with conn.cursor() as cur:  # no key overlap, so the append into orders lands
        cur.execute(f'DELETE FROM "{src}"."orders" WHERE id = 1001')
    _break_items_values(conn, dst)
    result, job = _run(store, pg, "full_refresh_append")
    assert not result.success
    state = _state(conn, dst)
    assert state["people"] is None and state["items"] == [], state
    message = str(job.get("message") or result.error)
    assert "people: dropped (this run created it)" in message, message
    assert "orders: kept as is (held 7 row(s) before" in message, message
    # Never another table's count: orders held 7, people and items held none.
    assert "'orders' before it failed (the table held 7 row(s) before" in message, message
