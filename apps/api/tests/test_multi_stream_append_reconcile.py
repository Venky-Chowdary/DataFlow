"""A multi-stream APPEND into tables that already hold rows reconciles per table.

Single-stream append counts the delta: the destination's pre-run rows are not
this run's, so ``target >= expected`` with the pre-run count on the summary is
a pass. The multi-stream job-level Gate-8 is bound to the last stream (its
summary, its pre-run count, its endpoints) but was handed the *job's* rows
written — every stream summed. Its read-back of the last table was then below
that total, which routed it to the data-loss branch: a plain count compare with
no notion of pre-run rows, so the last table's earlier rows failed a correct
append ("expected target 21 vs target 23 ... already held 2 row(s)"). The
check now uses the last stream's own write count, so the last stream is
reconciled exactly as a single-stream append of that table.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

import pytest

from src.transfer.models import EndpointConfig, TransferRequest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

# name -> (source rows, rows the destination already held). Different pre-run
# counts per table, so one table's count cannot pass for another's.
TABLES = {"people": (6, 4), "orders": (3, 0), "items": (5, 2)}


def _exec(path: Path, sql: str, rows=None) -> None:
    conn = sqlite3.connect(str(path))
    try:
        with conn:
            conn.executemany(sql, rows) if rows is not None else conn.execute(sql)
    finally:
        conn.close()


def _seed_source(path: Path) -> None:
    for name, (n, _old) in TABLES.items():
        _exec(path, f"CREATE TABLE {name} (id INTEGER PRIMARY KEY, label TEXT)")
        _exec(path, f"INSERT INTO {name} VALUES (?, ?)", [(i, f"new-{i}") for i in range(1, n + 1)])  # noqa: S608


def _old_rows(name: str) -> list[tuple[int, str]]:
    n, old = TABLES[name]
    # Keys above the source's, so append adds without colliding.
    return [(1000 + i, f"old-{i}") for i in range(1, old + 1)]


def _mappings() -> list[dict]:
    return [
        {"source": "id", "target": "id", "source_type": "INTEGER", "target_type": "INTEGER"},
        {"source": "label", "target": "label", "source_type": "TEXT", "target_type": "TEXT"},
    ]


def _request(src: Path, destination: EndpointConfig, sync_mode: str) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="people"),
        destination=destination,
        sync_mode=sync_mode,
        stream_contracts=[
            {"name": n, "primary_key": ["id"], "selected": True, "mappings": _mappings()}
            for n in TABLES
        ],
        validation_mode="strict",
    )


@pytest.fixture()
def job_store(monkeypatch, tmp_path):
    import src.transfer.engine as engine_mod
    from services import audit_log
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    class _Store(_FakeMongo):
        def update_job_fields(self, job_id, fields, **_kw):
            self.jobs.setdefault(job_id, {}).update(fields)
            return True

    store = _Store()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    return store


def _run(store, request):
    from src.transfer.engine import UniversalTransferEngine

    job_id = "ms1" + uuid.uuid4().hex[:12]
    store.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _assert_append_passed(result, counts: dict[str, int]) -> None:
    assert result.success, result.error
    assert counts == {n: new + old for n, (new, old) in TABLES.items()}, counts
    assert result.records_transferred == sum(new for new, _old in TABLES.values())
    recon = result.reconciliation or {}
    assert recon.get("passed") is True, recon.get("message")
    streams = (result.destination_summary or {}).get("streams") or []
    assert [s.get("name") for s in streams] == list(TABLES)


def test_sqlite_multi_stream_append_into_occupied_tables(tmp_path, job_store):
    src, dst = tmp_path / "s.sqlite", tmp_path / "d.sqlite"
    _seed_source(src)
    for name in TABLES:
        _exec(dst, f"CREATE TABLE {name} (id INTEGER PRIMARY KEY, label TEXT)")
        if _old_rows(name):
            _exec(dst, f"INSERT INTO {name} VALUES (?, ?)", _old_rows(name))  # noqa: S608
    destination = EndpointConfig(kind="database", format="sqlite", database=str(dst), table="people")
    result = _run(job_store, _request(src, destination, "full_refresh_append"))
    conn = sqlite3.connect(str(dst))
    try:
        counts = {n: conn.execute(f"SELECT count(*) FROM {n}").fetchone()[0] for n in TABLES}  # noqa: S608
    finally:
        conn.close()
    _assert_append_passed(result, counts)


def test_sqlite_multi_stream_overwrite_still_reconciles(tmp_path, job_store):
    """The overwrite control: pre-run rows are replaced, every table equals its source."""
    src, dst = tmp_path / "s.sqlite", tmp_path / "d.sqlite"
    _seed_source(src)
    for name in TABLES:
        _exec(dst, f"CREATE TABLE {name} (id INTEGER PRIMARY KEY, label TEXT)")
        if _old_rows(name):
            _exec(dst, f"INSERT INTO {name} VALUES (?, ?)", _old_rows(name))  # noqa: S608
    destination = EndpointConfig(kind="database", format="sqlite", database=str(dst), table="people")
    result = _run(job_store, _request(src, destination, "full_refresh_overwrite"))
    assert result.success, result.error
    conn = sqlite3.connect(str(dst))
    try:
        counts = {n: conn.execute(f"SELECT count(*) FROM {n}").fetchone()[0] for n in TABLES}  # noqa: S608
    finally:
        conn.close()
    assert counts == {n: new for n, (new, _old) in TABLES.items()}


def test_live_postgres_multi_stream_append_into_occupied_tables(tmp_path, job_store):
    psycopg2 = pytest.importorskip("psycopg2")
    from tests.helpers.live_env import pg_creds, pg_up

    if not pg_up():
        pytest.skip("live PostgreSQL not reachable")
    creds = pg_creds()
    schema = f"ms1_{uuid.uuid4().hex[:10]}"
    conn = psycopg2.connect(
        host=creds["host"], port=creds["port"], dbname=creds["database"],
        user=creds["username"], password=creds["password"],
    )
    conn.autocommit = True
    src = tmp_path / "s.sqlite"
    _seed_source(src)
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{schema}"')
            for name in TABLES:
                cur.execute(f'CREATE TABLE "{schema}"."{name}" (id BIGINT PRIMARY KEY, label TEXT)')
                for row in _old_rows(name):
                    cur.execute(f'INSERT INTO "{schema}"."{name}" VALUES (%s, %s)', row)  # noqa: S608
        destination = EndpointConfig(
            kind="database", format="postgresql", host=creds["host"], port=creds["port"],
            database=creds["database"], username=creds["username"], password=creds["password"],
            schema=schema, table="people",
        )
        result = _run(job_store, _request(src, destination, "full_refresh_append"))
        with conn.cursor() as cur:
            counts = {}
            for name in TABLES:
                cur.execute(f'SELECT count(*) FROM "{schema}"."{name}"')  # noqa: S608
                counts[name] = cur.fetchone()[0]
        _assert_append_passed(result, counts)
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()
