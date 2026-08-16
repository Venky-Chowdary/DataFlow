"""PROPERTY 4 — writes are exactly-once observable.

Insert-mode transfers with a job_id must arm ``_dataflow_write_ledger`` so a
kill mid-chunk + resume yields zero duplicates, zero gaps, and the same
checksum as an uninterrupted run. Delivery remains at-least-once; the
*observable* result is exactly-once via ledger skip + optional upsert keys.

Families WITHOUT a same-txn ledger are NOT_GUARANTEED (Mongo, Kafka, object
stores, warehouses) — see ``services.replay_safety``.
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
import uuid
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import connectors.sqlite_writer as sqlite_writer_mod
from connectors.writer_common import CHUNK_SIZE as WRITER_CHUNK_SIZE
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest
from tests.helpers.live_env import pg_creds, pg_up


def _ordered_checksum(ids_names: list[tuple]) -> str:
    payload = "\n".join(f"{i}|{n}" for i, n in ids_names).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def test_sqlite_insert_ledger_mid_chunk_kill_resume(tmp_path: Path, monkeypatch):
    """Kill after chunk 0 commits; resume must match clean-run checksum."""
    monkeypatch.setattr(sqlite_writer_mod, "CHUNK_SIZE", 2)

    src = tmp_path / "p4_src.sqlite"
    dst_clean = tmp_path / "p4_clean.sqlite"
    dst_kill = tmp_path / "p4_kill.sqlite"

    eng = create_engine(f"sqlite:///{src}")
    try:
        with eng.begin() as c:
            c.execute(text("CREATE TABLE t (id INTEGER PRIMARY KEY, nm TEXT)"))
            for i in range(1, 7):
                c.execute(
                    text("INSERT INTO t VALUES (:i, :n)"),
                    {"i": i, "n": f"r{i}"},
                )
    finally:
        eng.dispose()

    def _req(dst: Path) -> TransferRequest:
        # full_refresh_append → insert mode so the ledger (not upsert) is armed.
        return TransferRequest(
            source=EndpointConfig(
                kind="database", format="sqlite", database=str(src), table="t"
            ),
            destination=EndpointConfig(
                kind="database", format="sqlite", database=str(dst), table="t"
            ),
            sync_mode="full_refresh_append",
            validation_mode="warn",
            skip_preflight=True,
            mappings=[
                {"source": "id", "target": "id", "confidence": 0.99},
                {"source": "nm", "target": "nm", "confidence": 0.99},
            ],
        )

    engine = UniversalTransferEngine()
    clean = engine.execute_tracked(_req(dst_clean), uuid.uuid4().hex[:24])
    assert clean.success, clean.error
    assert clean.records_transferred == 6

    conn = sqlite3.connect(str(dst_clean))
    try:
        clean_rows = conn.execute("SELECT id, nm FROM t ORDER BY id").fetchall()
        ledger_n = conn.execute(
            "SELECT count(*) FROM _dataflow_write_ledger"
        ).fetchone()[0]
    finally:
        conn.close()
    assert clean_rows == [(i, f"r{i}") for i in range(1, 7)]
    assert ledger_n >= 1, "engine must arm SQLite write ledger with job_id"
    clean_digest = _ordered_checksum(clean_rows)

    # Simulate kill-9 after the first writer chunk commits.
    orig_mark = sqlite_writer_mod.mark_raw_chunk_committed
    state = {"marked": 0}

    def _mark_then_kill(*args, **kwargs):
        orig_mark(*args, **kwargs)
        state["marked"] += 1
        if state["marked"] == 1:
            raise RuntimeError("simulated kill-9 after chunk 0 commit")

    monkeypatch.setattr(sqlite_writer_mod, "mark_raw_chunk_committed", _mark_then_kill)
    job_id = uuid.uuid4().hex[:24]
    first = engine.execute_tracked(_req(dst_kill), job_id)
    assert not first.success, "kill simulation must abort the first attempt"
    monkeypatch.setattr(sqlite_writer_mod, "mark_raw_chunk_committed", orig_mark)

    # Resume / retry same job_id + same payload — ledger skips chunk 0.
    second = engine.execute_tracked(_req(dst_kill), job_id)
    assert second.success, second.error

    conn = sqlite3.connect(str(dst_kill))
    try:
        kill_rows = conn.execute("SELECT id, nm FROM t ORDER BY id").fetchall()
        distinct = conn.execute("SELECT count(DISTINCT id) FROM t").fetchone()[0]
        ledger_rows = conn.execute(
            "SELECT chunk_idx, rows_written, row_start, row_end, attempt "
            "FROM _dataflow_write_ledger ORDER BY chunk_idx"
        ).fetchall()
    finally:
        conn.close()

    assert kill_rows == clean_rows
    assert distinct == 6
    assert _ordered_checksum(kill_rows) == clean_digest
    assert ledger_rows, "ledger must record committed chunks"
    # Property 4 shape: row ranges present on new ledgers.
    assert all(r[2] is not None for r in ledger_rows), ledger_rows


@pytest.mark.skipif(not pg_up("P4"), reason="PostgreSQL not reachable")
def test_pg_insert_ledger_mid_chunk_kill_resume(monkeypatch):
    """Live PG: same kill/resume/checksum proof with the write ledger."""
    import psycopg2

    import connectors.postgresql_writer as pg_writer_mod

    monkeypatch.setattr(pg_writer_mod, "write_chunk_size", lambda *_a, **_k: 2)

    creds = pg_creds("P4")
    src_table = f"p4_src_{uuid.uuid4().hex[:8]}"
    dst_clean = f"p4_cln_{uuid.uuid4().hex[:8]}"
    dst_kill = f"p4_kil_{uuid.uuid4().hex[:8]}"

    conn = psycopg2.connect(
        host=creds["host"],
        port=creds["port"],
        dbname=creds["database"],
        user=creds["username"],
        password=creds["password"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'CREATE TABLE public."{src_table}" (id bigint PRIMARY KEY, nm text)'
            )
            for i in range(1, 7):
                cur.execute(
                    f'INSERT INTO public."{src_table}" VALUES (%s, %s)',
                    (i, f"r{i}"),
                )
        conn.commit()
    finally:
        conn.close()

    def _ep(table: str) -> EndpointConfig:
        return EndpointConfig(
            kind="database",
            format="postgresql",
            host=creds["host"],
            port=creds["port"],
            database=creds["database"],
            username=creds["username"],
            password=creds["password"],
            schema="public",
            table=table,
        )

    def _req(dst: str) -> TransferRequest:
        return TransferRequest(
            source=_ep(src_table),
            destination=_ep(dst),
            sync_mode="full_refresh_append",
            validation_mode="warn",
            skip_preflight=True,
            mappings=[
                {"source": "id", "target": "id", "confidence": 0.99},
                {"source": "nm", "target": "nm", "confidence": 0.99},
            ],
        )

    engine = UniversalTransferEngine()
    try:
        clean = engine.execute_tracked(_req(dst_clean), uuid.uuid4().hex[:24])
        assert clean.success, clean.error
        assert clean.records_transferred == 6

        conn = psycopg2.connect(
            host=creds["host"],
            port=creds["port"],
            dbname=creds["database"],
            user=creds["username"],
            password=creds["password"],
        )
        try:
            with conn.cursor() as cur:
                cur.execute(f'SELECT id, nm FROM public."{dst_clean}" ORDER BY id')
                clean_rows = cur.fetchall()
                cur.execute(
                    'SELECT count(*) FROM public."_dataflow_write_ledger" '
                    "WHERE batch_key LIKE %s",
                    (f"%{dst_clean}%",),
                )
                ledger_n = cur.fetchone()[0]
        finally:
            conn.close()
        assert [tuple(r) for r in clean_rows] == [(i, f"r{i}") for i in range(1, 7)]
        assert ledger_n >= 1
        clean_digest = _ordered_checksum([tuple(r) for r in clean_rows])

        orig_mark = pg_writer_mod.mark_raw_chunk_committed
        state = {"marked": 0}

        def _mark_then_kill(*args, **kwargs):
            orig_mark(*args, **kwargs)
            state["marked"] += 1
            if state["marked"] == 1:
                raise RuntimeError("simulated kill-9 after chunk 0 commit")

        monkeypatch.setattr(pg_writer_mod, "mark_raw_chunk_committed", _mark_then_kill)
        job_id = uuid.uuid4().hex[:24]
        first = engine.execute_tracked(_req(dst_kill), job_id)
        assert not first.success
        monkeypatch.setattr(pg_writer_mod, "mark_raw_chunk_committed", orig_mark)

        second = engine.execute_tracked(_req(dst_kill), job_id)
        assert second.success, second.error

        conn = psycopg2.connect(
            host=creds["host"],
            port=creds["port"],
            dbname=creds["database"],
            user=creds["username"],
            password=creds["password"],
        )
        try:
            with conn.cursor() as cur:
                cur.execute(f'SELECT id, nm FROM public."{dst_kill}" ORDER BY id')
                kill_rows = [tuple(r) for r in cur.fetchall()]
                cur.execute(f'SELECT count(DISTINCT id) FROM public."{dst_kill}"')
                distinct = cur.fetchone()[0]
        finally:
            conn.close()

        assert kill_rows == [tuple(r) for r in clean_rows]
        assert distinct == 6
        assert _ordered_checksum(kill_rows) == clean_digest
    finally:
        conn = psycopg2.connect(
            host=creds["host"],
            port=creds["port"],
            dbname=creds["database"],
            user=creds["username"],
            password=creds["password"],
        )
        try:
            with conn.cursor() as cur:
                for t in (src_table, dst_clean, dst_kill):
                    cur.execute(f'DROP TABLE IF EXISTS public."{t}"')
                # Leave shared ledger rows; they are keyed by job/batch.
            conn.commit()
        finally:
            conn.close()


def test_writer_chunk_size_import_smoke():
    """Guard: property proof depends on writer CHUNK_SIZE being patchable."""
    assert int(WRITER_CHUNK_SIZE) >= 1
