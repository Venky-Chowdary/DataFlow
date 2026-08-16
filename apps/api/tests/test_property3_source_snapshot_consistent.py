"""PROPERTY 3 — source reads are snapshot-consistent.

PostgreSQL full-refresh transfer pages must share one REPEATABLE READ
snapshot so concurrent source inserts after the snapshot starts are NOT
visible mid-pagination. SQLite binds a deferred transaction snapshot.
"""

from __future__ import annotations

import os
import threading
import time
import uuid
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from src.transfer import stream as stream_mod
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest
from tests.helpers.live_env import pg_creds, pg_up


@pytest.mark.skipif(not pg_up("P3"), reason="PostgreSQL not reachable")
def test_pg_full_refresh_hides_concurrent_inserts_under_rr(monkeypatch):
    """Live proof: inserts committed after RR starts must not appear in dest."""
    import psycopg2

    # Small pages so the transfer spans multiple round-trips.
    monkeypatch.setattr(stream_mod, "CHUNK_SIZE", 2)

    creds = pg_creds("P3")
    src_table = f"p3_src_{uuid.uuid4().hex[:8]}"
    dst_table = f"p3_dst_{uuid.uuid4().hex[:8]}"
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
                f'CREATE TABLE public."{src_table}" '
                "(id bigint PRIMARY KEY, nm text)"
            )
            for i in range(1, 11):
                cur.execute(
                    f'INSERT INTO public."{src_table}" VALUES (%s, %s)',
                    (i, f"r{i}"),
                )
        conn.commit()
    finally:
        conn.close()

    barrier = threading.Event()
    injected = threading.Event()

    # Hook: after the first real page read under the snapshot, inject rows.
    orig_read = stream_mod._read_batch_impl
    calls = {"n": 0}

    def _counting_read(*args, **kwargs):
        batch = orig_read(*args, **kwargs)
        calls["n"] += 1
        # First OFFSET page after snapshot bind (skip empty probes carefully).
        rows = getattr(batch, "rows", None) or (
            batch[0].rows if isinstance(batch, tuple) else []
        )
        if calls["n"] >= 2 and rows and not barrier.is_set():
            barrier.set()
            # Wait briefly for injector, then continue pagination.
            injected.wait(timeout=5.0)
        return batch

    monkeypatch.setattr(stream_mod, "_read_batch_impl", _counting_read)

    def _inject():
        barrier.wait(timeout=10.0)
        c = psycopg2.connect(
            host=creds["host"],
            port=creds["port"],
            dbname=creds["database"],
            user=creds["username"],
            password=creds["password"],
        )
        try:
            with c.cursor() as cur:
                for i in range(100, 110):
                    cur.execute(
                        f'INSERT INTO public."{src_table}" VALUES (%s, %s)',
                        (i, f"late{i}"),
                    )
            c.commit()
        finally:
            c.close()
        injected.set()

    thr = threading.Thread(target=_inject, daemon=True)
    thr.start()

    try:
        req = TransferRequest(
            source=EndpointConfig(
                kind="database",
                format="postgresql",
                host=creds["host"],
                port=creds["port"],
                database=creds["database"],
                username=creds["username"],
                password=creds["password"],
                schema="public",
                table=src_table,
            ),
            destination=EndpointConfig(
                kind="database",
                format="postgresql",
                host=creds["host"],
                port=creds["port"],
                database=creds["database"],
                username=creds["username"],
                password=creds["password"],
                schema="public",
                table=dst_table,
            ),
            sync_mode="full_refresh_overwrite",
            validation_mode="warn",
            skip_preflight=True,
            mappings=None,
        )
        result = UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])
        thr.join(timeout=15.0)
        assert result.success, result.error
        # Snapshot population was 10 rows — concurrent inserts must be invisible.
        assert result.records_transferred == 10, (
            result.records_transferred,
            result.destination_summary,
        )
        snap = (result.destination_summary or {}).get("source_snapshot") or (
            result.reconciliation or {}
        ).get("source_snapshot")
        assert snap, result.destination_summary
        assert snap.get("isolation") == "repeatable_read"
        assert snap.get("guarantee") == "mvcc_repeatable_read"
        assert snap.get("snapshot_lsn"), snap

        conn = psycopg2.connect(
            host=creds["host"],
            port=creds["port"],
            dbname=creds["database"],
            user=creds["username"],
            password=creds["password"],
        )
        try:
            with conn.cursor() as cur:
                cur.execute(f'SELECT id FROM public."{dst_table}" ORDER BY id')
                ids = [r[0] for r in cur.fetchall()]
        finally:
            conn.close()
        assert ids == list(range(1, 11)), ids
        assert all(i < 100 for i in ids)
    finally:
        injected.set()
        conn = psycopg2.connect(
            host=creds["host"],
            port=creds["port"],
            dbname=creds["database"],
            user=creds["username"],
            password=creds["password"],
        )
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS public."{src_table}"')
                cur.execute(f'DROP TABLE IF EXISTS public."{dst_table}"')
            conn.commit()
        finally:
            conn.close()


def test_sqlite_full_refresh_binds_transaction_snapshot(tmp_path: Path, monkeypatch):
    """Always-on: SQLite full-refresh stamps a deferred transaction snapshot."""
    monkeypatch.setattr(stream_mod, "CHUNK_SIZE", 2)
    src = tmp_path / "p3_src.sqlite"
    dst = tmp_path / "p3_dst.sqlite"
    eng = create_engine(f"sqlite:///{src}")
    try:
        with eng.begin() as c:
            c.execute(text("CREATE TABLE t (id INTEGER PRIMARY KEY, nm TEXT)"))
            for i in range(1, 9):
                c.execute(text("INSERT INTO t VALUES (:i, :n)"), {"i": i, "n": f"r{i}"})
    finally:
        eng.dispose()

    req = TransferRequest(
        source=EndpointConfig(
            kind="database", format="sqlite", database=str(src), table="t"
        ),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(dst), table="t"
        ),
        sync_mode="full_refresh_overwrite",
        validation_mode="warn",
        skip_preflight=True,
        mappings=None,
    )
    result = UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])
    assert result.success, result.error
    assert result.records_transferred == 8
    snap = (result.destination_summary or {}).get("source_snapshot") or {}
    assert snap.get("engine") == "sqlite"
    assert snap.get("guarantee") == "sqlite_transaction_snapshot"
    assert result.reconciliation.get("passed") is True
    # Certificate surface
    assert (result.reconciliation or {}).get("source_snapshot", {}).get(
        "guarantee"
    ) == "sqlite_transaction_snapshot"


def test_source_snapshot_helpers_roundtrip_meta():
    from services.source_snapshot import (
        activate_snapshot,
        get_source_snapshot_meta,
        release_active_snapshot,
    )

    class _Dummy:
        pass

    ended = {"n": 0}

    def _end(conn, *, commit=True):
        ended["n"] += 1
        ended["commit"] = commit

    dummy = _Dummy()
    activate_snapshot(
        dummy,
        {
            "engine": "postgresql",
            "isolation": "repeatable_read",
            "guarantee": "mvcc_repeatable_read",
            "snapshot_lsn": "0/1",
        },
        _end,
    )
    assert get_source_snapshot_meta()["snapshot_lsn"] == "0/1"
    meta = release_active_snapshot(commit=True)
    assert meta["snapshot_lsn"] == "0/1"
    assert ended["n"] == 1
    assert ended["commit"] is True
    assert get_source_snapshot_meta() is None
