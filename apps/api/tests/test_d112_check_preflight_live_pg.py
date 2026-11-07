"""D112-CHECK — Execute's preflight must see the destination's CHECK constraints.

Studio Validate forwarded ``destination_check_constraints`` to
``run_file_preflight``; the engine's own preflight (and the Pilot's) did not, so
a source row the live CHECK rejects sailed through a green preflight and the
operator got a raw PostgreSQL ``CheckViolation`` after Confirm.

Real SQLite source -> live PostgreSQL destination through the engine with
preflight ON.
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

PG = dict(host="127.0.0.1", port=5432, database="dataflow", username="dataflow", password="dataflow")


def _reachable() -> bool:
    try:
        with socket.create_connection((PG["host"], PG["port"]), timeout=1.5):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(not _reachable(), reason="PostgreSQL not reachable")


def _pg(sql: str) -> list[tuple]:
    import psycopg2

    conn = psycopg2.connect(
        host=PG["host"], port=PG["port"], dbname=PG["database"],
        user=PG["username"], password=PG["password"],
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return list(cur.fetchall()) if cur.description else []
    finally:
        conn.close()


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def test_engine_preflight_blocks_rows_the_destination_check_rejects(tmp_path, isolated):
    table = f"d112chk_{uuid.uuid4().hex[:10]}"
    src = tmp_path / "src.sqlite"
    conn = sqlite3.connect(str(src))
    with conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, amount INTEGER)")
        conn.executemany("INSERT INTO t VALUES (?, ?)", [(1, 10), (2, -5), (3, 7)])
    conn.close()
    _pg(
        f'CREATE TABLE public."{table}" (id BIGINT PRIMARY KEY, '
        f'amount BIGINT CONSTRAINT "{table}_amount_check" CHECK (amount >= 0))'
    )
    try:
        request = TransferRequest(
            source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
            destination=EndpointConfig(
                kind="database", format="postgresql", table=table, schema="public", **PG
            ),
            sync_mode="append",
            validation_mode="strict",
        )
        job_id = "d112chk" + uuid.uuid4().hex[:12]
        isolated.update_job_status(job_id, "pending", transfer_request={})
        result = UniversalTransferEngine().execute_tracked(request, job_id)

        error = str(result.error or "")
        assert not result.success
        # Named at preflight, before any write — not the driver's raw error.
        assert "CHECK" in error and f"{table}_amount_check" in error, error
        assert "violates check constraint" not in error.lower(), error
        assert _pg(f'SELECT COUNT(*) FROM public."{table}"') == [(0,)]
    finally:
        _pg(f'DROP TABLE IF EXISTS public."{table}"')
