"""Live PG append retry — dest COUNT(*) must not double after a post-write fail.

A job shell seeds records_processed=0. If dest write evidence is ignored,
_finalize_run schedules a from-zero retry and the next beat appends again.
committed_rows_of must see destination_summary.rows_written, park the
schedule, and leave dest COUNT(*)=3.

Does not claim overwrite retry / DST / overlap / 100K.
skip_preflight stays False.
"""

from __future__ import annotations

import socket
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

PG = dict(
    host="127.0.0.1",
    port=5432,
    database="dataflow",
    user="dataflow",
    password="dataflow",
)
_TERMINAL = {"completed", "success", "succeeded", "failed", "error", "cancelled"}


def _pg_or_skip():
    try:
        with socket.create_connection((PG["host"], PG["port"]), timeout=1):
            pass
    except OSError:
        pytest.skip("PostgreSQL 5432 not reachable")


def _isolate_stores(tmp_path, monkeypatch):
    import services.connector_store as connectors
    import services.schedule_store as schedules

    monkeypatch.setattr(schedules, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(schedules, "_mongo_backend", lambda: None)
    monkeypatch.setattr(connectors, "STORE_PATH", tmp_path / "connectors.json")
    monkeypatch.setattr(connectors, "_use_mongo", lambda: False)
    monkeypatch.setattr(connectors, "_resolve_backend", lambda: "file")
    monkeypatch.setattr(connectors, "_backend_choice", "file")


def _pg_connect():
    psycopg2 = pytest.importorskip("psycopg2")
    conn = psycopg2.connect(
        host=PG["host"],
        port=PG["port"],
        user=PG["user"],
        password=PG["password"],
        dbname=PG["database"],
    )
    conn.autocommit = True
    return conn


def _await_job(job_id: str, timeout: float = 180.0) -> dict:
    from services import schedule_runner

    deadline = time.time() + timeout
    doc: dict = {}
    while time.time() < deadline:
        doc = dict(schedule_runner._job_doc(job_id) or {})
        if str(doc.get("status") or "").lower() in _TERMINAL:
            return doc
        time.sleep(0.4)
    return doc


def test_studio_pg_append_retry_refuses_and_dest_count_stays_three(tmp_path, monkeypatch):
    _pg_or_skip()
    from services.connector_store import create_connector
    from services.execution_engine_contract import committed_rows_of
    from services.million_row_proof import ensure_memory_job_store_if_mongo_down
    from services.mongodb_service import get_mongodb_service
    from services.schedule_mapping_contract import persisted_mapping_rows
    from services.schedule_store import (
        create_schedule,
        due_schedules,
        get_schedule,
        has_open_approval,
    )
    from services import schedule_runner

    ensure_memory_job_store_if_mongo_down()
    _isolate_stores(tmp_path, monkeypatch)

    # Earlier tests may stub get_transfer_engine with a job-shell dummy.
    import src.transfer.background as bg
    import src.transfer.engine as engine_mod
    from src.transfer.engine import UniversalTransferEngine

    def _live_engine():
        current = getattr(engine_mod, "_engine", None)
        if current is None or not hasattr(current, "execute_tracked"):
            engine_mod._engine = UniversalTransferEngine()
        return engine_mod._engine

    monkeypatch.setattr(engine_mod, "get_transfer_engine", _live_engine)
    monkeypatch.setattr(bg, "get_transfer_engine", _live_engine)

    tag = uuid.uuid4().hex[:8]
    src_table = f"studio_retry_src_{tag}"
    dest_table = f"studio_retry_dst_{tag}"
    pg = _pg_connect()
    try:
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{src_table}"')
            cur.execute(f'DROP TABLE IF EXISTS public."{dest_table}"')
            cur.execute(
                f'CREATE TABLE public."{src_table}" '
                f"(id bigint PRIMARY KEY, label varchar(32) NOT NULL)"
            )
            cur.execute(
                f'INSERT INTO public."{src_table}" (id, label) VALUES '
                f"(1, 'a'), (2, 'b'), (3, 'c')"
            )

        src = create_connector({
            "name": f"studio-retry-src-{tag}",
            "type": "postgresql",
            "role": "both",
            "host": PG["host"],
            "port": PG["port"],
            "database": PG["database"],
            "username": PG["user"],
            "password": PG["password"],
            "schema": "public",
            "ssl": False,
        })
        dest = create_connector({
            "name": f"studio-retry-dst-{tag}",
            "type": "postgresql",
            "role": "both",
            "host": PG["host"],
            "port": PG["port"],
            "database": PG["database"],
            "username": PG["user"],
            "password": PG["password"],
            "schema": "public",
            "ssl": False,
        })
        mappings = [
            {"source": "id", "target": "id", "confidence": 1.0},
            {"source": "label", "target": "label", "confidence": 1.0},
        ]
        assert persisted_mapping_rows(mappings)
        sched = create_schedule({
            "name": f"Studio PG retry {tag}",
            "source_connector_id": src.id,
            "source_table": src_table,
            "dest_connector_id": dest.id,
            "dest_table": dest_table,
            "interval": "daily",
            "timezone": "UTC",
            "sync_mode": "full_refresh_append",
            "validation_mode": "balanced",
            "primary_key": "id",
            "enabled": True,
            "max_retries": 3,
            "retry_backoff_seconds": 0,
            "mappings": mappings,
        })

        job_id = schedule_runner._run_schedule(sched.id, manual=True)
        assert job_id, "first append did not start"
        job = _await_job(job_id)
        status = str(job.get("status") or "").lower()
        assert status in {"completed", "success", "succeeded"}, (
            f"first append {status}: {job.get('error') or job.get('message') or job}"
        )
        time.sleep(0.6)

        with pg.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM public."{dest_table}"')
            assert int(cur.fetchone()[0]) == 3

        mongo = get_mongodb_service()
        assert mongo.update_job_fields(
            job_id,
            {
                "status": "failed",
                "records_processed": 0,
                "destination_summary": {"rows_written": 3},
                "error": "connection reset after destination write",
            },
        )
        rows, known = committed_rows_of(mongo.get_job(job_id))
        assert known is True
        assert rows == 3

        schedule_runner._finalize_run(
            sched.id, job_id, attempt=0, started_at=datetime.now(timezone.utc)
        )
        finished = get_schedule(sched.id)
        assert finished is not None
        assert finished.retry_at is None, finished.retry_at
        assert has_open_approval(finished)
        ev = (finished.approval_request or {}).get("evidence") or {}
        assert ev.get("park_reason") == "committed_rows_cannot_be_replayed"
        assert sched.id not in {s.id for s in due_schedules()}

        with pytest.raises(schedule_runner.ScheduleStartError) as exc:
            schedule_runner._run_schedule(sched.id, manual=True)
        assert exc.value.http_status == 409
        assert exc.value.code == "committed_rows_cannot_be_replayed"

        with pg.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM public."{dest_table}"')
            dest_count = int(cur.fetchone()[0])
        assert dest_count == 3, (
            f"retry leaked a second append: dest COUNT(*)={dest_count} (want 3)"
        )
    finally:
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{src_table}"')
            cur.execute(f'DROP TABLE IF EXISTS public."{dest_table}"')
        pg.close()
