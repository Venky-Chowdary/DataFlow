"""QA MXD10 — a cancel that lands after rows committed must say so.

Live PG→SQLite (the QA route): the load commits, then Cancel lands before the
worker's success write (held here, a race in production). The job stays ``cancelled`` (sticky), but it must
report the rows the destination now holds and warn that nothing was rolled
back — not ``records_processed=0``.
"""

from __future__ import annotations

import socket
import sys
import threading
import time
import uuid
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


def test_cancel_after_commit_reports_committed_rows(tmp_path, monkeypatch):
    _pg_or_skip()
    from services.connector_store import create_connector
    from services.million_row_proof import ensure_memory_job_store_if_mongo_down
    from services.mongodb_service import get_mongodb_service
    from services.schedule_mapping_contract import persisted_mapping_rows
    from services.schedule_store import create_schedule
    from services import schedule_runner

    ensure_memory_job_store_if_mongo_down()
    _isolate_stores(tmp_path, monkeypatch)

    mongo = get_mongodb_service()
    orig_update = mongo.update_job_status
    writing = threading.Event()
    released = threading.Event()
    seen_job: dict[str, str] = {}

    def _hold_success_write(job_id: str, status: str, **kwargs):
        if not writing.is_set() and status in {
            "completed",
            "completed_with_quarantine",
        }:
            seen_job["id"] = job_id
            writing.set()
            released.wait(timeout=20)
        return orig_update(job_id, status, **kwargs)

    monkeypatch.setattr(mongo, "update_job_status", _hold_success_write)

    tag = uuid.uuid4().hex[:8]
    src_table = f"mxd10_cancel_src_{tag}"
    dest_table = f"mxd10_cancel_dst_{tag}"
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
                f'INSERT INTO public."{src_table}" (id, label) '
                f"SELECT g, 'r' || g::text FROM generate_series(1, 800) AS g"
            )

        src = create_connector({
            "name": f"mxd10-cancel-src-{tag}",
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
        dest_db = tmp_path / "mxd10_dest.db"
        dest = create_connector({
            "name": f"mxd10-cancel-dst-{tag}",
            "type": "sqlite",
            "role": "both",
            "database": str(dest_db),
        })
        mappings = [
            {"source": "id", "target": "id", "confidence": 1.0},
            {"source": "label", "target": "label", "confidence": 1.0},
        ]
        assert persisted_mapping_rows(mappings)
        sched = create_schedule({
            "name": f"MXD10 cancel {tag}",
            "source_connector_id": src.id,
            "source_table": src_table,
            "dest_connector_id": dest.id,
            "dest_table": dest_table,
            "interval": "daily",
            "timezone": "UTC",
            "sync_mode": "full_refresh_overwrite",
            "validation_mode": "balanced",
            "primary_key": "id",
            "enabled": True,
            "mappings": mappings,
        })

        started: dict[str, str | None] = {"id": None, "err": None}

        def _run() -> None:
            try:
                started["id"] = schedule_runner._run_schedule(sched.id, manual=True)
            except Exception as exc:  # noqa: BLE001 — surface in assert
                started["err"] = str(exc)

        worker = threading.Thread(target=_run, name="pg-cancel-beat")
        worker.start()
        assert writing.wait(timeout=60), (
            f"writing phase never reached: {started} {seen_job}"
        )
        job_id = seen_job.get("id") or ""
        assert job_id, "writing heartbeat did not name a job"

        import importlib

        from fastapi.testclient import TestClient
        from src.main import app

        cancel_mod = importlib.import_module("src.routers.connectors_router")
        monkeypatch.setattr(cancel_mod, "get_mongodb_service", lambda: mongo)
        monkeypatch.setattr(
            "src.middleware.auth_middleware._auth_service.auth_required",
            lambda: False,
        )
        with TestClient(app) as client:
            res = client.post(f"/api/v1/connectors/jobs/{job_id}/cancel")
        assert res.status_code == 200, res.text
        assert res.json().get("status") == "cancelled"

        mid = mongo.get_job(job_id) or {}
        assert mid.get("status") == "cancelled", mid
        assert mid.get("cancel_requested") is True

        released.set()
        worker.join(timeout=90)
        assert not worker.is_alive(), "transfer thread still running after cancel"
        # Pool worker may still be in COPY / late status writes. Settle while
        # isolated stores still exist so dest COUNT is post-attempt, not mid-wire.
        deadline = time.time() + 12
        while time.time() < deadline:
            time.sleep(0.4)
            latest = mongo.get_job(job_id) or {}
            if str(latest.get("status") or "") != "cancelled":
                break

        final = mongo.get_job(job_id) or {}
        status = str(final.get("status") or "").lower()
        assert status == "cancelled", (
            f"late worker rewrote cancel to {status}: "
            f"{final.get('error') or final.get('message') or final}"
        )
        assert status not in {"completed", "completed_with_quarantine", "success"}

        import sqlite3

        dest_count = None
        if dest_db.exists():
            with sqlite3.connect(dest_db) as lite:
                hit = lite.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (dest_table,),
                ).fetchone()
                if hit:
                    dest_count = int(
                        lite.execute(f'SELECT COUNT(*) FROM "{dest_table}"').fetchone()[0]
                    )
        assert dest_count, "precondition: the worker committed rows after Cancel"
        assert int(final.get("records_processed") or 0) == dest_count, (
            f"cancelled job reports {final.get('records_processed')} rows; "
            f"destination holds {dest_count}"
        )
        text = f"{final.get('message') or ''} {final.get('cancel_outcome') or ''}".lower()
        assert "committed" in text and "not rolled back" in text, final
        outcome = final.get("cancel_outcome") or {}
        assert outcome.get("partial_write") is True, final
        assert int(outcome.get("rows_committed") or 0) == dest_count, final
    finally:
        released.set()
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{src_table}"')
        pg.close()


@pytest.fixture
def _quiet_slot(monkeypatch):
    monkeypatch.setattr(
        "services.cdc_catchup.release_finished_cdc_slot",
        lambda *args, **kwargs: {"released": False, "reason": "test"},
    )


def _cancelled_job(job_id: str, rows_written: int):
    from services.error_handling import TransferCancelled
    from services.mongodb_service import MemoryMongoDBService
    from src.transfer.job_failure import _fail_runtime_job

    mongo = MemoryMongoDBService()
    mongo.create_transfer_job({"_id": job_id, "status": "running", "records_processed": 0})
    mongo.request_job_cancel(job_id)
    mongo.update_job_status(
        job_id, "cancelled", phase="cancelled", message="Transfer cancelled by user"
    )
    exc = TransferCancelled("Transfer cancelled by user", rows_written=rows_written)
    _fail_runtime_job(mongo, job_id, exc, request=None)
    return mongo.get_job(job_id) or {}


def test_cancel_after_a_committed_chunk_warns_nothing_was_rolled_back(_quiet_slot):
    job = _cancelled_job("mxd10-chunk", 90)
    assert job["status"] == "cancelled"
    assert job["records_processed"] == 90
    assert job["cancel_outcome"] == {
        "partial_write": True,
        "rows_committed": 90,
        "rolled_back": False,
        "message": job["message"],
    }
    assert "90 row(s) were already committed" in job["message"]
    assert "not rolled back" in job["message"]


def test_cancel_before_any_commit_is_a_clean_stop(_quiet_slot):
    job = _cancelled_job("mxd10-clean", 0)
    assert job["status"] == "cancelled"
    assert int(job.get("records_processed") or 0) == 0
    assert job["cancel_outcome"]["partial_write"] is False
    assert job["message"] == "Transfer cancelled by user"
