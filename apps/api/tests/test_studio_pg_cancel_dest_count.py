"""Live PG→PG cancel — final status stays cancelled, not rewritten to completed.

Holds the first writing heartbeat so Cancel lands before the worker's success
write. COPY may still finish after Cancel (at-least-once; not interruptible
mid-wire). Dest COUNT is reported, not required to be 0.

skip_preflight stays False.
"""

from __future__ import annotations

import socket
import sys
import threading
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


def test_studio_pg_cancel_final_status_is_cancelled(tmp_path, monkeypatch):
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

    def _hold_first_writing(job_id: str, status: str, **kwargs):
        if (
            not writing.is_set()
            and status == "running"
            and str(kwargs.get("phase") or "") == "writing"
        ):
            seen_job["id"] = job_id
            writing.set()
            released.wait(timeout=20)
        return orig_update(job_id, status, **kwargs)

    monkeypatch.setattr(mongo, "update_job_status", _hold_first_writing)

    tag = uuid.uuid4().hex[:8]
    src_table = f"studio_cancel_src_{tag}"
    dest_table = f"studio_cancel_dst_{tag}"
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
            "name": f"studio-cancel-src-{tag}",
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
            "name": f"studio-cancel-dst-{tag}",
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
            "name": f"Studio PG cancel {tag}",
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

        from fastapi.testclient import TestClient
        from src.main import app

        monkeypatch.setattr(
            "src.routers.connectors_router.get_mongodb_service", lambda: mongo
        )
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

        final = mongo.get_job(job_id) or {}
        status = str(final.get("status") or "").lower()
        assert status == "cancelled", (
            f"late worker rewrote cancel to {status}: "
            f"{final.get('error') or final.get('message') or final}"
        )
        assert status not in {"completed", "completed_with_quarantine", "success"}

        with pg.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema='public' AND table_name=%s",
                (dest_table,),
            )
            exists = int(cur.fetchone()[0])
            dest_count = None
            if exists:
                cur.execute(f'SELECT COUNT(*) FROM public."{dest_table}"')
                dest_count = int(cur.fetchone()[0])
        # Honesty: COPY may have finished after Cancel. Do not require empty dest.
        assert dest_count in (None, 0) or dest_count <= 800
        assert started["err"] is None or "cancel" in str(started["err"]).lower()
    finally:
        released.set()
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{src_table}"')
            cur.execute(f'DROP TABLE IF EXISTS public."{dest_table}"')
        pg.close()
