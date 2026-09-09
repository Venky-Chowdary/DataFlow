"""Live PG schedule overlap — dest COUNT(*) must not double.

Two append schedules can share a destination table from different sources.
The dest-object lock refuses the second beat while the first is in writing.
Independent dest COUNT stays the first writer's population (3), not 6.

Does not claim DST / retries / 100K / multi-instance Mongo CAS.
skip_preflight stays False.
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
        if str(doc.get("status") or "").lower() in {
            "completed",
            "success",
            "succeeded",
            "failed",
            "error",
            "cancelled",
        }:
            return doc
        time.sleep(0.4)
    return doc


def test_studio_pg_overlap_append_dest_count_stays_three(tmp_path, monkeypatch):
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

    def _hold_first_writing(job_id: str, status: str, **kwargs):
        if (
            not writing.is_set()
            and status == "running"
            and str(kwargs.get("phase") or "") == "writing"
        ):
            writing.set()
            released.wait(timeout=20)
        return orig_update(job_id, status, **kwargs)

    monkeypatch.setattr(mongo, "update_job_status", _hold_first_writing)

    tag = uuid.uuid4().hex[:8]
    src_a = f"studio_ov_src_a_{tag}"
    src_b = f"studio_ov_src_b_{tag}"
    dest_table = f"studio_ov_dst_{tag}"
    pg = _pg_connect()
    try:
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{src_a}"')
            cur.execute(f'DROP TABLE IF EXISTS public."{src_b}"')
            cur.execute(f'DROP TABLE IF EXISTS public."{dest_table}"')
            cur.execute(
                f'CREATE TABLE public."{src_a}" '
                f"(id bigint PRIMARY KEY, label varchar(32) NOT NULL)"
            )
            cur.execute(
                f'CREATE TABLE public."{src_b}" '
                f"(id bigint PRIMARY KEY, label varchar(32) NOT NULL)"
            )
            cur.execute(
                f'INSERT INTO public."{src_a}" (id, label) VALUES '
                f"(1, 'a'), (2, 'b'), (3, 'c')"
            )
            cur.execute(
                f'INSERT INTO public."{src_b}" (id, label) VALUES '
                f"(11, 'x'), (12, 'y'), (13, 'z')"
            )

        src_conn_a = create_connector({
            "name": f"studio-ov-src-a-{tag}",
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
        src_conn_b = create_connector({
            "name": f"studio-ov-src-b-{tag}",
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
            "name": f"studio-ov-dst-{tag}",
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

        sched_a = create_schedule({
            "name": f"Studio overlap A {tag}",
            "source_connector_id": src_conn_a.id,
            "source_table": src_a,
            "dest_connector_id": dest.id,
            "dest_table": dest_table,
            "interval": "daily",
            "timezone": "UTC",
            "sync_mode": "full_refresh_append",
            "validation_mode": "balanced",
            "primary_key": "id",
            "enabled": True,
            "mappings": mappings,
        })
        sched_b = create_schedule({
            "name": f"Studio overlap B {tag}",
            "source_connector_id": src_conn_b.id,
            "source_table": src_b,
            "dest_connector_id": dest.id,
            "dest_table": dest_table,
            "interval": "daily",
            "timezone": "UTC",
            "sync_mode": "full_refresh_append",
            "validation_mode": "balanced",
            "primary_key": "id",
            "enabled": True,
            "mappings": mappings,
        })
        assert sched_a.source_connector_id != sched_b.source_connector_id
        assert sched_a.dest_connector_id == sched_b.dest_connector_id
        assert sched_a.dest_table == sched_b.dest_table

        started: dict[str, str | None] = {"id": None, "err": None}

        def _run_a() -> None:
            try:
                started["id"] = schedule_runner._run_schedule(sched_a.id, manual=True)
            except Exception as exc:  # noqa: BLE001
                started["err"] = str(exc)

        worker = threading.Thread(target=_run_a, name="pg-overlap-a")
        worker.start()
        assert writing.wait(timeout=60), f"writing phase never reached: {started}"

        with pytest.raises(schedule_runner.ScheduleStartError) as exc:
            schedule_runner._run_schedule(sched_b.id, manual=True)
        assert exc.value.http_status == 409
        assert exc.value.code == "already_running"
        assert "destination table" in str(exc.value).lower()

        released.set()
        worker.join(timeout=90)
        assert not worker.is_alive()
        assert started["err"] is None, started["err"]
        job_id = started["id"]
        assert job_id, "first beat did not start a job"
        job = _await_job(job_id)
        status = str(job.get("status") or "").lower()
        assert status in {"completed", "success", "succeeded"}, (
            f"first beat {status}: {job.get('error') or job.get('message') or job}"
        )

        with pg.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM public."{dest_table}"')
            dest_count = int(cur.fetchone()[0])
        assert dest_count == 3, (
            f"overlap leaked a second writer: dest COUNT(*)={dest_count} (want 3)"
        )
    finally:
        released.set()
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{src_a}"')
            cur.execute(f'DROP TABLE IF EXISTS public."{src_b}"')
            cur.execute(f'DROP TABLE IF EXISTS public."{dest_table}"')
        pg.close()
