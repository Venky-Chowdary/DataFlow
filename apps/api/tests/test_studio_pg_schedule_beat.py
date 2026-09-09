"""Studio-persisted PG→PG schedule beat — dest COUNT(*) required.

A file-source Theater Schedule CTA cannot persist. This is the path that can:
create_schedule (with mappings) → _run_schedule → run_transfer_async →
independent dest COUNT(*). Overwrite beat 2 must stay the same population.

Does not claim DST / overlap / retries / 100K. skip_preflight stays False.
"""

from __future__ import annotations

import socket
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
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


def _await_idle(schedule_id: str, timeout: float = 30.0) -> None:
    from services.schedule_store import get_schedule

    deadline = time.time() + timeout
    while time.time() < deadline:
        sched = get_schedule(schedule_id)
        if sched is not None and not str(getattr(sched, "running_job_id", "") or "").strip():
            return
        time.sleep(0.2)
    raise TimeoutError(f"schedule {schedule_id} still marked running")


def test_studio_pg_overwrite_schedule_beat_dest_count(tmp_path, monkeypatch):
    _pg_or_skip()
    from services.million_row_proof import ensure_memory_job_store_if_mongo_down
    from services.schedule_mapping_contract import persisted_mapping_rows
    from services.schedule_store import create_schedule, get_schedule
    from services.connector_store import create_connector
    from services import schedule_runner

    ensure_memory_job_store_if_mongo_down()
    _isolate_stores(tmp_path, monkeypatch)

    tag = uuid.uuid4().hex[:8]
    src_table = f"studio_sched_src_{tag}"
    dest_table = f"studio_sched_dst_{tag}"
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
            "name": f"studio-sched-src-{tag}",
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
            "name": f"studio-sched-dst-{tag}",
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
            "name": f"Studio PG beat {tag}",
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
        assert persisted_mapping_rows(sched.mappings)
        assert sched.enabled is True

        due = datetime.now(timezone.utc) - timedelta(seconds=30)
        import services.schedule_store as store

        loaded = store._load_all()
        for i, row in enumerate(loaded):
            if row.id == sched.id:
                loaded[i] = store.PipelineSchedule.from_dict(
                    {**row.to_dict(), "next_run_at": due.isoformat(), "enabled": True}
                )
        store._save_all(loaded)

        job_id = schedule_runner._run_schedule(sched.id, manual=True)
        assert job_id, "first beat did not start a job"
        job = _await_job(job_id)
        status = str(job.get("status") or "").lower()
        assert status in {"completed", "success", "succeeded"}, (
            f"first beat {status}: {job.get('error') or job.get('message') or job}"
        )
        _await_idle(sched.id)

        with pg.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM public."{dest_table}"')
            assert int(cur.fetchone()[0]) == 3
            cur.execute(
                f'SELECT id, label FROM public."{dest_table}" ORDER BY id'
            )
            assert cur.fetchall() == [(1, "a"), (2, "b"), (3, "c")]

        job_id_2 = schedule_runner._run_schedule(sched.id, manual=True)
        assert job_id_2, "overwrite beat 2 did not start a job"
        assert job_id_2 != job_id
        job2 = _await_job(job_id_2)
        status2 = str(job2.get("status") or "").lower()
        assert status2 in {"completed", "success", "succeeded"}, (
            f"second beat {status2}: {job2.get('error') or job2.get('message') or job2}"
        )
        _await_idle(sched.id)

        with pg.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM public."{dest_table}"')
            assert int(cur.fetchone()[0]) == 3

        finished = get_schedule(sched.id)
        assert finished is not None
        history = list(getattr(finished, "run_history", None) or [])
        assert len(history) >= 2
    finally:
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{src_table}"')
            cur.execute(f'DROP TABLE IF EXISTS public."{dest_table}"')
        pg.close()
