"""D433 on a live route: no unrequested run, and every run starts from the committed watermark.

QA's sequence (incremental_upsert, yearly cron, max_retries 0): run 1 fails on
DNS, a Run now succeeds, the schedule is watched — the scheduler started a run
by itself seconds later, and that run re-read the whole source instead of the
delta since the manual run.

The second half had its own cause: the first run after a failed one is a full
pass of an incremental upsert (D210 — repair what the failed write may have
left), and a full pass neither uses nor moves the watermark. When the failed
run was the route's first, no watermark existed, so the manual run read every
row (unavoidable) and committed nothing — and the next run read every row
again. With no watermark stored the incremental read already covers the whole
source, so it runs as an incremental read and commits the watermark.

Drives the REST routes the UI uses and the real scheduler beat, PostgreSQL ->
PostgreSQL (CI's PG service; PG* env).
"""

from __future__ import annotations

import time
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(300)


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    import services.notification_service as ns

    monkeypatch.setattr(ns, "notify_workspace", lambda ws, payload: [])
    from fastapi.testclient import TestClient
    from src.main import app

    # No lifespan: the background loop must not race the beats this test drives.
    return TestClient(app)


def _recorded(schedule_id: str, job_id: str):
    from services.schedule_store import get_schedule

    h.wait_job(job_id, 120)
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        sched = get_schedule(schedule_id)
        if not sched.running and sched.last_job_id == job_id:
            return sched
        time.sleep(0.2)
    raise AssertionError(f"job {job_id} was not recorded on the schedule")


def _history(client, schedule_id: str) -> list[dict]:
    resp = client.get(f"/api/v1/schedules/{schedule_id}/history")
    assert resp.status_code == 200, resp.text
    return resp.json()["runs"]


def test_no_unrequested_run_and_each_run_reads_from_the_committed_watermark(client):
    from services import schedule_runner
    from services.connector_store import create_connector, update_connector
    from services.mongodb_service import get_mongodb_service
    from services.schedule_store import create_schedule, get_schedule, update_schedule

    pg = h.pg_conn()
    table = "d433_" + uuid.uuid4().hex[:8]
    try:
        cur = pg.cursor()
        cur.execute(  # nosec B608 - test-owned table name
            f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, name TEXT, updated_at TIMESTAMP NOT NULL)"
        )
        cur.execute(  # nosec B608
            f"INSERT INTO {table} SELECT g, 'n' || g, TIMESTAMP '2026-01-01' + g * INTERVAL '1 minute' "
            "FROM generate_series(1, 30) g"
        )
    finally:
        pg.close()
    try:
        src = create_connector({"name": f"src-{table}", "type": "postgresql", "role": "source",
                                "ssl": False, **h.PG, "schema": "public"})
        dst = create_connector({"name": f"dst-{table}", "role": "destination", "ssl": False,
                                **h.dest_config("postgresql")})
        sched = create_schedule({
            "name": f"d433 {table}", "source_connector_id": src.id, "source_table": table,
            "dest_connector_id": dst.id, "dest_table": f"{table}_d",
            "interval": "cron", "cron": "0 3 1 1 *", "timezone": "UTC",
            "sync_mode": "incremental_upsert", "primary_key": "id", "cursor_column": "updated_at",
            "cursor_semantics": "modification_timestamp", "max_retries": 0, "enabled": True,
            "mappings": [{"source": c, "target": c, "confidence": 1.0} for c in ("id", "name", "updated_at")],
        })
        window = sched.next_run_at

        # Run 1 fails: the source host does not resolve.
        update_connector(src.id, {"host": "d433-no-such-host.invalid"})
        resp = client.post(f"/api/v1/schedules/{sched.id}/run")
        assert resp.status_code == 200, resp.text
        failed = _recorded(sched.id, resp.json()["job_id"])
        assert failed.last_status != "completed"

        # Run now, with the source back: succeeds and commits a watermark.
        update_connector(src.id, {"host": h.PG["host"]})
        resp = client.post(f"/api/v1/schedules/{sched.id}/run")
        assert resp.status_code == 200, resp.text
        manual_job = resp.json()["job_id"]
        done = _recorded(sched.id, manual_job)
        assert done.last_status == "completed"
        assert done.next_run_at == window, "a manual run moved the cron window"
        report = get_mongodb_service().get_job(manual_job).get("incremental_watermark") or {}
        committed = report.get("after")
        assert committed, ("the run after the failed first run committed no watermark", report)

        # AC-1: watched, the scheduler starts nothing until the next window.
        runs_before = len(get_schedule(sched.id).run_history)
        for _ in range(3):
            assert schedule_runner._run_due_schedules() == 0
        assert len(get_schedule(sched.id).run_history) == runs_before

        # The cron window comes (stood in for by moving the stored slot): five
        # new rows since the manual run.
        pg = h.pg_conn()
        try:
            pg.cursor().execute(  # nosec B608
                f"INSERT INTO {table} SELECT g, 'n' || g, TIMESTAMP '2026-02-01' + g * INTERVAL '1 minute' "
                "FROM generate_series(31, 35) g"
            )
        finally:
            pg.close()
        slot = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat()
        update_schedule(sched.id, {"next_run_at": slot})
        assert schedule_runner._run_due_schedules() == 1
        window_job = get_schedule(sched.id).running_job_id or get_schedule(sched.id).last_job_id
        _recorded(sched.id, window_job)

        # AC-3 / AC-4: the window's run read the delta from the manual run's
        # watermark, and its record says why it ran and what it read from.
        run = _history(client, sched.id)[0]
        assert run["job_id"] == window_job and run["status"] == "completed", run
        assert run["trigger"] == "cadence" and run["fire_reason"] == "cron_window", run
        assert run["scheduled_for"] == slot, run
        assert run["watermark"]["scope"] == "delta" and run["watermark"]["from"] == committed, run
        job = get_mongodb_service().get_job(window_job)
        assert (job.get("incremental_watermark") or {}).get("before") == committed, job
        # Five new rows, plus at most the boundary row the commit-safe read re-checks.
        assert run["rows_read"] <= 6, run
        assert h.query("postgresql", f'SELECT COUNT(*) FROM "{table}_d"')[0][0] == 35  # nosec B608
    finally:
        h.drop_table("postgresql", table)
        h.drop_table("postgresql", f"{table}_d")
