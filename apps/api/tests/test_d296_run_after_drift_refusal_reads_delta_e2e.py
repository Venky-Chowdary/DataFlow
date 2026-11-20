"""D296: the run after a drift-refused run reads only the delta.

QA (TimescaleDB -> MySQL; PG -> new SQL Server; incremental_upsert schedule):
run 2 was correctly refused (a new unmapped source column ``region``). After
the operator dropped ``region``, run 3 read all 30 rows instead of the 3-row
delta, and an edit that had not bumped ``updated_at`` landed.

Root cause (services/full_pass_rerun.py: apply_schedule_full_pass): a
schedule's first run after a failed run is a full pass of an incremental
upsert, so a failed write that may have left a stale row is repaired (D210).
A pre-run refusal is recorded on the schedule as a failed run with no job,
and it triggered the same full pass, although nothing was written and there
is nothing to repair. The watermark is untouched by a refused run (R1), so the
next run continues from it. A run whose job actually failed still re-reads
from the start, and says so (``read_scope`` = ``full_source``).

Route: MCP ``create_schedule`` -> Confirm -> scheduler beats, PostgreSQL ->
PostgreSQL (local).
"""

from __future__ import annotations

import time
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from tests import r4_route_harness as h


@pytest.fixture()
def client(tmp_path, monkeypatch):
    if not h.port_up(5432):
        pytest.skip("live PostgreSQL not configured")
    import services.config  # noqa: F401 - settings load before isolate blanks SEED_DEMO
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "false")
    monkeypatch.setenv("DATAFLOW_TRAINING", "off")
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _fire(schedule_id: str) -> dict:
    from services import schedule_runner
    from services.schedule_store import get_schedule, update_schedule

    past = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    patch = {"next_run_at": past}
    if getattr(get_schedule(schedule_id), "retry_at", None):
        patch["retry_at"] = past
    update_schedule(schedule_id, patch)
    schedule_runner._run_due_schedules()
    sched = get_schedule(schedule_id)
    job_id = str(sched.running_job_id or "")
    if not job_id:
        return {}
    job = h.wait_job(job_id, timeout_s=300)
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline and str(get_schedule(schedule_id).running_job_id or ""):
        schedule_runner._finalize_finished_schedule_claims()
        time.sleep(0.2)
    return job


def _dest(dst_t: str) -> dict[int, tuple]:
    rows = h.query("postgresql", f'SELECT id, name, qty FROM "{dst_t}" ORDER BY id')  # nosec B608
    return {int(r[0]): (r[1], int(r[2])) for r in rows}


def _rows_read(job: dict) -> int:
    summary = job.get("destination_summary") or {}
    for key in ("rows_read", "source_rows_read", "source_row_count"):
        if summary.get(key) is not None:
            return int(summary[key])
    return int(job.get("records_processed") or 0)


def test_run_after_a_drift_refusal_reads_only_the_delta(client):
    from services.schedule_store import get_schedule, update_schedule

    token = uuid.uuid4().hex[:8]
    src_t, dst_t = f"d296_s_{token}", f"d296_d_{token}"
    try:
        h.execute(
            "postgresql",
            f'CREATE TABLE "{src_t}" (id INT PRIMARY KEY, name TEXT, qty INT, '
            "updated_at TIMESTAMP NOT NULL)",
        )
        h.execute(
            "postgresql",
            f'INSERT INTO "{src_t}" SELECT g, \'n\' || g, g, '  # nosec B608 - test table
            "TIMESTAMP '2026-10-01 00:00:00' + g * INTERVAL '1 minute' "
            "FROM generate_series(1, 30) g",
        )
        from services.connector_store import create_connector

        src = create_connector({"name": f"d296-s-{token}", "role": "source", "workspace_id": "",
                                "ssl": False, **h.dest_config("postgresql")})
        dst = create_connector({"name": f"d296-d-{token}", "role": "destination",
                                "workspace_id": "", "ssl": False, **h.dest_config("postgresql")})
        ok, staged = h.mcp_call(client, "create_schedule", {
            "source_connector_id": src.id, "source_table": src_t,
            "dest_connector_id": dst.id, "dest_table": dst_t,
            "sync_mode": "incremental_upsert", "primary_key": "id",
            "cursor_column": "updated_at", "cursor_semantics": "modification_timestamp",
            "cadence": "every 15 minutes", "name": f"d296-{token}",
        })
        assert ok, staged
        resp = client.post("/api/v1/copilot/confirm",
                           json={"ack_id": staged["ack_id"], "reason": "d296"})
        assert resp.status_code == 200, resp.text
        body = resp.json()
        sid = str(body.get("schedule_id") or (body.get("schedule") or {}).get("id") or "")
        assert sid, body

        j1 = _fire(sid)
        assert str(j1.get("status")) == "completed", (j1.get("status"), j1.get("error"))
        assert len(_dest(dst_t)) == 30

        # Drift: a new source column, three bumped edits and one edit that
        # does not move the cursor (it must not be read by an incremental run).
        h.execute("postgresql", f'ALTER TABLE "{src_t}" ADD COLUMN region TEXT')
        h.execute("postgresql", f'UPDATE "{src_t}" SET qty = qty + 1000, '  # nosec B608
                  "updated_at = TIMESTAMP '2026-10-02 00:00:00' + id * INTERVAL '1 minute' "
                  "WHERE id IN (3, 4, 5)")
        h.execute("postgresql", f'UPDATE "{src_t}" SET name = \'unbumped\' WHERE id = 10')  # nosec B608

        j2 = _fire(sid)
        refused = get_schedule(sid)
        assert str(refused.last_status) != "completed", (refused.last_status, j2)
        assert "region" in str(refused.to_dict()), "the refusal does not name the new column"

        # The operator drops the column and lets the schedule run again.
        h.execute("postgresql", f'ALTER TABLE "{src_t}" DROP COLUMN region')
        update_schedule(sid, {"paused": False, "approval_request": None,
                              "parked_finding": None, "status": "active"})
        j3 = _fire(sid)
        assert j3, get_schedule(sid).to_dict().get("run_history", [])[-1:]
        assert str(j3.get("status")) == "completed", (j3.get("status"), j3.get("error"))
        got = _dest(dst_t)
        # The three bumped edits landed ...
        assert [got[i][1] for i in (3, 4, 5)] == [1003, 1004, 1005], got
        # ... and nothing outside the delta was re-read.
        assert got[10][0] == "n10", got[10]
        # The 3-row delta, plus at most the watermark's own tie group (row 30)
        # that an inclusive cursor re-reads and the upsert absorbs (R1).
        assert 3 <= _rows_read(j3) <= 4, (_rows_read(j3), j3.get("destination_summary"))
        assert (j3.get("read_scope") or {}).get("scope") != "full_source", j3.get("read_scope")
    finally:
        h.drop_table("postgresql", src_t)
        h.drop_table("postgresql", dst_t)


def _req():
    from types import SimpleNamespace

    return SimpleNamespace(sync_mode="incremental_upsert", stream_contracts=[])


def _sched(last_status: str, last_job_id: str, history: list[dict]):
    from types import SimpleNamespace

    return SimpleNamespace(last_status=last_status, last_job_id=last_job_id, run_history=history)


@pytest.mark.parametrize(
    "sched, attempt, full_pass",
    [
        # A pre-run refusal after an accepted run: nothing to repair (D296).
        (_sched("failed", "", [{"job_id": "j1", "status": "completed"},
                               {"job_id": "", "status": "failed"}]), 0, False),
        # A refusal with no earlier run at all.
        (_sched("failed", "", [{"job_id": "", "status": "failed"}]), 0, False),
        # A job that failed may have left a stale row: re-read from the start (D210).
        (_sched("failed", "j2", [{"job_id": "j2", "status": "failed"}]), 0, True),
        # A refusal after such a failure does not forget the unrepaired job.
        (_sched("failed", "", [{"job_id": "j2", "status": "failed"},
                               {"job_id": "", "status": "failed"}]), 0, True),
        # The schedule's own retry of a failed attempt.
        (_sched("running", "j3", []), 1, True),
        # A completed last run continues from the watermark.
        (_sched("completed", "j4", [{"job_id": "j4", "status": "completed"}]), 0, False),
    ],
)
def test_full_pass_only_after_a_run_that_wrote_and_failed(sched, attempt, full_pass):
    from services.full_pass_rerun import apply_schedule_full_pass

    req = _req()
    scope = apply_schedule_full_pass(req, sched, attempt=attempt)
    assert bool(scope) is full_pass, scope
    if full_pass:
        # A re-snapshot is disclosed on the job (AC-2).
        assert scope["scope"] == "full_source" and scope["reason"], scope
        assert req.sync_mode == "upsert"
    else:
        assert req.sync_mode == "incremental_upsert"
