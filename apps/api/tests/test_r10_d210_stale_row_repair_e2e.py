"""D210 remainder (QA round 10): a stale Oracle row is repaired, never kept.

QA, on the #359 head: an Oracle destination row still holds an old value where
the source cell is now ``''`` (Oracle stores it as NULL). The job no longer
goes green (Gate-8 catches the stale cell), but the stale row was never
repaired. Owner decision 4: write NULL for ``''``, never keep a stale value,
and a retry really re-reads from the start.

Routes, on real PostgreSQL -> real Oracle Free:

* MCP ``start_transfer`` -> Confirm -> job (run 1, then a run that finds the
  stale row and fails), then the REST ``/retry`` of THAT failed job;
* MCP ``create_schedule`` -> Confirm -> scheduler beat (run 1, a beat that
  finds the stale row and fails), then the schedule's own rerun.

After the retry / rerun the row is NULL, the job is green, and the
destination equals the source cell by cell ('' expected as NULL).
"""

from __future__ import annotations

import time
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from tests import r4_route_harness as h
from tests import test_r8_d210_oracle_empty_string_e2e as o

pytestmark = pytest.mark.timeout(900)

client = o.client  # same isolated app fixture

_ROWS = [
    (1, "", "keep-1"),          # '' -> NULL in Oracle: the stale target
    (2, None, "keep-2"),
    (3, "  ", "keep-3"),
    (4, "Łukasz 山田 🎉", ""),
    (5, "five", "keep-5"),
]


def _create_source(table: str) -> None:
    """QA's shape: every row has its own modification time."""
    o._create_source(table, _ROWS)
    h.execute("postgresql", f"UPDATE \"{table}\" SET updated_at = "
              "TIMESTAMP '2026-10-01 00:00:00' + id * INTERVAL '1 minute'")


def _stale(dst_t: str) -> None:
    """The destination row left behind by an earlier (buggy) write."""
    name = o._ora_name(dst_t)
    o._ora(f'UPDATE "{name}" SET note = \'alpha\' WHERE id = 1')
    o._ora(f'UPDATE "{name}" SET tag = \'stale\' WHERE id = 4')


def _args(src, dst, src_t, dst_t) -> dict:
    return {
        "source_connector_id": src.id, "source_table": src_t,
        "dest_connector_id": dst.id, "dest_table": dst_t,
        "sync_mode": "incremental_upsert", "primary_key": "id",
        "cursor_column": "updated_at", "cursor_semantics": "modification_timestamp",
    }


def _stage_confirm(client, args: dict) -> dict:
    ok, staged = h.mcp_call(client, "start_transfer", args)
    assert ok, {"error": staged}
    assert not h.blockers_of(staged), h.blockers_of(staged)
    resp = client.post("/api/v1/copilot/confirm", json={"ack_id": staged["ack_id"], "reason": "d210"})
    assert resp.status_code == 200, resp.text
    return h.wait_job(str(resp.json().get("job_id") or ""))


def _expected(src_t: str) -> list[tuple]:
    return o._oracle_expected(o._source_rows(src_t))


def _edit_to_empty(src_t: str) -> None:
    """QA's source edit: a value becomes '' and its modification time moves."""
    h.execute("postgresql", f"UPDATE \"{src_t}\" SET note = '', tag = 'changed-1', "
              "updated_at = TIMESTAMP '2026-10-02 00:00:00' WHERE id = 5")


@pytest.fixture
def leave_stale_once(monkeypatch):
    """The next run's write leaves row 5's old value behind, as production
    087ac4bf's sparse write did, and Gate-8 then reads it: a genuinely failed
    attempt, committed, through the engine's own verification."""
    import src.transfer.engine as engine_mod

    state = {"dst": None}
    real = engine_mod.run_reconciliation

    def reconcile(*args, **kwargs):
        if state["dst"]:
            name = o._ora_name(state["dst"])
            state["dst"] = None
            o._ora(f'UPDATE "{name}" SET note = \'five\' WHERE id = 5')
            # An older stale row from an earlier write: outside this run's
            # cursor window, so only a read from the start repairs it.
            o._ora(f'UPDATE "{name}" SET note = \'alpha\' WHERE id = 1')
        return real(*args, **kwargs)

    monkeypatch.setattr(engine_mod, "run_reconciliation", reconcile)
    return state


def test_rest_retry_of_the_failed_run_repairs_the_stale_row(client, leave_stale_once):
    token = uuid.uuid4().hex[:8]
    src_t, dst_t = f"d210r_s_{token}", f"d210r_u_{token}"
    try:
        _create_source(src_t)
        src, dst = o._connectors(token)
        args = _args(src, dst, src_t, dst_t)
        j1 = _stage_confirm(client, args)
        assert str(j1.get("status")) == "completed", (j1.get("status"), j1.get("error"))
        _edit_to_empty(src_t)
        leave_stale_once["dst"] = dst_t
        j2 = _stage_confirm(client, args)
        assert str(j2.get("status")) != "completed", (j2.get("status"), j2.get("error"))
        assert o._dest_rows(dst_t)[4][1] == "five"

        resp = client.post(f"/api/v1/connectors/jobs/{o._jid(j2)}/retry")
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["retry_decision"].get("read_scope") == "full_source", body
        j3 = h.wait_job(str(body["job_id"]))
        assert str(j3.get("status")) == "completed", (j3.get("status"), j3.get("error"))
        got = o._dest_rows(dst_t)
        assert got == _expected(src_t), got
        assert got[4] == (5, None, "changed-1"), got[4]
        assert got[0] == (1, None, "keep-1"), got[0]
    finally:
        h.drop_table("postgresql", src_t)
        o._drop_ora(dst_t)


def _fire(schedule_id: str) -> dict:
    from services import schedule_runner
    from services.schedule_store import get_schedule, update_schedule

    past = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    patch = {"next_run_at": past}
    if getattr(get_schedule(schedule_id), "retry_at", None):
        patch["retry_at"] = past  # the schedule's own retry of the failed run, due now
    update_schedule(schedule_id, patch)
    assert schedule_runner._run_due_schedules() == 1
    sched = get_schedule(schedule_id)
    return h.wait_job(str(sched.running_job_id or sched.last_job_id or ""), timeout_s=300)


def _settle(schedule_id: str) -> None:
    from services import schedule_runner
    from services.schedule_store import get_schedule

    deadline = time.monotonic() + 60
    while time.monotonic() < deadline and str(get_schedule(schedule_id).running_job_id or ""):
        schedule_runner._finalize_finished_schedule_claims()
        time.sleep(0.2)


def test_scheduled_rerun_after_the_failed_run_repairs_the_stale_row(client, leave_stale_once):
    token = uuid.uuid4().hex[:8]
    src_t, dst_t = f"d210r_s_{token}", f"d210r_s2_{token}"
    try:
        _create_source(src_t)
        src, dst = o._connectors(token)
        ok, staged = h.mcp_call(client, "create_schedule", {
            **_args(src, dst, src_t, dst_t), "cadence": "every 15 minutes",
            "name": f"d210r-{token}"})
        assert ok, staged
        resp = client.post("/api/v1/copilot/confirm", json={"ack_id": staged["ack_id"], "reason": "d210"})
        assert resp.status_code == 200, resp.text
        body = resp.json()
        sid = str(body.get("schedule_id") or (body.get("schedule") or {}).get("id") or "")
        assert sid, body

        j1 = _fire(sid)
        _settle(sid)
        assert str(j1.get("status")) == "completed", (j1.get("status"), j1.get("error"))
        assert o._dest_rows(dst_t) == _expected(src_t)

        _edit_to_empty(src_t)
        leave_stale_once["dst"] = dst_t
        j2 = _fire(sid)
        _settle(sid)
        assert str(j2.get("status")) != "completed", (j2.get("status"), j2.get("error"))
        assert o._dest_rows(dst_t)[4][1] == "five"

        # The schedule's next run after that failure: its retry if it scheduled
        # one, otherwise the next beat. Either way it re-reads from the start.
        j3 = _fire(sid)
        _settle(sid)
        assert str(j3.get("status")) == "completed", (j3.get("status"), j3.get("error"))
        got = o._dest_rows(dst_t)
        assert got == _expected(src_t), got
        assert got[4] == (5, None, "changed-1"), got[4]
        assert got[0] == (1, None, "keep-1"), got[0]
        scope = j3.get("read_scope") or {}
        assert scope.get("scope") == "full_source", scope
    finally:
        h.drop_table("postgresql", src_t)
        o._drop_ora(dst_t)
