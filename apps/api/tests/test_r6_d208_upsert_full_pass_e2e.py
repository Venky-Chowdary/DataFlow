"""D208 — an upsert re-run is a full second pass, and "upsert" never means incremental.

QA (production 557f556c):

(a) Re-running an upsert job said "a second full pass merges onto the declared
    key", but the new job read only rows past the saved watermark: a row edited
    or deleted in the destination stayed edited / missing.
(b) ``sync_mode="upsert"`` without a cursor column was planned as
    ``incremental_upsert`` with an inferred ``updated_at`` watermark, so run 2
    skipped a source edit whose ``updated_at`` did not move.

Contract pinned here on QA's routes (MCP plan/start -> Confirm -> job, MCP
``rerun_job`` / REST ``/retry`` -> job, MCP ``create_schedule`` -> beat),
PostgreSQL -> PostgreSQL:

* a re-run or retry of an incremental upsert re-reads the whole source,
  ignoring the watermark, and upserts every row; the job reports
  ``read_scope`` (mode upsert, scope full_source, watermark ignored + its
  value, rows read) and that destination-only rows are kept;
* ``upsert`` without a cursor is a full-source upsert on every route; its
  report says the watermark was not used; a destination-only row stays and is
  counted;
* NULL, '', whitespace and unicode ride on every row set.
"""

from __future__ import annotations

import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(600)

_ROWS = [(1, "a"), (2, None), (3, ""), (4, "  "), (5, "Łukasz 山田 🎉"), (6, "f")]


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "false")  # Settings parses it as a bool
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _lit(v):
    return "NULL" if v is None else "'" + v.replace("'", "''") + "'"


def _create_source(table: str) -> None:
    h.execute("postgresql", f'CREATE TABLE "{table}" (id INT PRIMARY KEY, note TEXT NULL, '
              "updated_at TIMESTAMP NOT NULL)")
    for rid, note in _ROWS:
        h.execute("postgresql", f'INSERT INTO "{table}" VALUES ({rid}, {_lit(note)}, '
                  f"'2026-10-01 00:00:{rid:02d}')")


def _rows(table: str) -> list[tuple]:
    rows = h.query("postgresql", f'SELECT id, note FROM "{table}"')  # nosec B608
    return sorted(((int(r[0]), r[1]) for r in rows), key=lambda r: r[0])


def _connectors(token: str):
    from services.connector_store import create_connector

    cfg = h.dest_config("postgresql")
    src = create_connector({"name": f"d208-s-{token}", "role": "source", "workspace_id": "",
                            "ssl": False, **cfg})
    dst = create_connector({"name": f"d208-d-{token}", "role": "destination",
                            "workspace_id": "", "ssl": False, **cfg})
    return src, dst


def _tamper_destination(dest: str) -> None:
    """Hand edits QA made in the destination: edit, delete, add."""
    h.execute("postgresql", f"UPDATE \"{dest}\" SET note = 'edited by hand' WHERE id = 1")
    h.execute("postgresql", f'DELETE FROM "{dest}" WHERE id = 5')
    h.execute("postgresql", f"INSERT INTO \"{dest}\" (id, note, updated_at) "
              "VALUES (99, 'only in destination', '2026-10-01 00:00:00')")


def _jid(job: dict) -> str:
    return str(job.get("job_id") or job.get("_id") or job.get("id") or "")


def _assert_full_pass_report(job: dict, *, rows: int, watermark: Any) -> dict:
    scope = job.get("read_scope")
    assert isinstance(scope, dict), {k: job.get(k) for k in ("message", "incremental_watermark")}
    assert scope.get("mode") == "upsert", scope
    assert scope.get("scope") == "full_source", scope
    assert int(scope.get("rows_read")) == rows, scope
    if watermark is None:
        assert scope.get("watermark") == "not used", scope
    else:
        assert scope.get("watermark") == "ignored", scope
        assert str(scope.get("stored_watermark")) == str(watermark), scope
    assert "kept" in str(scope.get("destination_only_rows")), scope
    # Counted when the run measured the destination after the write.
    assert scope.get("destination_only_count") in (None, 1), scope
    assert job.get("incremental_watermark") in (None, {}), job.get("incremental_watermark")
    return scope


# --------------------------------------------------------------------------- #
# (a) re-run / retry of an incremental upsert
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("action", ["rerun_mcp", "retry_rest"])
def test_upsert_rerun_is_a_full_second_pass(client, action):
    token = uuid.uuid4().hex[:8]
    src_t, dst_t = f"d208_s_{token}", f"d208_d_{token}"
    try:
        _create_source(src_t)
        src, dst = _connectors(token)
        j1 = h.stage_and_run(client, "start_transfer", {
            "source_connector_id": src.id, "source_table": src_t,
            "dest_connector_id": dst.id, "dest_table": dst_t,
            "sync_mode": "incremental_upsert", "primary_key": "id",
            "cursor_column": "updated_at", "cursor_semantics": "modification_timestamp",
        })
        assert _rows(dst_t) == sorted(_ROWS)
        wm = (j1.get("incremental_watermark") or {}).get("after") or j1.get("cursor_value")
        assert wm

        _tamper_destination(dst_t)
        if action == "rerun_mcp":
            ok, staged = h.mcp_call(client, "rerun_job", {"job_id": _jid(j1)})
            assert ok, staged
            resp = client.post("/api/v1/copilot/confirm",
                               json={"ack_id": staged["ack_id"], "reason": "d208"})
            assert resp.status_code == 200, resp.text
            new_id = str(resp.json().get("job_id") or "")
            if not new_id:
                from services.mongodb_service import get_mongodb_service

                jobs = [j for j in get_mongodb_service().list_jobs(limit=50)
                        if j.get("rerun_of") == _jid(j1)]
                new_id = str(jobs[0]["job_id"]) if jobs else ""
        else:
            # A failed attempt of the same route (QA's retry path): the job
            # record is the one the engine wrote, marked failed.
            from services.mongodb_service import get_mongodb_service

            # Terminal statuses are sticky through update_job_status; flip the
            # in-memory record the way a failed attempt would have left it.
            record = get_mongodb_service()._jobs[_jid(j1)]
            record["status"] = "failed"
            record["error"] = "d208 probe: attempt failed after commit"
            resp = client.post(f"/api/v1/connectors/jobs/{_jid(j1)}/retry")
            assert resp.status_code == 200, resp.text
            body = resp.json()
            assert body["retry_decision"].get("read_scope") == "full_source", body
            new_id = str(body["job_id"])
        job = h.wait_job(new_id)
        assert str(job.get("status")) == "completed", (job.get("status"), job.get("error"))

        # Every source row upserted: the edit is undone, the delete restored,
        # the destination-only row kept.
        assert _rows(dst_t) == sorted(_ROWS + [(99, "only in destination")])
        _assert_full_pass_report(job, rows=len(_ROWS), watermark=wm)
        decision = job.get("retry_decision") or {}
        assert decision.get("read_scope") == "full_source", decision
        assert "ignored" in str(decision.get("reason")), decision
        # The saved watermark is neither used nor moved by the full pass.
        from services.sync_cursor import get_watermark, list_cursor_keys

        assert [get_watermark(k) for k in list_cursor_keys()] == [wm]
    finally:
        h.drop_table("postgresql", src_t)
        h.drop_table("postgresql", dst_t)


# --------------------------------------------------------------------------- #
# (b) upsert without a cursor never becomes incremental
# --------------------------------------------------------------------------- #


def _beat(schedule_id: str) -> dict:
    from services import schedule_runner
    from services.schedule_store import get_schedule, update_schedule

    past = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    update_schedule(schedule_id, {"next_run_at": past, "retry_at": None})
    assert schedule_runner._run_due_schedules() == 1
    sched = get_schedule(schedule_id)
    job = h.wait_job(str(sched.running_job_id or sched.last_job_id or ""))
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline and str(get_schedule(schedule_id).running_job_id or ""):
        schedule_runner._finalize_finished_schedule_claims()
        time.sleep(0.2)
    assert str(job.get("status")) == "completed", (job.get("status"), job.get("error"))
    return job


@pytest.mark.parametrize("route", ["start_transfer", "create_schedule"])
def test_upsert_without_a_cursor_is_a_full_source_upsert(client, route):
    token = uuid.uuid4().hex[:8]
    src_t, dst_t = f"d208_s_{token}", f"d208_d_{token}"
    try:
        _create_source(src_t)
        src, dst = _connectors(token)
        args = {
            "source_connector_id": src.id, "source_table": src_t,
            "dest_connector_id": dst.id, "dest_table": dst_t,
            "sync_mode": "upsert", "primary_key": "id",
        }
        ok, plan = h.mcp_call(client, "plan_transfer", dict(args))
        assert ok, plan
        plan_body = plan.get("plan") or plan
        assert plan_body.get("sync_mode") == "upsert", plan_body.get("sync_mode")
        rules = plan_body.get("data_rules") or {}
        assert not rules.get("cursor_column"), rules
        for contract in plan_body.get("stream_contracts") or []:
            assert not contract.get("cursor_field"), contract

        if route == "start_transfer":
            def run() -> dict:
                return h.stage_and_run(client, "start_transfer", args)
        else:
            ok, staged = h.mcp_call(client, "create_schedule", {
                **args, "cadence": "every 15 minutes", "name": f"d208-{token}"})
            assert ok, staged
            resp = client.post("/api/v1/copilot/confirm",
                               json={"ack_id": staged["ack_id"], "reason": "d208"})
            assert resp.status_code == 200, resp.text
            body = resp.json()
            sid = str(body.get("schedule_id") or (body.get("schedule") or {}).get("id") or "")
            from services.schedule_store import get_schedule

            assert get_schedule(sid).sync_mode in ("upsert", "merge"), get_schedule(sid).sync_mode

            def run() -> dict:
                return _beat(sid)

        j1 = run()
        assert _rows(dst_t) == sorted(_ROWS)
        _tamper_destination(dst_t)
        # A source edit that leaves updated_at alone: a watermark would skip it.
        h.execute("postgresql", f"UPDATE \"{src_t}\" SET note = 'src edit ✓' WHERE id = 6")
        j2 = run()
        expected = sorted([r for r in _ROWS if r[0] != 6] + [(6, "src edit ✓"),
                                                             (99, "only in destination")])
        assert _rows(dst_t) == expected
        _assert_full_pass_report(j2, rows=len(_ROWS), watermark=None)
        assert (j1.get("read_scope") or {}).get("scope") == "full_source", j1.get("read_scope")
    finally:
        h.drop_table("postgresql", src_t)
        h.drop_table("postgresql", dst_t)
