"""QA batch 1 (item 6): a schedule created on the Schedules page could never run.

The page saved cadence and endpoints only. Run now then refused ("no persisted
mappings") and created no job; the only way out was a full transfer in
Transfer Studio and Schedule from its footer.

Now the page asks the API to plan the route (``plan_mappings: true``): the
same ``plan_transfer`` the Pilot's ``create_schedule`` and Studio run — live
schemas, mapping pipeline, Validate — and the schedule is saved armed with
those mappings only when the plan clears Execute (422, unsaved, otherwise).
A schedule already saved without mappings gets ``POST /{id}/plan-mappings``,
and Run now on it says what to do (409) instead of failing obscurely.

Live half: local PostgreSQL -> PostgreSQL through the REST routes.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest

from tests import r4_route_harness as h


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_TRAINING", "off")
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _connectors() -> tuple[str, str]:
    from tests.test_r14_mysql_latin1_cp1252_strict_e2e import _connector, _pg_cfg

    cfg = _pg_cfg()
    return _connector("postgresql", cfg, "source"), _connector("postgresql", cfg, "destination")


def _body(src: str, dst: str, table: str, **extra: Any) -> dict[str, Any]:
    return {
        "name": f"qab1-{uuid.uuid4().hex[:6]}", "source_connector_id": src,
        "source_table": table, "dest_connector_id": dst, "dest_table": f"{table}_d",
        "interval": "daily", "sync_mode": "full_refresh_overwrite", **extra,
    }


def _source() -> str:
    t = f"qab1s_{uuid.uuid4().hex[:8]}"
    h.execute("postgresql", f'CREATE TABLE "{t}" (id int PRIMARY KEY, name text, amt numeric(8,2))')
    h.execute("postgresql", f"INSERT INTO \"{t}\" VALUES (1, 'a', 1.50), (2, 'b', 2.00)")
    return t


def test_run_now_without_mappings_says_what_to_do_and_starts_nothing(client):
    src, dst = _connectors()
    t = _source()
    try:
        r = client.post("/api/v1/schedules/", json=_body(src, dst, t, enabled=False))
        assert r.status_code == 201, r.text
        sched = r.json()
        assert not sched.get("mappings")
        run = client.post(f"/api/v1/schedules/{sched['id']}/run")
        assert run.status_code == 409, run.text
        detail = str(run.json())
        assert "no column mappings" in detail and "Plan mappings" in detail
        assert "Transfer Studio" in detail and "no job was created" in detail
    finally:
        h.drop_table("postgresql", t)


def test_schedules_page_create_plans_validates_and_runs(client):
    src, dst = _connectors()
    t = _source()
    try:
        r = client.post("/api/v1/schedules/", json=_body(src, dst, t, plan_mappings=True))
        assert r.status_code == 201, r.text
        sched = r.json()
        targets = sorted(str(m.get("target")) for m in sched.get("mappings") or [])
        assert targets == ["amt", "id", "name"], sched.get("mappings")
        assert sched.get("enabled") is True, sched
        run = client.post(f"/api/v1/schedules/{sched['id']}/run")
        assert run.status_code == 200, run.text
        job = h.wait_job(run.json()["job_id"])
        assert job.get("status") == "completed", job
        assert h.query("postgresql", f'SELECT COUNT(*) FROM "{t}_d"')[0][0] == 2
    finally:
        h.drop_table("postgresql", t)
        h.drop_table("postgresql", f"{t}_d")


def test_plan_mappings_plans_an_existing_draft_and_leaves_it_paused(client):
    src, dst = _connectors()
    t = _source()
    try:
        r = client.post("/api/v1/schedules/", json=_body(src, dst, t, enabled=False))
        sched_id = r.json()["id"]
        planned = client.post(f"/api/v1/schedules/{sched_id}/plan-mappings")
        assert planned.status_code == 200, planned.text
        body = planned.json()
        # Planning does not change whether it runs on its cadence (review Q5).
        assert body.get("enabled") is False and len(body.get("mappings") or []) == 3, body
        again = client.post(f"/api/v1/schedules/{sched_id}/plan-mappings")
        assert again.status_code == 409
    finally:
        h.drop_table("postgresql", t)


def test_a_plan_that_would_not_run_is_refused_and_nothing_is_saved(client, monkeypatch):
    from services import schedule_planning as sp

    def refuse(payload):
        raise sp.SchedulePlanRefused(
            "Validate is blocked-grade for this route, so the schedule was not saved",
            blockers=[{"id": "g4", "message": "low confidence"}],
        )

    monkeypatch.setattr(sp, "plan_schedule_route", refuse)
    src, dst = _connectors()
    before = client.get("/api/v1/schedules/").json()
    r = client.post("/api/v1/schedules/", json=_body(src, dst, "t", plan_mappings=True))
    assert r.status_code == 422, r.text
    assert "not saved" in str(r.json())
    after = client.get("/api/v1/schedules/").json()
    assert len(after if isinstance(after, list) else after.get("schedules", [])) == len(
        before if isinstance(before, list) else before.get("schedules", []))


def test_plan_refusal_names_the_blockers():
    from services import schedule_planning as sp

    class Planned:
        success = True
        error = ""
        output = {"preflight": {"passed": False, "blockers": [
            {"id": "g9_cursor", "message": "no cursor", "details": {"recommended_fix": "pick updated_at"}},
        ]}}

    import src.ai.copilot.transfer_tools as tt

    original = tt.plan_transfer
    tt.plan_transfer = lambda **kw: Planned()
    try:
        with pytest.raises(sp.SchedulePlanRefused) as err:
            sp.plan_schedule_route({"source_connector_id": "s", "dest_connector_id": "d"})
    finally:
        tt.plan_transfer = original
    assert "g9_cursor: no cursor" in str(err.value) and "pick updated_at" in str(err.value)
    assert err.value.blockers and err.value.blockers[0]["id"] == "g9_cursor"
