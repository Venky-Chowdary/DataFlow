"""Q5 (train review): POST /schedules/{id}/plan-mappings.

* It planned against the destination connector's default namespace while the
  schedule's runs write to ``dest_schema`` / ``dest_database``
  (schedule_runner._apply_schedule_dest_location): the plan now receives them.
* It forced ``enabled: True`` and skipped what a PATCH of the same fields
  applies: the reversibility block, the upsert-key check and the
  ``schedule.definition.update`` security event.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi import HTTPException

from tests.test_qa_b1_schedules_page_plans_mappings_e2e import (  # noqa: F401
    _body,
    _connectors,
    _source,
    client,
)
from tests import r4_route_harness as h

_PLANNED = {
    "mappings": [
        {"source": "id", "target": "id", "source_type": "integer", "target_type": "integer",
         "confidence": 0.99},
        {"source": "name", "target": "name", "source_type": "text", "target_type": "text",
         "confidence": 0.99},
        {"source": "amt", "target": "amt", "source_type": "numeric(8,2)",
         "target_type": "numeric(8,2)", "confidence": 0.99},
    ],
    "sync_mode": "full_refresh_overwrite",
}


def test_the_plan_is_made_for_the_schedules_own_destination_namespace(monkeypatch):
    from src.ai.copilot import transfer_tools as tt
    from services.schedule_planning import SchedulePlanRefused, plan_schedule_route

    seen: dict[str, Any] = {}

    def spy(**kwargs):
        seen.update(kwargs)
        return tt._tool_result("plan_transfer", success=False, error="stop here")

    monkeypatch.setattr(tt, "plan_transfer", spy)
    with pytest.raises(SchedulePlanRefused):
        plan_schedule_route({"source_connector_id": "s", "dest_connector_id": "d",
                             "source_table": "t", "dest_table": "t_d",
                             "dest_schema": "analytics", "dest_database": "dw"})
    assert seen["dest_schema"] == "analytics" and seen["dest_database"] == "dw"


@pytest.mark.parametrize("kind, schema, expected", [
    ("snowflake", "dst", "DST"),   # the runner's folding: Snowflake upper-cases
    ("postgresql", "analytics", "analytics"),
    ("mysql", "ignored", None),    # no schema layer: the connector keeps none
])
def test_dest_location_is_folded_like_the_runner(kind, schema, expected):
    from src.ai.copilot.transfer_tools import _with_dest_location

    out = _with_dest_location({"type": kind, "schema": "default_s", "database": "db0"}, schema, "db1")
    assert out["database"] == "db1"
    assert out.get("schema") == (expected if expected is not None else "default_s")
    assert _with_dest_location({"type": kind, "schema": "s0"}, "", "") == {"type": kind, "schema": "s0"}


def _draft(client) -> tuple[str, str]:
    src, dst = _connectors()
    t = _source()
    r = client.post("/api/v1/schedules/", json=_body(src, dst, t, enabled=False))
    assert r.status_code == 201, r.text
    return r.json()["id"], t


def test_plan_mappings_runs_the_patch_checks_and_records_the_update(client, monkeypatch):
    from services import schedule_planning as sp
    import src.routers.schedules_router as router

    monkeypatch.setattr(sp, "plan_schedule_route", lambda payload: dict(_PLANNED))
    events: list[tuple[str, dict]] = []
    monkeypatch.setattr(router, "record_security_event",
                        lambda name, **kw: events.append((name, kw.get("details") or {})))
    sched_id, t = _draft(client)
    try:
        # The reversibility block refuses, and nothing is saved.
        monkeypatch.setattr("services.reversibility_schedule.schedule_reversibility_block",
                            lambda merged: "hash_pii cannot be undone")
        refused = client.post(f"/api/v1/schedules/{sched_id}/plan-mappings")
        assert refused.status_code == 422 and "cannot be undone" in refused.text
        assert not client.get(f"/api/v1/schedules/{sched_id}").json().get("mappings")
        monkeypatch.setattr("services.reversibility_schedule.schedule_reversibility_block",
                            lambda merged: "")
        # The upsert-key check refuses too.
        def no_key(plan):
            raise HTTPException(status_code=422, detail="sync_mode upsert needs a key")

        monkeypatch.setattr(router, "_assert_upsert_key", no_key)
        refused = client.post(f"/api/v1/schedules/{sched_id}/plan-mappings")
        assert refused.status_code == 422 and "needs a key" in refused.text
        monkeypatch.setattr(router, "_assert_upsert_key", lambda plan: None)
        # Planned: saved, recorded as a definition update, still paused.
        ok = client.post(f"/api/v1/schedules/{sched_id}/plan-mappings")
        assert ok.status_code == 200, ok.text
        assert ok.json().get("enabled") is False
        assert [n for n, _ in events] == ["schedule.definition.update"]
        assert events[0][1]["id"] == sched_id and events[0][1]["via"] == "plan-mappings"
    finally:
        h.drop_table("postgresql", t)
