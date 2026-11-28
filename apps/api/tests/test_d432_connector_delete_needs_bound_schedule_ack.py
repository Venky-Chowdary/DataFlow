"""D432: deleting a connector that schedules use needs an audited acknowledgement — REST, Studio and MCP alike.

QA (PostgreSQL -> PostgreSQL overwrite schedule, rounds 70-75): the MCP
``delete_connector`` preview listed the bound schedule and asked to confirm,
while ``DELETE /connectors/saved/{id}`` on the same connector answered 200
``{"ok": true}``. The schedule stayed listed, looking healthy, still pointing
at the deleted id, and failed only at its next run.

One rule now (``services/connector_delete_guard.py``), called by the REST
route, the MCP stage and its Confirm (which calls the REST route), and the
Studio delete (which calls the REST route).
"""

from __future__ import annotations

import pytest

from tests import r4_route_harness as h


@pytest.fixture
def env(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    import services.audit_coverage as audit
    import services.connector_delete_guard as guard

    events: list[dict] = []
    real = audit.record_security_event

    def _capture(event, **kw):
        events.append({"event": event, **kw})
        return real(event, **kw)

    monkeypatch.setattr(audit, "record_security_event", _capture)
    monkeypatch.setattr(guard, "_claim_live", lambda schedule_id: False)
    from fastapi.testclient import TestClient
    from src.main import app

    return {"client": TestClient(app), "events": events}


def _connector(name: str, kind: str = "postgresql"):
    from services.connector_store import create_connector

    return create_connector({
        "name": name, "type": kind, "role": "both", "ssl": False, "host": "127.0.0.1",
        "port": 5432 if kind == "postgresql" else 3306, "database": "d432",
        "username": "d432", "password": "not-a-real-secret",
    })


def _schedule(name: str, src, dst, **extra):
    from services.schedule_store import create_schedule

    return create_schedule({
        "name": name, "source_connector_id": src.id, "source_table": "orders",
        "dest_connector_id": dst.id, "dest_table": f"orders_{name[-1]}",
        "interval": "daily", "timezone": "UTC", "sync_mode": "full_refresh_overwrite",
        "enabled": True, "mappings": [{"source": "id", "target": "id"}], **extra,
    })


def _detail(resp) -> dict:
    body = resp.json()
    detail = body.get("detail")
    return detail if isinstance(detail, dict) else body


def test_rest_delete_of_a_bound_connector_is_refused_naming_the_schedules(env):
    from services.connector_store import get_connector
    from services.schedule_store import get_schedule

    client = env["client"]
    src, dst = _connector("d432-src"), _connector("d432-dst")
    s1 = _schedule("D432 nightly A", src, dst)
    s2 = _schedule("D432 nightly B", dst, src)  # the same connector as destination

    resp = client.delete(f"/api/v1/connectors/saved/{src.id}")
    assert resp.status_code == 409, resp.text
    detail = _detail(resp)
    assert detail["code"] == "connector_in_use", detail
    assert {r["id"] for r in detail["bound_schedules"]} == {s1.id, s2.id}
    assert {r["role"] for r in detail["bound_schedules"]} == {"source", "destination"}
    assert [o["action"] for o in detail["options"]] == ["pause", "repoint", "delete_schedules"]
    assert "D432 nightly A" in detail["message"] and "D432 nightly B" in detail["message"]
    # Nothing changed.
    assert get_connector(src.id) is not None
    assert get_schedule(s1.id).enabled and get_schedule(s2.id).enabled


def test_an_acknowledgement_that_misses_a_schedule_is_refused_again(env):
    client = env["client"]
    src, dst = _connector("d432-src"), _connector("d432-dst")
    s1 = _schedule("D432 nightly A", src, dst)
    s2 = _schedule("D432 nightly B", src, dst)

    resp = client.delete(
        f"/api/v1/connectors/saved/{src.id}",
        params={"bound_schedules": "pause", "acknowledge_schedules": s1.id},
    )
    assert resp.status_code == 409, resp.text
    assert "D432 nightly B" in _detail(resp)["message"]
    assert s2.id in {r["id"] for r in _detail(resp)["bound_schedules"]}


def test_pause_deletes_the_connector_pauses_the_schedules_and_audits_it(env):
    from services.connector_store import get_connector
    from services.schedule_store import get_schedule

    client = env["client"]
    src, dst = _connector("d432-src"), _connector("d432-dst")
    sched = _schedule("D432 nightly A", src, dst)

    resp = client.delete(
        f"/api/v1/connectors/saved/{src.id}",
        params={"bound_schedules": "pause", "acknowledge_schedules": sched.id},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["bound_schedules"]["schedules"][0]["result"] == "paused"
    assert get_connector(src.id) is None
    assert get_schedule(sched.id).enabled is False
    audited = [e for e in env["events"] if e["event"] == "connector.delete.bound_schedules"]
    assert audited and audited[0]["details"]["action"] == "pause", env["events"]
    assert audited[0]["details"]["schedules"][0]["id"] == sched.id

    # AC-4: listed and opened, the orphan says "connection deleted".
    one = client.get(f"/api/v1/schedules/{sched.id}").json()
    assert one["connection_deleted"] == ["source"], one
    assert one["stalled_reason"].startswith("Connection deleted"), one
    row = next(r for r in client.get("/api/v1/schedules/").json() if r["id"] == sched.id)
    assert row["connection_deleted"] == ["source"], row


def test_repoint_moves_the_schedules_to_a_connector_of_the_same_type(env):
    from services.connector_store import get_connector
    from services.schedule_store import get_schedule

    client = env["client"]
    src, dst, spare = _connector("d432-src"), _connector("d432-dst"), _connector("d432-spare")
    other = _connector("d432-mysql", kind="mysql")
    sched = _schedule("D432 nightly A", src, dst)

    wrong = client.delete(
        f"/api/v1/connectors/saved/{src.id}",
        params={"bound_schedules": "repoint", "acknowledge_schedules": sched.id, "repoint_to": other.id},
    )
    assert wrong.status_code == 422, wrong.text
    assert get_connector(src.id) is not None

    resp = client.delete(
        f"/api/v1/connectors/saved/{src.id}",
        params={"bound_schedules": "repoint", "acknowledge_schedules": sched.id, "repoint_to": spare.id},
    )
    assert resp.status_code == 200, resp.text
    assert get_schedule(sched.id).source_connector_id == spare.id
    assert get_connector(src.id) is None


def test_delete_schedules_removes_them_with_the_connector(env):
    from services.schedule_store import get_schedule

    client = env["client"]
    src, dst = _connector("d432-src"), _connector("d432-dst")
    sched = _schedule("D432 nightly A", src, dst)
    resp = client.delete(
        f"/api/v1/connectors/saved/{src.id}",
        params={"bound_schedules": "delete_schedules", "acknowledge_schedules": sched.id},
    )
    assert resp.status_code == 200, resp.text
    assert get_schedule(sched.id) is None


def test_a_connector_no_schedule_uses_deletes_without_a_prompt(env):
    client = env["client"]
    lone = _connector("d432-lone")
    resp = client.delete(f"/api/v1/connectors/saved/{lone.id}")
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"ok": True}


def test_a_bound_schedule_with_a_run_in_flight_refuses_the_delete(env, monkeypatch):
    import services.connector_delete_guard as guard
    from services.connector_store import get_connector
    from services.schedule_store import update_schedule

    client = env["client"]
    src, dst = _connector("d432-src"), _connector("d432-dst")
    sched = _schedule("D432 nightly A", src, dst)
    update_schedule(sched.id, {"running": True, "running_job_id": "job-live"})
    monkeypatch.setattr(guard, "_claim_live", lambda schedule_id: True)
    resp = client.delete(
        f"/api/v1/connectors/saved/{src.id}",
        params={"bound_schedules": "pause", "acknowledge_schedules": sched.id},
    )
    assert resp.status_code == 409, resp.text
    assert "in progress" in _detail(resp)["message"]
    assert get_connector(src.id) is not None


def test_mcp_applies_the_same_rule_and_its_confirm_carries_the_acknowledgement(env):
    from services.connector_store import get_connector
    from services.schedule_store import get_schedule

    client = env["client"]
    src, dst = _connector("d432-src"), _connector("d432-dst")
    sched = _schedule("D432 nightly A", src, dst)

    ok, refused = h.mcp_call(client, "delete_connector", {"connector_id": src.id})
    assert not ok and "D432 nightly A" in str(refused), refused

    ok, staged = h.mcp_call(client, "delete_connector", {"connector_id": src.id, "bound_schedules": "pause"})
    assert ok and staged.get("ack_id"), staged
    assert staged["preview"]["bound_schedules"] == ["D432 nightly A"]
    assert staged["preview"]["bound_schedules_action"] == "pause"

    resp = client.post("/api/v1/copilot/confirm", json={"ack_id": staged["ack_id"], "reason": "retire d432"})
    assert resp.status_code == 200, resp.text
    assert get_connector(src.id) is None
    assert get_schedule(sched.id).enabled is False
    assert any(e["event"] == "connector.delete.bound_schedules" for e in env["events"])


def test_mcp_confirm_is_refused_when_a_schedule_was_bound_after_the_preview(env):
    from services.connector_store import get_connector

    client = env["client"]
    src, dst = _connector("d432-src"), _connector("d432-dst")
    _schedule("D432 nightly A", src, dst)
    ok, staged = h.mcp_call(client, "delete_connector", {"connector_id": src.id, "bound_schedules": "pause"})
    assert ok, staged
    _schedule("D432 nightly B", src, dst)  # bound after the operator looked

    resp = client.post("/api/v1/copilot/confirm", json={"ack_id": staged["ack_id"], "reason": "retire d432"})
    assert resp.status_code == 409, resp.text
    assert "D432 nightly B" in resp.text
    assert get_connector(src.id) is not None
