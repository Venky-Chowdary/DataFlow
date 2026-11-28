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


# --- review round 1 ---------------------------------------------------------


def test_the_platform_connector_delete_route_follows_the_same_rule(env):
    """``DELETE /connectors/{id}`` removes a connector the scheduler also resolves
    (mongodb_service): it refused nothing before, so it was a way around D432."""
    from services.mongodb_service import get_mongodb_service
    from services.schedule_store import create_schedule, get_schedule

    client = env["client"]
    mongo = get_mongodb_service()
    legacy = mongo.save_connector({"name": "legacy-pg", "type": "postgresql", "host": "127.0.0.1",
                                   "port": 5432, "database": "d432"})
    dst = _connector("d432-dst")
    sched = create_schedule({
        "name": "D432 legacy bound", "source_connector_id": legacy, "source_table": "orders",
        "dest_connector_id": dst.id, "dest_table": "orders_l", "interval": "daily",
        "timezone": "UTC", "sync_mode": "full_refresh_overwrite", "enabled": True,
        "mappings": [{"source": "id", "target": "id"}],
    })

    refused = client.delete(f"/api/v1/connectors/{legacy}")
    assert refused.status_code == 409, refused.text
    assert "D432 legacy bound" in _detail(refused)["message"]
    assert mongo.get_connector(legacy) is not None

    resp = client.delete(f"/api/v1/connectors/{legacy}",
                         params={"bound_schedules": "pause", "acknowledge_schedules": sched.id})
    assert resp.status_code == 200, resp.text
    assert mongo.get_connector(legacy) is None
    assert get_schedule(sched.id).enabled is False
    assert any(e["event"] == "connector.delete.bound_schedules" for e in env["events"])


def test_a_connector_delete_that_fails_leaves_the_schedules_as_they_were(env, monkeypatch):
    import src.routers.saved_connectors_router as router
    from services.schedule_store import get_schedule

    client = env["client"]
    src, dst = _connector("d432-src"), _connector("d432-dst")
    sched = _schedule("D432 nightly A", src, dst)
    monkeypatch.setattr(router, "delete_connector", lambda *a, **k: False)

    resp = client.delete(f"/api/v1/connectors/saved/{src.id}",
                         params={"bound_schedules": "pause", "acknowledge_schedules": sched.id})
    assert resp.status_code == 404, resp.text
    assert get_schedule(sched.id).enabled is True, "a failed delete left the schedule paused"
    audited = [e for e in env["events"] if e["event"] == "connector.delete.bound_schedules"]
    assert audited and audited[-1]["outcome"] == "failed", audited
    assert audited[-1]["details"]["connector_deleted"] is False
    assert audited[-1]["details"]["schedules"][0]["result"] == "restored"


def test_delete_schedules_reports_what_was_actually_deleted(env, monkeypatch):
    import services.schedule_store as store
    from services.connector_store import get_connector

    client = env["client"]
    src, dst = _connector("d432-src"), _connector("d432-dst")
    sched = _schedule("D432 nightly A", src, dst)
    monkeypatch.setattr(store, "delete_schedule", lambda _sid: False)

    resp = client.delete(f"/api/v1/connectors/saved/{src.id}",
                         params={"bound_schedules": "delete_schedules", "acknowledge_schedules": sched.id})
    # Not a success: the schedule outlived its connector. It is paused, never
    # left enabled, and the answer names it.
    assert resp.status_code == 500, resp.text
    assert get_connector(src.id) is None
    detail = resp.json()["detail"]
    assert detail["schedules"][0]["result"] == "not_deleted_paused"
    assert "D432 nightly A" in detail["message"]
    assert store.get_schedule(sched.id).enabled is False
    audited = [e for e in env["events"] if e["event"] == "connector.delete.bound_schedules"]
    assert audited[-1]["outcome"] == "failed", audited


def test_repoint_or_delete_schedules_needs_schedule_manage(env, monkeypatch):
    import services.connector_delete_guard as guard
    from services.connector_store import get_connector
    from services.schedule_store import get_schedule

    client = env["client"]
    src, dst, spare = _connector("d432-src"), _connector("d432-dst"), _connector("d432-spare")
    sched = _schedule("D432 nightly A", src, dst)
    monkeypatch.setattr(guard, "may_manage_schedules", lambda request: False)
    for params in ({"bound_schedules": "repoint", "repoint_to": spare.id},
                   {"bound_schedules": "delete_schedules"}):
        resp = client.delete(f"/api/v1/connectors/saved/{src.id}",
                             params={**params, "acknowledge_schedules": sched.id})
        assert resp.status_code == 403, resp.text
        assert "schedule.manage" in resp.text
    assert get_connector(src.id) is not None and get_schedule(sched.id).source_connector_id == src.id
    # Pausing is the safe choice: it does not need schedule.manage.
    ok = client.delete(f"/api/v1/connectors/saved/{src.id}",
                       params={"bound_schedules": "pause", "acknowledge_schedules": sched.id})
    assert ok.status_code == 200, ok.text


def test_a_key_scoped_without_schedule_manage_may_not_change_schedules(monkeypatch):
    from types import SimpleNamespace

    import services.connector_delete_guard as guard
    from src.services import auth_service

    monkeypatch.setattr(auth_service, "auth_required", lambda: True)
    scoped = SimpleNamespace(state=SimpleNamespace(
        user={"role": "admin", "scopes": ["connector.read", "connector.delete"]}, effective_role="admin"))
    full = SimpleNamespace(state=SimpleNamespace(user={"role": "admin"}, effective_role="admin"))
    assert guard.may_manage_schedules(scoped) is False
    assert guard.may_manage_schedules(full) is True


def test_the_refusal_reads_as_plain_sentences_with_a_fitting_next_step(env):
    client = env["client"]
    src, dst = _connector("d432-src"), _connector("d432-dst")
    _schedule("D432 nightly A", src, dst)
    body = client.delete(f"/api/v1/connectors/saved/{src.id}").json()
    action = body["error"]["action"]["label"]
    assert "don't need changing" not in action and "pause" in action.lower(), action
    message = body["detail"]["message"]
    assert "pause: pause" not in message and " — " not in message, message
    assert "pause them" in message and "re-point them" in message, message


def test_a_connector_only_the_platform_store_holds_is_not_shown_deleted(env):
    from services.mongodb_service import get_mongodb_service
    from services.schedule_store import create_schedule

    client = env["client"]
    legacy = get_mongodb_service().save_connector({"name": "legacy-pg", "type": "postgresql"})
    dst = _connector("d432-dst")
    sched = create_schedule({
        "name": "D432 legacy only", "source_connector_id": legacy, "source_table": "orders",
        "dest_connector_id": dst.id, "dest_table": "orders_l", "interval": "daily",
        "timezone": "UTC", "sync_mode": "full_refresh_overwrite", "enabled": True,
        "mappings": [{"source": "id", "target": "id"}],
    })
    row = next(r for r in client.get("/api/v1/schedules/").json() if r["id"] == sched.id)
    assert row["connection_deleted"] == [], row
    assert client.get(f"/api/v1/schedules/{sched.id}").json()["connection_deleted"] == []


# --- review round 2: an honest rollback ---------------------------------------


def _ago(minutes: float) -> str:
    from datetime import datetime, timedelta, timezone

    return (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()


def _fail_connector_delete(monkeypatch) -> None:
    import src.routers.saved_connectors_router as router

    monkeypatch.setattr(router, "delete_connector", lambda *a, **k: False)


def _pause_delete(client, src, *scheds):
    return client.delete(f"/api/v1/connectors/saved/{src.id}", params={
        "bound_schedules": "pause", "acknowledge_schedules": ",".join(s.id for s in scheds)})


def _audits(env) -> list[dict]:
    return [e for e in env["events"] if e["event"] == "connector.delete.bound_schedules"]


def test_a_failed_delete_restores_a_pending_retry(env, monkeypatch):
    from services.schedule_store import get_schedule, update_schedule

    src, dst = _connector("d432-src"), _connector("d432-dst")
    sched = _schedule("D432 retrying", src, dst, max_retries=2)
    update_schedule(sched.id, {"retry_at": _ago(1), "retry_attempt": 1})
    before = get_schedule(sched.id)
    _fail_connector_delete(monkeypatch)

    assert _pause_delete(env["client"], src, sched).status_code == 404
    after = get_schedule(sched.id)
    assert after.enabled is True
    assert (after.retry_at, after.retry_attempt) == (before.retry_at, before.retry_attempt), after
    assert after.next_run_at == before.next_run_at


def test_a_failed_delete_keeps_an_approved_rerun_due_now(env, monkeypatch):
    """Re-enabling re-armed the slot: an approved re-run moved to the next window
    (a year away on a yearly cron)."""
    from services.schedule_store import get_schedule, update_schedule

    src, dst = _connector("d432-src"), _connector("d432-dst")
    sched = _schedule("D432 approved", src, dst, cron="0 3 1 1 *", interval="cron")
    update_schedule(sched.id, {"next_run_at": _ago(1), "next_run_reason": "approval"})
    before = get_schedule(sched.id)
    _fail_connector_delete(monkeypatch)

    assert _pause_delete(env["client"], src, sched).status_code == 404
    after = get_schedule(sched.id)
    assert (after.next_run_at, after.next_run_reason) == (before.next_run_at, "approval"), after


def test_the_undo_leaves_a_schedule_someone_changed_meanwhile(env, monkeypatch):
    import src.routers.saved_connectors_router as router
    from services.schedule_store import get_schedule, update_schedule

    src, dst = _connector("d432-src"), _connector("d432-dst")
    sched = _schedule("D432 contested", src, dst)

    def _operator_edits_then_delete_fails(*a, **k):
        # Between the pause and the undo, an operator edits the paused schedule.
        update_schedule(sched.id, {"notify_on_success": True})
        return False

    monkeypatch.setattr(router, "delete_connector", _operator_edits_then_delete_fails)
    resp = _pause_delete(env["client"], src, sched)
    assert resp.status_code == 404, resp.text
    assert "left as it is now" in resp.json()["detail"]["message"]
    after = get_schedule(sched.id)
    assert after.enabled is False and after.notify_on_success is True
    assert _audits(env)[-1]["details"]["schedules"][0]["result"] == "changed_since_left_as_is"


def test_an_undo_that_cannot_write_is_audited_and_named(env, monkeypatch):
    import services.schedule_store as store

    src, dst = _connector("d432-src"), _connector("d432-dst")
    sched = _schedule("D432 stuck paused", src, dst)
    _fail_connector_delete(monkeypatch)

    def _down(*a, **k):
        raise RuntimeError("store down")

    monkeypatch.setattr(store, "restore_if_unchanged", _down)
    resp = _pause_delete(env["client"], src, sched)
    assert resp.status_code == 500, resp.text
    assert "D432 stuck paused" in resp.json()["detail"]["message"]
    assert "store down" not in resp.text
    audited = _audits(env)
    assert audited and audited[-1]["outcome"] == "failed"
    assert audited[-1]["details"]["schedules"][0]["result"] == "paused_not_restored"


def test_a_pause_that_fails_midway_undoes_the_first_and_deletes_nothing(env, monkeypatch):
    import services.schedule_store as store
    from services.connector_store import get_connector

    src, dst = _connector("d432-src"), _connector("d432-dst")
    first = _schedule("D432 nightly A", src, dst)
    second = _schedule("D432 nightly B", src, dst)
    real = store.update_schedule

    def _second_fails(schedule_id, patch, *a, **k):
        if schedule_id == second.id and patch.get("enabled") is False:
            raise RuntimeError("store down")
        return real(schedule_id, patch, *a, **k)

    monkeypatch.setattr(store, "update_schedule", _second_fails)
    resp = _pause_delete(env["client"], src, first, second)
    assert resp.status_code == 500, resp.text
    assert get_connector(src.id) is not None
    assert store.get_schedule(first.id).enabled is True and store.get_schedule(second.id).enabled is True
    assert "D432 nightly B" in resp.json()["detail"]["message"]
    results = {r["name"]: r["result"] for r in _audits(env)[-1]["details"]["schedules"]}
    assert results == {"D432 nightly A": "restored", "D432 nightly B": "change_failed"}, results


def test_a_schedule_delete_that_raises_never_leaves_an_enabled_orphan(env, monkeypatch):
    import services.schedule_store as store
    from services.connector_store import get_connector

    src, dst = _connector("d432-src"), _connector("d432-dst")
    first = _schedule("D432 nightly A", src, dst)
    second = _schedule("D432 nightly B", src, dst)
    real = store.delete_schedule

    def _second_raises(schedule_id, *a, **k):
        if schedule_id == second.id:
            raise RuntimeError("store down")
        return real(schedule_id, *a, **k)

    monkeypatch.setattr(store, "delete_schedule", _second_raises)
    resp = env["client"].delete(f"/api/v1/connectors/saved/{src.id}", params={
        "bound_schedules": "delete_schedules", "acknowledge_schedules": f"{first.id},{second.id}"})
    assert resp.status_code == 500, resp.text
    assert get_connector(src.id) is None and store.get_schedule(first.id) is None
    survivor = store.get_schedule(second.id)
    assert survivor is not None and survivor.enabled is False, "an enabled schedule outlived its connector"
    assert "D432 nightly B" in resp.json()["detail"]["message"]
    audited = _audits(env)[-1]
    assert audited["outcome"] == "failed" and audited["details"]["connector_deleted"] is True
    results = {r["name"]: r["result"] for r in audited["details"]["schedules"]}
    assert results == {"D432 nightly A": "deleted", "D432 nightly B": "not_deleted_paused"}, results


def test_the_platform_route_guards_a_schedule_bound_by_either_id(env):
    """A legacy document has an ObjectId ``_id`` and a UUID ``id``; deleted by
    one, a schedule holding the other was not guarded."""
    from services.mongodb_service import get_mongodb_service
    from services.schedule_store import create_schedule

    client = env["client"]
    mongo = get_mongodb_service()
    object_id = mongo.save_connector({"name": "legacy-pg", "type": "postgresql"})
    mongo._connectors[object_id]["id"] = "uuid-legacy-pg"
    dst = _connector("d432-dst")
    create_schedule({
        "name": "D432 bound by uuid", "source_connector_id": "uuid-legacy-pg", "source_table": "orders",
        "dest_connector_id": dst.id, "dest_table": "orders_u", "interval": "daily",
        "timezone": "UTC", "sync_mode": "full_refresh_overwrite", "enabled": True,
        "mappings": [{"source": "id", "target": "id"}],
    })
    refused = client.delete(f"/api/v1/connectors/{object_id}")
    assert refused.status_code == 409, refused.text
    assert "D432 bound by uuid" in _detail(refused)["message"]


def test_a_connector_of_another_workspace_shows_as_the_runner_sees_it(env):
    from services.connector_store import create_connector
    from services.schedule_store import create_schedule

    client = env["client"]
    foreign = create_connector({"name": "other-ws", "type": "postgresql", "role": "both",
                                "host": "127.0.0.1", "port": 5432, "database": "x",
                                "username": "x", "password": "not-a-real-secret", "workspace_id": "ws-other"})
    dst = _connector("d432-dst")
    sched = create_schedule({
        "name": "D432 cross workspace", "source_connector_id": foreign.id, "source_table": "orders",
        "dest_connector_id": dst.id, "dest_table": "orders_x", "interval": "daily", "workspace_id": "ws-mine",
        "timezone": "UTC", "sync_mode": "full_refresh_overwrite", "enabled": True,
        "mappings": [{"source": "id", "target": "id"}],
    })
    from services.schedule_runner import _resolve_connector_for_schedule

    assert _resolve_connector_for_schedule(foreign.id, sched) is None  # the runner refuses it
    row = next(r for r in client.get("/api/v1/schedules/").json() if r["id"] == sched.id)
    assert row["connection_deleted"] == ["source"], row
    assert client.get(f"/api/v1/schedules/{sched.id}").json()["connection_deleted"] == ["source"]
