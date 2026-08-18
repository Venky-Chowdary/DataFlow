"""The approval inbox over HTTP, and who is allowed to delegate authority.

A parked schedule is only actionable if the control that clears it exists, names
the decider, refuses to cross a workspace boundary, and separates the ordinary
"approve this one run" from the far more powerful "sign for every later run".
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from fastapi.testclient import TestClient

import services.schedule_store as store
from services.schedule_approvals import build_approval_request, open_approval_request
from services.standing_authorization import (
    SCOPE_NET_ADDITIVE_DRIFT,
    SCOPE_SCHEMA_DRIFT,
    binding_from_schedule,
)

ACTOR = "dana.architect@example.com"
REASON = "Nightly finance load is signed off for the current mapping."


@pytest.fixture
def client(tmp_path, monkeypatch) -> TestClient:
    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "schedules_api.json")
    monkeypatch.setattr(store, "_mongo_backend", lambda: None)
    from src.main import app

    return TestClient(app)


@pytest.fixture
def parked(client) -> Any:
    sched = store.create_schedule({
        "name": "Excel to Snowflake",
        "source_connector_id": "src-xlsx",
        "source_table": "revenue",
        "dest_connector_id": "dst-snow",
        "dest_table": "FINANCE.REVENUE",
        "interval": "daily",
        "mappings": [{"source": "amount", "target": "AMOUNT"}],
    })
    request = build_approval_request(
        kind="source_drift",
        code="SOURCE_SCHEMA_DRIFT",
        finding="Source schema drift: column region was renamed",
        corrective_action="Confirm the mapping, then accept the new shape.",
        binding=binding_from_schedule(sched),
        requested_scopes=[SCOPE_NET_ADDITIVE_DRIFT, SCOPE_SCHEMA_DRIFT],
    )
    open_approval_request(sched.id, request)
    return sched, request


def test_every_control_the_refusal_points_at_is_registered():
    from src.routers.schedules_router import router

    paths = {getattr(r, "path", "") for r in router.routes}
    for expected in (
        "/schedules/approvals/open",
        "/schedules/{schedule_id}/approval",
        "/schedules/{schedule_id}/approvals/{approval_id}/approve",
        "/schedules/{schedule_id}/approvals/{approval_id}/reject",
        "/schedules/{schedule_id}/authorization",
    ):
        assert expected in paths, f"{expected} is not registered"


def test_a_parked_schedule_is_visible_without_opening_it(client, parked):
    sched, request = parked
    row = next(
        s for s in client.get("/api/v1/schedules/").json() if s["id"] == sched.id
    )
    assert row["needs_approval"] is True
    assert row["approval_id"] == request["id"]
    assert row["approval_code"] == "SOURCE_SCHEMA_DRIFT"
    assert row["approvable"] is True
    assert row["authorized"] is False

    inbox = client.get("/api/v1/schedules/approvals/open").json()
    assert inbox["count"] == 1
    assert [r["approval"]["id"] for r in inbox["approvals"]] == [request["id"]]
    assert inbox["approvals"][0]["schedule_name"] == "Excel to Snowflake"

    detail = client.get(f"/api/v1/schedules/{sched.id}/approval")
    assert detail.status_code == 200
    assert detail.json()["approval"]["corrective_action"]


def test_approving_names_the_decider_and_re_arms_the_run(client, parked):
    sched, request = parked
    res = client.post(
        f"/api/v1/schedules/{sched.id}/approvals/{request['id']}/approve",
        json={"reason": REASON, "schema_drift": True},
        headers={"X-Actor": ACTOR},
    )
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["approval"]["resolved_by"] == ACTOR
    assert body["approval"]["status"] == "approved"
    assert body["authorization"]["max_uses"] == 1, "approve-once must not be standing"
    assert body["schedule"]["approval_request"]["status"] == "approved"
    row = next(
        s for s in client.get("/api/v1/schedules/").json() if s["id"] == sched.id
    )
    assert row["needs_approval"] is False


def test_a_decision_without_a_recorded_reason_is_rejected(client, parked):
    sched, request = parked
    res = client.post(
        f"/api/v1/schedules/{sched.id}/approvals/{request['id']}/approve",
        json={"reason": "ok"},
        headers={"X-Actor": ACTOR},
    )
    assert res.status_code == 422


def test_an_anonymous_decision_is_refused(client, parked):
    """An audit trail with no name on it is not an audit trail."""
    sched, request = parked
    res = client.post(
        f"/api/v1/schedules/{sched.id}/approvals/{request['id']}/approve",
        json={"reason": REASON, "schema_drift": True},
    )
    assert res.status_code == 400
    reloaded = store.get_schedule(sched.id)
    assert reloaded is not None
    assert reloaded.approval_request["status"] == "open"


def test_a_signed_in_operator_decides_without_naming_themselves_twice(client, parked):
    """The shipped client sends a session, not a hand-typed actor.

    With enforcement off the browser still signs in, so the verified identity is
    the decider — requiring ``X-Actor`` as well made the shipped approval control
    unusable in the default single-operator configuration.
    """
    sched, request = parked
    from src.routers import schedules_router

    class _State:
        user = {"email": "sam.operator@example.com", "role": "admin"}

    class _Req:
        state = _State()
        headers: dict[str, str] = {}

    assert schedules_router._decider(_Req()) == "sam.operator@example.com"
    assert schedules_router._decider(_Req(), authorize=True) == "sam.operator@example.com"
    assert sched.id and request["id"]


def test_a_session_role_still_binds_when_enforcement_is_off(client, monkeypatch):
    """Auth off is not authority on: a viewer session cannot decide."""
    from src.routers import schedules_router
    from src.services import auth_service

    monkeypatch.setattr(auth_service, "auth_required", lambda: False)

    class _State:
        user = {"email": "vic.viewer@example.com", "role": "viewer"}

    class _Req:
        state = _State()
        # Even a hand-typed actor cannot re-open a door the role closed.
        headers = {"X-Actor": ACTOR}

    with pytest.raises(Exception) as excinfo:
        schedules_router._decider(_Req())
    assert "403" in str(excinfo.value) or "denied" in str(excinfo.value).lower()


def test_an_unauthenticated_single_operator_may_still_name_themselves(client, parked):
    sched, request = parked
    res = client.post(
        f"/api/v1/schedules/{sched.id}/approvals/{request['id']}/approve",
        json={"reason": REASON, "schema_drift": True},
        headers={"X-Actor": ACTOR},
    )
    assert res.status_code == 200, res.text
    assert res.json()["approval"]["resolved_by"] == ACTOR


def test_approving_after_the_plan_moved_is_a_conflict_not_a_silent_sign_off(
    client, parked
):
    sched, request = parked
    store.update_schedule(sched.id, {"mappings": [{"source": "amount", "target": "NET"}]})
    res = client.post(
        f"/api/v1/schedules/{sched.id}/approvals/{request['id']}/approve",
        json={"reason": REASON, "schema_drift": True},
        headers={"X-Actor": ACTOR},
    )
    assert res.status_code == 409
    assert "AUTH_BINDING_CHANGED" in res.text


def test_an_unknown_decision_is_not_found(client, parked):
    sched, _request = parked
    res = client.post(
        f"/api/v1/schedules/{sched.id}/approvals/appr-nope/approve",
        json={"reason": REASON},
        headers={"X-Actor": ACTOR},
    )
    assert res.status_code == 404


def test_rejecting_pauses_the_schedule(client, parked):
    sched, request = parked
    res = client.post(
        f"/api/v1/schedules/{sched.id}/approvals/{request['id']}/reject",
        json={"reason": "The upstream change is wrong."},
        headers={"X-Actor": ACTOR},
    )
    assert res.status_code == 200, res.text
    assert res.json()["approval"]["status"] == "rejected"
    assert client.get("/api/v1/schedules/approvals/open").json()["count"] == 0


def test_standing_authority_can_be_granted_then_revoked(client, parked):
    sched, _request = parked
    granted = client.post(
        f"/api/v1/schedules/{sched.id}/authorization",
        json={
            "reason": REASON,
            "scopes": [SCOPE_SCHEMA_DRIFT],
            "schema_drift": True,
            "expires_in_days": 14,
        },
        headers={"X-Actor": ACTOR},
    )
    assert granted.status_code == 200, granted.text
    assert granted.json()["authorization"]["actor"] == ACTOR

    row = next(s for s in client.get("/api/v1/schedules/").json() if s["id"] == sched.id)
    assert row["authorized"] is True

    revoked = client.delete(
        f"/api/v1/schedules/{sched.id}/authorization",
        headers={"X-Actor": ACTOR},
    )
    assert revoked.status_code == 200, revoked.text
    assert revoked.json()["authorization"]["revoked_by"] == ACTOR
    row = next(s for s in client.get("/api/v1/schedules/").json() if s["id"] == sched.id)
    assert row["authorized"] is False


def test_a_scope_nobody_may_delegate_is_refused_at_the_edge(client, parked):
    sched, _request = parked
    res = client.post(
        f"/api/v1/schedules/{sched.id}/authorization",
        json={"reason": REASON, "scopes": ["narrow_type"]},
        headers={"X-Actor": ACTOR},
    )
    assert res.status_code == 422
    reloaded = store.get_schedule(sched.id)
    assert reloaded is not None and not reloaded.standing_authorization


def test_delegating_authority_needs_more_than_permission_to_run_schedules(
    client, parked, monkeypatch
):
    """`schedule.manage` decides one run; minting standing authority is admin work."""
    from services.rbac import Permission, role_permissions
    from src.services import auth_service

    sched, request = parked
    monkeypatch.setattr(auth_service, "auth_required", lambda: True)
    editor = {"email": "eve.editor@example.com", "role": "editor"}

    perms = role_permissions("editor")
    assert Permission.SCHEDULE_MANAGE in perms
    assert Permission.SCHEDULE_AUTHORIZE not in perms, (
        "an editor must not be able to sign for every future run"
    )
    assert Permission.SCHEDULE_AUTHORIZE in role_permissions("admin")

    # And the endpoint enforces exactly that distinction.
    from src.routers import schedules_router

    class _State:
        user = editor

    class _Req:
        state = _State()
        headers: dict[str, str] = {}

    assert schedules_router._decider(_Req()) == editor["email"]
    with pytest.raises(Exception) as excinfo:
        schedules_router._decider(_Req(), authorize=True)
    assert "403" in str(excinfo.value) or "denied" in str(excinfo.value).lower()
    assert sched.id and request["id"]
