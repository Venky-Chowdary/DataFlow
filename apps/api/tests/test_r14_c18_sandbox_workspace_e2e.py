"""C18 (QA r14): an isolated throwaway workspace for security retests.

QA could not retest MFA, session timeout or the IP allowlist without a
platform administrator creating a workspace, and refused to risk the real
owner. ``POST /api/v1/team/sandbox-workspaces`` (platform admins only) creates
an expiring workspace whose only admin is a named (here synthetic) tester; the
platform admin is not a member, so a policy the tester turns on locks out no
one else. Through the real routes on the live MongoDB.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from r8_ent_harness import ent  # noqa: F401
from r14_sandbox_world import confirm, provision, sandbox  # noqa: F401

INSIDE, OUTSIDE = "203.0.113.50", "198.51.100.9"


@pytest.fixture
def one_proxy(monkeypatch):
    monkeypatch.setenv("DATAWRAP_TRUSTED_PROXY_COUNT", "1")
    for name in ("DATAWRAP_TENANT_IP_ALLOWLIST_ENFORCE", "DATAFLOW_TENANT_IP_ALLOWLIST_ENFORCE"):
        monkeypatch.delenv(name, raising=False)


def test_sandbox_is_isolated_and_the_tester_is_its_only_admin(ent, sandbox):
    from services import team_store

    members = team_store.list_workspace_members(sandbox.workspace_id)
    assert [(m["email"], m["role"]) for m in members] == [(sandbox.tester_email, "admin")]
    ws = sandbox.created["workspace"]
    assert ws["name"].startswith("[sandbox] ") and ws["sandbox_expires_at"]
    # The tester is a member of nothing else.
    assert [m["workspace_id"] for m in team_store.list_memberships_for_user(sandbox.tester_email)] == [
        sandbox.workspace_id
    ]
    listed = ent.admin.get("/api/v1/team/sandbox-workspaces").json()["workspaces"]
    assert [w["id"] for w in listed] == [sandbox.workspace_id]
    created = [e for e in ent.audit(sandbox.workspace_id, action="team.sandbox.create")]
    assert created and created[-1]["details"]["tester"] == sandbox.tester_email


def test_only_platform_admins_create_list_or_delete_sandboxes(ent, sandbox):
    ws = ent.workspace("Real workspace")
    admin = ent.member(email="ws-admin-c18@example.com", workspace_id=ws, role="admin")
    body = {"tester_email": "x-c18@example.test", "reason": "try", "create_account": True}
    for client in (admin, sandbox.tester):
        assert client.post("/api/v1/team/sandbox-workspaces", json=body).status_code == 403
        assert client.get("/api/v1/team/sandbox-workspaces").status_code == 403
        assert client.delete(f"/api/v1/team/sandbox-workspaces/{sandbox.workspace_id}").status_code == 403
    # A workspace that is not a sandbox is never deleted by this route.
    assert ent.admin.delete(f"/api/v1/team/sandbox-workspaces/{ws}", params=confirm(ws)).status_code == 404
    from services import team_store

    assert team_store.get_workspace(ws) is not None
    # A reason is required; the tester must be another account.
    assert ent.admin.post("/api/v1/team/sandbox-workspaces",
                          json={"tester_email": "y-c18@example.test", "reason": "  "}).status_code == 422
    # An existing login is never a sandbox tester (its other workspaces would
    # fall under the sandbox's IP allowlist, and teardown deletes the login).
    existing = ent.admin.post("/api/v1/team/sandbox-workspaces",
                              json={"tester_email": "ws-admin-c18@example.com", "reason": "r"})
    assert existing.status_code == 400, existing.text
    assert "already has a login" in existing.json()["detail"]


def test_ip_allowlist_and_session_policy_retest_inside_the_sandbox(ent, sandbox, one_proxy):
    """The S6–S8 retest QA was blocked on, run by the tester in the sandbox."""
    t = sandbox.tester
    created = t.post(
        "/api/v1/workspace/tenant",
        json={"workspace_id": sandbox.workspace_id, "name": "sandbox", "ip_allowlist": ["203.0.113.0/24"],
              "session_timeout_hours": 1},
        headers={"X-Forwarded-For": INSIDE},
    )
    assert created.status_code == 200, created.text
    enforcement = created.json()["enforcement"]
    assert enforcement["session_timeout"]["enforced"] is True
    blocked = t.get("/api/v1/workspace/settings", headers={"X-Forwarded-For": OUTSIDE})
    assert blocked.status_code == 403, blocked.text
    assert blocked.json()["reason"] == "ip_not_in_workspace_allowlist"
    assert t.get("/api/v1/workspace/settings", headers={"X-Forwarded-For": INSIDE}).status_code == 200
    posture = t.get("/api/v1/workspace/security/posture", headers={"X-Forwarded-For": INSIDE}).json()
    assert posture["ip_allowlist_enforced"] is True
    # Nobody outside the sandbox is affected: another workspace answers from anywhere.
    other = ent.workspace("Unaffected")
    member = ent.member(email="unaffected-c18@example.com", workspace_id=other, role="editor")
    assert member.get("/api/v1/workspace/settings", headers={"X-Forwarded-For": OUTSIDE}).status_code == 200


def test_delete_tears_down_tenant_channels_and_keys(ent, one_proxy):
    from services import team_store, tenant_store

    box = provision(ent)
    t = box.tester
    assert t.post("/api/v1/workspace/tenant", json={"workspace_id": box.workspace_id, "name": "s"},
                  headers={"X-Forwarded-For": INSIDE}).status_code == 200
    key = t.post("/api/v1/workspace/api-keys", json={"name": "k", "role": "viewer", "expires_in": "7d"},
                 headers={"X-Forwarded-For": INSIDE})
    assert key.status_code == 200, key.text
    hook = t.post("/api/v1/workspace/notifications",
                  json={"kind": "webhook", "label": "h", "config": {"url": "https://example.com/h"}},
                  headers={"X-Forwarded-For": INSIDE})
    assert hook.status_code == 200, hook.text

    gone = ent.admin.delete(f"/api/v1/team/sandbox-workspaces/{box.workspace_id}", params=confirm(box.workspace_id))
    assert gone.status_code == 200, gone.text
    summary = gone.json()
    assert summary["tenants_removed"] == 1 and summary["channels_removed"] == 1
    assert summary["api_keys_revoked"] == 1
    assert team_store.get_workspace(box.workspace_id) is None
    assert tenant_store.get_tenant_for_workspace(box.workspace_id) is None
    assert ent.bearer(key.json()["key"]).get("/api/v1/connectors/saved").status_code == 401
    assert [e for e in ent.audit(action="team.sandbox.delete") if e["details"]["workspace_id"] == box.workspace_id]


def test_expired_sandboxes_are_swept_and_live_ones_kept(ent):
    from services import team_store
    from services.sandbox_workspaces import sweep_expired_sandboxes

    short = provision(ent, ttl_hours=1)
    long = provision(ent, ttl_hours=48)
    real = ent.workspace("Real, never swept")
    swept = sweep_expired_sandboxes(now=datetime.now(timezone.utc) + timedelta(hours=2))
    assert [s["workspace_id"] for s in swept] == [short.workspace_id]
    assert team_store.get_workspace(short.workspace_id) is None
    assert team_store.get_workspace(long.workspace_id) is not None
    assert team_store.get_workspace(real) is not None
    events = [e for e in ent.audit(action="team.sandbox.delete")
              if e["details"]["workspace_id"] == short.workspace_id]
    assert events and events[-1]["actor"] == "system:sandbox-expiry"


# ── Review findings: the teardown removes everything the sandbox held ────────


def _seed_connector_and_schedule(box) -> tuple[str, str]:
    """A saved connector with a stored password, and an armed schedule, in the sandbox."""
    import uuid

    from services import connector_store
    from services.schedule_store import PipelineSchedule, _load_all, _save_all

    conn = connector_store.create_connector({
        "name": f"sandbox pg {uuid.uuid4().hex[:6]}", "type": "postgresql",
        "host": "db.sandbox.invalid", "port": 5432, "username": "qa",
        "password": "Sandbox-Secret-c18", "database": "qa", "workspace_id": box.workspace_id,
    })
    assert conn.workspace_id == box.workspace_id
    sched = PipelineSchedule(
        id=f"c18-sched-{uuid.uuid4().hex[:6]}", name="sandbox nightly", source_connector_id=conn.id,
        source_table="t", dest_connector_id=conn.id, dest_table="t_out", interval="hourly",
        enabled=True, workspace_id=box.workspace_id, next_run_at="2000-01-01T00:00:00+00:00",
        mappings=[{"source": "id", "target": "id", "confidence": 1.0}],
    )
    _save_all(_load_all() + [sched])
    return conn.id, sched.id


def _assert_gone(ent, box, conn_id: str, sched_id: str, key_raw: str) -> None:
    from services import connector_store, team_store, user_store
    from services.schedule_store import get_schedule, list_schedules

    assert team_store.get_workspace(box.workspace_id) is None
    assert get_schedule(sched_id) is None
    assert not [s for s in list_schedules() if s.workspace_id == box.workspace_id]
    assert connector_store.get_connector(conn_id) is None
    # No copy of the stored credential survives in any control-plane collection.
    for name in ent.db.list_collection_names():
        assert ent.db[name].count_documents({"id": conn_id}) == 0, name
    assert ent.bearer(key_raw).get("/api/v1/connectors/saved").status_code == 401
    # The synthetic tester's login is gone: its session and its password are refused.
    assert user_store.get_user(box.tester_email) is None
    assert box.tester.get("/api/v1/auth/me").status_code == 401
    login = ent.anonymous().post("/api/v1/auth/login",
                                 json={"email": box.tester_email, "password": box.created["temporary_password"]})
    assert login.status_code in (401, 403), login.text


@pytest.mark.parametrize("how", ["delete", "expire"])
def test_teardown_removes_login_schedule_connector_credentials_and_keys(ent, how):
    from services.sandbox_workspaces import sweep_expired_sandboxes

    box = provision(ent, ttl_hours=1)
    conn_id, sched_id = _seed_connector_and_schedule(box)
    key = box.tester.post("/api/v1/workspace/api-keys", json={"name": "k", "role": "viewer", "expires_in": "7d"})
    assert key.status_code == 200, key.text
    assert box.tester.get("/api/v1/auth/me").status_code == 200
    if how == "delete":
        gone = ent.admin.delete(f"/api/v1/team/sandbox-workspaces/{box.workspace_id}", params=confirm(box.workspace_id))
        assert gone.status_code == 200, gone.text
        summary = gone.json()
    else:
        swept = sweep_expired_sandboxes(now=datetime.now(timezone.utc) + timedelta(hours=2), force=True)
        assert [s["workspace_id"] for s in swept] == [box.workspace_id]
        summary = swept[0]
    assert summary["schedules_removed"] == 1 and summary["connectors_removed"] == 1
    assert summary["api_keys_revoked"] == 1 and summary["tester_login_removed"] is True
    _assert_gone(ent, box, conn_id, sched_id, key.json()["key"])


def test_plain_workspace_delete_runs_the_same_cascade(ent):
    """DELETE /team/workspaces/{id} left tenants, channels, keys, connectors and schedules."""
    from services import tenant_store
    from services.schedule_store import get_schedule

    box = provision(ent)
    conn_id, sched_id = _seed_connector_and_schedule(box)
    assert box.tester.post("/api/v1/workspace/tenant",
                           json={"workspace_id": box.workspace_id, "name": "t"}).status_code == 200
    key = box.tester.post("/api/v1/workspace/api-keys", json={"name": "k", "role": "viewer", "expires_in": "7d"})
    gone = ent.admin.delete(f"/api/v1/team/workspaces/{box.workspace_id}", params=confirm(box.workspace_id))
    assert gone.status_code == 200, gone.text
    body = gone.json()
    assert body["tenants_removed"] == 1 and body["schedules_removed"] == 1
    assert body["connectors_removed"] == 1 and body["api_keys_revoked"] == 1
    assert tenant_store.get_tenant_for_workspace(box.workspace_id) is None
    assert get_schedule(sched_id) is None
    assert ent.bearer(key.json()["key"]).get("/api/v1/connectors/saved").status_code == 401
    # Idempotent: a second cascade over the same id removes nothing and does not fail.
    from services.workspace_teardown import teardown_workspace

    again = teardown_workspace(box.workspace_id)
    assert again["workspace_removed"] is False and again["schedules_removed"] == 0


def test_active_job_makes_the_teardown_wait(ent):
    from services.mongodb_service import get_mongodb_service

    box = provision(ent)
    svc = get_mongodb_service()
    job_id = svc.create_transfer_job({"name": "c18-running", "workspace_id": box.workspace_id})
    svc.get_database()["transfer_jobs"].update_one({"name": "c18-running"}, {"$set": {"status": "running"}})
    busy = ent.admin.delete(f"/api/v1/team/sandbox-workspaces/{box.workspace_id}", params=confirm(box.workspace_id))
    assert busy.status_code == 409, busy.text
    from services import team_store

    assert team_store.get_workspace(box.workspace_id) is not None  # nothing deleted
    svc.get_database()["transfer_jobs"].update_one({"name": "c18-running"}, {"$set": {"status": "cancelled"}})
    assert ent.admin.delete(f"/api/v1/team/sandbox-workspaces/{box.workspace_id}", params=confirm(box.workspace_id)).status_code == 200
    assert svc.get_database()["transfer_jobs"].count_documents({"workspace_id": box.workspace_id}) == 0
    assert job_id


def test_failed_setup_rolls_back_and_leaves_no_login_or_workspace(ent, monkeypatch):
    from services import sandbox_workspaces, team_store, user_store

    def boom(**_kw):
        raise RuntimeError("membership store down")

    monkeypatch.setattr(team_store, "add_workspace_member", boom)
    with pytest.raises(RuntimeError):
        sandbox_workspaces.create_sandbox(
            name="half", tester_email="half-c18@example.test", reason="r", actor="root-admin@example.com"
        )
    assert user_store.get_user("half-c18@example.test") is None
    assert not [w for w in team_store.list_workspaces() if w.name == "[sandbox] half"]


def test_sweep_is_claimed_once_per_period(ent):
    from services.sandbox_workspaces import sweep_expired_sandboxes

    box = provision(ent, ttl_hours=1)
    later = datetime.now(timezone.utc) + timedelta(hours=2)
    first = sweep_expired_sandboxes(now=later)
    assert [s["workspace_id"] for s in first] == [box.workspace_id]
    second_box = provision(ent, ttl_hours=1)
    # Same period: another replica's beat does nothing, even with work waiting.
    assert sweep_expired_sandboxes(now=later + timedelta(minutes=1)) == []
    next_period = sweep_expired_sandboxes(now=later + timedelta(minutes=16))
    assert [s["workspace_id"] for s in next_period] == [second_box.workspace_id]


@pytest.mark.parametrize("route", ["team", "sandbox"])
def test_teardown_from_another_workspace_header_removes_the_targets_connectors(ent, route):
    """B1 (second review): the admin's bound workspace filter hid B's connectors."""
    from services import connector_store

    home = ent.workspace("Admin's current workspace")
    box = provision(ent)
    conn_id, sched_id = _seed_connector_and_schedule(box)
    path = (
        f"/api/v1/team/workspaces/{box.workspace_id}"
        if route == "team"
        else f"/api/v1/team/sandbox-workspaces/{box.workspace_id}"
    )
    # The web sends X-Workspace-Id on every call: here, a different workspace.
    gone = ent.admin.delete(path, headers={"X-Workspace-Id": home}, params=confirm(box.workspace_id))
    assert gone.status_code == 200, gone.text
    assert gone.json()["connectors_removed"] == 1
    assert gone.json()["schedules_removed"] == 1
    assert connector_store.get_connector(conn_id) is None
    for name in ent.db.list_collection_names():
        assert ent.db[name].count_documents({"id": conn_id}) == 0, name
    assert not [c for c in connector_store.list_connectors() if c.workspace_id == box.workspace_id]


def test_plain_workspace_delete_of_a_sandbox_removes_the_tester_login(ent):
    """S1 (second review): /auth/me answered 200 after DELETE /team/workspaces/{sandbox}."""
    from services import user_store

    box = provision(ent)
    assert box.tester.get("/api/v1/auth/me").status_code == 200
    gone = ent.admin.delete(f"/api/v1/team/workspaces/{box.workspace_id}", params=confirm(box.workspace_id))
    assert gone.status_code == 200, gone.text
    assert gone.json()["tester_login_removed"] is True
    assert user_store.get_user(box.tester_email) is None
    assert box.tester.get("/api/v1/auth/me").status_code == 401


def test_rollback_after_a_lost_create_race_never_deletes_the_real_account(ent, monkeypatch):
    """S2 (second review): the rollback deleted the login by email even when create_user failed."""
    from services import sandbox_workspaces, team_store, user_store

    real = "race-c18@example.test"
    original_create = user_store.create_user

    def lost_race(**kw):
        # Another request created the real account first; this create fails.
        original_create(email=real, password="Real-Account-Pass-2026!", role="member",
                               created_by="someone-else@example.test")
        raise user_store.UserStoreError(f"{real} already has an account")

    monkeypatch.setattr(user_store, "create_user", lost_race)
    with pytest.raises(user_store.UserStoreError):
        sandbox_workspaces.create_sandbox(
            name="race", tester_email=real, reason="r", actor="root-admin@example.com"
        )
    assert user_store.get_user(real) is not None, "rollback deleted a real account"
    assert not [w for w in team_store.list_workspaces() if w.name == "[sandbox] race"]


def test_expiry_never_deletes_a_recreated_account_with_the_same_email(ent):
    """S2: only the record the sandbox minted (by created_at) is removed."""
    from services import user_store
    from services.sandbox_workspaces import sweep_expired_sandboxes

    box = provision(ent, ttl_hours=1)
    user_store.delete_user(email=box.tester_email)
    user_store.create_user(email=box.tester_email, password="Real-Account-Pass-2026!",
                           role="member", created_by="hr@example.test")
    swept = sweep_expired_sandboxes(now=datetime.now(timezone.utc) + timedelta(hours=2), force=True)
    assert [s["workspace_id"] for s in swept] == [box.workspace_id]
    assert swept[0]["tester_login_removed"] is False
    assert user_store.get_user(box.tester_email)["created_by"] == "hr@example.test"
