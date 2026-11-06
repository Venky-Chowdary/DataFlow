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
from r14_sandbox_world import provision, sandbox  # noqa: F401

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
    assert ent.admin.delete(f"/api/v1/team/sandbox-workspaces/{ws}").status_code == 404
    from services import team_store

    assert team_store.get_workspace(ws) is not None
    # A reason is required; the tester must be another account.
    assert ent.admin.post("/api/v1/team/sandbox-workspaces",
                          json={"tester_email": "y-c18@example.test", "reason": "  "}).status_code == 422
    assert ent.admin.post("/api/v1/team/sandbox-workspaces",
                          json={"tester_email": "nobody-c18@example.test", "reason": "r"}).status_code == 400


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

    gone = ent.admin.delete(f"/api/v1/team/sandbox-workspaces/{box.workspace_id}")
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
