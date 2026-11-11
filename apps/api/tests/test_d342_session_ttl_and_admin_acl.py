"""D342: the issued token follows the tenant session policy; an ACL grant does
not hide a resource from its workspace's admin.

(a) Production QA: with ``session_timeout_hours=1`` (enforced) a new login
    still returned ``expires_at = now + 24 h``: ``create_token`` used only the
    platform TTL. The token a member's client stores now expires no later
    than the strictest enforced policy among their workspaces.
(b) After a workspace admin granted ``viewer`` on a connector to an editor,
    the admin no longer saw the connector: ``assert_resource_acl`` exempted
    platform admins only. A workspace admin can grant and revoke every ACL in
    the workspace (resource_acl_router), so hiding a resource from them
    protects nothing and reads as data loss.
"""

from __future__ import annotations

import importlib
import time
from types import SimpleNamespace

import pytest
from starlette.requests import Request

from r8_ent_harness import ent  # noqa: F401

# ── (a) token lifetime ──────────────────────────────────────────────────────


def _login_ttl(client_login) -> int:
    body = client_login.json()
    return int(body["expires_at"]) - int(time.time())


def test_login_expires_at_follows_the_enforced_tenant_timeout(ent):
    from r8_ent_harness import ADMIN_EMAIL, ADMIN_PASSWORD

    ws = ent.workspace("D342 ttl")
    created = ent.admin.post("/api/v1/team/users", json={
        "email": "d342-ttl@example.com", "platform_role": "member",
        "workspace_id": ws, "workspace_role": "editor"})
    assert created.status_code == 200, created.text
    password = created.json()["temporary_password"]

    def login(email, pw):
        res = ent.anonymous().post("/api/v1/auth/login", json={"email": email, "password": pw})
        assert res.status_code == 200, res.text
        return res

    # No tenant policy: the platform lifetime (24 h by default).
    assert _login_ttl(login("d342-ttl@example.com", password)) > 23 * 3600

    tenant = ent.admin.post("/api/v1/workspace/tenant",
                            json={"workspace_id": ws, "name": "TTL", "session_timeout_hours": 1,
                                  "enforce_session_timeout": True},
                            headers={"X-Workspace-Id": ws})
    assert tenant.status_code == 200, tenant.text
    for email, pw in (("d342-ttl@example.com", password), (ADMIN_EMAIL, ADMIN_PASSWORD)):
        ttl = _login_ttl(login(email, pw))
        assert 3590 <= ttl <= 3600, (email, ttl)

    # Recorded as policy only: the token is not shortened.
    ent.admin.patch(f"/api/v1/workspace/tenant/{tenant.json()['id']}",
                    json={"session_timeout_hours": 1, "enforce_session_timeout": False},
                    headers={"X-Workspace-Id": ws})
    assert _login_ttl(login("d342-ttl@example.com", password)) > 23 * 3600


def test_token_past_the_policy_lifetime_is_refused(monkeypatch):
    from src.services import auth_service

    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "d342-secret-" + "x" * 40)
    monkeypatch.setattr("services.session_policy.idle_timeout_seconds", lambda email: 3600)
    monkeypatch.setattr("services.auth_sessions.create_session", lambda *a, **k: "jti-d342")
    now = time.time()
    token, expires = auth_service.create_token("ttl@example.com")
    assert expires - int(now) <= 3600
    monkeypatch.setattr(auth_service.time, "time", lambda: now + 3601)
    assert auth_service.verify_token(token) is None


# ── (b) workspace admin keeps sight of an ACL-restricted resource ───────────

OWNER = "d342-owner@example.com"
EDITOR = "d342-editor@example.com"
OTHER = "d342-other@example.com"


@pytest.fixture
def workspace(tmp_path, monkeypatch):
    from services import resource_acl, team_store

    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "test-secret-not-production")
    monkeypatch.setattr(team_store, "mongo_database", lambda: None)
    monkeypatch.setattr(resource_acl, "STORE_PATH", tmp_path / "resource-acls.jsonl")
    ws = team_store.create_workspace(name="D342", created_by=OWNER)  # OWNER is its admin
    team_store.add_workspace_member(workspace_id=ws.id, email=EDITOR, role="editor", added_by=OWNER)
    team_store.add_workspace_member(workspace_id=ws.id, email=OTHER, role="editor", added_by=OWNER)
    other_ws = team_store.create_workspace(name="D342 other", created_by=OTHER)
    return ws.id, other_ws.id


def _request(email: str) -> Request:
    req = Request({"type": "http", "method": "GET", "path": "/", "headers": []})
    req.state.user_email = email
    req.state.user = {"email": email, "role": "member"}
    return req


def _grant(ws: str, rtype: str, rid: str) -> None:
    from services import resource_acl

    resource_acl.upsert_grant(tenant_id=ws, resource_type=rtype, resource_id=rid,
                              principal=EDITOR, role="viewer", created_by=OWNER)


def test_workspace_admin_still_sees_a_connector_after_a_grant(workspace):
    ws, _ = workspace
    connectors = importlib.import_module("src.routers.saved_connectors_router")
    conn = SimpleNamespace(id="D342_CONN", workspace_id=ws)
    _grant(ws, "connector", conn.id)
    assert connectors._can_access_connector(_request(OWNER), conn) is True
    assert connectors._can_access_connector(_request(OWNER), conn, write=True) is True
    # The grant still restricts everyone else, as documented.
    assert connectors._can_access_connector(_request(EDITOR), conn) is True
    assert connectors._can_access_connector(_request(EDITOR), conn, write=True) is False
    assert connectors._can_access_connector(_request(OTHER), conn) is False


def test_workspace_admin_still_sees_a_job_after_a_grant(workspace):
    ws, _ = workspace
    transfers = importlib.import_module("src.routers.transfer_router")
    job = {"_id": "d342-job", "workspace_id": ws}
    _grant(ws, "job", "d342-job")
    assert transfers._can_access_job(_request(OWNER), job) is True
    assert transfers._can_access_job(_request(EDITOR), job) is True
    assert transfers._can_access_job(_request(OTHER), job) is False


def test_admin_of_another_workspace_gets_no_exemption(workspace):
    ws, other_ws = workspace
    connectors = importlib.import_module("src.routers.saved_connectors_router")
    # OTHER administers other_ws, not ws: no cross-workspace bypass.
    conn = SimpleNamespace(id="D342_CONN2", workspace_id=ws)
    _grant(ws, "connector", conn.id)
    assert connectors._can_access_connector(_request(OTHER), conn) is False
    from services.resource_acl import acl_exempt
    from services.tenant_scope import resolve_tenant_scope

    assert acl_exempt(resolve_tenant_scope(_request(OTHER)), ws) is False
    assert acl_exempt(resolve_tenant_scope(_request(OTHER)), other_ws) is True
