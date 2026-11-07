"""A workspace admin administers its workspace, not the deployment.

Membership ``admin`` resolves to the gate role ``admin``, and that role used to
carry every permission — so the admin of one self-provisioned workspace could
mint an admin API key or a SCIM token, provision or deprovision any user, and
list or revoke every tenant's API keys. Credentials and provisioning are
deployment-wide, so they stay with principals whose own role is ``admin``.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import pytest
from fastapi.testclient import TestClient

from services import audit_log, integrations_store, scim_store, team_store, user_store
from services.auth_rate_limit import reset_auth_rate_limits
from services.rbac import PLATFORM_ADMIN_PERMISSIONS, Permission, principal_permissions
from src.main import app


@pytest.fixture(autouse=True)
def _strict_rule(monkeypatch):
    # These tests pin the strict rule; the flag's default is covered in
    # tests/test_workspace_admin_iam_flag.py.
    monkeypatch.setenv("DATAFLOW_WORKSPACE_ADMIN_IAM", "0")


@pytest.fixture
def root(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", "root@example.com")
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", "Bootstrap-Admin-2026")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "test-secret-not-production")
    monkeypatch.setattr(team_store, "mongo_database", lambda: None)
    monkeypatch.setattr(user_store, "mongo_database", lambda: None)
    monkeypatch.setattr(integrations_store, "STORE_PATH", tmp_path / "integrations.json")
    monkeypatch.setattr(integrations_store, "_keys_collection", lambda: None)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    monkeypatch.setattr(scim_store, "mongo_database", lambda: None)
    monkeypatch.setattr(scim_store, "STORE_PATH", tmp_path / "scim.json")
    reset_auth_rate_limits()
    client = TestClient(app)
    login = client.post(
        "/api/v1/auth/login",
        json={"email": "root@example.com", "password": "Bootstrap-Admin-2026"},
    )
    assert login.status_code == 200, login.text
    client.headers["Authorization"] = f"Bearer {login.json()['token']}"
    return client


def _workspace_admin(root: TestClient) -> tuple[TestClient, str]:
    created = root.post("/api/v1/team/workspaces", json={"name": "Mallory Co"})
    assert created.status_code == 200, created.text
    ws_id = created.json()["workspace"]["id"]
    user = root.post(
        "/api/v1/team/users",
        json={
            "email": "mallory@example.com",
            "platform_role": "member",
            "workspace_id": ws_id,
            "workspace_role": "admin",
        },
    )
    assert user.status_code == 200, user.text
    peer = TestClient(app)
    login = peer.post(
        "/api/v1/auth/login",
        json={"email": "mallory@example.com", "password": user.json()["temporary_password"]},
    )
    assert login.status_code == 200, login.text
    peer.headers["Authorization"] = f"Bearer {login.json()['token']}"
    peer.headers["X-Workspace-Id"] = ws_id
    return peer, ws_id


def test_membership_admin_lacks_platform_permissions():
    member = {"email": "m@example.com", "role": "member"}
    granted = principal_permissions(member, "admin")
    assert not granted & PLATFORM_ADMIN_PERMISSIONS
    # Workspace-level administration is untouched.
    assert Permission.CONNECTOR_DELETE in granted
    assert Permission.SCHEDULE_AUTHORIZE in granted
    assert PLATFORM_ADMIN_PERMISSIONS <= principal_permissions(
        {"email": "root@example.com", "role": "admin"}, "admin"
    )
    # A key minted by a platform admin as role admin is still a platform admin.
    assert PLATFORM_ADMIN_PERMISSIONS <= principal_permissions(
        {"role": "admin", "auth_kind": "api_key"}, "admin"
    )


def test_workspace_admin_resolves_to_admin_but_cannot_reach_iam(root):
    peer, _ws = _workspace_admin(root)
    me = peer.get("/api/v1/auth/me")
    assert me.status_code == 200, me.text
    assert me.json()["effective_role"] == "admin"
    assert "iam.manage" not in me.json()["permissions"]
    assert "scim.provision" not in me.json()["permissions"]

    minted = peer.post(
        "/api/v1/iam/service-accounts",
        json={"name": "Escalated", "role": "admin", "scopes": ["iam.manage"]},
    )
    assert minted.status_code == 403, minted.text
    assert minted.json()["required_permission"] == "iam.manage"
    assert peer.post("/api/v1/iam/scim-token", json={}).status_code == 403
    assert peer.get("/api/v1/iam/service-accounts").status_code == 403
    assert peer.get("/api/v1/scim/v2/Users").status_code == 403
    assert (
        peer.post(
            "/api/v1/scim/v2/Users",
            json={
                "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
                "userName": "victim@example.com",
            },
        ).status_code
        == 403
    )
    assert integrations_store.list_api_keys() == []


def test_workspace_admin_cannot_list_mint_or_revoke_deployment_api_keys(root):
    foreign = integrations_store.create_api_key(
        "Other tenant", "root@example.com", role="editor", expires_in="30d"
    )
    peer, _ws = _workspace_admin(root)

    assert peer.get("/api/v1/workspace/api-keys").status_code == 403
    minted = peer.post(
        "/api/v1/workspace/api-keys", json={"name": "Escalated", "role": "admin"}
    )
    assert minted.status_code == 403, minted.text
    revoked = peer.delete(f"/api/v1/workspace/api-keys/{foreign['id']}")
    assert revoked.status_code == 403, revoked.text
    assert [key["id"] for key in integrations_store.list_api_keys()] == [foreign["id"]]

    # The platform admin keeps every one of these routes.
    assert root.get("/api/v1/workspace/api-keys").status_code == 200
    assert root.post("/api/v1/iam/scim-token", json={}).status_code == 200


def test_grant_bounds_value_an_admin_target_at_its_full_authority(root, monkeypatch):
    """A caller holding admin-minus-IAM must not be able to mint an admin key."""
    from fastapi import HTTPException

    from src.routers import iam_router

    class _Req:
        class state:  # noqa: N801
            user = {"email": "mallory@example.com", "role": "member"}
            effective_role = "admin"
            user_email = "mallory@example.com"

        headers: dict[str, str] = {}

    with pytest.raises(HTTPException) as exc:
        iam_router.enforce_grant_bounds(
            _Req(), route="POST /api/v1/iam/service-accounts", target_role="admin", target_scopes=None
        )
    assert exc.value.status_code == 403
    assert "iam.manage" in exc.value.detail
