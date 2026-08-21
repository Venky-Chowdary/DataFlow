"""Tenant lifecycle over HTTP: created, amended, removed — never a 500.

Creating an enterprise tenant answered ``500 Internal Server Error`` for every
caller: the route asked ``can_admin_workspace`` for the authority to proceed and
that name was never imported, so the first authorization check raised
``NameError`` inside the handler. These tests drive the real routes so the
authority question is asked of the resolver the API gate uses — the platform
label plus the membership row — and pin the refusals that must stay refusals.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import pytest
from fastapi.testclient import TestClient

from services import team_store, user_store
from services.auth_rate_limit import reset_auth_rate_limits
from src.main import app


@pytest.fixture
def stores(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(team_store, "mongo_database", lambda: None)
    monkeypatch.setattr(user_store, "mongo_database", lambda: None)
    import services.tenant_store as tenant_store

    monkeypatch.setattr(tenant_store, "STORE_PATH", tmp_path / "tenants.json")
    return tmp_path


@pytest.fixture
def admin(stores, monkeypatch):
    """A deployment with authentication on, signed in as the platform admin."""
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", "root@example.com")
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", "Bootstrap-Admin-2026")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "test-secret-not-production")
    reset_auth_rate_limits()
    c = TestClient(app)
    login = c.post(
        "/api/v1/auth/login",
        json={"email": "root@example.com", "password": "Bootstrap-Admin-2026"},
    )
    assert login.status_code == 200, login.text
    c.headers["Authorization"] = f"Bearer {login.json()['token']}"
    return c


def _workspace(client, name: str) -> str:
    response = client.post("/api/v1/team/workspaces", json={"name": name})
    assert response.status_code == 200, response.text
    return response.json()["workspace"]["id"]


def _signed_in(client, *, email: str, workspace_id: str, workspace_role: str) -> TestClient:
    """A second identity, created by the admin and signed in for itself."""
    created = client.post(
        "/api/v1/team/users",
        json={
            "email": email,
            "platform_role": "member",
            "workspace_id": workspace_id,
            "workspace_role": workspace_role,
        },
    )
    assert created.status_code == 200, created.text
    issued = created.json()["temporary_password"]
    peer = TestClient(app)
    login = peer.post("/api/v1/auth/login", json={"email": email, "password": issued})
    assert login.status_code == 200, login.text
    peer.headers["Authorization"] = f"Bearer {login.json()['token']}"
    return peer


def test_platform_admin_creates_a_tenant_for_a_workspace(admin):
    ws_id = _workspace(admin, "Acme")

    created = admin.post(
        "/api/v1/workspace/tenant",
        json={
            "workspace_id": ws_id,
            "name": "Acme Enterprise",
            "custom_domain": "data.acme.com",
            "data_region": "us-east-1",
            "security_contact_email": "security@acme.com",
            "mfa_required": True,
            "session_timeout_hours": 4,
            "ip_allowlist": ["203.0.113.0/24"],
        },
    )
    assert created.status_code == 200, created.text
    tenant = created.json()
    assert tenant["workspace_id"] == ws_id
    assert tenant["name"] == "Acme Enterprise"
    assert tenant["mfa_required"] is True
    assert tenant["session_timeout_hours"] == 4
    assert tenant["ip_allowlist"] == ["203.0.113.0/24"]

    listed = admin.get("/api/v1/workspace/tenants")
    assert listed.status_code == 200, listed.text
    assert [t["id"] for t in listed.json()["tenants"]] == [tenant["id"]]

    events = admin.get("/api/v1/audit/events", params={"limit": 100})
    assert events.status_code == 200, events.text
    assert "workspace.tenant.create" in {
        e.get("action") for e in events.json().get("events", [])
    }


def test_tenant_is_amended_and_removed_by_its_workspace_admin(admin):
    ws_id = _workspace(admin, "Beta")
    lead = _signed_in(admin, email="lead@example.com", workspace_id=ws_id, workspace_role="admin")

    created = lead.post(
        "/api/v1/workspace/tenant",
        json={"workspace_id": ws_id, "name": "Beta", "data_region": "eu-west-1"},
    )
    assert created.status_code == 200, created.text
    tenant_id = created.json()["id"]

    amended = lead.patch(
        f"/api/v1/workspace/tenant/{tenant_id}",
        json={"name": "Beta Renamed", "session_timeout_hours": 12},
    )
    assert amended.status_code == 200, amended.text
    assert amended.json()["name"] == "Beta Renamed"
    assert amended.json()["session_timeout_hours"] == 12

    removed = lead.delete(f"/api/v1/workspace/tenant/{tenant_id}")
    assert removed.status_code == 200, removed.text
    assert admin.get("/api/v1/workspace/tenants").json()["tenants"] == []


def test_workspace_editor_is_refused_in_words_not_by_a_fault(admin):
    ws_id = _workspace(admin, "Gamma")
    editor = _signed_in(
        admin, email="editor@example.com", workspace_id=ws_id, workspace_role="editor"
    )

    refused = editor.post(
        "/api/v1/workspace/tenant",
        json={"workspace_id": ws_id, "name": "Gamma"},
    )
    assert refused.status_code == 403, refused.text
    body = refused.json()
    assert body["required_permission"] == "workspace.manage"
    assert body["effective_role"] == "editor"


def test_admin_of_one_workspace_cannot_create_a_tenant_for_another(admin):
    """The body names the workspace, so the header cannot borrow its authority."""
    mine = _workspace(admin, "Mine")
    theirs = _workspace(admin, "Theirs")
    lead = _signed_in(admin, email="mine@example.com", workspace_id=mine, workspace_role="admin")

    refused = lead.post(
        "/api/v1/workspace/tenant",
        headers={"X-Workspace-Id": mine},
        json={"workspace_id": theirs, "name": "Not mine"},
    )
    assert refused.status_code == 403, refused.text
    assert "Workspace admin required" in refused.text
    assert admin.get("/api/v1/workspace/tenants").json()["tenants"] == []


def test_second_tenant_for_one_workspace_is_a_stated_conflict(admin):
    ws_id = _workspace(admin, "Delta")
    first = admin.post(
        "/api/v1/workspace/tenant", json={"workspace_id": ws_id, "name": "Delta"}
    )
    assert first.status_code == 200, first.text

    again = admin.post(
        "/api/v1/workspace/tenant", json={"workspace_id": ws_id, "name": "Delta again"}
    )
    assert again.status_code == 400, again.text
    assert "already has a tenant" in again.text


def test_domain_already_in_use_is_stated_not_a_fault(admin):
    first_ws = _workspace(admin, "Eps1")
    second_ws = _workspace(admin, "Eps2")
    assert (
        admin.post(
            "/api/v1/workspace/tenant",
            json={"workspace_id": first_ws, "name": "Eps1", "custom_domain": "data.eps.com"},
        ).status_code
        == 200
    )
    clash = admin.post(
        "/api/v1/workspace/tenant",
        json={"workspace_id": second_ws, "name": "Eps2", "custom_domain": "www.data.eps.com"},
    )
    assert clash.status_code == 400, clash.text
    assert "already in use" in clash.text


def test_missing_tenant_is_a_404_on_every_verb(admin):
    assert admin.patch("/api/v1/workspace/tenant/nope", json={"name": "x"}).status_code == 404
    assert admin.delete("/api/v1/workspace/tenant/nope").status_code == 404


def test_store_missing_its_tenants_list_still_accepts_a_write(stores, monkeypatch):
    """A store file written in another shape must not fault the next create."""
    import services.tenant_store as tenant_store

    tenant_store.STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tenant_store.STORE_PATH.write_text('{"version": 2}', encoding="utf-8")

    tenant = tenant_store.create_tenant(workspace_id="ws-legacy", name="Legacy")
    assert tenant.id
    assert [t.id for t in tenant_store.list_tenants()] == [tenant.id]


def test_www_prefix_is_the_same_domain(stores):
    import services.tenant_store as tenant_store

    tenant_store.create_tenant(workspace_id="ws-1", name="One", custom_domain="www.acme.com")
    found = tenant_store.get_tenant_by_domain("acme.com")
    assert found is not None and found.workspace_id == "ws-1"
