"""Audit reads authorize the requested workspace — a header is not a membership.

Any signed-in identity holds ``audit.read``, so the router itself must refuse a
forged ``X-Workspace-Id`` (and audit the attempt), and an unscoped read by a
non-admin returns only their own workspaces' events (D154), never the platform's.
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from services import audit_log, team_store, user_store
from services.auth_rate_limit import reset_auth_rate_limits
from src.main import app

ADMIN_EMAIL = "root@example.com"
ADMIN_PASSWORD = "Bootstrap-Admin-2026"
READ_PATHS = (
    "/api/v1/audit/events",
    "/api/v1/audit/export?format=ndjson",
    "/api/v1/audit/secret-scan",
    "/api/v1/audit/verify",
)


@pytest.fixture
def admin(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", ADMIN_EMAIL)
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", ADMIN_PASSWORD)
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "test-secret-not-production")
    monkeypatch.setattr(team_store, "mongo_database", lambda: None)
    monkeypatch.setattr(user_store, "mongo_database", lambda: None)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    import services.tenant_store as tenant_store

    monkeypatch.setattr(tenant_store, "STORE_PATH", tmp_path / "tenants.json")
    reset_auth_rate_limits()
    client = TestClient(app)
    login = client.post(
        "/api/v1/auth/login", json={"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD}
    )
    assert login.status_code == 200, login.text
    client.headers["Authorization"] = f"Bearer {login.json()['token']}"
    return client


def _workspace(client: TestClient, name: str) -> str:
    response = client.post("/api/v1/team/workspaces", json={"name": name})
    assert response.status_code == 200, response.text
    return response.json()["workspace"]["id"]


def _member(admin: TestClient, *, email: str, workspace_id: str) -> TestClient:
    created = admin.post(
        "/api/v1/team/users",
        json={
            "email": email,
            "platform_role": "member",
            "workspace_id": workspace_id,
            "workspace_role": "viewer",
        },
    )
    assert created.status_code == 200, created.text
    peer = TestClient(app)
    login = peer.post(
        "/api/v1/auth/login",
        json={"email": email, "password": created.json()["temporary_password"]},
    )
    assert login.status_code == 200, login.text
    peer.headers["Authorization"] = f"Bearer {login.json()['token']}"
    return peer


@pytest.fixture
def tenants(admin):
    victim = _workspace(admin, "Acme")
    own = _workspace(admin, "Mallory Co")
    audit_log.append_audit_event(
        action="connector.create",
        resource="/connectors",
        actor="ops@acme.example",
        workspace_id=victim,
        details={"name": "acme-secret-connector"},
    )
    audit_log.append_audit_event(
        action="connector.create",
        resource="/connectors",
        actor="mallory@example.com",
        workspace_id=own,
        details={"name": "mallory-connector"},
    )
    mallory = _member(admin, email="mallory@example.com", workspace_id=own)
    return {"admin": admin, "mallory": mallory, "victim": victim, "own": own}


@pytest.mark.parametrize("path", READ_PATHS)
def test_forged_workspace_header_is_refused(tenants, path):
    res = tenants["mallory"].get(path, headers={"X-Workspace-Id": tenants["victim"]})
    assert res.status_code == 403, res.text
    assert "acme-secret-connector" not in res.text


@pytest.mark.parametrize("path", READ_PATHS)
def test_unscoped_read_by_non_admin_never_names_another_workspace(tenants, path):
    res = tenants["mallory"].get(path)
    assert res.status_code == 200, res.text
    assert "acme-secret-connector" not in res.text
    assert tenants["victim"] not in res.text


def test_member_reads_only_own_workspace(tenants):
    headers = {"X-Workspace-Id": tenants["own"]}
    events = tenants["mallory"].get("/api/v1/audit/events?limit=200", headers=headers)
    assert events.status_code == 200, events.text
    names = {e.get("details", {}).get("name") for e in events.json()["events"]}
    assert "mallory-connector" in names
    assert "acme-secret-connector" not in names
    exported = tenants["mallory"].get(
        "/api/v1/audit/export?format=ndjson&limit=200", headers=headers
    )
    assert exported.status_code == 200, exported.text
    assert "acme-secret-connector" not in exported.text


def test_platform_admin_reads_platform_wide_and_any_workspace(tenants):
    admin = tenants["admin"]
    wide = admin.get("/api/v1/audit/events?limit=200")
    assert wide.status_code == 200, wide.text
    names = {e.get("details", {}).get("name") for e in wide.json()["events"]}
    assert {"acme-secret-connector", "mallory-connector"} <= names

    scoped = admin.get(
        "/api/v1/audit/events?limit=200", headers={"X-Workspace-Id": tenants["own"]}
    )
    assert scoped.status_code == 200, scoped.text
    names = {e.get("details", {}).get("name") for e in scoped.json()["events"]}
    assert "mallory-connector" in names
    assert "acme-secret-connector" not in names


def test_cross_workspace_denial_is_audited(tenants, monkeypatch):
    from collections import OrderedDict

    from services import audit_coverage

    # Denials are deduplicated per (actor, permission, path) for 60 s in-process;
    # earlier cases in this module hit the same key.
    monkeypatch.setattr(audit_coverage, "_DENIALS", OrderedDict())
    res = tenants["mallory"].get(
        "/api/v1/audit/events", headers={"X-Workspace-Id": tenants["victim"]}
    )
    assert res.status_code == 403, res.text
    denials = [
        e
        for e in audit_log.list_audit_events(limit=500)
        if e.get("action") == "authz.denied"
        and (e.get("details") or {}).get("reason") == "cross_workspace"
    ]
    assert denials, "a forged audit scope must leave an authz.denied record"
