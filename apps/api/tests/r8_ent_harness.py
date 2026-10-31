"""Round-8 enterprise settings harness: the real app on a live MongoDB.

Every control-plane store the Settings, SSO, SCIM, audit and team routes touch
resolves its database through ``services.mongodb_service.get_mongodb_service``.
The fixture points that one singleton at a throwaway database on the local
MongoDB, so members, accounts, SCIM state, API keys, the integrations document
and the audit chain all live in Mongo exactly as they do in production. Only
stores that are file-backed in production too (workspace settings, tenants,
notification channels, auth sessions) are redirected to ``tmp_path``.

Import the fixtures into a test module::

    from r8_ent_harness import ent  # noqa: F401
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import pytest
from fastapi.testclient import TestClient

MONGO_URI = os.environ.get("DATAFLOW_TEST_MONGO_URI") or "mongodb://localhost:27017"
ADMIN_EMAIL = "root-admin@example.com"
ADMIN_PASSWORD = "Bootstrap-Admin-2026"


class Ent:
    """Handle for one isolated deployment."""

    def __init__(self, app, db, tmp_path):
        self.app = app
        self.db = db
        self.tmp = tmp_path
        self.admin = self.login(ADMIN_EMAIL, ADMIN_PASSWORD)

    # ── clients ──────────────────────────────────────────────────────────
    def login(self, email: str, password: str) -> TestClient:
        client = TestClient(self.app)
        res = client.post("/api/v1/auth/login", json={"email": email, "password": password})
        assert res.status_code == 200, res.text
        client.headers["Authorization"] = f"Bearer {res.json()['token']}"
        return client

    def bearer(self, token: str, workspace_id: str | None = None) -> TestClient:
        client = TestClient(self.app)
        client.headers["Authorization"] = f"Bearer {token}"
        if workspace_id:
            client.headers["X-Workspace-Id"] = workspace_id
        return client

    def anonymous(self) -> TestClient:
        return TestClient(self.app)

    # ── fixtures through the real routes ─────────────────────────────────
    def workspace(self, name: str = "R8 workspace") -> str:
        res = self.admin.post("/api/v1/team/workspaces", json={"name": name})
        assert res.status_code == 200, res.text
        return res.json()["workspace"]["id"]

    def member(self, *, email: str, workspace_id: str, role: str) -> TestClient:
        created = self.admin.post(
            "/api/v1/team/users",
            json={
                "email": email,
                "platform_role": "member",
                "workspace_id": workspace_id,
                "workspace_role": role,
            },
        )
        assert created.status_code == 200, created.text
        client = self.login(email, created.json()["temporary_password"])
        client.headers["X-Workspace-Id"] = workspace_id
        return client

    def api_key(self, *, role: str, workspace_id: str, name: str = "r8 key") -> str:
        res = self.admin.post(
            "/api/v1/workspace/api-keys",
            json={"name": name, "role": role, "expires_in": "7d"},
            headers={"X-Workspace-Id": workspace_id},
        )
        assert res.status_code == 200, res.text
        return res.json()["key"]

    def audit(self, workspace_id: str | None = None, **params: Any) -> list[dict[str, Any]]:
        headers = {"X-Workspace-Id": workspace_id} if workspace_id else {}
        res = self.admin.get("/api/v1/audit/events", params={"limit": 500, **params}, headers=headers)
        assert res.status_code == 200, res.text
        body = res.json()
        return body["events"] if isinstance(body, dict) else body


@pytest.fixture
def ent(monkeypatch, tmp_path):
    pymongo = pytest.importorskip("pymongo")
    probe = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=1500)
    try:
        probe.admin.command("ping")
    except Exception:  # noqa: BLE001
        pytest.skip("MongoDB not reachable")
    finally:
        probe.close()

    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "r8-enterprise-auth-secret-" + "x" * 40)
    monkeypatch.setenv("DATAFLOW_SECRETS_KEY", "r8" * 16)
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", ADMIN_EMAIL)
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", ADMIN_PASSWORD)
    monkeypatch.delenv("DATAFLOW_JOB_STORE", raising=False)
    monkeypatch.delenv("DATAWRAP_JOB_STORE", raising=False)

    from services import mongodb_service

    name = f"r8_ent_{uuid.uuid4().hex[:10]}"
    svc = mongodb_service.MongoDBService(MONGO_URI)
    svc.db_name = name
    assert svc.connect()
    monkeypatch.setattr(mongodb_service, "_mongodb_service", svc)

    from services import (
        audit_log,
        auth_sessions,
        integrations_store,
        notification_store,
        scim_store,
        tenant_store,
        workspace_store,
    )
    from services import auth_service
    from services.auth_rate_limit import reset_auth_rate_limits

    monkeypatch.setattr(auth_service, "_REQUIRE_AUTH", True, raising=False)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(integrations_store, "STORE_PATH", tmp_path / "integrations.json")
    monkeypatch.setattr(workspace_store, "STORE_PATH", tmp_path / "workspace.json")
    monkeypatch.setattr(tenant_store, "STORE_PATH", tmp_path / "tenants.json")
    monkeypatch.setattr(notification_store, "STORE_PATH", tmp_path / "notifications.json")
    monkeypatch.setattr(scim_store, "STORE_PATH", tmp_path / "scim.json")
    monkeypatch.setattr(auth_sessions, "_path", lambda: tmp_path / "sessions.json")
    reset_auth_rate_limits()

    from src.main import app

    db = svc.get_database()
    try:
        yield Ent(app, db, tmp_path)
    finally:
        svc.client.drop_database(name)
        svc.disconnect()
