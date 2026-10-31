"""A real deployment for the round-8 security tests, on either metadata store.

Authentication is on, the bootstrap platform admin comes from the environment
(``root@example.test``), the workspace owner is a stored account that signs in
with a password, and every credential is minted through the REST routes an
operator uses. ``backend`` is ``"file"`` (JSON files, no Mongo) or ``"mongo"``
(a throwaway database on the live local MongoDB; skipped when it is down).
"""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

ROOT = "root@example.test"
ROOT_PASSWORD = "Bootstrap-Admin-2026!"
OWNER = "owner@acme.test"
OWNER_PASSWORD = "Owner-Password-2026!"
PASSWORD = "Member-Password-2026!"


def _mongo_db():
    from pymongo import MongoClient
    from pymongo.errors import PyMongoError

    uri = os.environ.get("MONGODB_URI", "mongodb://127.0.0.1:27017")
    client = MongoClient(uri, serverSelectionTimeoutMS=1500)
    try:
        client.admin.command("ping")
    except PyMongoError:
        client.close()
        pytest.skip(f"no live MongoDB at {uri}")
    return client, client[f"dataflow_r8sec_{uuid.uuid4().hex[:10]}"]


@dataclass
class World:
    backend: str
    tmp: Path
    app: Any
    workspace_id: str
    db: Any = None
    sessions: dict[str, TestClient] = field(default_factory=dict)

    # ── identities ──────────────────────────────────────────────────────────
    def client(self, token: str | None = None) -> TestClient:
        c = TestClient(self.app)
        if token:
            c.headers["Authorization"] = f"Bearer {token}"
        c.headers["X-Workspace-Id"] = self.workspace_id
        return c

    def login(self, email: str, password: str) -> TestClient:
        from services.auth_rate_limit import reset_auth_rate_limits

        reset_auth_rate_limits()
        response = TestClient(self.app).post(
            "/api/v1/auth/login", json={"email": email, "password": password}
        )
        assert response.status_code == 200, response.text
        return self.client(response.json()["token"])

    def account(self, email: str, *, role: str = "member") -> None:
        from services import user_store

        user_store.create_user(email=email, password=PASSWORD, role=role, created_by=ROOT)

    def member(self, email: str, role: str, *, with_account: bool = True) -> None:
        """Seed a membership directly in the store (fixture setup, not under test)."""
        from services import team_store

        if with_account:
            self.account(email)
        team_store._write_membership(
            team_store.Membership(
                workspace_id=self.workspace_id, email=email, role=role, added_by=OWNER
            )
        )

    @property
    def owner(self) -> TestClient:
        if "owner" not in self.sessions:
            self.sessions["owner"] = self.login(OWNER, OWNER_PASSWORD)
        return self.sessions["owner"]

    @property
    def root(self) -> TestClient:
        if "root" not in self.sessions:
            self.sessions["root"] = self.login(ROOT, ROOT_PASSWORD)
        return self.sessions["root"]

    def mint_key(
        self,
        role: str,
        *,
        by: TestClient | None = None,
        scopes: list[str] | None = None,
        name: str | None = None,
    ) -> dict[str, Any]:
        """A workspace API key minted through POST /workspace/api-keys."""
        body: dict[str, Any] = {"name": name or f"qa-{role}", "role": role, "expires_in": "7d"}
        if scopes is not None:
            body["scopes"] = scopes
        response = (by or self.owner).post("/api/v1/workspace/api-keys", json=body)
        assert response.status_code == 200, response.text
        return response.json()

    def mint_service_account(
        self, role: str, scopes: list[str], *, by: TestClient | None = None
    ) -> dict[str, Any]:
        response = (by or self.owner).post(
            "/api/v1/iam/service-accounts",
            json={"name": f"qa-sa-{role}", "role": role, "scopes": scopes, "expires_in": "7d"},
        )
        assert response.status_code == 200, response.text
        return response.json()

    def as_key(self, key: dict[str, Any] | str) -> TestClient:
        return self.client(key["key"] if isinstance(key, dict) else key)

    # ── reads ───────────────────────────────────────────────────────────────
    def role_of(self, email: str) -> str:
        from services import team_store

        return team_store.get_workspace_role(workspace_id=self.workspace_id, email=email)

    def audit(self, action: str) -> list[dict[str, Any]]:
        from services import audit_log

        path = Path(audit_log.STORE_PATH)
        if not path.exists():
            return []
        events = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]
        return [e for e in events if e.get("action") == action]

    # ── team routes ─────────────────────────────────────────────────────────
    def add_member(self, c: TestClient, email: str, role: str):
        return c.post(
            f"/api/v1/team/workspaces/{self.workspace_id}/members",
            json={"email": email, "role": role},
        )

    def set_role(self, c: TestClient, email: str, role: str):
        return c.patch(
            f"/api/v1/team/workspaces/{self.workspace_id}/members/{email}",
            json={"role": role},
        )

    def remove(self, c: TestClient, email: str):
        return c.delete(f"/api/v1/team/workspaces/{self.workspace_id}/members/{email}")


def build_world(backend: str, tmp_path: Path, monkeypatch) -> tuple[World, Any]:
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "r8-security-" + "s" * 40)
    monkeypatch.setenv("DATAFLOW_ENV", "development")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", ROOT)
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", ROOT_PASSWORD)
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))
    monkeypatch.setenv("DATAFLOW_REQUIRE_WORKSPACE", "1")

    from services import audit_log, integrations_store, team_store, user_store
    from src.services import auth_service

    monkeypatch.setattr(auth_service, "_REQUIRE_AUTH", True)
    monkeypatch.setattr(integrations_store, "STORE_PATH", tmp_path / "integrations.json")
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    monkeypatch.setattr(team_store, "_store_path", lambda: tmp_path / "teams.json")
    from services import notification_store

    monkeypatch.setattr(notification_store, "STORE_PATH", tmp_path / "notifications.json")

    client = db = None
    if backend == "mongo":
        client, db = _mongo_db()
        monkeypatch.setattr(team_store, "mongo_database", lambda: db)
        monkeypatch.setattr(user_store, "mongo_database", lambda: db)
        monkeypatch.setattr(integrations_store, "_keys_collection", lambda: db["workspace_api_keys"])
        team_store._PREPARED_DATABASES.discard(db.name)
    else:
        monkeypatch.setattr(team_store, "mongo_database", lambda: None)
        monkeypatch.setattr(user_store, "mongo_database", lambda: None)
        monkeypatch.setattr(integrations_store, "_keys_collection", lambda: None)

    from src.main import app

    user_store.create_user(email=OWNER, password=OWNER_PASSWORD, role="member", created_by=ROOT)
    workspace = team_store.create_workspace(name="Acme", created_by=OWNER)
    world = World(backend=backend, tmp=tmp_path, app=app, workspace_id=workspace.id, db=db)
    return world, client


@pytest.fixture(params=["file", "mongo"])
def world(request, tmp_path, monkeypatch):
    w, client = build_world(request.param, tmp_path, monkeypatch)
    try:
        yield w
    finally:
        if client is not None:
            client.drop_database(w.db.name)
            client.close()
            from services import team_store

            team_store._PREPARED_DATABASES.discard(w.db.name)
