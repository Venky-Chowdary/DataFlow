"""Two workspaces, one editor in each, through the real REST app (file stores).

The cross-workspace regression tests share this: auth on, workspace isolation
enforced, a bootstrap platform admin, and two members who each belong to one
workspace only. ``world.a`` and ``world.b`` are TestClients that carry their
own bearer token and ``X-Workspace-Id``; ``world.as_workspace(ws)`` binds the
same tenant scope for store-level seeding without a request.
"""

from __future__ import annotations

import contextlib
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

import pytest
from fastapi.testclient import TestClient

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

ADMIN_EMAIL = "root-tenancy@example.com"
ADMIN_PASSWORD = "Bootstrap-Admin-2026"
EDITOR_A = "editor-a@tenancy.example.com"
EDITOR_B = "editor-b@tenancy.example.com"


@dataclass
class TenancyWorld:
    app: Any
    admin: TestClient
    a: TestClient
    b: TestClient
    workspace_a: str
    workspace_b: str
    data_dir: Path

    @contextlib.contextmanager
    def as_workspace(self, workspace_id: str, actor: str = "") -> Iterator[None]:
        """Seed through a store exactly as a request in ``workspace_id`` would."""
        from services.workspace_filter import WorkspaceFilter, bound_workspace_filter

        with bound_workspace_filter(
            WorkspaceFilter.only(workspace_id), stamp=workspace_id, actor=actor
        ):
            yield

    def member(self, email: str, workspace_id: str, role: str) -> TestClient:
        return _member(self.app, self.admin, email, workspace_id, role)

    def job(self, workspace_id: str, **fields: Any) -> str:
        """A stored job owned by ``workspace_id`` (in-memory job store)."""
        from services.mongodb_service import get_mongodb_service

        return get_mongodb_service().create_transfer_job(
            {"status": "completed", "workspace_id": workspace_id, "transfer_request": {}, **fields}
        )

    def add_membership(self, email: str, workspace_id: str, role: str) -> None:
        """A second membership for an account that already exists."""
        from services import team_store

        team_store.add_workspace_member(
            workspace_id=workspace_id, email=email, role=role, added_by=ADMIN_EMAIL
        )


def _login(app: Any, email: str, password: str) -> TestClient:
    client = TestClient(app)
    response = client.post("/api/v1/auth/login", json={"email": email, "password": password})
    assert response.status_code == 200, response.text
    client.headers["Authorization"] = f"Bearer {response.json()['token']}"
    return client


def _workspace(admin: TestClient, name: str) -> str:
    response = admin.post("/api/v1/team/workspaces", json={"name": name})
    assert response.status_code == 200, response.text
    return response.json()["workspace"]["id"]


def _member(app: Any, admin: TestClient, email: str, workspace_id: str, role: str) -> TestClient:
    created = admin.post(
        "/api/v1/team/users",
        json={
            "email": email,
            "platform_role": "member",
            "workspace_id": workspace_id,
            "workspace_role": role,
        },
    )
    assert created.status_code == 200, created.text
    client = _login(app, email, created.json()["temporary_password"])
    client.headers["X-Workspace-Id"] = workspace_id
    return client


@pytest.fixture
def tenancy_world(tmp_path, monkeypatch) -> Iterator[TenancyWorld]:
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))
    monkeypatch.delenv("DATAWRAP_DATA_DIR", raising=False)
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_CONTRACTS_PATH", str(tmp_path / "contracts.json"))
    # Exports land in the test's own folder, never in apps/api/exports.
    monkeypatch.delenv("DATAWRAP_EXPORTS_DIR", raising=False)
    monkeypatch.setenv("DATAFLOW_EXPORTS_DIR", str(tmp_path / "exports"))
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_REQUIRE_WORKSPACE", "1")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", ADMIN_EMAIL)
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", ADMIN_PASSWORD)
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "tenancy-test-secret-not-production")

    from services import (
        audit_log,
        connector_store,
        contract_store,
        mongodb_service,
        resource_acl,
        schedule_store,
        team_store,
        tenant_store,
        transfer_plan_store,
        user_store,
    )
    from services.auth_rate_limit import reset_auth_rate_limits
    from src.main import app

    monkeypatch.setattr(team_store, "mongo_database", lambda: None)
    monkeypatch.setattr(user_store, "mongo_database", lambda: None)
    monkeypatch.setattr(schedule_store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(schedule_store, "_mongo_backend", lambda: None)
    monkeypatch.setattr(connector_store, "STORE_PATH", tmp_path / "connectors.json")
    monkeypatch.setattr(connector_store, "_backend_choice", "file")
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    monkeypatch.setattr(resource_acl, "STORE_PATH", tmp_path / "resource-acls.jsonl")
    monkeypatch.setattr(transfer_plan_store, "STORE_PATH", tmp_path / "transfer_plans.json")
    monkeypatch.setattr(tenant_store, "STORE_PATH", tmp_path / "tenants.json")
    # One in-memory job store per test: jobs seeded here are the jobs the
    # routes (and every store that follows a job to its workspace) read.
    jobs = mongodb_service.MemoryMongoDBService()
    jobs.connect()
    monkeypatch.setattr(mongodb_service, "_mongodb_service", jobs)
    contract_store.reset_contract_store()
    reset_auth_rate_limits()

    admin = _login(app, ADMIN_EMAIL, ADMIN_PASSWORD)
    workspace_a = _workspace(admin, "Tenancy workspace A")
    workspace_b = _workspace(admin, "Tenancy workspace B")
    world = TenancyWorld(
        app=app,
        admin=admin,
        a=_member(app, admin, EDITOR_A, workspace_a, "editor"),
        b=_member(app, admin, EDITOR_B, workspace_b, "editor"),
        workspace_a=workspace_a,
        workspace_b=workspace_b,
        data_dir=tmp_path,
    )
    try:
        yield world
    finally:
        contract_store.reset_contract_store()


__all__ = ["ADMIN_EMAIL", "EDITOR_A", "EDITOR_B", "TenancyWorld", "tenancy_world"]
