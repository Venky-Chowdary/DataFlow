"""Cross-workspace schedule routes must 404, not run or delete the other tenant."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services import schedule_store, team_store, user_store
from services.auth_rate_limit import reset_auth_rate_limits
from src.main import app


@pytest.fixture
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_REQUIRE_WORKSPACE", "1")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", "root@example.com")
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", "Bootstrap-Admin-2026")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "test-secret-not-production")
    monkeypatch.setattr(team_store, "mongo_database", lambda: None)
    monkeypatch.setattr(user_store, "mongo_database", lambda: None)
    monkeypatch.setattr(schedule_store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(schedule_store, "_mongo_backend", lambda: None)
    reset_auth_rate_limits()
    return tmp_path


def _admin(isolated) -> TestClient:
    c = TestClient(app)
    login = c.post(
        "/api/v1/auth/login",
        json={"email": "root@example.com", "password": "Bootstrap-Admin-2026"},
    )
    assert login.status_code == 200, login.text
    c.headers["Authorization"] = f"Bearer {login.json()['token']}"
    return c


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
            "workspace_role": "editor",
        },
    )
    assert created.status_code == 200, created.text
    issued = created.json()["temporary_password"]
    peer = TestClient(app)
    login = peer.post("/api/v1/auth/login", json={"email": email, "password": issued})
    assert login.status_code == 200, login.text
    peer.headers["Authorization"] = f"Bearer {login.json()['token']}"
    peer.headers["X-Workspace-Id"] = workspace_id
    return peer


def _schedule(workspace_id: str, name: str = "Nightly") -> schedule_store.PipelineSchedule:
    return schedule_store.create_schedule(
        {
            "name": name,
            "source_connector_id": "src-1",
            "source_table": "orders",
            "dest_connector_id": "dst-1",
            "dest_table": "orders_wh",
            "interval": "daily",
            "workspace_id": workspace_id,
        }
    )


def test_editor_cannot_run_or_delete_another_workspace_schedule(isolated):
    admin = _admin(isolated)
    mine = _workspace(admin, "Mine")
    theirs = _workspace(admin, "Theirs")
    foreign = _schedule(theirs, "Theirs nightly")
    editor = _member(admin, email="ed@example.com", workspace_id=mine)

    listed = editor.get("/api/v1/schedules/")
    assert listed.status_code == 200, listed.text
    assert all(row["id"] != foreign.id for row in listed.json())

    for method, path in (
        ("GET", f"/api/v1/schedules/{foreign.id}"),
        ("GET", f"/api/v1/schedules/{foreign.id}/history"),
        ("POST", f"/api/v1/schedules/{foreign.id}/run"),
        ("DELETE", f"/api/v1/schedules/{foreign.id}"),
    ):
        resp = editor.request(method, path)
        assert resp.status_code == 404, (method, path, resp.status_code, resp.text)

    assert schedule_store.get_schedule(foreign.id) is not None


def test_unscoped_schedule_hidden_when_isolation_required(isolated):
    admin = _admin(isolated)
    ws = _workspace(admin, "Acme")
    schedule_store.create_schedule(
        {
            "name": "Legacy unscoped",
            "source_connector_id": "src-1",
            "source_table": "orders",
            "dest_connector_id": "dst-1",
            "dest_table": "orders_wh",
            "interval": "daily",
            "workspace_id": "",
        }
    )
    editor = _member(admin, email="ed2@example.com", workspace_id=ws)
    listed = editor.get("/api/v1/schedules/")
    assert listed.status_code == 200, listed.text
    assert listed.json() == []
