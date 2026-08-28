"""Run now on a paused schedule is a one-shot, not 'check connectors'."""

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
from services.schedule_runner import ScheduleStartError
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
    client = TestClient(app)
    login = client.post(
        "/api/v1/auth/login",
        json={"email": "root@example.com", "password": "Bootstrap-Admin-2026"},
    )
    assert login.status_code == 200, login.text
    client.headers["Authorization"] = f"Bearer {login.json()['token']}"
    return client


def _workspace(client: TestClient, name: str) -> str:
    response = client.post("/api/v1/team/workspaces", json={"name": name})
    assert response.status_code == 200, response.text
    return response.json()["workspace"]["id"]


def _paused_schedule(workspace_id: str) -> schedule_store.PipelineSchedule:
    return schedule_store.create_schedule(
        {
            "name": "TestJob",
            "source_connector_id": "snowflake-venky",
            "source_table": "users",
            "dest_connector_id": "snowflake-venky",
            "dest_table": "yes123",
            "interval": "hourly",
            "sync_mode": "full_refresh_append",
            "enabled": False,
            "workspace_id": workspace_id,
            "mappings": [{"source": "id", "target": "id", "confidence": 1.0}],
        }
    )


def test_run_now_paused_forwards_manual_flag(isolated, monkeypatch):
    admin = _admin(isolated)
    workspace_id = _workspace(admin, "Acme")
    sched = _paused_schedule(workspace_id)
    assert sched.enabled is False
    seen: dict[str, object] = {}

    def fake_run(schedule_id: str, *, manual: bool = False):
        seen["sid"] = schedule_id
        seen["manual"] = manual
        return "job-now"

    monkeypatch.setattr("src.services.schedule_runner._run_schedule", fake_run)
    admin.headers["X-Workspace-Id"] = workspace_id
    resp = admin.post(f"/api/v1/schedules/{sched.id}/run")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["job_id"] == "job-now"
    assert seen.get("manual") is True
    assert seen.get("sid") == sched.id
    assert "check connectors" not in resp.text.lower()


def test_run_now_surfaces_conflict_not_connector_guess(isolated, monkeypatch):
    admin = _admin(isolated)
    workspace_id = _workspace(admin, "Acme")
    sched = _paused_schedule(workspace_id)

    def fake_run(schedule_id: str, *, manual: bool = False):
        raise ScheduleStartError(
            "A run is already in progress for this schedule or the same "
            "source→destination pair.",
            http_status=409,
            code="already_running",
        )

    monkeypatch.setattr("src.services.schedule_runner._run_schedule", fake_run)
    admin.headers["X-Workspace-Id"] = workspace_id
    resp = admin.post(f"/api/v1/schedules/{sched.id}/run")
    assert resp.status_code == 409, resp.text
    assert "already in progress" in resp.json()["detail"].lower()
    assert "check connectors" not in resp.text.lower()
