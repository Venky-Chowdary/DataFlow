from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services import audit_log, connector_store, schedule_store, team_store, user_store
from services.auth_rate_limit import reset_auth_rate_limits
from src.main import app


class _MongoConnectors:
    def __init__(self):
        self.rows = {}

    def get_connector(self, connector_id):
        return self.rows.get(connector_id)

    def list_connectors(self):
        return list(self.rows.values())


class _Report:
    def to_dict(self):
        return {"passed": True, "source": {"table": "orders"}}


@pytest.mark.parametrize("isolation", [True, False])
def test_legacy_resources_follow_isolation_mode(tmp_path, monkeypatch, isolation):
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_REQUIRE_WORKSPACE", "1")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", "root-legacy@example.com")
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", "Bootstrap-Admin-2026")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "legacy-workspace-test-secret")
    monkeypatch.setattr(team_store, "mongo_database", lambda: None)
    monkeypatch.setattr(user_store, "mongo_database", lambda: None)
    monkeypatch.setattr(schedule_store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(schedule_store, "_mongo_backend", lambda: None)
    monkeypatch.setattr(connector_store, "STORE_PATH", tmp_path / "connectors.json")
    monkeypatch.setattr(connector_store, "_backend_choice", "file")
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    reset_auth_rate_limits()

    admin = TestClient(app)
    login = admin.post(
        "/api/v1/auth/login",
        json={"email": "root-legacy@example.com", "password": "Bootstrap-Admin-2026"},
    )
    assert login.status_code == 200, login.text
    admin.headers["Authorization"] = f"Bearer {login.json()['token']}"
    workspace_ids = []
    for label in ("A", "B"):
        response = admin.post("/api/v1/team/workspaces", json={"name": f"Legacy {label}"})
        assert response.status_code == 200, response.text
        workspace_ids.append(response.json()["workspace"]["id"])
    workspace_a, workspace_b = workspace_ids
    created = admin.post(
        "/api/v1/team/users",
        json={
            "email": "legacy-editor@example.com",
            "platform_role": "member",
            "workspace_id": workspace_a,
            "workspace_role": "editor",
        },
    )
    assert created.status_code == 200, created.text
    editor = TestClient(app)
    editor_login = editor.post(
        "/api/v1/auth/login",
        json={
            "email": "legacy-editor@example.com",
            "password": created.json()["temporary_password"],
        },
    )
    assert editor_login.status_code == 200, editor_login.text
    editor.headers["Authorization"] = f"Bearer {editor_login.json()['token']}"
    editor.headers["X-Workspace-Id"] = workspace_a

    legacy_connector = connector_store.create_connector({
        "name": "legacy-connector",
        "type": "postgresql",
        "host": "localhost",
        "port": 5432,
        "workspace_id": "",
    })
    foreign_connector = connector_store.create_connector({
        "name": "foreign-connector",
        "type": "postgresql",
        "host": "localhost",
        "port": 5432,
        "workspace_id": workspace_b,
    })
    legacy_schedule = schedule_store.create_schedule({
        "name": "legacy-schedule",
        "source_connector_id": legacy_connector.id,
        "source_table": "orders",
        "dest_connector_id": legacy_connector.id,
        "dest_table": "orders_copy",
        "interval": "daily",
        "workspace_id": "",
        "mappings": [{"source": "id", "target": "id"}],
    })
    foreign_schedule = schedule_store.create_schedule({
        "name": "foreign-schedule",
        "source_connector_id": foreign_connector.id,
        "source_table": "orders",
        "dest_connector_id": foreign_connector.id,
        "dest_table": "orders_copy",
        "interval": "daily",
        "workspace_id": workspace_b,
        "mappings": [{"source": "id", "target": "id"}],
    })
    mongo = _MongoConnectors()
    for connector in (legacy_connector, foreign_connector):
        mongo.rows[connector.id] = {
            **connector.to_dict(),
            "_id": connector.id,
            "id": connector.id,
            "workspace_id": connector.workspace_id,
        }
    import importlib

    connectors_router = importlib.import_module("src.routers.connectors_router")
    fidelity_router = importlib.import_module("src.routers.fidelity_router")
    monkeypatch.setattr(connectors_router, "get_mongodb_service", lambda: mongo)
    monkeypatch.setattr(fidelity_router, "run_fidelity_check", lambda **_kwargs: _Report())

    monkeypatch.setenv("DATAFLOW_REQUIRE_WORKSPACE", "1" if isolation else "0")
    headers = {"X-Workspace-Id": workspace_a}
    schedules = editor.get("/api/v1/schedules/", headers=headers)
    assert schedules.status_code == 200, schedules.text
    listed_schedule_ids = {row["id"] for row in schedules.json()}
    connectors = editor.get("/api/v1/connectors/", headers=headers)
    assert connectors.status_code == 200, connectors.text
    listed_connector_ids = {row["id"] for row in connectors.json()["connectors"]}

    legacy_schedule_read = editor.get(f"/api/v1/schedules/{legacy_schedule.id}", headers=headers)
    legacy_connector_read = editor.get(f"/api/v1/connectors/{legacy_connector.id}", headers=headers)
    foreign_schedule_read = editor.get(f"/api/v1/schedules/{foreign_schedule.id}", headers=headers)
    foreign_connector_read = editor.get(f"/api/v1/connectors/{foreign_connector.id}", headers=headers)
    plan_legacy = editor.post(
        "/api/v1/schedules/gitops/plan",
        headers=headers,
        json={
            "kind": "PipelineSchedule",
            "spec": {"id": legacy_schedule.id, "name": legacy_schedule.name},
        },
    )
    plan_foreign = editor.post(
        "/api/v1/schedules/gitops/plan",
        headers=headers,
        json={
            "kind": "PipelineSchedule",
            "spec": {"id": foreign_schedule.id, "name": foreign_schedule.name},
        },
    )
    fidelity_legacy = editor.post(
        "/api/v1/fidelity/check",
        headers=headers,
        json={"schedule_id": legacy_schedule.id},
    )
    fidelity_foreign = editor.post(
        "/api/v1/fidelity/check",
        headers=headers,
        json={"schedule_id": foreign_schedule.id},
    )

    if isolation:
        assert legacy_schedule.id not in listed_schedule_ids
        assert legacy_connector.id not in listed_connector_ids
        assert legacy_schedule_read.status_code in {403, 404}
        assert legacy_connector_read.status_code in {403, 404}
        assert plan_legacy.status_code in {403, 404}
        assert fidelity_legacy.status_code in {403, 404}
    else:
        assert legacy_schedule.id in listed_schedule_ids
        assert legacy_connector.id in listed_connector_ids
        assert legacy_schedule_read.status_code == 200, legacy_schedule_read.text
        assert legacy_connector_read.status_code == 200, legacy_connector_read.text
        assert plan_legacy.status_code == 200, plan_legacy.text
        assert fidelity_legacy.status_code == 200, fidelity_legacy.text

    assert foreign_schedule.id not in listed_schedule_ids
    assert foreign_connector.id not in listed_connector_ids
    assert foreign_schedule_read.status_code in {403, 404}
    assert foreign_connector_read.status_code in {403, 404}
    assert plan_foreign.status_code in {403, 404}
    assert fidelity_foreign.status_code in {403, 404}
