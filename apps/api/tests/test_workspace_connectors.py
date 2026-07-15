"""Workspace-scoped connector isolation tests."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from fastapi.testclient import TestClient

from services.team_store import create_workspace
from src.main import app


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))
    with TestClient(app) as c:
        yield c


def test_connector_created_with_workspace_id(client, tmp_path, monkeypatch):
    ws = create_workspace(name="Analytics", created_by="anonymous")
    payload = {
        "name": "Analytics SQLite",
        "type": "sqlite",
        "host": str(tmp_path / "w1.db"),
        "port": 0,
        "database": str(tmp_path / "w1.db"),
        "role": "both",
    }
    response = client.post("/api/v1/connectors/", json=payload, headers={"X-Workspace-Id": ws.id})
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["workspace_id"] == ws.id

    # Listing without workspace returns empty (connector is scoped)
    response = client.get("/api/v1/connectors/")
    assert response.status_code == 200
    assert data["id"] not in {c["id"] for c in response.json()["connectors"]}

    # Listing with workspace returns the connector
    response = client.get("/api/v1/connectors/", headers={"X-Workspace-Id": ws.id})
    assert response.status_code == 200
    assert data["id"] in {c["id"] for c in response.json()["connectors"]}


def test_unrelated_user_cannot_read_workspace_connector(client, tmp_path, monkeypatch):
    ws = create_workspace(name="Finance", created_by="finance@example.com")
    payload = {
        "name": "Finance SQLite",
        "type": "sqlite",
        "host": str(tmp_path / "fin.db"),
        "port": 0,
        "database": str(tmp_path / "fin.db"),
        "role": "both",
    }
    create = client.post("/api/v1/connectors/", json=payload, headers={"X-Workspace-Id": ws.id})
    # Anonymous is not a member of Finance workspace.
    assert create.status_code == 403, create.text
