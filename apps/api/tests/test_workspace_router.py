"""Workspace / team management router tests."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import pytest
from fastapi.testclient import TestClient

from src.main import app


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))
    with TestClient(app) as c:
        yield c


def test_create_and_list_workspaces(client):
    response = client.post("/api/v1/workspace/workspaces", json={"name": "Data Platform"})
    assert response.status_code == 200, response.text
    ws = response.json()
    assert ws["name"] == "Data Platform"
    assert ws["id"]

    response = client.get("/api/v1/workspace/workspaces")
    assert response.status_code == 200
    assert ws["id"] in {w["id"] for w in response.json()["workspaces"]}


def test_add_and_remove_member(client):
    ws = client.post("/api/v1/workspace/workspaces", json={"name": "Team A"}).json()
    ws_id = ws["id"]

    response = client.post(
        f"/api/v1/workspace/workspaces/{ws_id}/members",
        json={"email": "editor@example.com", "role": "editor"},
    )
    assert response.status_code == 200, response.text
    assert response.json()["role"] == "editor"

    response = client.get(f"/api/v1/workspace/workspaces/{ws_id}/members")
    assert response.status_code == 200
    emails = {m["email"] for m in response.json()["members"]}
    assert "editor@example.com" in emails

    response = client.delete(
        f"/api/v1/workspace/workspaces/{ws_id}/members/editor@example.com"
    )
    assert response.status_code == 200
