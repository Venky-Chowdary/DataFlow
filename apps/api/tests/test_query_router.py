"""Query playground endpoint tests."""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from fastapi.testclient import TestClient

from services import connector_store


def _sqlite_db(tmp_path: Path):
    db_path = tmp_path / "playground.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    conn.execute("INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob')")
    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def test_client():
    from src.main import app
    return TestClient(app)


def test_query_sqlite_select(test_client, tmp_path, monkeypatch):
    db_path = _sqlite_db(tmp_path)
    connector_store.create_connector({
        "name": "Test SQLite",
        "type": "sqlite",
        "role": "both",
        "connection_string": f"sqlite:///{db_path}",
        "workspace_id": "",
    })
    # refresh list to get assigned id
    all_conns = connector_store.list_connectors()
    conn = [c for c in all_conns if c.name == "Test SQLite"][0]

    response = test_client.post("/api/v1/query/execute", json={
        "connector_id": conn.id,
        "query": "SELECT * FROM users ORDER BY id",
        "limit": 100,
    })
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["row_count"] == 2
    assert data["columns"] == ["id", "name"]
    assert data["rows"][0]["name"] == "alice"


def test_query_blocks_non_select(test_client, tmp_path, monkeypatch):
    db_path = _sqlite_db(tmp_path)
    connector_store.create_connector({
        "name": "Test SQLite 2",
        "type": "sqlite",
        "role": "both",
        "connection_string": f"sqlite:///{db_path}",
        "workspace_id": "",
    })
    all_conns = connector_store.list_connectors()
    conn = [c for c in all_conns if c.name == "Test SQLite 2"][0]

    response = test_client.post("/api/v1/query/execute", json={
        "connector_id": conn.id,
        "query": "DROP TABLE users",
    })
    assert response.status_code == 400


def test_query_export_csv(test_client, tmp_path, monkeypatch):
    db_path = _sqlite_db(tmp_path)
    connector_store.create_connector({
        "name": "Test SQLite 3",
        "type": "sqlite",
        "role": "both",
        "connection_string": f"sqlite:///{db_path}",
        "workspace_id": "",
    })
    all_conns = connector_store.list_connectors()
    conn = [c for c in all_conns if c.name == "Test SQLite 3"][0]

    response = test_client.post("/api/v1/query/export", json={
        "connector_id": conn.id,
        "query": "SELECT * FROM users",
        "format": "csv",
    })
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["success"] is True
    assert data["row_count"] == 2
    assert data["download_url"].startswith("/api/v1/transfer/download/")
