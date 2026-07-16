"""Query playground endpoint tests."""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

from src.routers.query_router import _is_read_only_sql, _validate_mongodb_aggregate

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


def _isolated_store(monkeypatch, tmp_path: Path):
    """Force connector_store to use a fresh file store under tmp_path."""
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    connector_store._backend_choice = None


@pytest.fixture
def test_client():
    from src.main import app
    return TestClient(app)


def test_query_sqlite_select(test_client, tmp_path, monkeypatch):
    _isolated_store(monkeypatch, tmp_path)
    db_path = _sqlite_db(tmp_path)
    conn = connector_store.create_connector({
        "name": "Test SQLite",
        "type": "sqlite",
        "role": "both",
        "connection_string": f"sqlite:///{db_path}",
        "workspace_id": "",
    })

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
    _isolated_store(monkeypatch, tmp_path)
    db_path = _sqlite_db(tmp_path)
    conn = connector_store.create_connector({
        "name": "Test SQLite 2",
        "type": "sqlite",
        "role": "both",
        "connection_string": f"sqlite:///{db_path}",
        "workspace_id": "",
    })

    response = test_client.post("/api/v1/query/execute", json={
        "connector_id": conn.id,
        "query": "DROP TABLE users",
    })
    assert response.status_code == 400


def test_query_export_csv(test_client, tmp_path, monkeypatch):
    _isolated_store(monkeypatch, tmp_path)
    db_path = _sqlite_db(tmp_path)
    conn = connector_store.create_connector({
        "name": "Test SQLite 3",
        "type": "sqlite",
        "role": "both",
        "connection_string": f"sqlite:///{db_path}",
        "workspace_id": "",
    })

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


def test_read_only_sql_guard():
    assert _is_read_only_sql("SELECT * FROM users") is True
    assert _is_read_only_sql("SELECT * FROM users;") is True
    assert _is_read_only_sql("DROP TABLE users") is False
    assert _is_read_only_sql("WITH d AS (DELETE FROM users RETURNING *) SELECT * FROM d") is False
    assert _is_read_only_sql("SELECT * INTO OUTFILE '/tmp/x' FROM users") is False
    assert _is_read_only_sql("SELECT * FROM users; DROP TABLE users") is False


def test_aggregate_stage_guard_blocks_writes():
    _validate_mongodb_aggregate([{"$match": {}}])  # ok
    with pytest.raises(Exception) as exc:
        _validate_mongodb_aggregate([{"$match": {}}, {"$out": "stolen"}])
    assert "$out" in str(exc.value)
