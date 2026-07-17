"""Verify saved connector test status is persisted and reflected in list responses."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def _fake_probe_ok(*args, **kwargs):
    return True, "Connection successful"


def _fake_probe_fail(*args, **kwargs):
    return False, "Connection refused"


def test_saved_connector_test_success_updates_card_status(monkeypatch, tmp_path: Path) -> None:
    """A successful probe must make /connectors/saved return status 'configured'."""
    store = tmp_path / "connectors.json"
    store.write_text('{"connectors": []}', encoding="utf-8")
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(store))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    # Ensure no leftover backend cache from other imports.
    import services.connector_store as cs
    monkeypatch.setattr(cs, "_backend_choice", "file")

    monkeypatch.setattr("src.transfer.connector_registry.run_probe", _fake_probe_ok)

    from fastapi.testclient import TestClient

    from src.main import app

    client = TestClient(app)
    payload = {
        "name": "Test PG",
        "type": "postgresql",
        "role": "both",
        "host": "localhost",
        "port": 5432,
        "database": "test_db",
        "username": "user",
        "password": "pass",
        "schema": "public",
    }
    r = client.post("/api/v1/connectors/saved", json=payload)
    assert r.status_code == 200, r.text
    created = r.json()
    connector_id = created["id"]

    # First test fails
    monkeypatch.setattr("src.transfer.connector_registry.run_probe", _fake_probe_fail)
    r = client.post(f"/api/v1/connectors/saved/{connector_id}/test")
    assert r.status_code == 200, r.text
    assert r.json()["success"] is False

    # Card should show error
    r = client.get("/api/v1/connectors/saved")
    assert r.status_code == 200, r.text
    found = [c for c in r.json()["connectors"] if c["id"] == connector_id][0]
    assert found["status"] == "error"

    # Re-test succeeds
    monkeypatch.setattr("src.transfer.connector_registry.run_probe", _fake_probe_ok)
    r = client.post(f"/api/v1/connectors/saved/{connector_id}/test")
    assert r.status_code == 200, r.text
    assert r.json()["success"] is True

    # Card should now show configured
    r = client.get("/api/v1/connectors/saved")
    assert r.status_code == 200, r.text
    found = [c for c in r.json()["connectors"] if c["id"] == connector_id][0]
    assert found["status"] == "configured"


def test_connector_store_env_override(monkeypatch, tmp_path: Path) -> None:
    """DATAFLOW_CONNECTOR_STORE should make connector CRUD use the given file."""
    store = tmp_path / "custom_connectors.json"
    store.write_text('{"connectors": []}', encoding="utf-8")
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(store))

    import services.connector_store as cs
    monkeypatch.setattr(cs, "_backend_choice", "file")
    importlib = __import__("importlib")
    importlib.reload(cs)

    conn = cs.create_connector({"name": "Env PG", "type": "postgresql", "role": "both"})
    assert conn.id
    assert any(c.id == conn.id for c in cs.list_connectors())
    raw = json.loads(store.read_text(encoding="utf-8"))
    assert any(c["id"] == conn.id for c in raw["connectors"])
