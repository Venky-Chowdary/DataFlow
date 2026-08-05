"""Tests for the Migration Decision Kernel API."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from fastapi.testclient import TestClient

from src.main import app


@pytest.fixture(scope="module")
def client() -> TestClient:
    with TestClient(app) as c:
        yield c


def test_migration_decision_returns_decision_artifact(client: TestClient) -> None:
    payload = {
        "source": {
            "kind": "database",
            "format": "postgresql",
            "name": "src",
            "columns": [
                {"name": "id", "carrier": {"logical": "integer", "native": "INTEGER"}},
                {"name": "amount", "carrier": {"logical": "float", "native": "FLOAT"}},
            ],
        },
        "destination": {
            "kind": "database",
            "format": "postgresql",
            "name": "dst",
            "columns": [
                {"name": "id", "carrier": {"logical": "integer", "native": "INTEGER"}},
                {"name": "amount", "carrier": {"logical": "integer", "native": "INTEGER"}},
            ],
        },
        "dest_db": "postgresql",
        "validation_mode": "strict",
    }
    response = client.post("/api/v1/migration/decision", json=payload)
    assert response.status_code == 200, response.text
    decision = response.json()
    assert decision["decision_id"]
    assert decision["hash"]
    assert decision["version"] == "1.0.0"
    assert decision["mapping"]["requires_review"] is True
    assert any(
        c.get("verdict", {}).get("classification") == "lossy"
        for c in decision["mapping"]["columns"]
    )


def test_migration_decision_rejects_missing_dest_db(client: TestClient) -> None:
    payload = {
        "source": {"kind": "database", "format": "postgresql", "name": "src", "columns": []},
        "destination": {"kind": "database", "format": "postgresql", "name": "dst", "columns": []},
    }
    response = client.post("/api/v1/migration/decision", json=payload)
    assert response.status_code == 400


def test_migration_decision_identity_mapping(client: TestClient) -> None:
    payload = {
        "source": {
            "kind": "database",
            "format": "postgresql",
            "name": "src",
            "columns": [
                {"name": "name", "carrier": {"logical": "string", "native": "VARCHAR(100)"}},
            ],
        },
        "destination": {
            "kind": "database",
            "format": "postgresql",
            "name": "dst",
            "columns": [
                {"name": "name", "carrier": {"logical": "string", "native": "VARCHAR(100)"}},
            ],
        },
        "dest_db": "postgresql",
    }
    response = client.post("/api/v1/migration/decision", json=payload)
    assert response.status_code == 200, response.text
    decision = response.json()
    assert decision["validation"]["passed"] is True
    assert decision["validation"]["write_permitted"] is True
