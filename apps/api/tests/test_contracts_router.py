"""Tests for the Data Contract REST endpoints."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from fastapi.testclient import TestClient

from src.main import app
from src.services.contract_store import get_contract_store, reset_contract_store
from src.services.data_contract import ColumnRule, DataContract


@pytest.fixture(autouse=True)
def _reset_store(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_CONTRACTS_PATH", str(tmp_path / "contracts.json"))
    reset_contract_store()
    yield
    reset_contract_store()


def test_get_contract_404():
    with TestClient(app) as client:
        response = client.get("/api/v1/contracts/nonexistent")
        assert response.status_code == 404


def test_create_contract_from_transfer():
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/contracts/from-transfer",
            json={
                "name": "orders-to-warehouse",
                "source": {"kind": "file", "format": "csv"},
                "destination": {"kind": "database", "format": "postgresql"},
                "mappings": [
                    {"source": "id", "target": "id", "confidence": 0.99},
                    {"source": "amount", "target": "amount", "confidence": 0.9, "target_type": "DECIMAL"},
                ],
                "column_types": {"id": "INTEGER", "amount": "STRING"},
                "preflight_gates": [{"id": "mapping", "status": "pass", "message": "ok"}],
                "strict": True,
            },
        )
        assert response.status_code == 200, response.text
        data = response.json()
        assert data["name"] == "orders-to-warehouse"
        assert data["status"] == "draft"
        assert len(data["columns"]) == 2
        assert data["id"]

        listed = client.get("/api/v1/contracts")
        assert listed.status_code == 200
        assert any(c["id"] == data["id"] for c in listed.json()["contracts"])


def test_sign_and_get_contract():
    store = get_contract_store()
    contract = DataContract(
        name="test-contract",
        columns=[ColumnRule(source_name="id", target_name="id", source_type="INTEGER", target_type="INTEGER")],
    )
    store.save_contract(contract)

    with TestClient(app) as client:
        response = client.post(f"/api/v1/contracts/{contract.id}/sign", json={"strict": False})
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "signed"
        assert data["strict"] is False

        get_resp = client.get(f"/api/v1/contracts/{contract.id}")
        assert get_resp.status_code == 200
        assert get_resp.json()["id"] == contract.id


def test_breaker_reset():
    store = get_contract_store()
    contract = DataContract(name="breaker-test")
    store.save_contract(contract)
    breaker = store.get_breaker(contract.id)
    breaker.record_failure()
    breaker.record_failure()
    breaker.record_failure()
    store.save_breaker(breaker)

    with TestClient(app) as client:
        response = client.get(f"/api/v1/contracts/{contract.id}/breaker")
        assert response.status_code == 200
        assert response.json()["state"] == "open"

        reset = client.post(f"/api/v1/contracts/{contract.id}/breaker/reset")
        assert reset.status_code == 200
        assert reset.json()["state"] == "closed"


def test_contract_test_valid():
    store = get_contract_store()
    contract = DataContract(
        columns=[ColumnRule(source_name="id", target_name="id", source_type="INTEGER", target_type="INTEGER")],
    )
    store.save_contract(contract)

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/contracts/test",
            json={
                "contract_id": contract.id,
                "source": {"kind": "file", "format": "csv"},
                "destination": {"kind": "database", "format": "postgresql"},
                "column_types": {"id": "INTEGER"},
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["valid"] is True
        assert data["violations"] == []


def test_contract_test_invalid():
    store = get_contract_store()
    contract = DataContract(
        columns=[ColumnRule(source_name="id", target_name="id", source_type="INTEGER", target_type="INTEGER")],
    )
    store.save_contract(contract)

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/contracts/test",
            json={
                "contract_id": contract.id,
                "source": {"kind": "file", "format": "csv"},
                "destination": {"kind": "database", "format": "postgresql"},
                "column_types": {"id": "TEXT"},
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["valid"] is False
        assert data["violations"][0]["rule"] == "source_type_change"
