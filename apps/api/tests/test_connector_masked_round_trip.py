from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services import connector_store
from services.secret_config import RedactedConfig, redact_config
from src.main import app


WEBHOOK_URL = "https://hooks.slack.com/services/T0/B0/SENTINEL1"
SERVICE_ACCOUNT = "SENTINEL2-json"
PASSWORD = "SENTINEL3"
API_KEY = "SENTINEL4"
PRIVATE_KEY = "SENTINEL5-private-key"


@pytest.fixture
def connector_api(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "0")
    monkeypatch.setenv("DATAFLOW_REQUIRE_WORKSPACE", "0")
    monkeypatch.setattr(connector_store, "_backend_choice", "file")

    with TestClient(app) as client:
        yield client


def _payload(name: str = "Masked round-trip connector") -> dict[str, object]:
    return {
        "name": name,
        "type": "postgresql",
        "role": "both",
        "host": "db.example.test",
        "port": 5432,
        "database": "warehouse",
        "username": "svc_dataflow",
        "password": PASSWORD,
        "service_account": SERVICE_ACCOUNT,
        "api_key": API_KEY,
        "private_key": PRIVATE_KEY,
    }


def test_saved_connector_drops_unmodeled_webhook_url_without_leaking(connector_api):
    payload = _payload()
    payload["webhook_url"] = WEBHOOK_URL
    created = connector_api.post("/api/v1/connectors/saved", json=payload)
    assert created.status_code == 200, created.text
    connector_id = created.json()["id"]

    read = connector_api.get(f"/api/v1/connectors/saved/{connector_id}")
    assert read.status_code == 200, read.text
    assert WEBHOOK_URL not in read.text

    stored = connector_store.get_connector(connector_id)
    assert stored is not None
    raw_record = stored.to_dict()
    assert "webhook_url" not in raw_record
    assert WEBHOOK_URL not in json.dumps(raw_record, default=str)


def test_connector_api_round_trip_preserves_all_config_values(connector_api):
    created = connector_api.post("/api/v1/connectors/saved", json=_payload())
    assert created.status_code == 200, created.text
    connector_id = created.json()["id"]
    read = connector_api.get(f"/api/v1/connectors/saved/{connector_id}")
    assert read.status_code == 200, read.text
    read_payload = read.json()
    originals = {
        "password": PASSWORD,
        "api_key": API_KEY,
        "service_account": SERVICE_ACCOUNT,
        "private_key": PRIVATE_KEY,
    }
    for field in originals:
        assert read_payload[field] == "****"

    updated = connector_api.put(
        f"/api/v1/connectors/saved/{connector_id}", json=read_payload
    )
    assert updated.status_code == 200, updated.text

    stored = connector_store.get_connector(connector_id)
    assert stored is not None
    assert {field: getattr(stored, field) for field in originals} == originals

    for marker in ("***", "****"):
        for field in originals:
            update_payload = dict(read_payload)
            update_payload[field] = marker
            response = connector_api.put(
                f"/api/v1/connectors/saved/{connector_id}", json=update_payload
            )
            assert response.status_code == 200, response.text
            current = connector_store.get_connector(connector_id)
            assert current is not None
            assert {name: getattr(current, name) for name in originals} == originals


def test_redact_config_masks_sensitive_keys_without_changing_mapping_values():
    config = {
        "webhook_url": WEBHOOK_URL,
        "service_account_json": SERVICE_ACCOUNT,
        "db_passcode": PASSWORD,
        "pin": API_KEY,
    }

    assert redact_config(config) == {key: "***" for key in config}
    assert RedactedConfig(config)["webhook_url"] == WEBHOOK_URL
