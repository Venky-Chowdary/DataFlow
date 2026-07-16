"""Notification channel store, service, and API tests."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from fastapi.testclient import TestClient


def _client():
    from src.main import app
    return TestClient(app)


def test_notification_store_crud(tmp_path, monkeypatch):
    from services import notification_store

    monkeypatch.setenv("DATAFLOW_NOTIFICATION_STORE", str(tmp_path / "notifications.json"))
    ch = notification_store.create_channel(
        workspace_id="ws-1",
        kind="slack",
        label="Ops Slack",
        config={"webhook_url": "https://hooks.slack.com/services/secret"},
    )
    assert ch.workspace_id == "ws-1"
    assert ch.kind == "slack"
    # Stored config should be encrypted.
    stored = notification_store.get_channel(ch.id)
    assert stored
    assert stored.config["webhook_url"].startswith("enc:") or stored.config["webhook_url"] != "https://hooks.slack.com/services/secret"

    decrypted = notification_store.get_channel_decrypted(ch.id)
    assert decrypted
    assert decrypted.config["webhook_url"] == "https://hooks.slack.com/services/secret"

    updated = notification_store.update_channel(ch.id, updates={"enabled": False})
    assert updated and updated.enabled is False
    assert notification_store.delete_channel(ch.id) is True


def test_build_job_payload():
    from services.notification_service import build_job_payload

    payload = build_job_payload(
        job_id="job-123",
        status="failed_with_quarantine",
        source="file/csv",
        destination="database/postgresql",
        records_transferred=999,
        rejected_rows=1,
        error="column age invalid",
        retry_url="/api/v1/connectors/jobs/job-123/resume",
    )
    assert payload["status"] == "failed_with_quarantine"
    assert "999" in payload["text"]
    assert "quarantined" in payload["text"].lower()


def test_workspace_notification_api(tmp_path, monkeypatch):
    from services import connector_store, notification_store

    monkeypatch.setenv("DATAFLOW_NOTIFICATION_STORE", str(tmp_path / "notifications.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    connector_store._backend_choice = None
    notification_store._load_raw()  # ensure path initialized

    client = _client()
    resp = client.post("/api/v1/workspace/notifications", json={
        "workspace_id": "",
        "kind": "slack",
        "label": "Test Slack",
        "config": {"webhook_url": "https://example.com/hook"},
    })
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["id"]

    get_resp = client.get("/api/v1/workspace/notifications?workspace_id=")
    assert get_resp.status_code == 200
    assert len(get_resp.json()["channels"]) == 1

    test_resp = client.post(f"/api/v1/workspace/notifications/{data['id']}/test")
    assert test_resp.status_code == 200
    # Webhook to example.com will fail network, but the endpoint should return the result.
    assert test_resp.json()["success"] is False

    del_resp = client.delete(f"/api/v1/workspace/notifications/{data['id']}")
    assert del_resp.status_code == 200
