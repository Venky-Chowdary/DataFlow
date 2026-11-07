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


def test_smtp_password_is_encrypted_at_rest(tmp_path, monkeypatch):
    from services import notification_store

    monkeypatch.setenv("DATAFLOW_NOTIFICATION_STORE", str(tmp_path / "notifications.json"))
    ch = notification_store.create_channel(
        workspace_id="ws-1",
        kind="email",
        label="SMTP",
        config={"smtp_password": "super-secret-mail", "host": "smtp.example.com"},
    )
    stored = notification_store.get_channel(ch.id)
    assert stored
    assert stored.config["smtp_password"] != "super-secret-mail"
    decrypted = notification_store.get_channel_decrypted(ch.id)
    assert decrypted
    assert decrypted.config["smtp_password"] == "super-secret-mail"


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


def test_only_slack_and_teams_acknowledgements_count_as_delivered(monkeypatch):
    """A 200 from something that is not Slack/Teams is not a delivered alert.

    "Test message sent" was rendered for any HTTP 2xx, so a webhook aimed at the
    wrong host read as a working alert route until an incident went unnoticed.
    """
    from services import notification_service
    from services.notification_store import NotificationChannel

    answers: dict[str, object] = {}
    monkeypatch.setattr(notification_service, "_http_post", lambda *a, **k: answers)

    slack = NotificationChannel(
        id="c1", workspace_id="ws", kind="slack", label="Ops",
        config={"webhook_url": "https://hooks.slack.com/services/x"},
    )
    teams = NotificationChannel(
        id="c2", workspace_id="ws", kind="teams", label="Ops",
        config={"webhook_url": "https://example.webhook.office.com/x"},
    )

    # An intranet page answering 200 is not an acknowledgement.
    answers = {"ok": True, "status": 200, "body": "<html>hello</html>"}
    refused = notification_service._send_slack(slack, {"text": "hi"})
    assert refused["ok"] is False
    assert "did not acknowledge as Slack" in refused["error"]
    refused_teams = notification_service._send_teams(teams, {"text": "hi"})
    assert refused_teams["ok"] is False
    assert "Microsoft Teams" in refused_teams["error"]

    # What each provider actually answers.
    answers = {"ok": True, "status": 200, "body": "ok"}
    assert notification_service._send_slack(slack, {"text": "hi"})["ok"] is True
    answers = {"ok": True, "status": 200, "body": '{"ok": true}'}
    assert notification_service._send_slack(slack, {"text": "hi"})["ok"] is True
    answers = {"ok": True, "status": 200, "body": "1"}
    assert notification_service._send_teams(teams, {"text": "hi"})["ok"] is True
    # Teams Workflows accepts without a body.
    answers = {"ok": True, "status": 202, "body": ""}
    assert notification_service._send_teams(teams, {"text": "hi"})["ok"] is True

    # A transport failure keeps its own reason.
    answers = {"ok": False, "error": "HTTP Error 404: Not Found"}
    assert notification_service._send_slack(slack, {"text": "hi"})["error"] == "HTTP Error 404: Not Found"
