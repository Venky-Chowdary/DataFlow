from __future__ import annotations

import json
import sys
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services import audit_log


def _event(
    event_id: str,
    *,
    workspace_id: str = "",
    details: dict | None = None,
    resource: str = "",
) -> dict:
    return {
        "id": event_id,
        "time": "2026-10-12T12:00:00+00:00",
        "action": "legacy.test",
        "workspace_id": workspace_id,
        "details": details or {},
        "resource": resource,
    }


def _write_events(path: Path, events: list[dict]) -> bytes:
    raw = b"".join(
        (json.dumps(event, separators=(",", ":")) + "\n").encode("utf-8")
        for event in events
    )
    path.write_bytes(raw)
    return raw


@pytest.fixture
def audit_scan_context(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_REQUIRE_WORKSPACE", "1")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "audit-scan-secret-" + "x" * 40)
    monkeypatch.setenv("DATAFLOW_ENV", "development")
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))

    from services import integrations_store, team_store
    from src.services import auth_service

    monkeypatch.setattr(auth_service, "_REQUIRE_AUTH", True)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    monkeypatch.setattr(integrations_store, "STORE_PATH", tmp_path / "integrations.json")
    monkeypatch.setattr(integrations_store, "_keys_collection", lambda: None)
    monkeypatch.setattr(team_store, "mongo_database", lambda: None)
    monkeypatch.setattr(team_store, "_store_path", lambda: tmp_path / "teams.json")

    actor = "audit-scan@example.test"
    admin = integrations_store.create_api_key("Audit scan admin", actor, role="admin")
    viewer = integrations_store.create_api_key("Audit scan viewer", actor, role="viewer")
    workspace = team_store.create_workspace(name="Audit scan", created_by=actor)

    from src.main import app

    with TestClient(app) as client:
        yield {
            "admin_key": admin["key"],
            "viewer_key": viewer["key"],
            "workspace_id": workspace.id,
            "client": client,
            "path": audit_log.STORE_PATH,
        }


def _headers(context, key: str, *, workspace_id: str | None = None) -> dict[str, str]:
    headers = {"Authorization": f"Bearer {key}"}
    selected_workspace = (
        context["workspace_id"] if workspace_id is None else workspace_id
    )
    if selected_workspace:
        headers["X-Workspace-Id"] = selected_workspace
    return headers


def test_scan_audit_events_flags_only_unredacted_keys_and_secret_shapes(
    audit_scan_context,
):
    secret_key = f"SENTINEL{uuid4().hex}"
    note_shape = f"dfk_SENTINEL{uuid4().hex}"
    resource_shape = f"Bearer SENTINEL{uuid4().hex}"
    events = [
        _event("legacy-key", details={"client_secret": secret_key}),
        _event(
            "legacy-shape",
            details={"note": note_shape},
            resource=resource_shape,
        ),
        _event("clean", details={"note": "nothing sensitive"}),
        _event(
            "redacted",
            details={"client_secret": "[REDACTED]", "token": "***"},
        ),
    ]
    before = _write_events(audit_scan_context["path"], events)

    result = audit_log.scan_audit_events_for_secrets()

    assert result["scanned"] == 4
    assert result["flagged"] == 2
    findings = {event["id"]: event for event in result["events"]}
    assert set(findings) == {"legacy-key", "legacy-shape"}
    assert findings["legacy-key"] == {
        "id": "legacy-key",
        "time": events[0]["time"],
        "action": "legacy.test",
        "keys": ["details.client_secret"],
        "shapes": [],
    }
    assert findings["legacy-shape"] == {
        "id": "legacy-shape",
        "time": events[1]["time"],
        "action": "legacy.test",
        "keys": [],
        "shapes": ["details.note", "resource"],
    }
    serialized = json.dumps(result)
    assert all(
        secret not in serialized
        for secret in (secret_key, note_shape, resource_shape)
    )
    assert audit_scan_context["path"].read_bytes() == before


def test_secret_scan_endpoint_matches_verify_access_and_scopes_workspace(
    audit_scan_context,
):
    workspace_id = audit_scan_context["workspace_id"]
    own_secret = f"SENTINEL{uuid4().hex}"
    foreign_secret = f"dfk_SENTINEL{uuid4().hex}"
    events = [
        _event(
            "own-event",
            workspace_id=workspace_id,
            details={"client_secret": own_secret},
        ),
        _event(
            "foreign-event",
            workspace_id="ws-foreign",
            details={"note": foreign_secret},
        ),
    ]
    before = _write_events(audit_scan_context["path"], events)
    client = audit_scan_context["client"]

    # The scanner itself does not rewrite history. Each successful scan and
    # verify then appends its own access event (audit.secret_scan.read /
    # audit.verify.read) after the read. The next call counts those records.
    # They are not secret findings, and the foreign workspace stays invisible.
    expected_scanned = 1
    for role in ("admin", "viewer"):
        key = audit_scan_context[f"{role}_key"]
        headers = _headers(audit_scan_context, key)
        scan = client.get("/api/v1/audit/secret-scan", headers=headers)
        verify = client.get("/api/v1/audit/verify", headers=headers)

        assert scan.status_code == verify.status_code == 200, scan.text
        body = scan.json()
        assert body["scanned"] == expected_scanned and body["flagged"] == 1, body
        assert [event["id"] for event in body["events"]] == ["own-event"]
        assert own_secret not in scan.text
        assert foreign_secret not in scan.text
        expected_scanned += 2

    missing_workspace = _headers(
        audit_scan_context,
        audit_scan_context["admin_key"],
        workspace_id="",
    )
    scan_without_workspace = client.get(
        "/api/v1/audit/secret-scan", headers=missing_workspace
    )
    verify_without_workspace = client.get(
        "/api/v1/audit/verify", headers=missing_workspace
    )
    # D154: no header defaults to the key owner's own workspace (it used to
    # 400) — still that workspace only, never the foreign event.
    assert scan_without_workspace.status_code == verify_without_workspace.status_code == 200
    assert [event["id"] for event in scan_without_workspace.json()["events"]] == ["own-event"]
    assert foreign_secret not in scan_without_workspace.text
    assert verify_without_workspace.json()["workspace_id"] == audit_scan_context["workspace_id"]
    raw = audit_scan_context["path"].read_bytes()
    assert raw.startswith(before)
    appended = [
        json.loads(line).get("action")
        for line in raw[len(before):].decode().splitlines()
        if line.strip()
    ]
    assert appended == [
        "audit.secret_scan.read",
        "audit.verify.read",
        "audit.secret_scan.read",
        "audit.verify.read",
        "audit.secret_scan.read",
        "audit.verify.read",
    ]
