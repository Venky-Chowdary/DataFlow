"""SIEM formats and incremental audit cursor paging."""

from __future__ import annotations

import json
import os
import sys
import uuid
from datetime import datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


@pytest.fixture
def audit_client(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "0")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "test-audit-siem-" + "x" * 40)
    monkeypatch.setenv("DATAFLOW_ENV", "development")
    monkeypatch.setenv("DATAFLOW_VERSION", "1")

    from services import audit_log
    from src.services import auth_service

    monkeypatch.setattr(auth_service, "_REQUIRE_AUTH", False)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    from src.main import app

    with TestClient(app) as client:
        yield client, audit_log


def _append(audit_log, *, action="test.event", actor="operator", resource="/test", **kwargs):
    return audit_log.append_audit_event(
        action=action,
        actor=actor,
        resource=resource,
        workspace_id="workspace-siem",
        **kwargs,
    )


def test_ndjson_contains_one_parseable_hash_bearing_event_per_line(audit_client):
    client, audit_log = audit_client
    _append(audit_log, action="siem.one", details={"secret_field": "included-in-ndjson"})
    _append(audit_log, action="siem.two", details={"sequence": 2})

    response = client.get(
        "/api/v1/audit/export?format=ndjson",
        headers={"X-Workspace-Id": "workspace-siem"},
    )
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/x-ndjson")
    rows = [json.loads(line) for line in response.text.splitlines()]
    assert len(rows) == 2
    assert all(row["event_hash"] and "chain_seq" in row for row in rows)
    assert all("prev_hash" in row for row in rows)
    assert any(row["details"].get("secret_field") == "included-in-ndjson" for row in rows)


def test_cef_golden_escaping_and_details_omission(audit_client):
    client, audit_log = audit_client
    event = _append(
        audit_log,
        action="event|=\\tail",
        actor="actor|=\\first\nsecond",
        resource="resource|=\\first\nsecond",
        details={"must_not_appear": "CEF intentionally excludes details"},
    )

    response = client.get(
        "/api/v1/audit/export?format=cef",
        headers={"X-Workspace-Id": "workspace-siem"},
    )
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    lines = response.text.splitlines()
    assert len(lines) == 1

    def header_escape(value):
        return str(value).replace("\\", "\\\\").replace("|", "\\|")

    def extension_escape(value):
        return (
            str(value)
            .replace("\\", "\\\\")
            .replace("=", "\\=")
            .replace("\r\n", "\n")
            .replace("\r", "\n")
            .replace("\n", "\\n")
        )

    event_time = datetime.fromisoformat(event["time"].replace("Z", "+00:00"))
    epoch_ms = int(event_time.timestamp() * 1000)
    escaped_action = header_escape(event["action"])
    expected = (
        "CEF:0|Datawrap|Datawrap|1|"
        f"{escaped_action}|{escaped_action}|3|"
        f"rt={epoch_ms} "
        f"suser={extension_escape(event['actor'])} "
        f"act={extension_escape(event['action'])} "
        f"request={extension_escape(event['resource'])} "
        "cs1Label=workspace_id "
        f"cs1={extension_escape(event['workspace_id'])} "
        "cs2Label=event_hash "
        f"cs2={extension_escape(event['event_hash'])} "
        "cs3Label=prev_hash "
        f"cs3={extension_escape(event['prev_hash'] or '')} "
        "cn1Label=chain_seq "
        f"cn1={event['chain_seq']} "
        f"externalId={extension_escape(event['id'])}"
    )
    assert lines[0] == expected
    assert "must_not_appear" not in lines[0]


def test_after_seq_export_pages_are_contiguous_and_non_overlapping(audit_client):
    client, audit_log = audit_client
    expected = [
        _append(audit_log, action=f"page.{index}")["chain_seq"]
        for index in range(7)
    ]
    collected = []
    cursor = 0
    while True:
        response = client.get(
            "/api/v1/audit/export?format=json&limit=3&after_seq=" + str(cursor),
            headers={"X-Workspace-Id": "workspace-siem"},
        )
        assert response.status_code == 200
        payload = response.json()
        page = [row["chain_seq"] for row in payload["events"]]
        assert payload["next_after_seq"] == (page[-1] if page else cursor)
        assert response.headers["X-Next-After-Seq"] == str(payload["next_after_seq"])
        assert page == sorted(page)
        collected.extend(page)
        if len(page) < 3:
            break
        cursor = payload["next_after_seq"]

    assert collected == expected
    final = client.get(
        f"/api/v1/audit/export?format=json&after_seq={collected[-1]}",
        headers={"X-Workspace-Id": "workspace-siem"},
    )
    assert final.json()["events"] == []
    assert final.json()["next_after_seq"] == collected[-1]


LIVE_MONGO_URI = os.getenv("DATAFLOW_LIVE_MONGO_URI", "").strip()


@pytest.mark.skipif(not LIVE_MONGO_URI, reason="DATAFLOW_LIVE_MONGO_URI is not configured")
def test_live_mongo_after_seq_cursor_paging(monkeypatch, tmp_path):
    from pymongo import MongoClient
    from services import audit_log

    client = MongoClient(LIVE_MONGO_URI, serverSelectionTimeoutMS=3000)
    client.admin.command("ping")
    db = client[f"dataflow_audit_siem_{uuid.uuid4().hex}"]
    collection = db["audit_events"]
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: collection)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit-fallback.jsonl")
    try:
        expected = [
            audit_log.append_audit_event(
                action=f"live.page.{index}",
                actor="live-test",
                resource="/live",
                workspace_id="workspace-live",
            )["chain_seq"]
            for index in range(5)
        ]
        first = audit_log.list_audit_events(
            workspace_id="workspace-live", after_seq=0, limit=2
        )
        second = audit_log.list_audit_events(
            workspace_id="workspace-live", after_seq=first[-1]["chain_seq"], limit=2
        )
        third = audit_log.list_audit_events(
            workspace_id="workspace-live", after_seq=second[-1]["chain_seq"], limit=2
        )
        collected = [row["chain_seq"] for row in first + second + third]
        assert collected == expected
    finally:
        client.drop_database(db.name)
        client.close()
