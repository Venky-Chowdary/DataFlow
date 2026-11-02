"""Tenant-scoped audit export — one workspace cannot sample another's events.

Not a SOC 2 Type II letter. Proves the evidence slice an auditor can
download is isolated by X-Workspace-Id.
"""
from __future__ import annotations

import csv
import io
import json
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services import audit_log as audit
from src.main import app


@pytest.fixture
def isolated_audit(tmp_path, monkeypatch):
    monkeypatch.setattr(audit, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit, "_mongo_collection", lambda: None)
    return tmp_path


@pytest.fixture
def client(isolated_audit):
    return TestClient(app)


def _auth(workspace_id: str) -> dict[str, str]:
    return {"X-Workspace-Id": workspace_id}


def test_list_and_export_are_workspace_scoped(client):
    a = "ws-audit-scope-a"
    b = "ws-audit-scope-b"
    audit.append_audit_event(
        action="connector.create",
        resource="/connectors",
        actor="auditor@example.com",
        workspace_id=a,
        details={"name": "alpha"},
    )
    audit.append_audit_event(
        action="connector.create",
        resource="/connectors",
        actor="auditor@example.com",
        workspace_id=b,
        details={"name": "beta"},
    )
    audit.append_audit_event(
        action="system.unscoped",
        resource="/system",
        actor="auditor@example.com",
    )

    listed_a = client.get("/api/v1/audit/events?limit=200", headers=_auth(a))
    assert listed_a.status_code == 200
    actions_a = {e["action"] for e in listed_a.json()["events"]}
    assert "connector.create" in actions_a
    assert "system.unscoped" not in actions_a
    names_a = [e.get("details", {}).get("name") for e in listed_a.json()["events"]]
    assert "alpha" in names_a
    assert "beta" not in names_a

    listed_b = client.get("/api/v1/audit/events?limit=200", headers=_auth(b))
    names_b = [e.get("details", {}).get("name") for e in listed_b.json()["events"]]
    assert "beta" in names_b
    assert "alpha" not in names_b

    csv_res = client.get("/api/v1/audit/export?format=csv&limit=200", headers=_auth(a))
    assert csv_res.status_code == 200
    assert "text/csv" in csv_res.headers.get("content-type", "")
    assert "workspace-id=ws-audit-scope-a" in csv_res.headers.get("content-disposition", "")
    body = csv_res.text
    assert "NOT a SOC 2 Type II letter" in body
    lines = [ln for ln in body.splitlines() if not ln.startswith("#")]
    rows = list(csv.DictReader(io.StringIO("\n".join(lines))))
    assert rows
    assert all(r.get("workspace_id") == a for r in rows)
    assert any(r.get("action") == "connector.create" for r in rows)
    assert not any("beta" in (r.get("details") or "") for r in rows)
    assert any("alpha" in (r.get("details") or "") for r in rows)

    json_res = client.get("/api/v1/audit/export?format=json&limit=200", headers=_auth(b))
    assert json_res.status_code == 200
    payload = json_res.json()
    assert payload["workspace_id"] == b
    assert payload["attestation"]["official"] is False
    assert payload["count"] == len(payload["events"])
    assert all(e.get("workspace_id") == b for e in payload["events"])
    assert any(e.get("details", {}).get("name") == "beta" for e in payload["events"])
    assert not any(e.get("details", {}).get("name") == "alpha" for e in payload["events"])


def test_export_rejects_unknown_format(client):
    res = client.get("/api/v1/audit/export?format=pdf", headers=_auth("ws-x"))
    assert res.status_code == 400


def test_export_requires_workspace_header(client):
    res = client.get("/api/v1/audit/export?format=csv")
    assert res.status_code == 400


def test_unscoped_events_stay_out_of_scoped_list(isolated_audit):
    ws = "ws-only-scoped"
    audit.append_audit_event(action="keep.scoped", resource="r", actor="a@b.c", workspace_id=ws)
    audit.append_audit_event(action="keep.global", resource="r", actor="a@b.c")

    scoped = audit.list_audit_events(limit=500, workspace_id=ws)
    actions = {e["action"] for e in scoped}
    assert "keep.scoped" in actions
    assert "keep.global" not in actions
