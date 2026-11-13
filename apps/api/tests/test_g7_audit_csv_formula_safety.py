"""G7 (CWE-1236): the audit CSV export never opens as a live formula.

Audit fields carry attacker-influenced text (actor names, resource paths,
details). Before the fix ``GET /audit/export?format=csv`` wrote them raw, so an
actor named ``=HYPERLINK("http://evil…")`` became a clickable formula in the
reviewer's spreadsheet. The CSV export is a human download: every cell is
neutralised, always. The JSON/NDJSON/CEF exports stay exact (machine formats).
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

HYPERLINK = '=HYPERLINK("http://evil.example/x","Click")'
WS = "workspace-g7-audit"


@pytest.fixture
def audit_client(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "0")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "test-audit-g7-" + "x" * 40)
    monkeypatch.setenv("DATAFLOW_ENV", "development")

    from services import audit_log
    from src.services import auth_service

    monkeypatch.setattr(auth_service, "_REQUIRE_AUTH", False)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    from src.main import app

    with TestClient(app) as client:
        yield client, audit_log


def _seed(audit_log):
    audit_log.append_audit_event(
        action="g7.formula", actor=HYPERLINK, resource="@SUM(A1)", workspace_id=WS,
        details={"note": "=1+1"},
    )
    audit_log.append_audit_event(
        action="g7.minus", actor="-2+3", resource="+cmd", workspace_id=WS,
    )
    audit_log.append_audit_event(
        action="g7.plain", actor="operator", resource="/plain", workspace_id=WS,
    )


def _csv_rows(text: str) -> list[dict[str, str]]:
    body = "\n".join(line for line in text.splitlines() if not line.startswith("#"))
    return list(csv.DictReader(io.StringIO(body)))


def test_audit_csv_export_neutralises_formula_cells(audit_client):
    client, audit_log = audit_client
    _seed(audit_log)
    response = client.get("/api/v1/audit/export?format=csv", headers={"X-Workspace-Id": WS})
    assert response.status_code == 200, response.text
    rows = {r["action"]: r for r in _csv_rows(response.text)}

    assert rows["g7.formula"]["actor"] == "'" + HYPERLINK
    assert rows["g7.formula"]["resource"] == "'@SUM(A1)"
    assert rows["g7.minus"]["actor"] == "'-2+3"
    assert rows["g7.minus"]["resource"] == "'+cmd"
    assert rows["g7.plain"]["actor"] == "operator"
    assert rows["g7.plain"]["resource"] == "/plain"
    # Hash columns are hex and unchanged — the chain still verifies from a CSV.
    assert all(not r["event_hash"].startswith("'") for r in rows.values())
    # No data cell in the CSV body starts with a formula leader.
    for row in rows.values():
        for value in row.values():
            assert not (value or "")[:1] in {"=", "+", "-", "@", "\t", "\r"}, row


def test_audit_ndjson_export_stays_exact(audit_client):
    client, audit_log = audit_client
    _seed(audit_log)
    response = client.get("/api/v1/audit/export?format=ndjson", headers={"X-Workspace-Id": WS})
    assert response.status_code == 200
    actors = {json.loads(line)["action"]: json.loads(line)["actor"] for line in response.text.splitlines()}
    assert actors["g7.formula"] == HYPERLINK
