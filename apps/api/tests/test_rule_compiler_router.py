"""Rule workbook import router — bounded read, strict form-field JSON."""

from __future__ import annotations

import json

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from services.rule_compiler.ingest import MAX_FILE_BYTES
from src.routers.rule_compiler_router import router


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    return TestClient(app)


def _csv_file(payload: bytes = b"Source Column,Destination Column,Rule\nfname,first_name,Direct\n"):
    return {"file": ("rules.csv", payload, "text/csv")}


def test_oversize_upload_is_413_before_compile():
    client = _client()
    payload = b"x" * (MAX_FILE_BYTES + 1)
    response = client.post("/api/v1/transfer/rules/import", files=_csv_file(payload))
    assert response.status_code == 413
    assert "MB" in response.json()["detail"] or "limited" in response.json()["detail"]


def test_malformed_json_form_field_is_400_naming_the_field():
    client = _client()
    response = client.post(
        "/api/v1/transfer/rules/import",
        files=_csv_file(),
        data={"source_columns": "{not json"},
    )
    assert response.status_code == 400
    assert "source_columns" in response.json()["detail"]


def test_wrong_type_json_form_field_is_400():
    client = _client()
    response = client.post(
        "/api/v1/transfer/rules/import",
        files=_csv_file(),
        data={"source_catalog": "[\"not\", \"a\", \"dict\"]"},
    )
    assert response.status_code == 400
    assert "source_catalog" in response.json()["detail"]


def test_valid_and_empty_form_fields_still_compile():
    client = _client()
    response = client.post(
        "/api/v1/transfer/rules/import",
        files=_csv_file(),
        data={
            "source_columns": json.dumps(["fname"]),
            "dest_columns": json.dumps(["first_name"]),
            "source_types": json.dumps({"fname": "text"}),
            "source_catalog": "",
        },
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["buckets"]["executable"] == 1


def test_xls_workbook_imports_via_biff_branch():
    from pathlib import Path

    fixture = Path(__file__).parent / "fixtures" / "legacy_events.xls"
    if not fixture.exists():
        pytest.skip("legacy_events.xls fixture not present")
    client = _client()
    response = client.post(
        "/api/v1/transfer/rules/import",
        files={"file": ("legacy_events.xls", fixture.read_bytes(), "application/vnd.ms-excel")},
    )
    assert response.status_code == 200, response.text
    assert response.json()["rule_count"] >= 1


def test_bad_extension_is_400():
    client = _client()
    response = client.post(
        "/api/v1/transfer/rules/import",
        files={"file": ("rules.exe", b"MZ", "application/octet-stream")},
    )
    assert response.status_code == 400


def _audit_store(tmp_path, monkeypatch):
    from services import audit_log as audit

    path = tmp_path / "audit.jsonl"
    monkeypatch.setattr(audit, "STORE_PATH", path)
    monkeypatch.setattr(audit, "_mongo_collection", lambda: None)
    return audit, path


def test_import_writes_one_compiled_event_and_is_idempotent(tmp_path, monkeypatch):
    audit, _path = _audit_store(tmp_path, monkeypatch)
    client = _client()
    r1 = client.post("/api/v1/transfer/rules/import", files=_csv_file())
    r2 = client.post("/api/v1/transfer/rules/import", files=_csv_file())
    assert r1.status_code == 200 and r2.status_code == 200
    h1, h2 = r1.json()["audit_event_hash"], r2.json()["audit_event_hash"]
    assert h1 and h1 == h2
    events = [
        e for e in audit.list_audit_events(limit=500)
        if e.get("action") == "business_rules.compiled"
    ]
    assert len(events) == 1
    assert events[0]["details"]["report_fingerprint"] == r1.json()["report_fingerprint"]
    assert events[0]["details"]["filename"] == "rules.csv"


def test_different_payload_writes_a_new_event(tmp_path, monkeypatch):
    audit, _path = _audit_store(tmp_path, monkeypatch)
    client = _client()
    r1 = client.post("/api/v1/transfer/rules/import", files=_csv_file())
    other = b"Source Column,Destination Column,Rule\nfname,first_name,Upper\n"
    r2 = client.post("/api/v1/transfer/rules/import", files=_csv_file(other))
    assert r1.status_code == 200 and r2.status_code == 200
    events = [
        e for e in audit.list_audit_events(limit=500)
        if e.get("action") == "business_rules.compiled"
    ]
    assert len(events) == 2
    assert r1.json()["audit_event_hash"] != r2.json()["audit_event_hash"]


def test_compiled_event_carries_no_cell_values_or_rule_text(tmp_path, monkeypatch):
    _audit, path = _audit_store(tmp_path, monkeypatch)
    client = _client()
    payload = (
        "Source Column,Destination Column,Rule\n"
        "ZXQWCOL,secret_target,Must equal TOPSECRET-MARKER\n"
    ).encode()
    r = client.post("/api/v1/transfer/rules/import", files=_csv_file(payload))
    assert r.status_code == 200, r.text
    raw = path.read_bytes()
    assert b"TOPSECRET-MARKER" not in raw
    assert b"secret_target" not in raw
    assert b"ZXQWCOL" not in raw
