"""Chain verification over HTTP — the surface an examiner actually uses.

An auditor who does not trust the UI needs two answers from the API: does the
stored chain still hold up, and does this pack I was handed match the record
filed for it.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services import audit_log as audit
from services import evidence_chain as chain
from services.signed_proof_pack import export_proof_pack_for_job
from src.main import app


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(audit, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit, "_mongo_collection", lambda: None)
    monkeypatch.setattr(chain, "truncation_store_path", lambda: tmp_path / "truncations.jsonl")
    return TestClient(app)


def test_verify_reports_an_intact_chain(client):
    audit.append_audit_event(action="connector.create", resource="/connectors", actor="a@b.c")
    audit.append_audit_event(action="job.run", resource="/jobs", actor="a@b.c")

    res = client.get("/api/v1/audit/verify")

    assert res.status_code == 200
    body = res.json()
    assert body["verified"] is True
    assert body["checked"] == 2
    assert body["findings"] == []
    assert "does not prove the recorded facts are true" in body["honesty"]


def test_verify_names_the_record_that_was_edited(client):
    audit.append_audit_event(action="job.run", resource="/jobs", actor="a@b.c")
    audit.append_audit_event(action="job.delete", resource="/jobs", actor="mallory")
    path = audit.STORE_PATH
    lines = path.read_text(encoding="utf-8").strip().splitlines()
    path.write_text(lines[0].replace('"a@b.c"', '"nobody"') + "\n" + lines[1] + "\n", encoding="utf-8")

    body = client.get("/api/v1/audit/verify").json()

    assert body["verified"] is False
    assert body["findings"][0]["kind"] == "event_hash_mismatch"
    assert body["findings"][0]["index"] == 0


def test_verify_pack_confirms_the_pack_and_its_chain_record(client):
    pack = export_proof_pack_for_job(
        {"_id": "job-http", "reconciliation": {"passed": True, "phase": "sample_verified"}},
        actor="examiner@example.com",
    )

    res = client.post("/api/v1/audit/verify-pack", json=pack)

    assert res.status_code == 200
    body = res.json()
    assert body["ok"] is True
    assert body["chain_record_found"] is True
    assert body["chain_record"]["event_hash"] == pack["chain_anchor"]["event_hash"]


def test_verify_pack_flags_a_pack_whose_chain_record_is_gone(client):
    pack = export_proof_pack_for_job(
        {"_id": "job-gone", "reconciliation": {"passed": True}}, actor="ops@example.com"
    )
    audit.STORE_PATH.write_text("", encoding="utf-8")

    body = client.post("/api/v1/audit/verify-pack", json=pack).json()

    # The pack itself is still intact — only the store lost the record, and the
    # response must not conflate the two.
    assert body["ok"] is True
    assert body["chain_record_found"] is False
    assert "retention alone can cause" in body["honesty"]


def test_verify_pack_rejects_a_non_pack_body(client):
    assert client.post("/api/v1/audit/verify-pack", json={}).status_code == 400
