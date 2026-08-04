"""Signed Gate-8 + mapping proof packs — hash + HMAC verify."""

from __future__ import annotations

from services.signed_proof_pack import (
    build_signed_proof_pack,
    export_proof_pack_for_job,
    verify_signed_proof_pack,
)


def test_build_and_verify_roundtrip():
    pack = build_signed_proof_pack(
        job_id="job-1",
        reconciliation={"passed": True, "source_rows": 10, "target_rows": 10},
        mapping_proof={"mappings": [{"source": "a", "target": "a"}]},
        actor="cto@example.com",
        prev_audit_hash="abc123",
    )
    assert pack["content_sha256"]
    assert pack["signature"]["alg"] == "HMAC-SHA256"
    assert pack["delivery_semantics"]["cdc_default"] == "at_least_once"
    result = verify_signed_proof_pack(pack)
    assert result["ok"] is True
    assert result["errors"] == []


def test_tampered_payload_fails_verify():
    pack = build_signed_proof_pack(
        job_id="job-2",
        reconciliation={"passed": True},
        actor="system",
    )
    pack["gate8"] = {"passed": False, "tampered": True}
    result = verify_signed_proof_pack(pack)
    assert result["ok"] is False
    assert "content_sha256 mismatch" in result["errors"] or "HMAC signature invalid" in result["errors"]


def test_export_from_job_document():
    job = {
        "_id": "job-xyz",
        "reconciliation": {"passed": True, "phase": "sample_verified"},
        "mapping_proof": {"mappings": []},
        "preflight": {
            "passed": True,
            "decision": "allow",
            "passed_count": 9,
            "total_gates": 9,
            "readiness_score": 100,
        },
    }
    pack = export_proof_pack_for_job(job, actor="ops@example.com")
    assert pack["job_id"] == "job-xyz"
    assert pack["preflight_summary"]["total_gates"] == 9
    assert verify_signed_proof_pack(pack)["ok"] is True
