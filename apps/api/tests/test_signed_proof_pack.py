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
    assert pack["delivery_semantics"]["exactly_once"] is False
    assert pack["delivery_semantics"]["at_least_once"] is True
    assert pack["delivery_semantics"]["at_most_once"] is False
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


def test_export_includes_accepted_risks_policies_and_rollback():
    """Module H — diligence export must not drop risk contracts / policies."""
    contract = {
        "risk_id": "mrc-abc123",
        "column": "amount",
        "source_type": "TEXT",
        "destination_type": "INTEGER",
        "execution_policy": "CAST_AND_CONTINUE",
        "quarantine_policy": "holdout_rejected_rows",
        "retry_policy": "none",
        "rollback_strategy": "DOCUMENT_ONLY",
        "approved_by": "ops@example.com",
        "reason": "legacy cast",
        "signature": "mrc-sha256:deadbeef",
    }
    job = {
        "_id": "job-risks",
        "mappings": [
            {
                "source": "amount",
                "target": "amount",
                "risk_contract": contract,
            }
        ],
        "reconciliation": {
            "passed": True,
            "coverage": "full_checksum",
            "source_checksum": "aaa",
            "target_checksum": "aaa",
        },
        "mapping_proof": {"mapping_hash": "map-h1"},
        "destination_summary": {
            "ddl_hash": "ddl-h1",
            "rejected_details": [{"row": 1, "reason": "cast"}],
            "rollback_plan": {
                "strategy": "DISCARD_STAGING",
                "executable": True,
                "staging_table": "t_df_staging",
            },
        },
        "source": {"format": "postgresql"},
        "destination": {"format": "snowflake"},
    }
    pack = export_proof_pack_for_job(job, actor="ops@example.com")
    assert len(pack["accepted_risks"]) == 1
    assert pack["accepted_risks"][0]["risk_id"] == "mrc-abc123"
    assert pack["risk_contracts"][0]["execution_policy"] == "CAST_AND_CONTINUE"
    assert pack["execution_policies"][0]["execution_policy"] == "CAST_AND_CONTINUE"
    assert pack["rollback_plan"]["strategy"] == "DISCARD_STAGING"
    assert pack["rejected_rows_count"] == 1
    assert pack["hashes"]["ddl_hash"] == "ddl-h1"
    assert pack["hashes"]["mapping_hash"] == "map-h1"
    assert pack["connector_versions"]["source"] == "postgresql"
    assert verify_signed_proof_pack(pack)["ok"] is True


def test_cdc_count_scope_does_not_ladder_veto_leftover_extras():
    from services.reconcile_coverage import CDC_SOURCE_IMAGE_COUNT
    from services.signed_proof_pack import (
        apply_fidelity_veto,
        classify_post_write_assurance,
        fidelity_veto,
    )

    recon = {
        "passed": True,
        "checksum_scope": CDC_SOURCE_IMAGE_COUNT,
        "checksum_match": False,
        "source_rows": 2,
        "target_rows": 3,
        "verification_ladder": {
            "passed": False,
            "localization_summary": "L4/L5 localized: row id='99'",
            "layers": {
                "L1": {"passed": True},
                "L3": {"passed": True},
                "L4": {"passed": False},
            },
        },
    }
    assert fidelity_veto(recon) is None
    out = apply_fidelity_veto(recon)
    assert out["passed"] is True
    assurance = classify_post_write_assurance(out)
    assert assurance["migration_proven"] is False
    assert assurance["post_write_verified"] is True
    assert assurance["claim_level"] == CDC_SOURCE_IMAGE_COUNT


def test_overwrite_ladder_fail_still_vetoes_when_not_cdc_count_scope():
    from services.signed_proof_pack import apply_fidelity_veto, fidelity_veto

    recon = {
        "passed": True,
        "checksum_scope": "",
        "checksum_match": False,
        "verification_ladder": {
            "passed": False,
            "localization_summary": "L4 extra dest key",
            "layers": {"L1": {"passed": True}, "L4": {"passed": False}},
        },
    }
    veto = fidelity_veto(recon)
    assert veto is not None
    assert veto.operational_passed is False
    out = apply_fidelity_veto(recon)
    assert out["passed"] is False
    assert out.get("migration_proven") is False


def _isolated_chain(tmp_path, monkeypatch):
    """File-backed audit chain scoped to one test."""
    from services import audit_log, evidence_chain

    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    monkeypatch.setattr(
        evidence_chain, "truncation_store_path", lambda: tmp_path / "truncations.jsonl"
    )


def test_building_a_pack_stays_pure_unless_anchoring_is_asked_for(tmp_path, monkeypatch):
    _isolated_chain(tmp_path, monkeypatch)
    from services.evidence_chain import read_chain

    pack = build_signed_proof_pack(job_id="job-a", reconciliation={"passed": True})

    assert "chain_anchor" not in pack
    assert read_chain() == []


def test_an_exported_pack_occupies_the_chain_position_it_claims(tmp_path, monkeypatch):
    _isolated_chain(tmp_path, monkeypatch)
    from services.evidence_chain import find_anchor, verify_chain
    from services.signed_proof_pack import pack_body_digest_excluding_anchor

    pack = export_proof_pack_for_job(
        {"_id": "job-anchored", "reconciliation": {"passed": True, "phase": "sample_verified"}},
        actor="ops@example.com",
    )

    anchor = pack["chain_anchor"]
    assert anchor["anchored"] is True
    assert anchor["evidence_sha256"] == pack_body_digest_excluding_anchor(pack)
    record = find_anchor(anchor["evidence_sha256"])
    assert record is not None
    assert record["event_hash"] == anchor["event_hash"]
    assert verify_signed_proof_pack(pack)["ok"] is True
    assert verify_chain()["verified"] is True


def test_an_altered_anchored_pack_no_longer_matches_its_chain_record(tmp_path, monkeypatch):
    _isolated_chain(tmp_path, monkeypatch)

    pack = export_proof_pack_for_job(
        {"_id": "job-altered", "reconciliation": {"passed": True}}, actor="ops@example.com"
    )
    pack["gate8"] = {"passed": True, "source_rows": 999}

    result = verify_signed_proof_pack(pack)

    assert result["ok"] is False
    assert any("chain_anchor digest does not match" in e for e in result["errors"])


def test_an_anchor_lifted_from_another_pack_is_rejected(tmp_path, monkeypatch):
    _isolated_chain(tmp_path, monkeypatch)

    first = export_proof_pack_for_job({"_id": "job-1", "reconciliation": {"passed": True}})
    second = export_proof_pack_for_job({"_id": "job-2", "reconciliation": {"passed": True}})
    second["chain_anchor"] = first["chain_anchor"]

    assert any(
        "chain_anchor digest does not match" in e
        for e in verify_signed_proof_pack(second)["errors"]
    )


def test_export_survives_an_unavailable_audit_store_without_claiming_an_anchor(
    tmp_path, monkeypatch
):
    _isolated_chain(tmp_path, monkeypatch)
    from services import audit_log

    def boom(**_kwargs):
        raise RuntimeError("audit store down")

    monkeypatch.setattr(audit_log, "append_audit_event", boom)

    pack = export_proof_pack_for_job({"_id": "job-3", "reconciliation": {"passed": True}})

    assert pack["chain_anchor"]["anchored"] is False
    assert verify_signed_proof_pack(pack)["ok"] is True
