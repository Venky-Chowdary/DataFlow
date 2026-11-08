"""Signed Gate-8 + mapping proof packs — hash + HMAC verify."""

from __future__ import annotations

import json

import pytest

from services.signed_proof_pack import (
    build_signed_proof_pack,
    export_proof_pack_for_job,
    verify_signed_proof_pack,
)


@pytest.mark.parametrize("verdict", ["false", "true", 1, False, None, True])
def test_full_checksum_assurance_requires_literal_true(verdict):
    from services.signed_proof_pack import classify_post_write_assurance

    report = {
        "phase": "post_write", "coverage": "full_checksum",
        "source_checksum_provenance": "independent_source_reread",
        "source_checksum": "abc", "target_checksum": "abc", "source_rows": 5, "target_rows": 5,
    }
    if verdict is not None:
        report["passed"] = verdict
    assurance = classify_post_write_assurance(report)

    assert assurance["migration_proven"] is (verdict is True)
    assert assurance["post_write_verified"] is (verdict is True)


@pytest.mark.parametrize(
    "changes",
    [
        {"target_checksum": "different"},
        {"target_checksum": "different", "checksum_match": True},
        {"source_checksum": ""},
        {"target_checksum": ""},
        {"source_checksum": None, "target_checksum": None},
        {"source_checksum": " ", "target_checksum": " "},
        {"source_checksum": "", "target_checksum": "", "checksum_match": True},
        {"checksum_match": False},
    ],
)
def test_full_checksum_rejects_contradictory_digest_evidence(changes):
    from services.signed_proof_pack import classify_post_write_assurance

    report = {
        "passed": True, "phase": "post_write", "coverage": "full_checksum",
        "source_checksum": "abc", "target_checksum": "abc",
        "source_checksum_provenance": "independent_source_reread",
        **changes,
    }
    original = dict(report)
    assurance = classify_post_write_assurance(report)

    assert assurance["migration_proven"] is False
    assert assurance["population_proof"] is False
    assert assurance["post_write_verified"] is False
    assert assurance["checksum_match"] is False
    assert report == original


@pytest.mark.parametrize("coverage", ["full_checksum", ""])
@pytest.mark.parametrize("provenance", [None, "writer_stream", "source_reread"])
def test_full_checksum_requires_independent_provenance(coverage, provenance):
    from services.signed_proof_pack import classify_post_write_assurance

    report = {
        "passed": True, "phase": "post_write", "coverage": coverage,
        "source_checksum": "abc", "target_checksum": "abc",
    }
    if provenance is not None:
        report["source_checksum_provenance"] = provenance
    assurance = classify_post_write_assurance(report)

    assert assurance["migration_proven"] is False
    assert assurance["population_proof"] is False
    assert assurance["post_write_verified"] is False
    assert assurance["checksum_match"] is True
    assert assurance["claim_level"] == "none"
    assert "independent" in assurance["note"].lower()
    assert report["passed"] is True


@pytest.mark.parametrize(
    "scope",
    ["written_batch_keys", "last_stream", "whole_table_not_comparable",
     "cdc_source_image_count", "no_op_destination_unchanged", "route_partitioned",
     "unrecognized_scope"],
)
def test_full_checksum_cannot_promote_narrow_or_unknown_scope(scope):
    from services.signed_proof_pack import classify_post_write_assurance

    assurance = classify_post_write_assurance({
        "passed": True, "phase": "post_write", "coverage": "full_checksum",
        "source_checksum": "abc", "target_checksum": "abc",
        "source_checksum_provenance": "independent_source_reread",
        "checksum_scope": scope,
    })

    assert assurance["migration_proven"] is False
    assert assurance["population_proof"] is False
    assert assurance["claim_level"] != "full_checksum"


@pytest.mark.parametrize("verdict", ["false", "true", 1, False, None, True])
def test_signed_roundtrip_verdict_cannot_hide_behind_completeness(verdict, monkeypatch):
    monkeypatch.setattr("services.signed_proof_pack._platform_secret", lambda: b"test-proof-secret")
    report = {
        "phase": "post_write", "coverage": "full_checksum",
        "source_checksum": "abc", "target_checksum": "abc",
        "source_checksum_provenance": "independent_source_reread",
        "identity_hash_aligned": True,
    }
    if verdict is not None:
        report["passed"] = verdict
    pack = build_signed_proof_pack(
        job_id="job-verdict", reconciliation=report,
        mapping_hash="map-h1",
        connector_versions={"source": "postgresql-16.4", "destination": "sqlite-3.45"},
    )
    shipped = json.loads(json.dumps(pack))

    assert shipped["proof_incomplete_reasons"] == []
    assert shipped["assurance"]["migration_proven"] is (verdict is True)
    assert shipped["assurance"]["post_write_verified"] is (verdict is True)
    assert shipped["gate8"] == report
    assert verify_signed_proof_pack(shipped)["ok"] is True
    assert verify_signed_proof_pack(shipped)["errors"] == []


@pytest.mark.parametrize("coverage", ["full_checksum", ""])
@pytest.mark.parametrize("scope", ["", "whole_table"])
def test_signed_full_checksum_real_contract_remains_proven(coverage, scope, monkeypatch):
    monkeypatch.setattr("services.signed_proof_pack._platform_secret", lambda: b"test-proof-secret")
    report = {
        "passed": True, "phase": "post_write", "coverage": coverage,
        "checksum_scope": scope,
        "source_checksum": "abc", "target_checksum": "abc",
        "source_checksum_provenance": "independent_source_reread",
        "identity_hash_aligned": True,
    }
    pack = build_signed_proof_pack(
        job_id="job-real-contract", reconciliation=report,
        mapping_hash="map-h1",
        connector_versions={"source": "postgresql-16.4", "destination": "sqlite-3.45"},
    )
    shipped = json.loads(json.dumps(pack))

    assert shipped["proof_incomplete_reasons"] == []
    assert shipped["assurance"]["claim_level"] == "full_checksum"
    assert shipped["assurance"]["migration_proven"] is True
    assert shipped["assurance"]["post_write_verified"] is True
    assert shipped["assurance"]["checksum_match"] is True
    assert shipped["assurance"]["population_proof"] is False
    assert shipped["assurance"]["referential_integrity_proven"] is False
    assert verify_signed_proof_pack(shipped)["ok"] is True


@pytest.mark.parametrize(
    "changes, expected_match",
    [
        ({"target_checksum": "different"}, False),
        ({"checksum_match": False}, False),
        ({"source_checksum_provenance": None}, True),
        ({"source_checksum_provenance": None,
          "source_independently_reread": True, "checksum_mode": "source_reread"}, True),
    ],
)
def test_signed_unproven_evidence_remains_authentic(changes, expected_match, monkeypatch):
    monkeypatch.setattr("services.signed_proof_pack._platform_secret", lambda: b"test-proof-secret")
    report = {
        "passed": True, "phase": "post_write", "coverage": "full_checksum",
        "source_checksum": "abc", "target_checksum": "abc",
        "source_checksum_provenance": "independent_source_reread",
        "identity_hash_aligned": True,
        **changes,
    }
    pack = build_signed_proof_pack(
        job_id="job-unproven", reconciliation=report,
        mapping_hash="map-h1",
        connector_versions={"source": "postgresql-16.4", "destination": "sqlite-3.45"},
    )
    shipped = json.loads(json.dumps(pack))

    assert shipped["proof_incomplete_reasons"] == []
    assert shipped["gate8"] == report
    assert shipped["gate8"]["passed"] is True
    assert shipped["assurance"]["migration_proven"] is False
    assert shipped["assurance"]["population_proof"] is False
    assert shipped["assurance"]["post_write_verified"] is False
    assert shipped["assurance"]["checksum_match"] is expected_match
    assert verify_signed_proof_pack(shipped)["ok"] is True
    assert verify_signed_proof_pack(shipped)["errors"] == []


@pytest.mark.parametrize(
    "changes, claim_level, verified, population",
    [
        ({"coverage": "sample"}, "sample", True, False),
        ({"coverage": "writer_ack"}, "writer_ack", False, False),
        ({"phase": "pre_write_simulation", "preview": True}, "pre_write_only", False, False),
        ({"checksum_scope": "written_batch_keys"}, "none", False, False),
        ({"checksum_scope": "last_stream"}, "per_stream_checksum", True, False),
        ({"checksum_scope": "whole_table_not_comparable"}, "row_count", True, False),
        ({"checksum_scope": "cdc_source_image_values"}, "cdc_source_image_values", True, True),
        ({"coverage": "none", "phase": "post_write_skipped", "unproven": True,
          "skipped_readback": True, "checksum_match": False}, "none", False, False),
        ({"coverage": "collapse"}, "none", False, False),
        ({"coerced_null_rows": 1}, "coerced", False, False),
        ({"verification_ladder": {"passed": False}}, "failed", False, False),
    ],
)
def test_existing_assurance_downgrades_remain_intact(changes, claim_level, verified, population):
    from services.signed_proof_pack import classify_post_write_assurance

    report = {
        "passed": True, "phase": "post_write", "coverage": "full_checksum",
        "source_checksum": "abc", "target_checksum": "abc",
        "source_checksum_provenance": "independent_source_reread",
        **changes,
    }
    assurance = classify_post_write_assurance(report)

    assert assurance["claim_level"] == claim_level
    assert assurance["post_write_verified"] is verified
    assert assurance["migration_proven"] is False
    assert assurance["population_proof"] is population


@pytest.mark.parametrize("secret", ["", "x" * 31, "dev-only-not-for-production"])
def test_production_proof_pack_rejects_missing_or_weak_signing_secret(
    monkeypatch: pytest.MonkeyPatch, secret: str
) -> None:
    monkeypatch.setattr("services.platform_config.is_production", lambda: True)
    monkeypatch.setattr("services.auth_service._token_secret", lambda: secret)

    with pytest.raises(RuntimeError, match="Proof pack signing secret"):
        build_signed_proof_pack(
            job_id="job-production-key-check",
            reconciliation={"passed": True},
        )


def test_production_proof_pack_does_not_fallback_when_auth_secret_lookup_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unavailable_secret() -> str:
        raise RuntimeError("secret provider unavailable")

    monkeypatch.setattr("services.platform_config.is_production", lambda: True)
    monkeypatch.setattr("services.auth_service._token_secret", unavailable_secret)

    with pytest.raises(RuntimeError, match="Proof pack signing secret"):
        build_signed_proof_pack(
            job_id="job-production-key-provider-check",
            reconciliation={"passed": True},
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
    assert pack["delivery_semantics"]["cdc_default"] == "auto"
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
    assert assurance["post_write_verified"] is False
    assert assurance["claim_level"] == "failed"


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
