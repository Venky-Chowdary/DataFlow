"""Phase C6–C10 — Profile / Validation / Risk / Proof kernel engines."""

from __future__ import annotations

from services.decision_kernel import (
    ConversionClass,
    RiskLevel,
    ValidationClass,
    aggregate_route_risk,
    assess_mapping_risk,
    build_artifact_from_mappings,
    build_migration_proof_pack,
    classify_gate_results,
    extract_population_checksum,
    orchestrate_validation_summary,
    profile_columns,
    risk_level_for_conversion,
    validation_class_for_gate,
)


def test_c6_profile_columns_null_cardinality_and_not_population_proof():
    rows = [
        {"id": 1, "email": "a@example.com"},
        {"id": 2, "email": "b@example.com"},
        {"id": None, "email": "c@example.com"},
    ]
    prof = profile_columns(["id", "email"], rows, sample_limit=10)
    assert prof.sample_is_population_proof is False
    assert prof.to_dict()["sample_is_population_proof"] is False
    by_name = {c.name: c for c in prof.columns}
    assert by_name["id"].null_rate > 0
    assert by_name["email"].distinct_count >= 2
    assert by_name["email"].pattern in {None, "email"} or by_name["email"].pattern == "email"


def test_c8_gate_classification_buckets():
    assert validation_class_for_gate("g3_schema_contract") is ValidationClass.SCHEMA
    assert validation_class_for_gate("g4_mapping_confidence") is ValidationClass.SEMANTIC
    assert validation_class_for_gate("g8_reconciliation") is ValidationClass.POPULATION
    out = classify_gate_results(
        [{"id": "g3_schema_contract", "status": "pass", "message": "ok"}],
        blockers=[{"id": "g4_mapping_confidence", "message": "low conf"}],
    )
    assert out["by_class"]["schema"]
    assert out["by_class"]["semantic"]
    assert "semantic" in out["blocked_classes"]
    assert "population" in out["population_note"].lower() or "checksum" in out["population_note"].lower()


def test_c8_orchestrator_requires_artifact_hash():
    summary = orchestrate_validation_summary(
        decision_artifact=None,
        gates=[{"id": "g1_source", "status": "pass", "message": "ok"}],
        blockers=[],
    )
    assert summary["decision_artifact_present"] is False
    assert "proof" in summary["blocked_classes"]


def test_c9_risk_bands_from_conversion_class():
    assert (
        risk_level_for_conversion({"conversion_class": ConversionClass.IDENTITY.value})
        is RiskLevel.SAFE
    )
    assert (
        risk_level_for_conversion(
            {
                "conversion_class": ConversionClass.NEEDS_USER_APPROVAL.value,
                "requires_risk_contract": True,
            }
        )
        is RiskLevel.APPROVAL
    )
    assert (
        risk_level_for_conversion({"conversion_class": ConversionClass.UNSUPPORTED.value})
        is RiskLevel.BLOCKED
    )
    risky = assess_mapping_risk(
        {
            "source": "a",
            "target": "a",
            "source_type": "TEXT",
            "target_type": "INTEGER",
            "transform": "none",
        },
        destination_db_type="postgresql",
    )
    assert risky["risk_level"] == RiskLevel.APPROVAL.value
    assert risky["requires_risk_contract"] is True
    assert (
        aggregate_route_risk(
            [{"risk_level": "safe"}, {"risk_level": "approval"}, {"risk_level": "review"}]
        )
        is RiskLevel.APPROVAL
    )


def test_c10_proof_pack_requires_full_checksum_and_artifact():
    art = build_artifact_from_mappings(
        [
            {
                "source": "id",
                "target": "id",
                "source_type": "BIGINT",
                "target_type": "BIGINT",
            }
        ],
        dest_db="postgresql",
        artifact_id="da_inline",
        created_at="1970-01-01T00:00:00+00:00",
    )
    # Truncated digest must not prove migration.
    truncated = extract_population_checksum({"final_checksum": "abcd" * 4, "matched": True})
    assert truncated["full_digest"] is False
    pack = build_migration_proof_pack(
        decision_artifact=art.to_dict(),
        reconciliation={
            "final_checksum": "a" * 64,
            "matched": True,
        },
        validation_summary={
            "decision_artifact_present": True,
            "blocked_classes": [],
        },
        job_id="j1",
        job_success=True,
    )
    assert pack["migration_proven"] is True
    assert pack["population_checksum"]["checksum_hex_chars"] == 64
    assert pack["proof_plan"]["sample_is_population_proof"] is False

    incomplete = build_migration_proof_pack(
        decision_artifact=art.to_dict(),
        reconciliation={"final_checksum": "abcd" * 4, "matched": True},
        validation_summary={"decision_artifact_present": True, "blocked_classes": []},
        job_success=True,
    )
    assert incomplete["migration_proven"] is False
    assert "population_checksum_not_full_sha256" in incomplete["assurance"]["reasons"]


def test_c11_validate_without_artifact_refuses_in_engine_helper():
    from src.transfer.engine import _enforce_decision_artifact

    err, art = _enforce_decision_artifact(
        {"proof_bundle": {}},  # Validate ran but no artifact stamp
        [{"source": "a", "target": "a", "target_type": "TEXT"}],
        dest_db="postgresql",
        skip_preflight=False,
    )
    assert err is not None
    assert "Decision Artifact missing" in err
    assert art is None
