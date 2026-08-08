"""Phase C1 — Decision Artifact schema + golden fixture + fail-closed hash."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from services.conversion_contract import ConversionClass
from services.decision_kernel import (
    DECISION_ARTIFACT_SCHEMA,
    AssignmentStrategy,
    CanonicalType,
    ColumnSpec,
    ConversionDecision,
    DdlPlan,
    MappingDecision,
    ProofPlan,
    RiskLevel,
    build_decision_artifact,
    compute_content_hash,
    decision_artifact_from_dict,
)

_GOLDEN = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "proofs"
    / "decision_artifact_v1_golden.json"
)


def _sample_artifact(**overrides):
    src = ColumnSpec(
        name="id",
        canonical=CanonicalType(
            logical="integer", native="bigint", bit_width=64, nullable=False
        ),
        role="id",
    )
    dst = ColumnSpec(
        name="id",
        canonical=CanonicalType(
            logical="integer", native="BIGINT", bit_width=64, nullable=False
        ),
        role="id",
    )
    mapping = MappingDecision(
        source="id",
        target="id",
        confidence=0.99,
        assignment_strategy=AssignmentStrategy.IDENTITY_PASSTHROUGH,
        conversion=ConversionDecision(
            conversion_class=ConversionClass.LOSSLESS,
            risk_level=RiskLevel.SAFE,
            lossy=False,
            reason="identity",
        ),
        create_new=True,
    )
    kwargs = dict(
        tenant_id="t1",
        route_id="pg→pg:t",
        source_fingerprint="s1",
        dest_fingerprint="d1",
        source_columns=[src],
        dest_columns=[dst],
        mappings=[mapping],
        ddl=DdlPlan(ddl_identity_hash="abc", column_ddl={"id": "BIGINT"}, dialect="postgresql"),
        proof=ProofPlan(),
        artifact_id="da_test",
        created_at="2026-08-08T00:00:00+00:00",
    )
    kwargs.update(overrides)
    return build_decision_artifact(**kwargs)


def test_build_stamps_schema_and_full_sha256_proof_plan():
    art = _sample_artifact()
    assert art.schema_version == DECISION_ARTIFACT_SCHEMA
    assert art.content_hash
    assert len(art.content_hash) == 64
    assert art.proof.checksum_hex_chars == 64
    assert art.proof.sample_is_population_proof is False
    assert art.source_columns[0].canonical.bit_width == 64


def test_round_trip_dict_preserves_content_hash():
    art = _sample_artifact()
    again = decision_artifact_from_dict(art.to_dict())
    assert again.content_hash == art.content_hash
    assert again.mappings[0].conversion.conversion_class == ConversionClass.LOSSLESS


def test_tampered_content_hash_refused():
    art = _sample_artifact()
    payload = art.to_dict()
    payload["content_hash"] = "0" * 64
    with pytest.raises(ValueError, match="content_hash mismatch"):
        decision_artifact_from_dict(payload)


def test_unsupported_schema_version_refused():
    art = _sample_artifact()
    payload = art.to_dict()
    payload["schema_version"] = "decision_artifact_v0"
    # Clear hash so we hit schema check first (or after parse fields)
    with pytest.raises(ValueError, match="unsupported decision artifact schema"):
        decision_artifact_from_dict(payload)


def test_golden_fixture_loads_and_hashes():
    raw = json.loads(_GOLDEN.read_text(encoding="utf-8"))
    assert raw["schema_version"] == DECISION_ARTIFACT_SCHEMA
    # Stamp hash if placeholder empty — golden ships without lock so edits are reviewable.
    if not raw.get("content_hash"):
        raw["content_hash"] = compute_content_hash(raw)
    art = decision_artifact_from_dict(raw)
    assert art.ddl.column_ddl["id"] == "BIGINT"
    assert art.ddl.column_ddl["big_val"] == "BIGINT"
    assert all(c.canonical.bit_width == 64 for c in art.source_columns)
    assert art.proof.checksum_hex_chars == 64
    # Never invent 32-bit invent in the golden migration assurance fixture.
    assert "INTEGER" not in art.ddl.column_ddl.values()
    assert "INT" not in art.ddl.column_ddl.values()


def test_assignment_strategy_honest_labels_exist():
    # Audit §4.1 — greedy-patched Hungarian must not claim globally optimal.
    assert AssignmentStrategy.HUNGARIAN_WITH_GREEDY_PATCH.value == (
        "hungarian_with_greedy_patch"
    )
    assert (
        AssignmentStrategy.OPTIMAL_BIPARTITE_HUNGARIAN.value
        == "optimal_bipartite_hungarian"
    )


def test_kernel_type_facade_never_narrower_and_lossy():
    from services.decision_kernel import ddl_type, is_lossy_coercion

    assert ddl_type("postgresql", "integer") == "BIGINT"
    assert is_lossy_coercion("BIGINT", "INTEGER", dest_db="postgresql") is True


def test_kernel_classify_conversion_ssot():
    from services.decision_kernel import ConversionClass, classify_conversion

    out = classify_conversion("VARCHAR", "INTEGER", dest_db="postgresql")
    assert out["conversion_class"] in {
        ConversionClass.LOSSY.value,
        ConversionClass.NEEDS_USER_APPROVAL.value,
    }
