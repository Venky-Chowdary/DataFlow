"""Module 13 — Mapping Engine Contract tests."""

from __future__ import annotations

from services.mapping_engine_contract import (
    is_operator_locked,
    merge_mappings_preserve_overrides,
    stamp_mapping_evidence,
)


def test_operator_locked_flags():
    assert is_operator_locked({"user_override": True})
    assert is_operator_locked({"risk_acknowledged": True})
    assert is_operator_locked({"risk_contract": {"risk_id": "r1"}})
    assert is_operator_locked({"intentional_omit": True})
    assert not is_operator_locked({"source": "a", "target": "b", "confidence": 0.9})


def test_merge_never_silently_overwrites_locked():
    baseline = [
        {
            "source": "amount",
            "target": "amount_usd",
            "target_type": "DECIMAL(18,2)",
            "transform": "none",
            "user_override": True,
            "confidence": 1.0,
        }
    ]
    proposed = [
        {
            "source": "amount",
            "target": "amt",
            "target_type": "FLOAT",
            "transform": "decimal",
            "confidence": 0.99,
            "reasoning": "engine prefers amt",
        }
    ]
    merged, report = merge_mappings_preserve_overrides(baseline, proposed)
    assert len(merged) == 1
    assert merged[0]["target"] == "amount_usd"
    assert merged[0]["target_type"] == "DECIMAL(18,2)"
    assert merged[0]["user_override"] is True
    assert merged[0]["engine_suggestion"]["target"] == "amt"
    assert merged[0]["engine_suggestion"]["suppressed"] is True
    assert report["silent_overwrite_of_locked"] == 0
    assert report["operator_locked_preserved"] == 1


def test_merge_applies_engine_when_unlocked():
    baseline = [{"source": "id", "target": "id_old", "confidence": 0.5}]
    proposed = [{"source": "id", "target": "id", "confidence": 0.95}]
    merged, report = merge_mappings_preserve_overrides(baseline, proposed)
    assert merged[0]["target"] == "id"
    assert report["engine_applied"] == 1


def test_stamp_mapping_evidence_fields():
    stamped = stamp_mapping_evidence(
        {
            "source": "email",
            "target": "email",
            "confidence": "0.91",
            "assignment_strategy": "optimal_bipartite_hungarian",
            "reasoning": "name match",
            "source_type": "VARCHAR",
            "target_type": "VARCHAR",
            "user_override": False,
        },
        version=3,
    )
    assert stamped["confidence"] == 0.91
    assert "semantic_evidence" in stamped
    assert "lexical_evidence" in stamped
    assert "datatype_compatibility" in stamped
    assert "constraint_compatibility" in stamped
    assert stamped["ai_explanation"] == "name match"
    assert stamped["user_overrides"]["locked"] is False
    assert stamped["version_history"][-1]["version"] == 3
    assert stamped["mapping_engine_contract"] == "mapping_engine_contract.v1"
