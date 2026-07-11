"""Regression tests for the unified preflight proof bundle."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.preflight_proof_bundle import build_preflight_proof_bundle  # noqa: E402
from src.services.preflight_service import apply_policy_gates  # noqa: E402


def test_build_preflight_proof_bundle_returns_unified_decision() -> None:
    columns = ["id", "email", "amount"]
    sample_rows = [
        {"id": "1", "email": "alice@example.com", "amount": "10.00"},
        {"id": "2", "email": "bob@example.com", "amount": "20.00"},
    ]
    mappings = [
        {"source": "id", "target": "id", "confidence": 0.96},
        {"source": "email", "target": "email", "confidence": 0.95},
        {"source": "amount", "target": "amount", "confidence": 0.9},
    ]
    source_records = [
        {"id": "1", "email": "alice@example.com", "amount": "10.00"},
        {"id": "2", "email": "bob@example.com", "amount": "20.00"},
    ]
    target_records = [
        {"id": "1", "email": "alice@example.com", "amount": "10.00"},
        {"id": "2", "email": "bob@example.com", "amount": "20.00"},
    ]

    bundle = build_preflight_proof_bundle(
        columns=columns,
        sample_rows=sample_rows,
        mappings=mappings,
        source_schemas=[
            {"name": "id", "inferred_type": "INTEGER", "samples": ["1", "2"]},
            {"name": "email", "inferred_type": "VARCHAR", "samples": ["alice@example.com", "bob@example.com"]},
            {"name": "amount", "inferred_type": "DECIMAL", "samples": ["10.00", "20.00"]},
        ],
        source_records=source_records,
        target_records=target_records,
        primary_key="id",
    )

    assert bundle["passed"] is True
    assert bundle["semantic_mapping_score"] >= 0.8
    assert bundle["quality_score"] >= 0
    assert bundle["compliance"]["risk_score"] >= 0.0
    assert bundle["reconciliation"]["passed"] is True
    assert bundle["transfer_decision"]["decision"] in {"approve", "review"}


def test_build_preflight_proof_bundle_blocks_on_pii_risk_and_missing_keys() -> None:
    columns = ["id", "ssn", "dob"]
    sample_rows = [
        {"id": "1", "ssn": "123-45-6789", "dob": "1990-01-01"},
        {"id": "2", "ssn": "987-65-4321", "dob": "1988-02-02"},
    ]
    mappings = [
        {"source": "id", "target": "id", "confidence": 0.95},
        {"source": "ssn", "target": "ssn", "confidence": 0.95},
        {"source": "dob", "target": "dob", "confidence": 0.95},
    ]

    bundle = build_preflight_proof_bundle(
        columns=columns,
        sample_rows=sample_rows,
        mappings=mappings,
        source_schemas=[
            {"name": "id", "inferred_type": "INTEGER", "samples": ["1", "2"]},
            {"name": "ssn", "inferred_type": "VARCHAR", "samples": ["123-45-6789", "987-65-4321"]},
            {"name": "dob", "inferred_type": "DATE", "samples": ["1990-01-01", "1988-02-02"]},
        ],
        source_records=[{"id": "1", "ssn": "123-45-6789", "dob": "1990-01-01"}],
        target_records=[],
        primary_key="id",
    )

    assert bundle["passed"] is False
    assert bundle["transfer_decision"]["decision"] == "block"
    assert bundle["compliance"]["requires_review"] is True


def test_build_preflight_proof_bundle_requires_review_when_mapping_confidence_is_low() -> None:
    bundle = build_preflight_proof_bundle(
        columns=["id", "email"],
        sample_rows=[{"id": "1", "email": "alice@example.com"}],
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.60},
            {"source": "email", "target": "email", "confidence": 0.58},
        ],
        source_schemas=[
            {"name": "id", "inferred_type": "INTEGER", "samples": ["1"]},
            {"name": "email", "inferred_type": "VARCHAR", "samples": ["alice@example.com"]},
        ],
        source_records=[{"id": "1", "email": "alice@example.com"}],
        target_records=[{"id": "1", "email": "alice@example.com"}],
        primary_key="id",
    )

    assert bundle["passed"] is False
    assert bundle["transfer_decision"]["decision"] == "review"


def test_apply_policy_gates_uses_proof_bundle_decision() -> None:
    result = {
        "passed": True,
        "passed_count": 9,
        "total_gates": 9,
        "readiness_score": 100.0,
        "gates": [{"id": "g1_source", "status": "pass", "message": "ok", "duration_ms": 0}],
        "blockers": [],
        "proof_bundle": {
            "passed": False,
            "transfer_decision": {
                "decision": "block",
                "blockers": ["PII/compliance review required"],
                "reason": "PII/compliance review required",
            },
        },
    }

    updated = apply_policy_gates(result, [])

    assert updated["passed"] is False
    assert any("PII/compliance review required" in blocker["message"] for blocker in updated["blockers"])


def test_build_preflight_proof_bundle_returns_enterprise_summary_signal() -> None:
    bundle = build_preflight_proof_bundle(
        columns=["id", "email", "amount"],
        sample_rows=[
            {"id": "1", "email": "alice@example.com", "amount": "10.00"},
            {"id": "2", "email": "bob@example.com", "amount": "20.00"},
        ],
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.96},
            {"source": "email", "target": "email", "confidence": 0.95},
            {"source": "amount", "target": "amount", "confidence": 0.92},
        ],
        source_schemas=[
            {"name": "id", "inferred_type": "INTEGER", "samples": ["1", "2"]},
            {"name": "email", "inferred_type": "VARCHAR", "samples": ["alice@example.com", "bob@example.com"]},
            {"name": "amount", "inferred_type": "DECIMAL", "samples": ["10.00", "20.00"]},
        ],
        source_records=[
            {"id": "1", "email": "alice@example.com", "amount": "10.00"},
            {"id": "2", "email": "bob@example.com", "amount": "20.00"},
        ],
        target_records=[
            {"id": "1", "email": "alice@example.com", "amount": "10.00"},
            {"id": "2", "email": "bob@example.com", "amount": "20.00"},
        ],
        primary_key="id",
    )

    assert bundle["confidence_band"] in {"high", "medium"}
    assert bundle["quality_grade"] in {"excellent", "good"}
    assert isinstance(bundle["evidence_summary"], str)
    assert len(bundle["evidence_summary"]) > 0
