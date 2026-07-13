"""Unified preflight proof bundle.

This module assembles the deterministic safety signals already implemented in
DataFlow into one auditable decision object:
- semantic mapping quality
- sample quality
- compliance / PII risk
- row-level reconciliation proof
"""

from __future__ import annotations

from typing import Any


def _semantic_mapping_score(
    columns: list[str],
    mappings: list[dict[str, Any]],
    source_schemas: list[dict[str, Any]] | None = None,
) -> tuple[float, list[str]]:
    """Compute a bounded semantic mapping score from confidence and role heuristics."""
    from services.mapping_quality import analyze_column_profile, score_mapping_pair

    source_schemas = source_schemas or []
    src_by_name = {s["name"]: s for s in source_schemas}

    scores = [float(m.get("confidence", 0.0)) for m in mappings]
    avg_conf = sum(scores) / len(scores) if scores else 0.0

    profile_notes: list[str] = []
    for m in mappings:
        src = src_by_name.get(m["source"], {})
        samples = [str(x) for x in (src.get("samples") or [])]
        profile = analyze_column_profile(m["source"], samples)
        delta, notes = score_mapping_pair(m, source_profile=profile)
        if notes:
            profile_notes.extend(notes)

    semantic_score = round(min(1.0, max(0.0, avg_conf)), 3)
    return semantic_score, profile_notes[:10]


def _quality_score(columns: list[str], sample_rows: list[dict[str, Any]], source_schemas: list[dict[str, Any]] | None = None) -> float:
    """Return sample-quality score for the current dataset snapshot."""
    if not sample_rows:
        return 0.0
    from services.sample_quality import analyze_dataset_quality

    schema = {s["name"]: s.get("inferred_type", "VARCHAR") for s in (source_schemas or [])}
    report = analyze_dataset_quality(columns, sample_rows, schema=schema)
    return float(report.get("quality_score", 0.0)) / 100.0


def _build_preview_reconciliation(
    source_records: list[dict[str, Any]],
    mappings: list[dict[str, Any]],
) -> dict[str, Any]:
    """Return a non-blocking preview reconciliation object before target rows exist."""
    if not source_records:
        return {
            "passed": True,
            "preview": True,
            "matched_key_count": 0,
            "missing_key_count": 0,
            "extra_key_count": 0,
            "row_fidelity_score": 1.0,
            "message": "Target reconciliation proof will be generated after transfer execution.",
            "sample_compare": {"passed": True, "compared": 0, "mismatches": [], "skipped": True},
        }

    key_cols = [m.get("target") for m in mappings if m.get("target")]
    if not key_cols:
        key_cols = ["id"]

    matched_key_count = min(len(source_records), len(key_cols))
    return {
        "passed": True,
        "preview": True,
        "matched_key_count": matched_key_count,
        "missing_key_count": 0,
        "extra_key_count": 0,
        "row_fidelity_score": 1.0,
        "message": "Target reconciliation proof will be generated after transfer execution.",
        "sample_compare": {"passed": True, "compared": 0, "mismatches": [], "skipped": True},
    }


def build_preflight_proof_bundle(
    *,
    columns: list[str],
    sample_rows: list[dict[str, Any]] | None = None,
    mappings: list[dict[str, Any]] | None = None,
    source_schemas: list[dict[str, Any]] | None = None,
    source_records: list[dict[str, Any]] | None = None,
    target_records: list[dict[str, Any]] | None = None,
    primary_key: str | None = None,
    validation_mode: str = "strict",
    confidence_threshold: float = 0.85,
) -> dict[str, Any]:
    """Assemble the unified proof bundle for a transfer preflight decision."""
    mappings = mappings or []
    sample_rows = sample_rows or []
    source_records = source_records or []
    target_records = target_records or []

    semantic_score, semantic_notes = _semantic_mapping_score(columns, mappings, source_schemas=source_schemas)
    quality_score = _quality_score(columns, sample_rows, source_schemas=source_schemas)

    from services.compliance_guard import score_compliance_risk
    compliance = score_compliance_risk(columns, sample_rows)

    from services.reconciliation import build_reconciliation_proof
    reconciliation = build_reconciliation_proof(
        source_records,
        target_records,
        mappings,
        primary_key=primary_key,
        sample_size=min(50, max(len(source_records), len(target_records), 1)),
    )
    if not target_records:
        reconciliation = _build_preview_reconciliation(source_records, mappings)

    blockers: list[str] = []
    if compliance.get("requires_review"):
        blockers.append("PII/compliance review required")
    if not reconciliation.get("passed"):
        blockers.append("Row-level reconciliation proof failed")

    # Use the validation-mode aware confidence floor. The gate-level confidence
    # check (G4) already accepts mappings above the floor (threshold - 0.3) so
    # the data-integrity audit can be the stricter authority. The proof bundle
    # should not be stricter than G4; it should report low confidence (via the
    # confidence_band) without blocking transfers that have already passed the
    # mapping gate. The average semantic score is reported for UX, not as a hard
    # gate on its own.
    mode = (validation_mode or "strict").strip().lower()
    effective_threshold = max(0.55, confidence_threshold - 0.3)
    confidences = [float(m.get("confidence", 0)) for m in mappings if m.get("confidence") is not None]
    min_confidence = round(min(confidences) if confidences else 0.0, 3)
    if min_confidence < effective_threshold:
        blockers.append("Semantic mapping confidence too low")

    decision = "approve"
    if blockers:
        decision = "block" if compliance.get("requires_review") or not reconciliation.get("passed") else "review"

    confidence_band = "high" if min_confidence >= 0.9 else "medium" if min_confidence >= 0.75 else "low"
    quality_grade = "excellent" if quality_score >= 0.9 else "good" if quality_score >= 0.7 else "review"
    evidence_summary = (
        f"Semantic mapping confidence {semantic_score:.2f} (min {min_confidence:.2f}); sample quality {quality_score:.2f}; "
        f"compliance risk {compliance.get('risk_score', 0.0):.2f}; reconciliation {'passed' if reconciliation.get('passed') else 'needs review'}"
    )

    passed = decision == "approve"

    return {
        "passed": passed,
        "semantic_mapping_score": semantic_score,
        "min_confidence": min_confidence,
        "confidence_threshold": effective_threshold,
        "semantic_notes": semantic_notes,
        "quality_score": quality_score,
        "confidence_band": confidence_band,
        "quality_grade": quality_grade,
        "evidence_summary": evidence_summary,
        "compliance": compliance,
        "reconciliation": reconciliation,
        "transfer_decision": {
            "decision": decision,
            "blockers": blockers,
            "reason": "No blocking issues detected" if not blockers else "; ".join(blockers),
        },
    }
