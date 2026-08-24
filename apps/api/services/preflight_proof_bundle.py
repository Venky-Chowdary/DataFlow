"""Unified preflight proof bundle.

This module assembles the deterministic safety signals already implemented in
Datawrap into one auditable decision object:
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


def _quality_score(
    columns: list[str],
    sample_rows: list[dict[str, Any]],
    source_schemas: list[dict[str, Any]] | None = None,
) -> float | None:
    """Return sample-quality score, or None when quality was not profiled."""
    if not sample_rows:
        return None
    from services.sample_quality import analyze_dataset_quality

    schema = {s["name"]: s.get("inferred_type", "VARCHAR") for s in (source_schemas or [])}
    report = analyze_dataset_quality(columns, sample_rows, schema=schema)
    return float(report.get("quality_score", 0.0)) / 100.0


def _build_preview_reconciliation(
    source_records: list[dict[str, Any]],
    mappings: list[dict[str, Any]],
) -> dict[str, Any]:
    """Return a non-blocking pre-write simulation object before target rows exist.

    Never claim post-write verification here — checksums and dest counts are pending.
    """
    sample_n = len(source_records)
    base = {
        "passed": True,
        "preview": True,
        "phase": "pre_write_simulation",
        "post_write_pending": True,
        "source_rows": sample_n,
        "target_rows": 0,
        "source_checksum": None,
        "target_checksum": None,
        "matched_key_count": 0,
        "missing_key_count": 0,
        "extra_key_count": 0,
        "row_fidelity_score": None,
        "message": (
            "Pre-write reconciliation simulation only — post-write row-count and "
            "checksum proof will be generated after transfer execution."
        ),
        "sample_compare": {"passed": True, "compared": 0, "mismatches": [], "skipped": True},
    }
    if not source_records:
        return base

    key_cols = [m.get("target") for m in mappings if m.get("target")]
    if not key_cols:
        key_cols = ["id"]

    return {
        **base,
        "matched_key_count": min(len(source_records), len(key_cols)),
        "message": (
            f"Pre-write simulation on {sample_n} sample row(s) — destination has not "
            "been written yet. Post-write Gate-8 checksum proof is pending."
        ),
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
    compliance_acknowledged: bool = False,
    acknowledgment_actor: str = "",
    acknowledgment_reason: str = "",
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
    compliance["acknowledged"] = bool(compliance_acknowledged)
    if compliance_acknowledged:
        from datetime import datetime, timezone

        compliance["review_status"] = "acknowledged"
        compliance["acknowledgment"] = {
            "actor": (acknowledgment_actor or "operator").strip() or "operator",
            "at": datetime.now(timezone.utc).isoformat(),
            "reason": (acknowledgment_reason or "Operator acknowledged PII governance for this transfer").strip(),
        }

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
    compliance_blockers: list[str] = []
    if compliance.get("requires_review") and not compliance_acknowledged:
        compliance_blockers.append("PII/compliance review required")
        blockers.append("PII/compliance review required")
    elif compliance.get("requires_review") and compliance_acknowledged:
        compliance["review_status"] = "acknowledged"
    # Preview reconciliation must never block as a failed post-write proof.
    if not reconciliation.get("preview") and not reconciliation.get("passed"):
        blockers.append("Row-level reconciliation proof failed")

    # Module 3: G4 owns hard mapping-confidence blocks. Proof bundle reports
    # min_confidence for evidence only — never invent a sibling "confidence too
    # low" blocker that duplicates g4_mapping_confidence.
    effective_threshold = max(0.55, float(confidence_threshold or 0.85))
    from services.migration_risk_contract import mapping_has_clearing_risk_contract

    confidences = [
        float(m.get("confidence", 0))
        for m in mappings
        if m.get("confidence") is not None
        and not m.get("user_override")
        and not mapping_has_clearing_risk_contract(m)
        and not m.get("risk_acknowledged")
        and not m.get("riskAcknowledged")
    ]
    min_confidence = round(min(confidences) if confidences else 1.0, 3)
    confidence_below_floor = bool(confidences and min_confidence < effective_threshold)
    # Intentionally do NOT append "Semantic mapping confidence too low" here.

    # Migration Risk Contract: boolean risk_acknowledged alone must never
    # unlock Execute-approve. Continue-policy signed contracts are required.
    from services.migration_risk_contract import lossy_mappings_missing_risk_contracts

    missing_contracts = lossy_mappings_missing_risk_contracts(mappings)
    if missing_contracts:
        cols = ", ".join(missing_contracts[:5])
        more = f" (+{len(missing_contracts) - 5} more)" if len(missing_contracts) > 5 else ""
        blockers.append(
            "Migration Risk Contract required (execution policy) for: "
            f"{cols}{more}"
        )

    decision = "approve"
    if blockers:
        # Compliance-only is a review gate with an explicit Approve CTA — not a
        # schema/data failure. Keep decision=review so the UI can unlock after ack.
        if blockers == compliance_blockers and compliance_blockers:
            decision = "review"
        elif not reconciliation.get("preview") and not reconciliation.get("passed"):
            # Post-write reconciliation failure is a hard block.
            decision = "block"
        else:
            # Low confidence / mixed review signals — surface as review, not silent approve.
            # G4 remains the hard mapping-confidence authority at gate level.
            decision = "review"

    confidence_band = "high" if min_confidence >= 0.9 else "medium" if min_confidence >= 0.75 else "low"
    if quality_score is None:
        quality_grade = "not_profiled"
        quality_display = "not profiled"
        # No sample profile ⇒ never Execute-ready approve. Operators must re-sample
        # before claiming quality; G3/G9 still own hard gates separately.
        if decision == "approve":
            decision = "review"
            blockers = list(blockers) + [
                "Sample quality not profiled — re-run Validate with sample rows before Execute-approve"
            ]
    else:
        quality_grade = "excellent" if quality_score >= 0.9 else "good" if quality_score >= 0.7 else "review"
        quality_display = f"{quality_score:.2f}"

    recon_label = (
        "pre-write simulation pending post-write proof"
        if reconciliation.get("preview") or reconciliation.get("post_write_pending")
        else ("passed" if reconciliation.get("passed") else "needs review")
    )
    evidence_summary = (
        f"Semantic mapping confidence {semantic_score:.2f} (min {min_confidence:.2f}); "
        f"sample quality {quality_display}; "
        f"compliance risk {compliance.get('risk_score', 0.0):.2f}; reconciliation {recon_label}"
    )
    if confidence_below_floor:
        evidence_summary += (
            f"; below G4 floor {effective_threshold:.2f} "
            "(hard block is g4_mapping_confidence — not re-stated here)"
        )

    passed = decision == "approve"
    post_write_proof = bool(
        not reconciliation.get("preview")
        and not reconciliation.get("post_write_pending")
        and reconciliation.get("passed")
        and str(reconciliation.get("phase") or "").startswith("post_write")
    )
    # Module 8: Execute-ready (decision=approve) is never migration_proven.
    # migration_proven requires post-write full_checksum — see signed_proof_pack.
    from services.signed_proof_pack import classify_post_write_assurance

    assurance = classify_post_write_assurance(reconciliation)
    migration_proven = bool(assurance.get("migration_proven"))

    return {
        "passed": passed,
        "migration_proven": migration_proven,
        "post_write_proof": post_write_proof,
        "proof_assurance": assurance,
        "semantic_mapping_score": semantic_score,
        "min_confidence": min_confidence,
        "confidence_threshold": effective_threshold,
        "confidence_below_floor": confidence_below_floor,
        "confidence_authority": "g4_mapping_confidence",
        "semantic_notes": semantic_notes,
        "quality_score": quality_score,
        "confidence_band": confidence_band,
        "quality_grade": quality_grade,
        "evidence_summary": evidence_summary,
        "compliance": compliance,
        "reconciliation": reconciliation,
        "risk_contracts": {
            "missing_columns": missing_contracts,
            "incomplete": bool(missing_contracts),
            "note": (
                "Boolean Accept Risk is not an execution contract. "
                "Execute-approve requires a signed Migration Risk Contract with a "
                "continue policy (CAST_AND_CONTINUE, QUARANTINE_ROW, …). "
                "Default policy is FAIL_JOB."
            ),
        },
        "transfer_decision": {
            "decision": decision,
            "blockers": blockers,
            "compliance_only": bool(blockers) and blockers == compliance_blockers,
            "reason": "No blocking issues detected" if not blockers else "; ".join(blockers),
            "note": (
                "decision=approve means Execute-ready under current gates — "
                "not migration_proven. Post-write Gate-8 proof is required for "
                "migration correctness claims."
                if decision == "approve"
                else None
            ),
        },
    }
