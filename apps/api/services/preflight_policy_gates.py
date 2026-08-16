"""Preflight policy-gate merge — extracted from preflight_service (F8)."""

from __future__ import annotations

from typing import Any

from preflight.models import GateStatus
from services.db_type_utils import normalize_dest_kind


def is_compliance_only_block(proof_blockers: list[str]) -> bool:
    """Return True when every proof blocker is purely a PII/compliance review."""
    if not proof_blockers:
        return False
    return all(
        "PII/compliance" in b or "compliance review" in b.lower()
        for b in proof_blockers
    )


def apply_policy_gates(
    result: dict[str, Any],
    policy_gates: list[dict[str, Any]],
    validation_mode: str = "strict",
    destination_db_type: str = "postgresql",
) -> dict[str, Any]:
    proof_bundle = result.get("proof_bundle") or {}
    transfer_decision = (proof_bundle.get("transfer_decision") or {}).get("decision")
    proof_blockers = (proof_bundle.get("transfer_decision") or {}).get("blockers") or []
    compliance_only = bool(
        (proof_bundle.get("transfer_decision") or {}).get("compliance_only")
    ) or is_compliance_only_block(proof_blockers)

    is_strict = (validation_mode or "strict").lower() in {"strict", "maximum"}

    # In non-strict modes, PII/compliance review is a warning, not a hard blocker.
    # In strict mode, compliance-only stays a dedicated ack gate (Approve PII CTA)
    # rather than masquerading as a failed schema/data check.
    if is_strict:
        active_proof_blockers = list(proof_blockers)
    else:
        active_proof_blockers = [
            b
            for b in proof_blockers
            if "PII/compliance" not in b and "compliance review" not in b.lower()
        ]

    blockers = [
        {"id": b["id"], "message": b["message"], "details": b.get("details", {})}
        for b in result.get("blockers", [])
    ]
    for idx, message in enumerate(active_proof_blockers):
        is_compliance = (
            "PII/compliance" in str(message) or "compliance review" in str(message).lower()
        )
        blockers.append({
            "id": f"proof_{idx}",
            "message": str(message),
            "details": {
                "compliance_ack_required": bool(is_compliance and compliance_only),
                "remediation_kind": "acknowledge_compliance" if is_compliance else "fix_proof",
            },
        })

    if policy_gates:
        policy_ids = {str(g.get("id") or "") for g in policy_gates}
        base = [
            g
            for g in (result.get("gates") or [])
            if str(g.get("id") or "") not in policy_ids
        ]
        gates = [*base, *policy_gates]
        blockers.extend(
            {"id": g["id"], "message": g["message"], "details": g.get("details", {})}
            for g in policy_gates
            if g.get("status") == GateStatus.BLOCK.value
        )
    else:
        gates = list(result.get("gates", []))

    passed_count = sum(1 for g in gates if g.get("status") == GateStatus.PASS.value)
    total_gates = len(gates)
    has_blocks = any(g.get("status") == GateStatus.BLOCK.value for g in gates)

    proof_blocks = (
        transfer_decision in {"block", "review"} or proof_bundle.get("passed") is False
    )
    if proof_blocks and not is_strict:
        if active_proof_blockers:
            proof_blocks = True
        else:
            proof_blocks = False

    if proof_blocks:
        has_blocks = True

    if proof_bundle:
        proof_bundle = {**proof_bundle}
        base_decision = proof_bundle.get("transfer_decision") or {}
        if has_blocks:
            gate_blocker_messages = [b["message"] for b in blockers]
            decision_blockers = list(base_decision.get("blockers") or [])
            for msg in gate_blocker_messages:
                if msg not in decision_blockers:
                    decision_blockers.append(msg)
            # Compliance-only: keep decision=review so the UI shows Approve PII,
            # not a generic "schema failed" block with contradictory 12/12 passed.
            decision_label = "review" if compliance_only and not any(
                g.get("status") == GateStatus.BLOCK.value for g in gates
            ) else "block"
            proof_bundle["passed"] = False
            proof_bundle["transfer_decision"] = {
                "decision": decision_label,
                "blockers": decision_blockers,
                "compliance_only": compliance_only,
                "reason": "; ".join(decision_blockers)
                if decision_blockers
                else "Preflight gates blocked the transfer",
                "warnings": [],
            }
        else:
            # No hard gate blocks; downgrade proof decision to review/approve and surface
            # compliance warnings so the UI shows the risk without disabling the transfer.
            warnings = [b for b in proof_blockers if b not in active_proof_blockers]
            decision = (
                "review"
                if (transfer_decision in {"block", "review"} or compliance_only)
                else "approve"
            )
            proof_bundle["passed"] = True
            proof_bundle["transfer_decision"] = {
                "decision": decision,
                "blockers": [],
                "compliance_only": False,
                "reason": (
                    "No blocking issues detected"
                    if not warnings
                    else "; ".join(warnings)
                ),
                "warnings": warnings,
            }

    from services.preflight_rules import enrich_blockers
    from services.root_cause_engine import apply_root_causes_to_preflight

    dest_kind = normalize_dest_kind(destination_db_type)
    enriched_blockers = enrich_blockers(
        blockers,
        dest_kind=dest_kind,
        validation_mode=validation_mode,
    )

    return apply_root_causes_to_preflight({
        **result,
        "passed": not has_blocks,
        "passed_count": passed_count,
        "total_gates": total_gates,
        "readiness_score": round(passed_count / max(total_gates, 1) * 100, 1),
        "gates": gates,
        "blockers": enriched_blockers,
        "proof_bundle": proof_bundle,
    })
