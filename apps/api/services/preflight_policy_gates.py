"""Preflight policy-gate merge — extracted from preflight_service (F8)."""

from __future__ import annotations

from typing import Any

from preflight.models import GateStatus
from services.blocker_titles import is_schema_drift_echo
from services.db_type_utils import normalize_dest_kind


def is_compliance_block_message(message: object) -> bool:
    """True when this blocker text is a PII/compliance review, not a data defect."""
    text = str(message or "")
    return "PII/compliance" in text or "compliance review" in text.lower()


def is_compliance_only_block(proof_blockers: list[str]) -> bool:
    """Return True when every proof blocker is purely a PII/compliance review."""
    if not proof_blockers:
        return False
    return all(is_compliance_block_message(b) for b in proof_blockers)


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

    # A compliance acknowledgment can only ever clear a compliance blocker. Gates
    # and non-proof blockers keep their own root causes, so the bundle's
    # compliance-only claim (which sees the proof blockers alone) must be
    # re-decided against the *merged* blocker list below — otherwise Validate
    # offers "Approve PII to unlock Execute" while a schema/connectivity gate is
    # the real reason, and the operator's approval looks like it did nothing.
    non_compliance_candidates = [
        *(str(b.get("message") or "") for b in result.get("blockers") or []),
        *(
            str(g.get("message") or "")
            for g in policy_gates or []
            if g.get("status") == GateStatus.BLOCK.value
        ),
    ]
    if any(not is_compliance_block_message(m) for m in non_compliance_candidates):
        compliance_only = False

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
    # A proof string that repeats a gate is one cause. Copying it as proof_N
    # made Validate say "1 gate" in the header and "Schema drift" plus
    # "Schema change detected" in the analysis.
    seen_messages = {
        str(b.get("message") or "").strip()
        for b in blockers
        if str(b.get("message") or "").strip()
    }
    seen_messages.update(
        str(g.get("message") or "").strip()
        for g in (result.get("gates") or [])
        if str(g.get("message") or "").strip()
    )
    drift_already_listed = any(is_schema_drift_echo(m) for m in seen_messages) or any(
        str(b.get("id") or "") == "schema_drift" for b in blockers
    ) or any(
        str(g.get("id") or "") == "schema_drift" and g.get("status") == GateStatus.BLOCK.value
        for g in (result.get("gates") or [])
    )
    for idx, message in enumerate(active_proof_blockers):
        text = str(message).strip()
        if text and text in seen_messages:
            continue
        if text and drift_already_listed and is_schema_drift_echo(text):
            continue
        is_compliance = is_compliance_block_message(message)
        blockers.append({
            "id": f"proof_{idx}",
            "message": str(message),
            "details": {
                "compliance_ack_required": bool(is_compliance and compliance_only),
                "remediation_kind": "acknowledge_compliance" if is_compliance else "fix_proof",
            },
        })
        if text:
            seen_messages.add(text)

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


def _drift_column_names(evolution: dict[str, Any]) -> list[str]:
    """Source columns manual_review keeps off the write when the operator acks."""
    names: list[str] = []
    seen: set[str] = set()

    def add(raw: object) -> None:
        name = str(raw or "").strip()
        key = name.lower()
        if not name or key in seen:
            return
        seen.add(key)
        names.append(name)

    for item in evolution.get("unmapped_sources") or []:
        if isinstance(item, str):
            add(item)
        elif isinstance(item, dict):
            add(item.get("column") or item.get("source"))
    for item in evolution.get("additive") or []:
        if not isinstance(item, dict):
            continue
        if str(item.get("kind") or "") not in {"add_column", "rename"}:
            continue
        add(item.get("column") or item.get("to") or item.get("source"))
    return names


def _coverage_message_cleared(message: object) -> bool:
    text = str(message or "").lower()
    return "neither mapped nor declared omitted" in text


def finalize_acknowledged_schema_drift(
    out: dict[str, Any],
    *,
    evolution: dict[str, Any],
    acknowledgment: dict[str, Any],
) -> None:
    """A recorded manual-review ack is the decision to keep existing mappings.

    New source columns named by that drift are an omission for this run, written
    on the coverage gate. They are not dropped quietly, and they must not leave
    Execute locked on the same change the operator just accepted. Hard-breaking
    drift never reaches this helper.
    """
    ignored = {name.lower() for name in _drift_column_names(evolution)}
    cleared_coverage = False
    for gate in out.get("gates") or []:
        if str(gate.get("id") or "") != "g13_source_coverage":
            continue
        if gate.get("status") != "block":
            continue
        details = gate.get("details") if isinstance(gate.get("details"), dict) else {}
        unaccounted = [str(c) for c in (details.get("unaccounted_sources") or []) if str(c).strip()]
        if not unaccounted or any(c.lower() not in ignored for c in unaccounted):
            continue
        named = ", ".join(unaccounted[:8])
        extra = f" (+{len(unaccounted) - 8} more)" if len(unaccounted) > 8 else ""
        gate["status"] = "pass"
        gate["message"] = (
            f"Schema drift acknowledged — {len(unaccounted)} new source column(s) "
            f"stay out of this run: {named}{extra}. The omission is recorded."
        )
        gate["details"] = {
            **details,
            "unaccounted_sources": [],
            "omitted_sources": unaccounted,
            "omission_reason": "schema_drift_acknowledged",
            "acknowledgment": acknowledgment,
            "rule_id": "g13_source_coverage.drift_omitted",
        }
        cleared_coverage = True

    def _drop_blocker(blocker: dict[str, Any]) -> bool:
        bid = str(blocker.get("id") or "")
        message = str(blocker.get("message") or "")
        if bid == "schema_drift" or is_schema_drift_echo(message):
            return False
        if cleared_coverage and (
            bid == "g13_source_coverage" or _coverage_message_cleared(message)
        ):
            return False
        return True

    out["blockers"] = [b for b in (out.get("blockers") or []) if _drop_blocker(b)]
    gates = [g for g in (out.get("gates") or []) if isinstance(g, dict)]
    blocking = [g for g in gates if g.get("status") == "block"]
    out["passed_count"] = sum(1 for g in gates if g.get("status") == "pass")
    out["total_gates"] = len(gates)
    out["readiness_score"] = round(
        out["passed_count"] / max(out["total_gates"], 1) * 100, 1
    )
    out["passed"] = not blocking
    if blocking:
        return

    pb = out.get("proof_bundle")
    if not isinstance(pb, dict):
        return
    td = pb.get("transfer_decision")
    if not isinstance(td, dict):
        return
    kept: list[object] = []
    for message in td.get("blockers") or []:
        text = str(message or "")
        if is_schema_drift_echo(text):
            continue
        if cleared_coverage and _coverage_message_cleared(text):
            continue
        kept.append(message)
    if kept:
        out["passed"] = False
        out["proof_bundle"] = {
            **pb,
            "transfer_decision": {**td, "blockers": kept},
        }
        return
    out["proof_bundle"] = {
        **pb,
        "passed": True,
        "transfer_decision": {
            **td,
            "decision": "approve",
            "blockers": [],
            "compliance_only": False,
            "reason": (
                "Schema change acknowledged for this run — existing mappings kept"
            ),
        },
    }
