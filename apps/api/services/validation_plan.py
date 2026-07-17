"""Validation plan: hard gates vs soft gates.

A validation plan is a transfer-specific checklist of HARD gates (must pass to
commit) and SOFT gates (inform confidence but do not block alone).  It is built
from the connector capabilities, mapping confidence, and the user's validation
mode, so the UI can explain *why* each gate is being applied.
"""

from __future__ import annotations

from typing import Any

from services.connector_capability_registry import get_connector_capability
from services.preflight_rules import PREFLIGHT_GATE_RULES


class ValidationGate:
    """One gate in a validation plan."""

    def __init__(
        self,
        gate_id: str,
        *,
        hard: bool,
        threshold: float | None = None,
        reason: str = "",
    ):
        self.gate_id = gate_id
        self.hard = hard
        self.threshold = threshold
        self.reason = reason

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.gate_id,
            "title": PREFLIGHT_GATE_RULES.get(self.gate_id, {}).get("title", self.gate_id),
            "category": "hard" if self.hard else "soft",
            "threshold": self.threshold,
            "reason": self.reason,
        }


class ValidationPlan:
    def __init__(self, gates: list[ValidationGate]):
        self.gates = gates

    @property
    def hard_gates(self) -> list[ValidationGate]:
        return [g for g in self.gates if g.hard]

    @property
    def soft_gates(self) -> list[ValidationGate]:
        return [g for g in self.gates if not g.hard]

    def to_dict(self) -> dict[str, Any]:
        return {
            "hard_gates": [g.to_dict() for g in self.hard_gates],
            "soft_gates": [g.to_dict() for g in self.soft_gates],
            "total": len(self.gates),
        }


def _is_hard_gate(
    gate_id: str,
    *,
    source_capability: dict[str, Any],
    target_capability: dict[str, Any],
    validation_mode: str,
    write_semantics: str = "append",
) -> bool:
    """Decide whether a gate is hard for this transfer."""
    if gate_id in {"g1_source", "g2_destination", "g5_dry_run", "g6_target_ddl"}:
        return True
    if gate_id in {"g3_schema_contract"}:
        # For schemaless stores, contract is advisory unless lossiness is certain.
        if not source_capability.get("requires_schema") or not target_capability.get("requires_schema"):
            if validation_mode in {"strict", "maximum"}:
                return True
            return False
        return True
    if gate_id == "g4_mapping_confidence":
        return validation_mode in {"strict", "maximum"}
    if gate_id == "g9_data_integrity":
        return True
    if gate_id in {"proof_bundle", "schema_drift"}:
        return True
    if gate_id == "g7_capacity":
        return False
    if gate_id == "g8_reconciliation":
        return write_semantics in {"merge", "upsert"}
    if gate_id == "g9_sync_contract":
        return True
    if gate_id == "g10_schema_policy":
        return False
    if gate_id == "g11_validation_posture":
        return False
    return False


def build_validation_plan(
    *,
    source_format: str = "",
    target_format: str = "",
    validation_mode: str = "strict",
    write_semantics: str = "append",
    confidence_threshold: float = 0.85,
) -> ValidationPlan:
    """Build a validation plan from transfer context and connector capabilities."""
    src = get_connector_capability(source_format)
    tgt = get_connector_capability(target_format)

    gate_ids = [
        "g1_source",
        "g2_destination",
        "g3_schema_contract",
        "g4_mapping_confidence",
        "g5_dry_run",
        "g6_target_ddl",
        "g7_capacity",
        "g8_reconciliation",
        "g9_data_integrity",
        "proof_bundle",
        "schema_drift",
    ]

    gates: list[ValidationGate] = []
    for gate_id in gate_ids:
        hard = _is_hard_gate(
            gate_id,
            source_capability=src,
            target_capability=tgt,
            validation_mode=validation_mode,
            write_semantics=write_semantics,
        )
        threshold = None
        reason = ""
        if gate_id == "g4_mapping_confidence":
            threshold = confidence_threshold
            reason = f"Confidence must be at or above {confidence_threshold} for {validation_mode} mode."
        elif gate_id == "g3_schema_contract":
            reason = "Lossy type coercion is a data-loss risk."
            if not src.get("requires_schema") or not tgt.get("requires_schema"):
                reason += " Schemaless destinations relax this gate unless strict mode is active."
        elif gate_id == "g5_dry_run":
            reason = "Sample transforms must succeed before any rows are committed."
        elif gate_id == "g6_target_ddl":
            reason = "Target constraints (PK uniqueness, column existence, width) must be satisfied."
        elif gate_id == "g9_data_integrity":
            reason = "Required keys cannot be null and duplicates would violate target uniqueness."

        gates.append(ValidationGate(gate_id, hard=hard, threshold=threshold, reason=reason))

    return ValidationPlan(gates)
