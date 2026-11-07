"""Kernel Validation Orchestrator (Phase C8).

Classifies G1–G9 (and host policy) results into schema / semantic / population /
runtime / policy / proof buckets. Gates must not invent ConversionClass or DDL —
they consume Decision Artifact authority when present.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Mapping


class ValidationClass(str, Enum):
    SCHEMA = "schema"
    SEMANTIC = "semantic"
    POPULATION = "population"
    RUNTIME = "runtime"
    POLICY = "policy"
    PROOF = "proof"


# Honest labels: G8/G9 in preflight are sample screening, not population proof.
_GATE_CLASS: dict[str, ValidationClass] = {
    "g1_source": ValidationClass.RUNTIME,
    "g2_destination": ValidationClass.RUNTIME,
    "g3_schema_contract": ValidationClass.SCHEMA,
    "g4_mapping_confidence": ValidationClass.SEMANTIC,
    "g5_dry_run": ValidationClass.RUNTIME,
    "g6_target_ddl": ValidationClass.SCHEMA,
    "g7_capacity": ValidationClass.RUNTIME,
    "g8_reconciliation": ValidationClass.POPULATION,
    "g9_data_integrity": ValidationClass.POPULATION,
    "ddl_identity": ValidationClass.PROOF,
    "decision_artifact": ValidationClass.PROOF,
    "policy": ValidationClass.POLICY,
    "fk_constraint": ValidationClass.POLICY,
    "schema_drift": ValidationClass.SCHEMA,
    "compliance": ValidationClass.POLICY,
}


def validation_class_for_gate(gate_id: str) -> ValidationClass:
    gid = (gate_id or "").strip().lower()
    return _GATE_CLASS.get(gid, ValidationClass.RUNTIME)


def classify_gate_results(
    gates: list[Mapping[str, Any]] | None,
    *,
    blockers: list[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Bucket gate rows by ValidationClass for Validate UI / proof packs."""
    buckets: dict[str, list[dict[str, Any]]] = {c.value: [] for c in ValidationClass}
    for raw in list(gates or []) + list(blockers or []):
        if not isinstance(raw, Mapping):
            continue
        gid = str(raw.get("id") or raw.get("gate_id") or raw.get("gate") or "")
        vclass = validation_class_for_gate(gid)
        row = {
            "id": gid,
            "validation_class": vclass.value,
            "status": str(raw.get("status") or ("block" if raw in (blockers or []) else "")),
            "message": str(raw.get("message") or ""),
            "sample_is_population_proof": False
            if vclass is ValidationClass.POPULATION
            else None,
        }
        buckets[vclass.value].append(row)
    blocked_classes = sorted(
        {
            validation_class_for_gate(str(b.get("id") or b.get("gate_id") or "")).value
            for b in (blockers or [])
            if isinstance(b, Mapping)
        }
    )
    return {
        "by_class": buckets,
        "blocked_classes": blocked_classes,
        "population_note": (
            "G8/G9 preflight rows are sample screening — migration_proven requires "
            "full-population checksum proof (Proof Engine)."
        ),
    }


def orchestrate_validation_summary(
    *,
    decision_artifact: Mapping[str, Any] | None,
    gates: list[Mapping[str, Any]] | None,
    blockers: list[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Validation Orchestrator output stamped onto proof_bundle.

    Fail-closed honesty: missing Decision Artifact is a proof-class blocker
    signal (Execute will refuse). Does not re-run gates.
    """
    classified = classify_gate_results(gates, blockers=blockers)
    art = decision_artifact if isinstance(decision_artifact, Mapping) else None
    artifact_hash = str((art or {}).get("content_hash") or "")
    proof_ok = bool(artifact_hash) and len(artifact_hash) == 64
    if not proof_ok:
        classified["by_class"][ValidationClass.PROOF.value].append(
            {
                "id": "decision_artifact",
                "validation_class": ValidationClass.PROOF.value,
                "status": "block",
                "message": (
                    "Decision Artifact missing or incomplete — Validate must stamp "
                    "content_hash before Execute."
                ),
                "sample_is_population_proof": None,
            }
        )
        if ValidationClass.PROOF.value not in classified["blocked_classes"]:
            classified["blocked_classes"] = sorted(
                set(classified["blocked_classes"]) | {ValidationClass.PROOF.value}
            )
    return {
        "orchestrator": "decision_kernel.validation.v1",
        "decision_artifact_hash": artifact_hash or None,
        "decision_artifact_present": proof_ok,
        **classified,
    }


__all__ = [
    "ValidationClass",
    "classify_gate_results",
    "orchestrate_validation_summary",
    "validation_class_for_gate",
]
