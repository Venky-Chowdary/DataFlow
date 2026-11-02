"""Blocking message and evidence details for the `constraint_fk` preflight gate.

Kept beside the FK findings themselves so the coverage label (destination FK
metadata vs sample orphan probe vs population orphan probe) is decided in one
place — a sample probe must never be reported as population proof.
"""

from __future__ import annotations

from typing import Any

_BLOCKING = {"block", "ack_required"}


def _blocking(finding: Any) -> bool:
    return (
        isinstance(finding, dict)
        and str(finding.get("severity") or "").lower() in _BLOCKING
    )


def build_fk_block(
    findings: list[dict[str, Any]],
    *,
    ri_posture: dict[str, Any],
    population_orphan_probe_ran: bool,
    sample_orphan_probe_ran: bool,
) -> tuple[str, dict[str, Any]]:
    """Return ``(message, details)`` for a blocking FK gate."""
    block_msgs = [
        str(f.get("message") or f.get("code") or "Foreign key coverage incomplete")
        for f in findings
        if _blocking(f)
    ]
    message = (
        block_msgs[0]
        if block_msgs
        else "Destination FK columns unmapped — transfer blocked"
    )

    if any(
        _blocking(f) and f.get("coverage") == "population_orphan_probe"
        for f in findings
    ):
        coverage = "population_orphan_probe"
    elif any(
        _blocking(f) and f.get("coverage") == "sample_orphan_probe" for f in findings
    ):
        coverage = "sample_orphan_probe"
    else:
        coverage = "destination_fk_metadata"

    rule_id = {
        "population_orphan_probe": "constraint_fk.population_orphan",
        "sample_orphan_probe": "constraint_fk.sample_orphan",
    }.get(coverage, "constraint_fk.unmapped")

    return message, {
        "findings": findings,
        "coverage": coverage,
        "remediation_kind": "acknowledge_fk_risk",
        "ack_required": True,
        "population_orphan_proven": bool(ri_posture.get("proven")),
        "population_orphan_probe_ran": population_orphan_probe_ran,
        "sample_orphan_probe_ran": sample_orphan_probe_ran,
        "rule_id": rule_id,
    }
