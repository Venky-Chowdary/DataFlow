"""Gate G13 — every source column is written, declared omitted, or blocks.

A source column that nobody mapped is an unanswered question, not a decision.
Writing anyway drops it, and the drop is invisible afterwards: the destination
looks complete and the certificate reports success. This gate turns that into
an explicit operator choice — map it, or record the omission in the Decision
Artifact and proof bundle.
"""

from __future__ import annotations

from typing import Any

from services.mapping_constraints import classify_source_coverage

GATE_ID = "g13_source_coverage"
_NAMED_LIMIT = 8


def carried_statement(carried: list[dict[str, str]] | None) -> str:
    """``"nested_city (carried inside nested)"``, one entry per carried child."""
    return ", ".join(
        f"{e['column']} (carried inside {e['parent']})" for e in carried or [] if e.get("column")
    )


def build_source_coverage_gate(
    *,
    source_columns: list[str],
    mappings: list[dict[str, Any]],
    lineage: dict[str, str] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return ``(coverage, gate)`` for the given mapping set.

    ``lineage`` (``flatten_protection.coverage_lineage``) lets a flattened child
    whose parent object is written whole count as carried inside it.
    """
    coverage = classify_source_coverage(source_columns, mappings, lineage)
    unaccounted = coverage["unaccounted"]
    carried = coverage.get("carried") or []
    if not unaccounted:
        message = (
            f"All {coverage['source_count']} source column(s) accounted for — "
            f"{len(coverage['written'])} written, "
            f"{len(coverage['omitted'])} declared omitted"
        )
        if carried:
            message += f", {len(carried)} carried inside their parent: {carried_statement(carried)}"
        return coverage, {
            "id": GATE_ID,
            "status": "pass",
            "message": message,
            "duration_ms": 0,
            "details": {
                "omitted_sources": coverage["omitted"],
                "carried_sources": carried,
                "mapped_count": len(coverage["written"]),
                "source_count": coverage["source_count"],
            },
        }

    named = ", ".join(unaccounted[:_NAMED_LIMIT])
    more = (
        f" (+{len(unaccounted) - _NAMED_LIMIT} more)"
        if len(unaccounted) > _NAMED_LIMIT
        else ""
    )
    return coverage, {
        "id": GATE_ID,
        "status": "block",
        "message": (
            f"{len(unaccounted)} source column(s) are neither mapped nor declared "
            f"omitted: {named}{more} — Datawrap will not drop them silently."
        ),
        "duration_ms": 0,
        "details": {
            "unaccounted_sources": unaccounted,
            "carried_sources": carried,
            "mapped_count": len(coverage["written"]),
            "omitted_count": len(coverage["omitted"]),
            "source_count": coverage["source_count"],
            "rule_id": f"{GATE_ID}.unaccounted",
            "remediation_kind": "review_mappings",
        },
    }
