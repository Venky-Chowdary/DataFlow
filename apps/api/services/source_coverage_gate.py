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


def build_source_coverage_gate(
    *,
    source_columns: list[str],
    mappings: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return ``(coverage, gate)`` for the given mapping set."""
    coverage = classify_source_coverage(source_columns, mappings)
    unaccounted = coverage["unaccounted"]
    if not unaccounted:
        return coverage, {
            "id": GATE_ID,
            "status": "pass",
            "message": (
                f"All {coverage['source_count']} source column(s) accounted for — "
                f"{len(coverage['written'])} written, "
                f"{len(coverage['omitted'])} declared omitted"
            ),
            "duration_ms": 0,
            "details": {
                "omitted_sources": coverage["omitted"],
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
            "mapped_count": len(coverage["written"]),
            "omitted_count": len(coverage["omitted"]),
            "source_count": coverage["source_count"],
            "rule_id": f"{GATE_ID}.unaccounted",
            "remediation_kind": "review_mappings",
        },
    }
