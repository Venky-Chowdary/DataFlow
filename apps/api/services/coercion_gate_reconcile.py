"""Keep the coercion report and the schema-contract gate telling one story.

The coercion probe judges *values*: it casts the sampled cells and reports what
the write path would do with them. G3 judges the *declared conversion*: some
pairs change the domain even when every sampled value casts cleanly (a bare
scalar wrapped into a JSON document, ObjectId flattened to unbounded text,
DECIMAL narrowed within the sample's range).

When those two disagree the operator sees a run that is blocked while the report
under it says "no blocking failures" — and the AI assistant, the root-cause
panel and the proof bundle all read the report, not the gate. The gate is the
authority on a declared-type block, so its verdict is carried into the report:
the column keeps its *value-level* severity (the cells really did cast) and
gains the gate's block as its own fact, which is what the blocking flag then
reflects. Both readings stay true and neither surface can claim green while the
other blocks.
"""

from __future__ import annotations

from typing import Any

_BLOCKED_BY_GATE = "declared conversion blocked by the schema contract gate"


def _blocking_sources(gate: dict[str, Any] | None) -> dict[str, str]:
    """Source columns G3 blocked, with the reason it gave."""
    if not isinstance(gate, dict) or str(gate.get("status")) != "block":
        return {}
    details = gate.get("details")
    out: dict[str, str] = {}
    for item in (details or {}).get("issues_detail") or []:
        if not isinstance(item, dict):
            continue
        if str(item.get("severity") or "").lower() != "block":
            continue
        if item.get("risk_acknowledged") or item.get("contracted_holdout"):
            continue
        source = str(item.get("source") or item.get("column") or "")
        if source:
            out[source] = str(item.get("message") or item.get("reason") or "")
    return out


def reconcile_coercion_report(
    report: dict[str, Any] | None, gates: list[dict[str, Any]] | None
) -> dict[str, Any]:
    """Promote G3's declared-type blocks into the value-level report.

    Returns a new report; sampled counts and per-value severities are untouched
    — the cells did cast. What changes is that the declared-type block is now
    recorded on the column and in ``has_blocking_failures``, naming the gate so
    nobody re-derives the verdict.
    """
    if not isinstance(report, dict) or not report:
        return report or {}
    gate = next(
        (
            g
            for g in gates or []
            if isinstance(g, dict) and g.get("id") == "g3_schema_contract"
        ),
        None,
    )
    blocked = _blocking_sources(gate)
    if not blocked:
        return report

    def promote(entry: dict[str, Any]) -> dict[str, Any]:
        source = str(entry.get("source") or "")
        if source not in blocked:
            return entry
        return {
            **entry,
            "blocked_by": "g3_schema_contract",
            "block_reason": blocked[source] or _BLOCKED_BY_GATE,
        }

    columns = [
        promote(c) if isinstance(c, dict) else c for c in report.get("columns") or []
    ]
    by_source = {
        key: (promote(value) if isinstance(value, dict) else value)
        for key, value in (report.get("by_source") or {}).items()
    }
    return {
        **report,
        "columns": columns,
        "by_source": by_source,
        "declared_type_blocks": [
            {"source": source, "reason": reason or _BLOCKED_BY_GATE}
            for source, reason in sorted(blocked.items())
        ],
        "has_blocking_failures": bool(blocked)
        or any(
            isinstance(c, dict) and c.get("severity") == "block" for c in columns
        ),
    }
