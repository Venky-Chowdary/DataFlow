"""Which source columns a finished run did not migrate, in one place.

Gate-8 compares the columns a run wrote. A source column that was left out is
outside that comparison, so "Row fidelity verified" and a Trust Report verdict
of "the destination matches the source" said more than was checked
(``docs/migration-rules.md`` rule 4: "An omitted column is not part of the
write and is not part of read-back verification"; rule 12 and R9: a check
reports what this run wrote, and all trust surfaces agree).

The engine records the plan on the job before the first write
(``schema_propagation``: added, omitted and unmapped columns). This module
turns that record into the sentence Gate-8, the trust score and the Trust
Report all use, so the three cannot disagree.
"""

from __future__ import annotations

from typing import Any

#: Why a source column is not written by a successful run.
REASON_OMITTED = "declared_omitted"
REASON_ACKNOWLEDGED = "schema_drift_acknowledged"
#: No mapping and no recorded decision. Execute refuses this since G13 runs
#: there too; a job stored before that can still carry it.
REASON_UNDECIDED = "no_decision"

_REASON_WORDS = {
    REASON_OMITTED: "declared omitted",
    REASON_ACKNOWLEDGED: "left out under the acknowledged schema drift",
    REASON_UNDECIDED: "no mapping and no recorded decision",
}
_NAMED_LIMIT = 8


def _acknowledged_at_validate(preflight: dict[str, Any] | None) -> set[str]:
    """Columns Validate's G13 passed as an acknowledged-drift omission."""
    pf = preflight if isinstance(preflight, dict) else {}
    names: set[str] = set()
    for gate in pf.get("gates") or []:
        if not isinstance(gate, dict) or str(gate.get("id") or "") != "g13_source_coverage":
            continue
        details = gate.get("details") if isinstance(gate.get("details"), dict) else {}
        if str(details.get("omission_reason") or "") != REASON_ACKNOWLEDGED:
            continue
        names.update(str(c).strip().lower() for c in details.get("omitted_sources") or [])
    return names


def not_migrated_columns(
    propagation: dict[str, Any] | None, preflight: dict[str, Any] | None = None
) -> list[dict[str, str]]:
    """``[{column, reason}]`` for the source columns a finished run did not write."""
    recorded = propagation if isinstance(propagation, dict) else {}
    acknowledged = _acknowledged_at_validate(preflight)
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    for key in ("omitted_columns", "skipped_columns"):
        for raw in recorded.get(key) or []:
            name = str(raw or "").strip()
            if not name or name.lower() in seen:
                continue
            seen.add(name.lower())
            if key == "omitted_columns":
                reason = REASON_OMITTED
            else:
                reason = REASON_ACKNOWLEDGED if name.lower() in acknowledged else REASON_UNDECIDED
            out.append({"column": name, "reason": reason})
    return out


def undecided_columns(coverage: dict[str, Any] | None) -> list[str]:
    """Columns left out with no recorded decision."""
    return [
        str(e.get("column") or "")
        for e in ((coverage or {}).get("not_migrated") or [])
        if str(e.get("reason") or "") == REASON_UNDECIDED and str(e.get("column") or "").strip()
    ]


def not_migrated_sentence(entries: list[dict[str, str]] | None, *, source_columns: int = 0) -> str:
    """One sentence naming the columns left out. Empty when every column moved."""
    rows = [e for e in (entries or []) if str(e.get("column") or "").strip()]
    if not rows:
        return ""
    named = ", ".join(
        f"{e['column']} ({_REASON_WORDS.get(str(e.get('reason')), 'left out')})"
        for e in rows[:_NAMED_LIMIT]
    )
    more = f" (+{len(rows) - _NAMED_LIMIT} more)" if len(rows) > _NAMED_LIMIT else ""
    total = int(source_columns or 0)
    scope = (
        f"The check covers the {total - len(rows)} of {total} source columns this run wrote. "
        if total > len(rows)
        else "The check covers only the columns this run wrote. "
    )
    return f"{scope}{len(rows)} source column(s) were not migrated: {named}{more}."


def carried_inside(propagation: dict[str, Any] | None) -> list[dict[str, str]]:
    """``[{column, parent}]``: flattened children written inside their parent object."""
    recorded = propagation if isinstance(propagation, dict) else {}
    return [
        {"column": str(e.get("column")), "parent": str(e.get("parent"))}
        for e in recorded.get("carried_columns") or []
        if isinstance(e, dict) and e.get("column") and e.get("parent")
    ]


def _coverage(
    entries: list[dict[str, str]], total: int, carried: list[dict[str, str]] | None = None
) -> dict[str, Any]:
    statement = not_migrated_sentence(entries, source_columns=total)
    if carried:
        named = ", ".join(f"{e['column']} (carried inside {e['parent']})" for e in carried[:_NAMED_LIMIT])
        more = f" (+{len(carried) - _NAMED_LIMIT} more)" if len(carried) > _NAMED_LIMIT else ""
        statement = (
            f"{statement} {len(carried)} source column(s) are written inside their parent "
            f"object, not as columns of their own: {named}{more}."
        ).strip()
    return {
        "source_columns": total or None,
        "columns_written": max(total - len(entries), 0) if total else None,
        "not_migrated": entries,
        "carried_inside": list(carried or []),
        "statement": statement,
    }


def column_coverage_of(job: dict[str, Any] | None) -> dict[str, Any] | None:
    """The column coverage of a job, or None when every source column moved."""
    doc = job if isinstance(job, dict) else {}
    recon = doc.get("reconciliation") if isinstance(doc.get("reconciliation"), dict) else {}
    stored = recon.get("column_coverage")
    if isinstance(stored, dict) and (stored.get("not_migrated") or stored.get("carried_inside")):
        return stored
    # A job finished before the coverage was stamped on Gate-8 still names the
    # columns it skipped (D165) on ``schema_propagation``.
    propagation = doc.get("schema_propagation")
    entries = not_migrated_columns(propagation, doc.get("preflight"))
    carried = carried_inside(propagation)
    if not entries and not carried:
        return None
    return _coverage(entries, int((propagation or {}).get("source_column_count") or 0), carried)


def stamp_column_coverage(mongo: Any, job_id: str, fields: dict[str, Any]) -> None:
    """Put the columns this run left out on its Gate-8 report and its closing message.

    Called with the success fields before they are written. Reads the plan the
    run recorded (``schema_propagation``), so the COPY path and the row path
    report the same thing.
    """
    recon = fields.get("reconciliation")
    if not isinstance(recon, dict):
        return
    try:
        job = mongo.get_job(job_id) or {}
    except Exception:  # noqa: BLE001 - the report is additive; the run already succeeded
        return
    job = job if isinstance(job, dict) else {}
    propagation = job.get("schema_propagation")
    entries = not_migrated_columns(propagation, job.get("preflight"))
    carried = carried_inside(propagation)
    if not entries and not carried:
        return
    coverage = _coverage(entries, int((propagation or {}).get("source_column_count") or 0), carried)
    recon["column_coverage"] = coverage
    statement = coverage["statement"]
    for holder in (recon, fields):
        message = str(holder.get("message") or "").strip()
        if message and statement not in message:
            holder["message"] = f"{message}{'' if message.endswith('.') else '.'} {statement}"


__all__ = [
    "REASON_ACKNOWLEDGED",
    "REASON_OMITTED",
    "REASON_UNDECIDED",
    "column_coverage_of",
    "not_migrated_columns",
    "not_migrated_sentence",
    "stamp_column_coverage",
    "undecided_columns",
]
