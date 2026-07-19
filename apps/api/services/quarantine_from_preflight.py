"""Build inspectable quarantine rows from preflight / integrity findings.

When a job fails at preflight (before any write), operators still need to see
which rows/columns/values are bad — the same Inspect Quarantine UI used after
write-time rejection.
"""

from __future__ import annotations

from typing import Any


def _as_issue_dict(item: Any) -> dict[str, Any] | None:
    if isinstance(item, dict):
        return item
    if isinstance(item, str) and item.strip():
        return {"message": item.strip(), "reason": item.strip()}
    return None


def _collect_issue_lists(preflight: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not preflight:
        return []
    out: list[dict[str, Any]] = []
    for gate in preflight.get("gates") or []:
        if not isinstance(gate, dict):
            continue
        details = gate.get("details") or {}
        if not isinstance(details, dict):
            continue
        for key in ("encoding_issues", "issues", "errors", "issue_texts"):
            raw = details.get(key) or []
            if not isinstance(raw, list):
                continue
            for item in raw:
                parsed = _as_issue_dict(item)
                if parsed:
                    out.append(parsed)
        # Nested integrity payload
        for nested_key in ("integrity_issues", "checks"):
            nested = details.get(nested_key)
            if isinstance(nested, list):
                for item in nested:
                    parsed = _as_issue_dict(item)
                    if parsed:
                        out.append(parsed)
    for blocker in preflight.get("blockers") or []:
        if not isinstance(blocker, dict):
            continue
        guidance = blocker.get("guidance") or {}
        details = blocker.get("details") or {}
        for item in (details.get("encoding_issues") or details.get("issues") or []):
            parsed = _as_issue_dict(item)
            if parsed:
                out.append(parsed)
        msg = blocker.get("message")
        if msg and not out:
            out.append({"message": str(msg), "reason": str(msg)})
        if isinstance(guidance, dict) and guidance.get("fix") and out:
            for row in out:
                row.setdefault("suggested_fix", guidance.get("fix"))
    return out


def quarantine_rows_from_preflight(preflight: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Return rejected_details-shaped rows for Inspect Quarantine."""
    issues = _collect_issue_lists(preflight)
    rows: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for issue in issues:
        column = str(issue.get("column") or issue.get("source") or issue.get("field") or "")
        row_num = issue.get("row")
        try:
            row_i = int(row_num) if row_num is not None else None
        except (TypeError, ValueError):
            row_i = None
        value = issue.get("sample")
        if value is None:
            value = issue.get("value")
        reason = str(
            issue.get("reason")
            or issue.get("message")
            or issue.get("suggested_fix")
            or "Preflight integrity finding"
        )
        key = (row_i, column, reason[:120], str(value)[:80])
        if key in seen:
            continue
        seen.add(key)
        detail: dict[str, Any] = {
            "row": row_i,
            "column": column or None,
            "target": issue.get("target") or column or None,
            "value": "" if value is None else str(value)[:500],
            "reason": reason[:500],
            "policy": "preflight_quarantine",
            "chars": issue.get("chars"),
            "suggested_transform": issue.get("suggested_transform") or "strip_controls",
            "suggested_fix": issue.get("suggested_fix") or issue.get("suggested_fix"),
        }
        if column and value is not None:
            detail["values"] = {column: str(value)[:500]}
        rows.append(detail)
        if len(rows) >= 200:
            break
    return rows


def merge_job_quarantine(job: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Prefer write-time rejected_details; fall back to preflight findings."""
    if not job:
        return []
    details = list(job.get("rejected_details") or [])
    if not details:
        dest = job.get("destination_summary") or {}
        details = list(dest.get("rejected_details") or [])
    if details:
        return details
    return quarantine_rows_from_preflight(job.get("preflight"))
