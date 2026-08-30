"""Build inspectable quarantine rows from preflight / integrity findings.

When a job fails at preflight (before any write), operators still need to see
which rows/columns/values are bad — the same Inspect Quarantine UI used after
write-time rejection.
"""

from __future__ import annotations

import re
from typing import Any

_PAIR_RE = re.compile(
    r"(?P<source>[A-Za-z_][\w.]*)\s*\((?P<source_type>[^)]+)\)\s*→\s*"
    r"(?P<target>[A-Za-z_][\w.]*)\s*\((?P<target_type>[^)]+)\)",
)


def _as_issue_dict(item: Any) -> dict[str, Any] | None:
    if isinstance(item, dict):
        return item
    if isinstance(item, str) and item.strip():
        return {"message": item.strip(), "reason": item.strip()}
    return None


def _enrich_from_message(issue: dict[str, Any]) -> dict[str, Any]:
    """Fill source/target/column from Lossy coercion / integrity message text."""
    if issue.get("source") or issue.get("column") or issue.get("field"):
        return issue
    text = str(issue.get("reason") or issue.get("message") or "")
    m = _PAIR_RE.search(text)
    if not m:
        return issue
    enriched = dict(issue)
    enriched.setdefault("source", m.group("source"))
    enriched.setdefault("column", m.group("source"))
    enriched.setdefault("target", m.group("target"))
    enriched.setdefault("source_type", m.group("source_type"))
    enriched.setdefault("target_type", m.group("target_type"))
    return enriched


def _issue_is_non_blocking_warn(issue: dict[str, Any]) -> bool:
    """Skip Risk-Contract-signed / severity=warn noise — not write rejects."""
    sev = str(issue.get("severity") or "").strip().lower()
    if sev in ("warn", "warning", "info", "ok", "pass"):
        return True
    text = str(issue.get("reason") or issue.get("message") or "").lower()
    if "risk contract signed" in text or "continue-policy risk contract" in text:
        return True
    return False


def _collect_issue_lists(preflight: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not preflight:
        return []
    out: list[dict[str, Any]] = []
    for gate in preflight.get("gates") or []:
        if not isinstance(gate, dict):
            continue
        # Passed/warn gates often still carry review-grade coercion notes.
        # Quarantine is for blocking findings — never invent rejects from green gates.
        status = str(gate.get("status") or "").strip().lower()
        if status in ("pass", "passed", "ok", "skip", "skipped", "warn", "warning"):
            continue
        details = gate.get("details") or {}
        if not isinstance(details, dict):
            continue
        # Prefer structured issues_detail (has source/target) over flat strings.
        detail_raw = details.get("issues_detail") or []
        has_structured = isinstance(detail_raw, list) and any(
            isinstance(x, dict) for x in detail_raw
        )
        keys = ("issues_detail", "encoding_issues")
        if not has_structured:
            keys = ("issues_detail", "encoding_issues", "issues", "errors", "issue_texts")
        for key in keys:
            raw = details.get(key) or []
            if not isinstance(raw, list):
                continue
            for item in raw:
                parsed = _as_issue_dict(item)
                if parsed and not _issue_is_non_blocking_warn(parsed):
                    out.append(_enrich_from_message(parsed))
        # Nested integrity payload
        for nested_key in ("integrity_issues", "checks"):
            nested = details.get(nested_key)
            if isinstance(nested, list):
                for item in nested:
                    parsed = _as_issue_dict(item)
                    if parsed and not _issue_is_non_blocking_warn(parsed):
                        out.append(_enrich_from_message(parsed))
    for blocker in preflight.get("blockers") or []:
        if not isinstance(blocker, dict):
            continue
        guidance = blocker.get("guidance") or {}
        details = blocker.get("details") or {}
        for item in (
            details.get("issues_detail")
            or details.get("encoding_issues")
            or details.get("issues")
            or []
        ):
            parsed = _as_issue_dict(item)
            if parsed and not _issue_is_non_blocking_warn(parsed):
                out.append(_enrich_from_message(parsed))
        msg = blocker.get("message")
        if msg and not out:
            parsed = _enrich_from_message({"message": str(msg), "reason": str(msg)})
            if not _issue_is_non_blocking_warn(parsed):
                out.append(parsed)
        if isinstance(guidance, dict) and guidance.get("fix") and out:
            for row in out:
                row.setdefault("suggested_fix", guidance.get("fix"))
    return out


def _pair_key(issue: dict[str, Any]) -> tuple[str, str, str]:
    """Dedupe G3 lossy + G9 integrity for the same source→target pair."""
    source = str(issue.get("source") or issue.get("column") or issue.get("field") or "").lower()
    target = str(issue.get("target") or "").lower()
    reason = str(issue.get("reason") or issue.get("message") or "").lower()
    # Same column→target type-contract findings collapse (G3 + G9 noise).
    if source and target and (
        "lossy" in reason
        or "integrity failed" in reason
        or "coercion" in reason
        or "→" in reason
        or "->" in reason
    ):
        return (source, target, "schema_coercion")
    return (source, target, reason[:80])


def quarantine_rows_from_preflight(preflight: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Return rejected_details-shaped rows for Inspect Quarantine."""
    from connectors.writer_common import quarantine_cell_wire

    issues = _collect_issue_lists(preflight)
    rows: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for issue in issues:
        column = str(issue.get("column") or issue.get("source") or issue.get("field") or "")
        target = str(issue.get("target") or "") or None
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
        pair = _pair_key(issue)
        key = (row_i, pair[0], pair[1], pair[2], str(value)[:80] if value is not None else "")
        if key in seen:
            continue
        seen.add(key)
        # Prefer the richer specialty/lossy label over bare integrity text.
        if "specialty polarity" in reason.lower() or reason.lower().startswith("lossy"):
            pass
        # Never default schema/policy findings to strip_controls — that misleads
        # operators into encoding remediations for DDL/policy blockers.
        suggested_transform = issue.get("suggested_transform")
        if not suggested_transform:
            low = reason.lower()
            if any(
                k in low
                for k in (
                    "format-control",
                    "replacement character",
                    "encoding",
                    "zero-width",
                    "null byte",
                )
            ):
                suggested_transform = "strip_controls"
            else:
                suggested_transform = None
        wired = quarantine_cell_wire(value)
        detail: dict[str, Any] = {
            "row": row_i,
            "column": column or None,
            "target": target or column or None,
            "value": wired[:500],
            "reason": reason[:500],
            "policy": "preflight_quarantine",
            "chars": issue.get("chars"),
            "suggested_transform": suggested_transform,
            "suggested_fix": issue.get("suggested_fix") or issue.get("suggested_fix"),
            "suggested_target_type": issue.get("suggested_target_type"),
            "source_type": issue.get("source_type"),
            "target_type": issue.get("target_type"),
        }
        if column:
            detail["values"] = {column: wired}
        rows.append(detail)
        if len(rows) >= 200:
            break
    try:
        from services.quarantine_row_contract import normalize_quarantine_rows

        return normalize_quarantine_rows(rows, job_id="", connector="preflight")
    except Exception:
        return rows


def merge_job_quarantine(
    job: dict[str, Any] | None,
    *,
    hydrate_dlq: bool = True,
) -> list[dict[str, Any]]:
    """Prefer write-time rejected_details; hydrate durable DLQ when truncated.

    Job status stores a sample (``[:2000]``). Full quarantine bodies live in the
    control-plane DLQ — Inspect must not pretend the sample is complete.
    """
    if not job:
        return []
    details = list(job.get("rejected_details") or [])
    dest = job.get("destination_summary") if isinstance(job.get("destination_summary"), dict) else {}
    if not details:
        details = list((dest or {}).get("rejected_details") or [])

    # Job documents identify themselves with ``_id``; ``id``/``job_id`` are the
    # API-shaped aliases. Reading only the aliases meant a raw Mongo document
    # hydrated nothing, so Inspect showed "5,000 quarantined / 0 findings" while
    # 2,500 durable findings sat in the DLQ under that very job.
    job_id = str(
        job.get("id") or job.get("job_id") or job.get("_id") or ""
    ).strip()
    truncated = bool(
        job.get("rejected_details_truncated")
        or (dest or {}).get("rejected_details_truncated")
    )
    try:
        total_hint = int(
            job.get("rejected_details_total")
            or job.get("rejected_rows")
            or (dest or {}).get("rejected_details_total")
            or (dest or {}).get("rejected_rows")
            or 0
        )
    except (TypeError, ValueError):
        total_hint = 0

    if hydrate_dlq and job_id:
        try:
            from services.quarantine_dlq import quarantine_details_from_dlq

            dlq_rows = quarantine_details_from_dlq(job_id)
        except Exception:
            dlq_rows = []
        if dlq_rows and (
            not details
            or truncated
            or len(dlq_rows) > len(details)
            or (total_hint > 0 and len(details) < total_hint)
        ):
            details = dlq_rows

    if not details:
        details = quarantine_rows_from_preflight(job.get("preflight"))

    if details:
        try:
            from services.quarantine_dlq import apply_replay_overlay, job_quarantine_closure

            return apply_replay_overlay(
                details,
                job_id=job_id,
                closure=job_quarantine_closure(job),
            )
        except Exception:
            return details
    return []
