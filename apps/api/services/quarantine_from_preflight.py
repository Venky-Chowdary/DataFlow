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
# Gate-8 / dry-run cell line: ``row 1 phone→phone: Empty value cannot coerce to integer``.
# Type-pair prose (``name (TYPE) → name (TYPE)``) is handled by ``_PAIR_RE`` first.
_GATE_CELL_RE = re.compile(
    r"^(?:row\s+(?P<row>\d+)\s+)?"
    r"(?P<source>[A-Za-z_][\w.]*)\s*→\s*"
    r"(?P<target>[A-Za-z_][\w.]*)\s*:\s*"
    r"(?P<reason>.+)$",
    re.IGNORECASE,
)
_BLANK_CELL_FIX = (
    "Blank cell. A nullable destination stores SQL NULL and keeps the row. "
    "A NOT NULL column needs a source value or a nullability change — "
    "replay cannot invent a typed value from an empty cell."
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
    if m:
        enriched = dict(issue)
        enriched.setdefault("source", m.group("source"))
        enriched.setdefault("column", m.group("source"))
        enriched.setdefault("target", m.group("target"))
        enriched.setdefault("source_type", m.group("source_type"))
        enriched.setdefault("target_type", m.group("target_type"))
        return enriched
    cell = _GATE_CELL_RE.match(text.strip())
    if not cell:
        return issue
    enriched = dict(issue)
    source = cell.group("source").strip()
    target = cell.group("target").strip()
    inner = cell.group("reason").strip()
    enriched.setdefault("source", source)
    enriched.setdefault("column", source)
    enriched.setdefault("target", target)
    if cell.group("row") and enriched.get("row") is None:
        enriched["row"] = int(cell.group("row"))
    enriched["reason"] = inner
    enriched["message"] = inner
    if inner.lower().startswith("empty value cannot coerce"):
        if enriched.get("sample") is None and enriched.get("value") is None:
            # The cell was blank. Wiring a missing sample as SQL NULL told the
            # operator the probe saw ``__DF_SQL_NULL__`` and that replay had
            # no payload.
            enriched["sample"] = ""
        enriched.setdefault("suggested_fix", _BLANK_CELL_FIX)
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
        # A collapsed root cause explains the gate. It is not a cell, and
        # wiring a missing sample as SQL NULL invented a rejected row of
        # ``__DF_SQL_NULL__`` on a load the destination accepted in full.
        if (details or {}).get("root_cause"):
            continue
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
        had_cell = "sample" in issue or "value" in issue
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
        # No sample means the finding is a policy sentence, not a SQL NULL cell.
        wired = quarantine_cell_wire(value) if had_cell else ""
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
    # A column-level dry-run line (no row index) duplicates the per-row Gate-8
    # findings for the same cell. Keep the row that names the source record.
    located = {
        (str(r.get("column") or ""), str(r.get("target") or ""))
        for r in rows
        if r.get("row") is not None and r.get("column")
    }
    if located:
        rows = [
            r
            for r in rows
            if r.get("row") is not None
            or (str(r.get("column") or ""), str(r.get("target") or "")) not in located
        ]
    rows = drop_phantom_identity_rows(rows)
    try:
        from services.quarantine_row_contract import normalize_quarantine_rows

        return normalize_quarantine_rows(rows, job_id="", connector="preflight")
    except Exception:
        return rows


def drop_phantom_identity_rows(details: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    """Drop a Validate identity sentence that was stored as a rejected cell.

    The row has no source record and no column. Its value is the SQL NULL
    wire sentinel because nothing was sampled. A destination that holds every
    row is not missing that cell.
    """
    from services.value_serializer import SQL_NULL_SENTINEL

    kept: list[dict[str, Any]] = []
    for detail in details or []:
        if not isinstance(detail, dict):
            continue
        reason = str(detail.get("reason") or "")
        value = str(detail.get("value") or "")
        column = detail.get("column")
        if (
            detail.get("row") is None
            and not column
            and value in {"", SQL_NULL_SENTINEL}
            and reason.lower().startswith("duplicate identity keys")
        ):
            continue
        kept.append(detail)
    return kept


def quarantine_evidence_source(
    job: dict[str, Any] | None,
    details: list[dict[str, Any]] | None,
) -> str:
    """Label preflight findings that were stored before any destination write.

    A blocked Execute persists those rows on ``rejected_details``. Calling that
    a write-time reject offered replay for a load that committed nothing.
    """
    job = job or {}
    rows = [d for d in (details or []) if isinstance(d, dict)]
    summary = job.get("destination_summary")
    wrote = bool(summary.get("rejected_details")) if isinstance(summary, dict) else False
    if wrote:
        return "write"
    policies = {str(d.get("policy") or "") for d in rows}
    if rows and policies <= {"preflight_quarantine"}:
        return "preflight"
    if job.get("rejected_details"):
        return "write"
    return "preflight" if rows else "none"


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
    details = drop_phantom_identity_rows(details)

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
        status = str(job.get("status") or "").strip().lower()
        # A completed load with no write rejects did not quarantine the
        # Validate root. Hydrating that root made get_job report rejected_rows
        # while list_jobs, which reads the stored counter, reported 0.
        if status not in {"completed", "succeeded", "success"}:
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
