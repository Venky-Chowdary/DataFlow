"""Audit-trail helpers for compiled rule reports.

``business_rules.compiled`` is appended once per report fingerprint (idempotent
across re-imports); ``business_rules.executed`` links a job to the compile
event so the run can cite the exact workbook contract it enforced. Events carry
ids, counts and hashes only — never cell values or rule text.
"""

from __future__ import annotations

from typing import Any

from services.audit_log import append_audit_event, list_audit_events

COMPILED_ACTION = "business_rules.compiled"
EXECUTED_ACTION = "business_rules.executed"


def find_rule_report_event(
    fingerprint: str, workspace_id: str = ""
) -> dict[str, Any] | None:
    """Existing ``business_rules.compiled`` event for this report fingerprint."""
    fp = (fingerprint or "").strip()
    if not fp:
        return None
    ws = (workspace_id or "").strip()
    try:
        events = list_audit_events(limit=500, workspace_id=ws or None)
    except Exception:
        return None
    resource = f"rule_report:{fp}"
    for event in events:
        if event.get("action") == COMPILED_ACTION and event.get("resource") == resource:
            return event
    return None


def record_rule_report_compiled(
    *, report: dict[str, Any], filename: str, actor: str, workspace_id: str = ""
) -> str:
    """Append the compiled event once per fingerprint; return its event_hash.

    Returns the existing event's hash on re-import, or "" when the report has
    no fingerprint or the audit store is unavailable — compile must never fail
    because evidence could not be written.
    """
    fp = str(report.get("report_fingerprint") or "").strip()
    if not fp:
        return ""
    existing = find_rule_report_event(fp, workspace_id)
    if existing is not None:
        return str(existing.get("event_hash") or "")
    counts: dict[str, int] = {}
    ids_by_status: dict[str, list[str]] = {}
    for rule in report.get("rules") or []:
        if not isinstance(rule, dict):
            continue
        status = str(rule.get("status") or "unknown")
        counts[status] = counts.get(status, 0) + 1
        rid = str(rule.get("rule_id") or "")
        if rid:
            ids_by_status.setdefault(status, []).append(rid)
    try:
        event = append_audit_event(
            action=COMPILED_ACTION,
            resource=f"rule_report:{fp}",
            actor=actor,
            level="info",
            workspace_id=(workspace_id or "").strip() or None,
            details={
                "compiler_version": report.get("compiler_version"),
                "input_sha256": report.get("input_sha256"),
                "report_fingerprint": fp,
                "filename": filename,
                "rule_counts_by_status": counts,
                "rule_ids_by_status": ids_by_status,
            },
        )
    except Exception:
        return ""
    return str(event.get("event_hash") or "")
