"""Source-drift approvals, classified by gate code.

The coverage gate says "1 source column(s) are neither mapped nor declared
omitted: currency". That sentence does not contain the words "schema drift",
so a matcher on the message parked the schedule as RUN_REFUSED and left a
manual run with no approval at all. The gate id is the decision.
"""

from __future__ import annotations

from typing import Any, Mapping

from services.schedule_approvals import (
    KIND_SOURCE_DRIFT,
    build_approval_request,
    open_approval_request,
)
from services.standing_authorization import SCOPE_SCHEMA_DRIFT

SOURCE_DRIFT_CODE = "SOURCE_DRIFT"

#: Gate ids and rule ids that are a source-shape decision, not a generic refusal.
_DRIFT_GATE_IDS = frozenset({"schema_drift", "g13_source_coverage"})

DRIFT_CORRECTIVE = (
    "Map the new source column, or declare it omitted and acknowledge the "
    "drift, before the next run."
)
BREAKING_CORRECTIVE = (
    "Review the breaking schema change in Map. A type narrowing cannot be "
    "approved away."
)


def is_source_drift_code(code: object) -> bool:
    """True for a structured gate id or rule id, never for prose."""
    token = str(code or "").strip().lower()
    if not token:
        return False
    if token in _DRIFT_GATE_IDS:
        return True
    if token.startswith("schema_drift.") or token.startswith("g13_source_coverage"):
        return True
    # Field-reduction records the same unanswered column as ``no_disposition``
    # and points at G13. That code is the drift, even when the gate id is G16.
    if token == "no_disposition" or token.endswith(".no_disposition"):
        return True
    return False


def message_is_unmapped_column_drift(message: str) -> bool:
    """The coverage sentence, including the field-reduction wording at G13."""
    text = str(message or "").lower()
    return "neither mapped nor declared omitted" in text


def _rule_id(node: Mapping[str, Any]) -> str:
    details = node.get("details")
    if isinstance(details, Mapping):
        return str(details.get("rule_id") or "")
    return ""


def gate_codes_from_preflight(preflight: Mapping[str, Any] | None) -> list[str]:
    """Blocking gate ids and rule ids. Message text is not a code."""
    out: list[str] = []
    seen: set[str] = set()

    def add(raw: object) -> None:
        token = str(raw or "").strip()
        key = token.lower()
        if not token or key in seen:
            return
        seen.add(key)
        out.append(token)

    pf = preflight if isinstance(preflight, Mapping) else {}
    for gate in pf.get("gates") or []:
        if not isinstance(gate, Mapping):
            continue
        if str(gate.get("status") or "").strip().lower() != "block":
            continue
        add(gate.get("id"))
        add(_rule_id(gate))
    for blocker in pf.get("blockers") or []:
        if not isinstance(blocker, Mapping):
            continue
        add(blocker.get("id"))
        add(_rule_id(blocker))
    return out


def gate_codes_from_job(job: Mapping[str, Any] | None) -> list[str]:
    """Codes stamped on a finished job: preflight gates and error guidance."""
    doc = job if isinstance(job, Mapping) else {}
    codes = gate_codes_from_preflight(
        doc.get("preflight") if isinstance(doc.get("preflight"), Mapping) else {}
    )
    seen = {c.lower() for c in codes}
    details = doc.get("error_details") if isinstance(doc.get("error_details"), Mapping) else {}
    for item in details.get("guidance") or []:
        if not isinstance(item, Mapping):
            continue
        token = str(item.get("gate") or "").strip()
        if token and token.lower() not in seen:
            seen.add(token.lower())
            codes.append(token)
    return codes


def unaccounted_sources_from_job(job: Mapping[str, Any] | None) -> list[str]:
    """Source columns G13 named as neither mapped nor omitted, from its details.

    A run refused inside the job carries the gate, not the drift guard's
    evidence; the finding needs the column names so a drift acknowledgement
    can record exactly those as omitted.
    """
    doc = job if isinstance(job, Mapping) else {}
    pf = doc.get("preflight") if isinstance(doc.get("preflight"), Mapping) else {}
    out: list[str] = []
    for node in list(pf.get("gates") or []) + list(pf.get("blockers") or []):
        if not isinstance(node, Mapping) or str(node.get("id") or "") != "g13_source_coverage":
            continue
        details = node.get("details") if isinstance(node.get("details"), Mapping) else {}
        for col in details.get("unaccounted_sources") or []:
            name = str(col or "").strip()
            if name and name not in out:
                out.append(name)
    return out


def drift_codes(codes: list[str] | tuple[str, ...]) -> list[str]:
    return [code for code in codes if is_source_drift_code(code)]


def drift_is_breaking(codes: list[str] | tuple[str, ...]) -> bool:
    for code in codes:
        token = str(code or "").strip().lower()
        if token.startswith("schema_drift.") and "breaking" in token:
            return True
    return False


def schedule_id_from_job(job: Mapping[str, Any] | None) -> str:
    doc = job if isinstance(job, Mapping) else {}
    direct = str(doc.get("schedule_id") or "").strip()
    if direct:
        return direct
    request = doc.get("transfer_request")
    if not isinstance(request, Mapping):
        return ""
    for side in ("source", "destination"):
        endpoint = request.get(side)
        if not isinstance(endpoint, Mapping):
            continue
        extra = endpoint.get("extra")
        if isinstance(extra, Mapping):
            sched = str(extra.get("schedule_id") or "").strip()
            if sched:
                return sched
    return ""


def build_source_drift_approval(
    *,
    message: str,
    codes: list[str],
    job_id: str = "",
    attempt: int = 0,
    binding: dict[str, Any] | None = None,
    evidence: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """One SOURCE_DRIFT request, or None when no drift gate blocked."""
    found = drift_codes(codes)
    if not found:
        return None
    breaking = drift_is_breaking(found)
    scopes = () if breaking else (SCOPE_SCHEMA_DRIFT,)
    found_evidence = {
        "gate_codes": found,
        **(evidence or {}),
    }
    return build_approval_request(
        kind=KIND_SOURCE_DRIFT,
        code=SOURCE_DRIFT_CODE,
        finding=str(message or "").strip()[:1000],
        corrective_action=BREAKING_CORRECTIVE if breaking else DRIFT_CORRECTIVE,
        binding=dict(binding or {"binding_hash": job_id or "manual"}),
        requested_scopes=scopes,
        job_id=job_id,
        run_attempt=attempt,
        evidence=found_evidence,
    )


def open_schedule_drift_approval(
    schedule_id: str,
    approval: dict[str, Any],
) -> None:
    """Park the schedule unless this job already opened the same finding."""
    if not str(schedule_id or "").strip() or not approval:
        return
    from services.schedule_store import get_schedule

    sched = get_schedule(schedule_id)
    if not sched:
        return
    existing = sched.approval_request if isinstance(sched.approval_request, dict) else {}
    if (
        str(existing.get("status") or "").strip().lower() == "open"
        and str(existing.get("code") or "") == SOURCE_DRIFT_CODE
        and str(existing.get("job_id") or "") == str(approval.get("job_id") or "")
        and str(existing.get("job_id") or "")
    ):
        return
    from services.standing_authorization import binding_from_schedule

    payload = dict(approval)
    payload["binding"] = binding_from_schedule(sched)
    # The fingerprint includes the binding. Rebuild so the schedule inbox
    # identity matches a later beat of the same plan.
    rebuilt = build_approval_request(
        kind=str(payload.get("kind") or KIND_SOURCE_DRIFT),
        code=str(payload.get("code") or SOURCE_DRIFT_CODE),
        finding=str(payload.get("finding") or ""),
        corrective_action=str(payload.get("corrective_action") or DRIFT_CORRECTIVE),
        binding=payload["binding"],
        requested_scopes=tuple(payload.get("requested_scopes") or ()),
        job_id=str(payload.get("job_id") or ""),
        run_attempt=int(payload.get("run_attempt") or 0),
        evidence=payload.get("evidence") if isinstance(payload.get("evidence"), dict) else {},
    )
    open_approval_request(schedule_id, rebuilt)
