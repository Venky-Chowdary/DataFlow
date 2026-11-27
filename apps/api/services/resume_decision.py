"""The decision an operator makes when resuming (or re-running) a stopped job.

A run under ``manual_review`` stops before the batch that brings a field it may
not add, and before a write that would leave a source column undecided. The
remedy is a decision, not a different job: resume the same job with

* ``schema_policy`` ``propagate_columns`` / ``propagate_all``, or
  ``backfill_new_fields`` true, so the run adds the column; and/or
* ``omit_columns``: the columns to leave out, recorded as declared omissions;

plus a ``reason``. It needs ``job.manage``, the permission that approves a
destination change elsewhere (the Trust Report's widen-column fix). The actor,
the reason and the decision are recorded on the job and in the audit log, and
the resume continues from the job's checkpoint.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field

ADDING_POLICIES = ("propagate_columns", "propagate_all")


class SchemaDecision(BaseModel):
    schema_policy: str | None = Field(default=None, max_length=40)
    backfill_new_fields: bool | None = None
    omit_columns: list[str] = Field(default_factory=list, max_length=200)
    reason: str = Field(default="", max_length=500)

    def has_decision(self) -> bool:
        return bool(self.schema_policy or self.backfill_new_fields or self.omit_columns)


def decision_refusal(user: dict[str, Any] | None, decision: SchemaDecision) -> tuple[int, str] | None:
    """``(status, reason)`` when this caller may not make this decision, else ``None``.

    No bound principal means authentication is off (a single local operator),
    the same reading the RBAC middleware gives it.
    """
    from services.acknowledgment_contract import MIN_REASON_LEN
    from services.rbac import Permission, has_permission

    if user is not None and not has_permission(user, Permission.JOB_MANAGE):
        return 403, (
            "Resuming with a schema decision needs the job.manage permission, the "
            "permission that approves destination changes."
        )
    policy = (decision.schema_policy or "").strip().lower()
    if policy and policy not in ADDING_POLICIES:
        return 400, (
            f"schema_policy on resume adds columns: one of {', '.join(ADDING_POLICIES)}. "
            "To leave a column out, name it in omit_columns."
        )
    if len((decision.reason or "").strip()) < MIN_REASON_LEN:
        return 400, f"reason is required (at least {MIN_REASON_LEN} characters)"
    return None


def _named_by(details: Any) -> list[Any]:
    if not isinstance(details, dict):
        return []
    evolution = details.get("schema_evolution") if isinstance(details.get("schema_evolution"), dict) else {}
    return [
        *(details.get("unaccounted_sources") or []),
        *(evolution.get("unmapped_sources") or []),
        *(a.get("source") or a.get("column") for a in evolution.get("additive") or [] if isinstance(a, dict)),
    ]


def stop_named_columns(job: dict[str, Any] | None) -> list[str]:
    """The source columns this job's stop named.

    An Execute stop names them in ``error_details.unaccounted_sources``; a
    stop at the run's own preflight names them in its blocking gate (the
    columns it could not account for, or the ADD COLUMN it may not make).
    """
    doc = job or {}
    named = _named_by(doc.get("error_details"))
    preflight = doc.get("preflight") if isinstance(doc.get("preflight"), dict) else {}
    for gate in [*(preflight.get("blockers") or []), *(preflight.get("gates") or [])]:
        if isinstance(gate, dict) and (gate in (preflight.get("blockers") or []) or gate.get("status") == "block"):
            named.extend(_named_by(gate.get("details")))
    return list(dict.fromkeys(str(c).strip() for c in named if str(c or "").strip()))


def omit_refusal(job: dict[str, Any] | None, decision: SchemaDecision) -> tuple[int, str] | None:
    """``(422, reason)`` when ``omit_columns`` names a column the stop did not name.

    Leaving out a column the job already writes would write the rows still to
    come without it (a blanked value reported only as "declared omitted"). The
    stop names the columns it could not account for; only those can be left
    out by a decision here. Any other change of the map is a new plan (Studio).
    """
    asked = [str(c).strip() for c in decision.omit_columns if str(c or "").strip()]
    if not asked:
        return None
    named = stop_named_columns(job)
    allowed = {c.lower() for c in named}
    others = [c for c in asked if c.lower() not in allowed]
    if not others:
        return None
    stated = ", ".join(named) if named else "none"
    return 422, (
        f"omit_columns may name only the source columns this job stopped on "
        f"(unaccounted_sources: {stated}); {', '.join(others)} "
        f"{'is' if len(others) == 1 else 'are'} not among them. Leaving out a column "
        "the job already writes would write the remaining rows without its values. "
        "To change the column map, plan the transfer again in Transfer Studio."
    )


def _omit_rows(columns: list[str], actor: str) -> list[dict[str, Any]]:
    return [
        {"source": c, "target": "", "confidence": 0.0, "intentional_omit": True, "decided_by": actor}
        for c in dict.fromkeys(str(c).strip() for c in columns if str(c).strip())
    ]


def _with_omits(mappings: list[dict[str, Any]] | None, omits: list[dict[str, Any]]) -> list[dict[str, Any]]:
    names = {str(o["source"]).lower() for o in omits}
    kept = [m for m in mappings or [] if str(m.get("source") or "").strip().lower() not in names]
    return [*kept, *omits]


def apply_schema_decision(target: Any, decision: SchemaDecision, *, actor: str) -> None:
    """Apply the decision to a TransferRequest or to its stored payload dict."""
    def put(key: str, value: Any) -> None:
        if isinstance(target, dict):
            target[key] = value
        else:
            setattr(target, key, value)

    def get(key: str) -> Any:
        return target.get(key) if isinstance(target, dict) else getattr(target, key, None)

    policy = (decision.schema_policy or "").strip().lower()
    if policy:
        put("schema_policy", policy)
        put("schema_policy_declared", True)
    if decision.backfill_new_fields:
        put("backfill_new_fields", True)
    if decision.omit_columns:
        omits = _omit_rows(decision.omit_columns, actor)
        put("mappings", _with_omits(get("mappings"), omits))
        contracts = []
        for contract in get("stream_contracts") or []:
            if isinstance(contract, dict) and isinstance(contract.get("mappings"), list) and contract["mappings"]:
                contract = {**contract, "mappings": _with_omits(contract["mappings"], omits)}
            contracts.append(contract)
        put("stream_contracts", contracts)
    put("acknowledgment_actor", actor)
    put("acknowledgment_reason", (decision.reason or "").strip())


def decision_record(decision: SchemaDecision, *, actor: str, action: str) -> dict[str, Any]:
    return {
        "action": action,
        "actor": actor,
        "reason": (decision.reason or "").strip(),
        "schema_policy": (decision.schema_policy or "").strip().lower() or None,
        "backfill_new_fields": bool(decision.backfill_new_fields),
        "omit_columns": [str(c).strip() for c in decision.omit_columns if str(c).strip()],
        "at": datetime.now(timezone.utc).isoformat(),
    }


def audit_schema_decision(record: dict[str, Any], *, job_id: str, workspace_id: str | None) -> None:
    from services.audit_log import append_audit_event

    append_audit_event(
        action=f"job.{record['action']}.schema_decision",
        resource="job",
        actor=record["actor"],
        details={"job_id": job_id, **{k: v for k, v in record.items() if k != "actor"}},
        workspace_id=workspace_id,
        outcome="succeeded",
    )
