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
