"""The approval inbox — a refused unattended run becomes a decision, not a dead end.

Before this, a schedule that hit a deterministic refusal recorded one more failed
run and waited for its next cadence beat to fail identically. Nothing named what
a human had to do, and nothing stopped the repetition.

An **approval request** is durable state on the schedule holding exactly one
finding: its refusal code, what was found, the corrective action, and the binding
(mapping + source shape + policies) the decision applies to. While a request is
open the cadence is suppressed (see ``schedule_store.has_open_approval``).

Resolving it is two distinct decisions, deliberately:

* **Approve once** — resume *this* plan and re-arm the cadence, minting no
  standing authority.
* **Approve and authorize** — additionally mint a scoped, expiring, hash-bound
  standing authorization so later runs of the same plan proceed unattended.

Both re-bind to the binding the request *displayed*. If the source moves between
inbox and decision the hashes no longer match, and the next run opens a fresh
request rather than running on a signature for a plan nobody looked at.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any

from services.schedule_store import (
    PipelineSchedule,
    get_schedule,
    update_schedule,
)
from services.standing_authorization import (
    CODE_BINDING_CHANGED,
    DEFAULT_GRANT_DAYS,
    AuthorizationDecision,
    AuthorizationRefused,
    StandingAuthorization,
    binding_differences,
    binding_from_schedule,
    grant_authorization,
    rebind_authorization,
    record_use,
    revoke_authorization,
)

#: How long an approve-once signature stays usable before it lapses. A decision
#: made today must not silently authorize a run a week later.
APPROVE_ONCE_DAYS = 1

STATUS_OPEN = "open"
STATUS_APPROVED = "approved"
STATUS_REJECTED = "rejected"

KIND_AUTHORIZATION = "authorization"
KIND_SOURCE_DRIFT = "source_drift"
KIND_RUN_REFUSED = "run_refused"

#: Marks the run entry / schedule status of a run parked on a human decision.
NEEDS_APPROVAL_STATUS = "needs_approval"


class ApprovalRequired(ValueError):
    """A refusal that a human can decide on, carrying what they need to decide.

    Subclasses ``ValueError`` deliberately: every caller that already treats a
    pre-run refusal as a failed run keeps working unchanged, while callers that
    know about the inbox can read the structured finding off the exception.
    """

    def __init__(
        self,
        message: str,
        *,
        kind: str = KIND_RUN_REFUSED,
        code: str = "",
        corrective_action: str = "",
        scopes: tuple[str, ...] | list[str] = (),
        evidence: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.kind = kind
        self.code = code or "RUN_REFUSED"
        self.corrective_action = corrective_action
        self.scopes = tuple(scopes or ())
        self.evidence = dict(evidence or {})


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _fingerprint(payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def build_approval_request(
    *,
    kind: str,
    code: str,
    finding: str,
    corrective_action: str,
    binding: dict[str, Any],
    requested_scopes: list[str] | tuple[str, ...] = (),
    binding_approved: dict[str, Any] | None = None,
    differences: list[dict[str, str]] | tuple[dict[str, str], ...] = (),
    job_id: str = "",
    run_attempt: int = 0,
    evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Describe one finding for a human, with everything needed to decide.

    The identity is a fingerprint of the finding *and* the binding it was raised
    against, so the same refusal on the same plan reopens the same request
    (counted) rather than growing a queue of duplicates — while the same refusal
    on a changed plan is correctly a new decision.
    """
    scopes = sorted({str(s) for s in (requested_scopes or ()) if str(s)})
    identity = {
        "code": str(code or ""),
        "finding": str(finding or ""),
        "binding_hash": str((binding or {}).get("binding_hash") or ""),
    }
    stamp = _now()
    return {
        "id": _fingerprint(identity),
        "status": STATUS_OPEN,
        "kind": str(kind or KIND_RUN_REFUSED),
        "code": str(code or ""),
        "finding": str(finding or "").strip(),
        "corrective_action": str(corrective_action or "").strip(),
        "binding": dict(binding or {}),
        "binding_approved": dict(binding_approved or {}),
        "differences": [dict(d) for d in (differences or ())],
        "requested_scopes": scopes,
        # Whether an operator signature is even the right instrument. False means
        # the finding names a configuration change (remap, widen the type, set a
        # primary key); the inbox offers "re-arm once I have fixed it", never a
        # signature that the next beat would refuse again.
        "approvable": bool(scopes),
        "job_id": str(job_id or ""),
        "run_attempt": int(run_attempt or 0),
        "evidence": dict(evidence or {}),
        "occurrences": 1,
        "created_at": stamp,
        "last_seen_at": stamp,
        "resolved_at": "",
        "resolved_by": "",
        "resolved_reason": "",
    }


def open_approval_request(
    schedule_id: str,
    request: dict[str, Any],
) -> PipelineSchedule | None:
    """Park the schedule on a human decision.

    Idempotent by request id: an identical finding on an identical plan is
    counted, not duplicated. Any parked retry is released, because a retry cannot
    change a deterministic answer and would otherwise race the decision.
    """
    sched = get_schedule(schedule_id)
    if not sched:
        return None
    existing = sched.approval_request if isinstance(sched.approval_request, dict) else {}
    payload = dict(request)
    if (
        existing
        and str(existing.get("id") or "") == str(payload.get("id") or "")
        and str(existing.get("status") or STATUS_OPEN) == STATUS_OPEN
    ):
        payload = {
            **existing,
            "occurrences": int(existing.get("occurrences") or 1) + 1,
            "last_seen_at": _now(),
            "job_id": payload.get("job_id") or existing.get("job_id") or "",
            "run_attempt": payload.get("run_attempt") or existing.get("run_attempt") or 0,
        }
    return update_schedule(
        schedule_id,
        {
            "approval_request": payload,
            "last_status": NEEDS_APPROVAL_STATUS,
            "retry_at": None,
            "retry_attempt": 0,
        },
    )


def approval_from_decision(
    decision: AuthorizationDecision,
    *,
    binding_now: dict[str, Any],
    finding: str,
    corrective_action: str = "",
    requested_scopes: list[str] | tuple[str, ...] = (),
    kind: str = KIND_AUTHORIZATION,
    job_id: str = "",
    run_attempt: int = 0,
    evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Turn "no authority for this" into a decision a human can actually make."""
    return build_approval_request(
        kind=kind,
        code=decision.code,
        finding=finding,
        corrective_action=corrective_action or decision.corrective_action,
        binding=binding_now,
        binding_approved={d["field"]: d["approved"] for d in decision.differences},
        differences=decision.differences,
        requested_scopes=requested_scopes,
        job_id=job_id,
        run_attempt=run_attempt,
        evidence={**(evidence or {}), "authorization_reason": decision.reason},
    )


def _resolved(
    request: dict[str, Any],
    *,
    status: str,
    actor: str,
    reason: str,
) -> dict[str, Any]:
    return {
        **request,
        "status": status,
        "resolved_at": _now(),
        "resolved_by": str(actor or "").strip(),
        "resolved_reason": str(reason or "").strip(),
    }


def approve_request(
    schedule_id: str,
    approval_id: str,
    *,
    actor: str,
    reason: str,
    acknowledgments: dict[str, bool] | None = None,
    scopes: list[str] | None = None,
    grant_standing: bool = False,
    expires_in_days: int | None = None,
    run_at: datetime | None = None,
) -> dict[str, Any]:
    """Approve an open finding; optionally mint the standing authorization.

    Raises ``LookupError`` when the request is not open, and
    ``AuthorizationRefused`` when a standing grant is asked for that the product
    will not delegate — in which case *nothing* is written, so a refused grant
    cannot leave a half-approved schedule behind.
    """
    sched = get_schedule(schedule_id)
    if not sched:
        raise LookupError("Schedule not found")
    request = sched.approval_request if isinstance(sched.approval_request, dict) else {}
    if not request or str(request.get("id") or "") != str(approval_id or ""):
        raise LookupError("No such approval request on this schedule")
    if str(request.get("status") or STATUS_OPEN) != STATUS_OPEN:
        raise LookupError("That approval request is already resolved")

    binding = dict(request.get("binding") or {})
    # The plan must still be the plan the request displayed. If the source moved
    # while the request sat in the inbox, the operator would be signing for
    # something they never saw, so this fails closed and the next beat raises a
    # fresh request against the new shape.
    live = binding_from_schedule(sched)
    drifted = binding_differences(binding, live)
    if drifted:
        raise AuthorizationRefused(
            f"{CODE_BINDING_CHANGED}: "
            + ", ".join(d["label"] for d in drifted)
            + " changed after this request was raised. Re-open the schedule and "
            "decide against what it looks like now."
        )

    grant: StandingAuthorization | None = None
    if grant_standing and not bool(request.get("approvable")):
        raise AuthorizationRefused(
            "This finding is resolved by changing the plan, not by delegating "
            "authority — an unattended run would be refused for it again. Fix "
            "what the corrective action names, then re-arm the schedule."
        )
    requested = list(scopes if scopes is not None else request.get("requested_scopes") or [])
    if grant_standing:
        grant = grant_authorization(
            actor=actor,
            reason=reason,
            scopes=requested,
            binding=binding,
            acknowledgments=acknowledgments,
            expires_in_days=int(expires_in_days or DEFAULT_GRANT_DAYS),
        )
    elif requested and bool(request.get("approvable")):
        # Approve once, through the same mechanism rather than a weaker parallel
        # one: a single-use, one-day grant carries this operator's signature into
        # exactly one unattended run and then stops applying.
        grant = grant_authorization(
            actor=actor,
            reason=reason,
            scopes=requested,
            binding=binding,
            acknowledgments=acknowledgments,
            expires_in_days=APPROVE_ONCE_DAYS,
            max_uses=1,
        )

    patch: dict[str, Any] = {
        "approval_request": _resolved(
            request, status=STATUS_APPROVED, actor=actor, reason=reason
        ),
        # Approving is an instruction to proceed, so the next beat is now rather
        # than a full cadence away — the operator is not asked to wait a day for
        # the decision they just made to take effect.
        "next_run_at": (run_at or datetime.now(timezone.utc))
        .astimezone(timezone.utc)
        .isoformat(),
        "retry_at": None,
        "retry_attempt": 0,
        "last_status": "approved",
    }
    if grant is not None:
        patch["standing_authorization"] = grant.to_dict()

    updated = update_schedule(schedule_id, patch)
    _audit(
        action="schedule.approval.approved",
        schedule_id=schedule_id,
        actor=actor,
        details={
            "approval_id": approval_id,
            "code": request.get("code"),
            "finding": request.get("finding"),
            "reason": reason,
            "standing_authorization": bool(grant_standing),
            "single_use": bool(grant is not None and grant.max_uses == 1),
            "authorization_id": grant.id if grant else "",
            "scopes": list(grant.scopes) if grant else [],
            "expires_at": grant.expires_at if grant else "",
            "binding_hash": binding.get("binding_hash", ""),
        },
    )
    return {
        "schedule": updated,
        "approval": patch["approval_request"],
        "authorization": grant.to_dict() if grant else {},
    }


def reject_request(
    schedule_id: str,
    approval_id: str,
    *,
    actor: str,
    reason: str,
    disable: bool = True,
) -> dict[str, Any]:
    """Reject a finding. The schedule is paused by default rather than left to
    re-raise the same refusal on its next beat."""
    sched = get_schedule(schedule_id)
    if not sched:
        raise LookupError("Schedule not found")
    request = sched.approval_request if isinstance(sched.approval_request, dict) else {}
    if not request or str(request.get("id") or "") != str(approval_id or ""):
        raise LookupError("No such approval request on this schedule")
    if str(request.get("status") or STATUS_OPEN) != STATUS_OPEN:
        raise LookupError("That approval request is already resolved")

    patch: dict[str, Any] = {
        "approval_request": _resolved(
            request, status=STATUS_REJECTED, actor=actor, reason=reason
        ),
        "last_status": "rejected",
    }
    if disable:
        patch["enabled"] = False
    updated = update_schedule(schedule_id, patch)
    _audit(
        action="schedule.approval.rejected",
        schedule_id=schedule_id,
        actor=actor,
        details={
            "approval_id": approval_id,
            "code": request.get("code"),
            "reason": reason,
            "schedule_disabled": bool(disable),
        },
    )
    return {"schedule": updated, "approval": patch["approval_request"]}


def resolve_plan_change(
    schedule_id: str,
    *,
    actor: str,
    reason: str,
) -> dict[str, Any] | None:
    """Close a non-approvable finding after the operator changed the plan.

    Source-schema Accept is the instrument for ``SOURCE_SCHEMA_DRIFT``: a
    signature would only refuse again, so the inbox must not leave the schedule
    parked after the baseline has been recorded.
    """
    sched = get_schedule(schedule_id)
    if not sched:
        return None
    request = sched.approval_request if isinstance(sched.approval_request, dict) else {}
    if not request or str(request.get("status") or STATUS_OPEN) != STATUS_OPEN:
        return None
    patch = {
        "approval_request": _resolved(
            request, status=STATUS_APPROVED, actor=actor, reason=reason
        ),
        "next_run_at": datetime.now(timezone.utc).isoformat(),
        "retry_at": None,
        "retry_attempt": 0,
        "last_status": "approved",
    }
    updated = update_schedule(schedule_id, patch)
    _audit(
        action="schedule.approval.plan_changed",
        schedule_id=schedule_id,
        actor=actor,
        details={
            "approval_id": request.get("id"),
            "code": request.get("code"),
            "reason": reason,
        },
    )
    return {
        "schedule": updated,
        "approval": patch["approval_request"],
    }


def release_same_declaration_source_drift(workspace_id: str = "") -> int:
    """Unpark schedules whose only finding is a type-narrow of itself.

    ``joining_date: TIMESTAMP_NTZ → TIMESTAMP_NTZ (narrow_type)`` is dest-floor
    invent, not a plan change. Leaving it in the inbox after the kernel stopped
    classifying it would keep the hourly beat suppressed forever.
    """
    from services.schema_drift import is_same_declaration_narrow
    from services.schedule_store import has_open_approval, list_schedules

    released = 0
    for sched in list_schedules():
        if workspace_id and (sched.workspace_id or "") != workspace_id:
            continue
        if not has_open_approval(sched):
            continue
        request = sched.approval_request if isinstance(sched.approval_request, dict) else {}
        if str(request.get("code") or "") != "SOURCE_SCHEMA_DRIFT":
            continue
        evidence = request.get("evidence") if isinstance(request.get("evidence"), dict) else {}
        breaking = list(evidence.get("breaking") or [])
        if not is_same_declaration_narrow(breaking):
            continue
        resolve_plan_change(
            sched.id,
            actor="system:source-schema-kernel",
            reason=(
                "Released: the parked finding named a type-narrow of an unchanged "
                "declaration (same spelling both sides). That is dest-floor invent, "
                "not a source change."
            ),
        )
        released += 1
    return released


def is_decision_artifact_park_finding(finding: str) -> bool:
    """Parked copy that named a Decision Artifact refuse (or the old budget mask)."""
    text = (finding or "").strip().lower()
    if "decision artifact" in text and (
        "diverged from current map" in text
        or "dest schema drifted since validate" in text
        or "content_hash mismatch" in text
        or "content_hash does not match" in text
    ):
        return True
    return bool(re.search(r"retry budget exhausted after 0 attempt", text))


def dest_engine_from_schedule(sched: Any, dest_db: str = "") -> str:
    """Dest dialect the Validate stamp was hashed with (connector type)."""
    if dest_db:
        return str(dest_db).strip().lower()
    try:
        from services.connector_store import get_connector

        conn = get_connector(
            getattr(sched, "dest_connector_id", "") or "",
            workspace_id=getattr(sched, "workspace_id", None) or None,
        )
    except Exception:
        return ""
    if conn is None:
        return ""
    data = conn.to_dict() if hasattr(conn, "to_dict") else {}
    return str(data.get("type") or data.get("format") or "").strip().lower()


def create_new_stamp_matches_schedule(sched: Any, dest_db: str = "") -> bool:
    """True when the schedule's create-new Validate hash still matches Map."""
    from services.decision_kernel import build_artifact_from_mappings
    from services.schedule_mapping_contract import persisted_mapping_rows

    maps = persisted_mapping_rows(getattr(sched, "mappings", None))
    approved = str(getattr(sched, "approved_decision_artifact_hash", "") or "").strip()
    if not maps or len(approved) != 64:
        return False
    engine = dest_engine_from_schedule(sched, dest_db=dest_db)
    if not engine:
        return False
    mode = str(getattr(sched, "sync_mode", "") or "").strip() or "full_refresh_overwrite"
    source_engine = ""
    try:
        from services.connector_store import get_connector

        src = get_connector(
            getattr(sched, "source_connector_id", "") or "",
            workspace_id=getattr(sched, "workspace_id", None) or None,
        )
        if src is not None:
            data = src.to_dict() if hasattr(src, "to_dict") else {}
            source_engine = str(data.get("type") or data.get("format") or "").strip().lower()
    except Exception:
        source_engine = ""
    art = build_artifact_from_mappings(
        maps,
        dest_db=engine,
        source_db=source_engine,
        dest_fingerprint="",
        sync_mode=mode,
        route_id=f"validate:{engine or 'unknown'}",
        tenant_id="anonymous",
        artifact_id="da_inline",
        created_at="1970-01-01T00:00:00+00:00",
    )
    return art.content_hash.lower() == approved.lower()


def release_create_new_dest_exists_false_refuse(
    workspace_id: str = "",
    *,
    dest_db: str = "",
) -> int:
    """Unpark schedules whose only finding was dest-exists after create-new Validate.

    The first write created the table; later beats re-preflighted dest-exists and
    refused "DDL identity diverged from current Map". That is not a plan change
    when the operator Map hash still matches. A real Map edit stays parked.
    """
    from services.schedule_store import has_open_approval, list_schedules

    released = 0
    for sched in list_schedules():
        if workspace_id and (sched.workspace_id or "") != workspace_id:
            continue
        if not has_open_approval(sched):
            continue
        request = sched.approval_request if isinstance(sched.approval_request, dict) else {}
        code = str(request.get("code") or "").upper()
        if code not in {"RUN_REFUSED", ""}:
            continue
        finding = str(request.get("finding") or request.get("corrective_action") or "")
        if not is_decision_artifact_park_finding(finding):
            continue
        if not create_new_stamp_matches_schedule(sched, dest_db=dest_db):
            continue
        resolve_plan_change(
            sched.id,
            actor="system:decision-artifact-kernel",
            reason=(
                "Released: create-new Validate stamp still matches Map after dest "
                "exists. Dest appearing after the first write is not a plan change."
            ),
        )
        released += 1
    return released


def close_dest_exists_park_after_success(schedule_id: str) -> None:
    """Close a stale DA dest-exists park after a write succeeded."""
    from services.schedule_store import has_open_approval

    sched = get_schedule(schedule_id)
    if not sched or not has_open_approval(sched):
        return
    request = sched.approval_request if isinstance(sched.approval_request, dict) else {}
    finding = str(request.get("finding") or "")
    if not is_decision_artifact_park_finding(finding):
        return
    resolve_plan_change(
        schedule_id,
        actor="system:decision-artifact-kernel",
        reason=(
            "Scheduled write succeeded — create-new Validate stamp held after "
            "dest exists. Cadence is re-armed."
        ),
    )


def record_authorization_use(
    schedule_id: str,
    *,
    rebind: bool = False,
) -> dict[str, Any]:
    """Record that delegated authority was exercised on an unattended run.

    Counting uses is what makes a single-use approval single-use and what lets an
    auditor see how often a standing grant actually spoke. ``rebind`` additionally
    carries the grant across a source-shape advance it authorized — refused, and
    left untouched, if anything else in the plan moved.

    Best-effort by design: a run that has already been authorized is not failed
    because the bookkeeping write lost a race.
    """
    sched = get_schedule(schedule_id)
    if not sched:
        return {}
    grant = StandingAuthorization.from_dict(sched.standing_authorization)
    if grant is None:
        return {}
    updated = record_use(grant)
    if rebind:
        try:
            updated = rebind_authorization(updated, binding_now=binding_from_schedule(sched))
        except AuthorizationRefused:
            # Not our call to widen: leave the grant bound where the human left it.
            pass
    payload = updated.to_dict()
    update_schedule(schedule_id, {"standing_authorization": payload})
    return payload


def set_standing_authorization(
    schedule_id: str,
    *,
    actor: str,
    reason: str,
    scopes: list[str],
    acknowledgments: dict[str, bool] | None = None,
    expires_in_days: int = 30,
) -> dict[str, Any]:
    """Grant delegated authority for a schedule's *current* plan.

    Bound to the live mapping and source shape, so a grant can never be created
    for a plan the granter did not have in front of them.
    """
    sched = get_schedule(schedule_id)
    if not sched:
        raise LookupError("Schedule not found")
    grant = grant_authorization(
        actor=actor,
        reason=reason,
        scopes=scopes,
        binding=binding_from_schedule(sched),
        acknowledgments=acknowledgments,
        expires_in_days=expires_in_days,
    )
    updated = update_schedule(schedule_id, {"standing_authorization": grant.to_dict()})
    _audit(
        action="schedule.authorization.granted",
        schedule_id=schedule_id,
        actor=actor,
        details={
            "authorization_id": grant.id,
            "scopes": list(grant.scopes),
            "expires_at": grant.expires_at,
            "reason": reason,
            "binding_hash": grant.binding.get("binding_hash", ""),
        },
    )
    return {"schedule": updated, "authorization": grant.to_dict()}


def revoke_standing_authorization(
    schedule_id: str,
    *,
    actor: str,
    reason: str = "",
) -> dict[str, Any]:
    """Revoke delegated authority. The record is kept, permanently unusable."""
    sched = get_schedule(schedule_id)
    if not sched:
        raise LookupError("Schedule not found")
    grant = StandingAuthorization.from_dict(sched.standing_authorization)
    if grant is None:
        raise LookupError("This schedule has no standing authorization")
    revoked = revoke_authorization(grant, actor=actor, reason=reason)
    updated = update_schedule(schedule_id, {"standing_authorization": revoked.to_dict()})
    _audit(
        action="schedule.authorization.revoked",
        schedule_id=schedule_id,
        actor=actor,
        details={"authorization_id": revoked.id, "reason": reason},
    )
    return {"schedule": updated, "authorization": revoked.to_dict()}


def open_approvals(workspace_id: str = "") -> list[dict[str, Any]]:
    """The inbox: every schedule currently parked on a decision."""
    from services.schedule_mapping_contract import (
        is_empty_mapping_finding,
        persisted_mapping_rows,
    )
    from services.schedule_store import has_open_approval, list_schedules

    out: list[dict[str, Any]] = []
    for sched in list_schedules():
        if not has_open_approval(sched):
            continue
        if workspace_id and (sched.workspace_id or "") != workspace_id:
            continue
        request = dict(sched.approval_request or {})
        # Stale EMPTY_MAPPING after Studio persisted a contract — hide it even
        # if resolve_plan_change has not run yet. A signature still cannot
        # invent names; the plan already changed.
        if persisted_mapping_rows(sched.mappings) and is_empty_mapping_finding(
            str(request.get("code") or ""),
            str(request.get("finding") or ""),
        ):
            continue
        out.append({
            "schedule_id": sched.id,
            "schedule_name": sched.name,
            "workspace_id": sched.workspace_id or "",
            "source": f"{sched.source_connector_id}:{sched.source_table}",
            "destination": f"{sched.dest_connector_id}:{sched.dest_table}",
            "sync_mode": sched.sync_mode,
            "enabled": bool(sched.enabled),
            "approval": request,
        })
    out.sort(key=lambda r: str(r["approval"].get("created_at") or ""), reverse=True)
    return out


def _audit(
    *,
    action: str,
    schedule_id: str,
    actor: str,
    details: dict[str, Any],
) -> None:
    from services.audit_log import append_audit_event

    append_audit_event(
        action=action,
        resource=f"schedule:{schedule_id}",
        actor=str(actor or "").strip() or "system",
        details=details,
    )


__all__ = [
    "APPROVE_ONCE_DAYS",
    "ApprovalRequired",
    "AuthorizationRefused",
    "KIND_AUTHORIZATION",
    "KIND_RUN_REFUSED",
    "KIND_SOURCE_DRIFT",
    "NEEDS_APPROVAL_STATUS",
    "STATUS_APPROVED",
    "STATUS_OPEN",
    "STATUS_REJECTED",
    "approval_from_decision",
    "approve_request",
    "build_approval_request",
    "open_approval_request",
    "open_approvals",
    "record_authorization_use",
    "reject_request",
    "release_same_declaration_source_drift",
    "resolve_plan_change",
    "revoke_standing_authorization",
    "set_standing_authorization",
]
