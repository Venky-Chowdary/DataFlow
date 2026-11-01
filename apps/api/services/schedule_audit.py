"""One audit and acknowledgement surface for schedule definitions.

QA Snowflake D12 found a schedule created through MCP ``create_schedule`` →
``confirm_action`` with no audit event at all: only the REST routes wrote
``schedule.definition.*``. Every path that creates or deletes a schedule (REST,
the Pilot/MCP confirm, the orphan reconciler) now records through here, so the
event name, the resource key and the details are the same whichever door the
change came through.

QA Snowflake D10: ``compliance_acknowledged`` on a REST schedule was dropped.
:func:`schedule_acknowledgment_fields` validates the attestation the way a
one-off ``/transfer/run`` does (actor defaults to the caller) and stamps when it
was given; :func:`audit_schedule_acknowledgment` writes the governance record.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from services.audit_coverage import record_security_event


def schedule_audit_details(sched: Any, **extra: Any) -> dict[str, Any]:
    details = {
        "id": str(getattr(sched, "id", "") or ""),
        "workspace_id": str(getattr(sched, "workspace_id", "") or ""),
        "source_connector_id": str(getattr(sched, "source_connector_id", "") or ""),
        "source_table": str(getattr(sched, "source_table", "") or ""),
        "dest_connector_id": str(getattr(sched, "dest_connector_id", "") or ""),
        "dest_table": str(getattr(sched, "dest_table", "") or ""),
        "dest_schema": str(getattr(sched, "dest_schema", "") or ""),
        "dest_database": str(getattr(sched, "dest_database", "") or ""),
        "sync_mode": str(getattr(sched, "sync_mode", "") or ""),
        "enabled": bool(getattr(sched, "enabled", False)),
        "compliance_acknowledged": bool(getattr(sched, "compliance_acknowledged", False)),
    }
    details.update(extra)
    return details


def _workspace(sched: Any) -> str | None:
    return str(getattr(sched, "workspace_id", "") or "") or None


def record_schedule_created(sched: Any, *, request: Any = None, via: str = "rest", status: int = 201) -> None:
    record_security_event(
        "schedule.definition.create",
        outcome="succeeded",
        request=request,
        resource=f"schedule:{getattr(sched, 'id', '')}",
        status=status,
        workspace_id=_workspace(sched),
        details=schedule_audit_details(sched, via=via),
    )


def record_schedule_deleted(sched: Any, *, request: Any = None, via: str = "rest", actor: str | None = None) -> None:
    record_security_event(
        "schedule.definition.delete",
        outcome="succeeded",
        request=request,
        actor=actor,
        resource=f"schedule:{getattr(sched, 'id', '')}",
        status=200,
        workspace_id=_workspace(sched),
        details=schedule_audit_details(sched, via=via),
    )


def schedule_acknowledgment_fields(
    payload: dict[str, Any],
    *,
    default_actor: str,
    previous: Any = None,
) -> dict[str, Any]:
    """The acknowledgement fields to store for this create/PATCH.

    ``compliance_acknowledged: true`` is accepted exactly as ``/transfer/run``
    accepts it: the actor defaults to the authenticated caller. A PATCH that
    turns it off clears who/when; one that leaves it alone keeps the stored
    record. Raises ``AcknowledgmentRefused`` (a ``ValueError``) when the
    attestation names no usable actor.
    """
    from services.acknowledgment_contract import MIN_ACTOR_LEN, AcknowledgmentRefused

    if "compliance_acknowledged" not in payload:
        return {}
    if not bool(payload.get("compliance_acknowledged")):
        return {
            "compliance_acknowledged": False,
            "acknowledgment_actor": "",
            "acknowledgment_reason": "",
            "acknowledgment_at": "",
        }
    actor = str(payload.get("acknowledgment_actor") or "").strip() or str(default_actor or "").strip()
    if actor == "anonymous":
        actor = ""
    if len(actor) < MIN_ACTOR_LEN:
        raise AcknowledgmentRefused(
            "compliance_acknowledged needs acknowledgment_actor (who accepts the "
            f"PII risk, at least {MIN_ACTOR_LEN} characters)."
        )
    reason = str(payload.get("acknowledgment_reason") or "").strip()
    unchanged = (
        previous is not None
        and bool(getattr(previous, "compliance_acknowledged", False))
        and str(getattr(previous, "acknowledgment_actor", "") or "") == actor
        and str(getattr(previous, "acknowledgment_reason", "") or "") == reason
        and str(getattr(previous, "acknowledgment_at", "") or "")
    )
    return {
        "compliance_acknowledged": True,
        "acknowledgment_actor": actor,
        "acknowledgment_reason": reason,
        "acknowledgment_at": (
            str(getattr(previous, "acknowledgment_at", "") or "")
            if unchanged
            else datetime.now(timezone.utc).isoformat()
        ),
    }


def audit_schedule_acknowledgment(sched: Any, *, request: Any = None, via: str = "rest") -> None:
    """One governance record per stored PII acknowledgement."""
    if not bool(getattr(sched, "compliance_acknowledged", False)):
        return
    record_security_event(
        "schedule.compliance.acknowledged",
        outcome="succeeded",
        request=request,
        actor=str(getattr(sched, "acknowledgment_actor", "") or "") or None,
        resource=f"schedule:{getattr(sched, 'id', '')}",
        workspace_id=_workspace(sched),
        details=schedule_audit_details(
            sched,
            via=via,
            acknowledgment_actor=str(getattr(sched, "acknowledgment_actor", "") or ""),
            acknowledgment_reason=str(getattr(sched, "acknowledgment_reason", "") or ""),
            acknowledgment_at=str(getattr(sched, "acknowledgment_at", "") or ""),
        ),
    )
