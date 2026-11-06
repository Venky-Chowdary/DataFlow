"""Recover a schedule whose destination changed under it (QA C9).

An incremental append proves its destination against the fingerprint the last
verified run left (``services.append_continuity``). When the table is dropped,
changed outside the pipeline, or cannot be re-read, every run is refused with
"Nothing was written" — correctly — and the schedule parks on that finding.

Before this module the only way out was a route-level rebaseline (REST
``/transfer/continuity/rebaseline`` or MCP ``rebaseline_transfer``) that moved
the baseline but left the schedule parked: enabled, no next run, and nothing
on the schedule saying what to do. Recovery is now one schedule action, the
same through REST, MCP and the Schedules UI:

* ``rebaseline`` — accept the destination as it is now (or accept that it was
  removed); the next beat appends from the cursor on top of it;
* ``rebuild`` — reload the destination from the source with a full re-sync
  (upsert on the schedule's key, or replace without one); the cadence then
  continues incrementally from the new high-water mark.

Both are audited with the operator and reason, close the continuity finding
and re-arm the schedule. A route-level rebaseline of the same table also
releases every schedule parked on it (``release_continuity_parks``).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from services.append_continuity import CONTINUITY_ERROR_CODE

logger = logging.getLogger(__name__)

#: The finding code a continuity refusal parks under: the refusal's own stable
#: code, so nothing matches on its wording.
DESTINATION_CONTINUITY_CODE = CONTINUITY_ERROR_CODE
ACTION_REBASELINE = "rebaseline"
ACTION_REBUILD = "rebuild"
ACTIONS = (ACTION_REBASELINE, ACTION_REBUILD)

class ScheduleRecoveryRefused(ValueError):
    """The recovery cannot apply; ``str()`` is operator text (HTTP 409)."""


def continuity_corrective(schedule_id: str) -> str:
    """What the parked schedule tells the operator to do, with both doors."""
    return (
        "Recover the destination: rebaseline to accept it as it is now (or accept "
        "that it was removed), or rebuild to reload it from the source. Schedules "
        f"→ this pipeline → Recover destination; POST /api/v1/schedules/{schedule_id}/recover "
        '{"action": "rebaseline" | "rebuild", "reason": "…"}; or MCP '
        "recover_schedule_destination. Either one is audited and re-arms the schedule."
    )


def _destination_endpoint(sched: Any) -> Any:
    from services.schedule_runner import (
        _apply_schedule_dest_location,
        _endpoint_from_connector,
        _resolve_connector_for_schedule,
    )

    dst = _resolve_connector_for_schedule(sched.dest_connector_id, sched)
    if not dst:
        raise ScheduleRecoveryRefused(
            f"Destination connector {sched.dest_connector_id} no longer resolves; "
            "re-select the destination on this schedule first."
        )
    return _apply_schedule_dest_location(_endpoint_from_connector(dst, sched.dest_table), sched)


def _target_keys(endpoint: Any) -> set[str]:
    from services.append_continuity import resolve_target

    try:
        target = resolve_target(endpoint)
    except Exception:  # noqa: BLE001 - an unresolvable target matches nothing
        return set()
    return set(target.all_keys) if target is not None else set()


def _open_continuity_finding(sched: Any) -> dict[str, Any] | None:
    from services.schedule_store import has_open_approval

    if not has_open_approval(sched):
        return None
    req = sched.approval_request if isinstance(sched.approval_request, dict) else {}
    if str(req.get("code") or "") == DESTINATION_CONTINUITY_CODE:
        return req
    return None


def _close_and_rearm(sched: Any, *, action: str, actor: str, reason: str) -> Any:
    """Close an open continuity finding (if any) and make the next beat due now."""
    from services.schedule_approvals import STATUS_APPROVED, _resolved
    from services.schedule_store import update_schedule

    patch: dict[str, Any] = {
        "next_run_at": datetime.now(timezone.utc).isoformat(),
        "retry_at": None,
        "retry_attempt": 0,
    }
    finding = _open_continuity_finding(sched)
    if finding is not None:
        patch["approval_request"] = _resolved(
            finding, status=STATUS_APPROVED, actor=actor,
            reason=f"Destination recovered ({action}): {reason}",
        )
        patch["last_status"] = "approved"
    return update_schedule(sched.id, patch)


def _audit(sched: Any, *, action: str, actor: str, reason: str, details: dict[str, Any]) -> None:
    from services.audit_coverage import record_security_event
    from services.logging_config import log_context

    with log_context(workspace_id=getattr(sched, "workspace_id", "") or ""):
        record_security_event(
            "schedule.destination.recovered",
            outcome="succeeded",
            actor=str(actor or "").strip() or "system",
            resource=f"schedule:{sched.id}",
            status=200,
            details={"action": action, "reason": reason, **details},
        )


def recover_schedule_destination(
    schedule_id: str,
    *,
    action: str,
    actor: str,
    reason: str,
) -> dict[str, Any]:
    """Rebaseline or rebuild a schedule's destination, then re-arm it.

    Refuses (``ScheduleRecoveryRefused``) an unknown action, a blank reason, and
    a schedule parked on some other finding — a destination recovery must not
    clear a refusal it does not answer.
    """
    from services.append_continuity import RebaselineError, rebaseline
    from services.schedule_store import get_schedule, has_open_approval

    action = str(action or "").strip().lower()
    reason = str(reason or "").strip()
    if action not in ACTIONS:
        raise ScheduleRecoveryRefused(
            f"Unknown recovery action {action!r}. Use one of: {', '.join(ACTIONS)}."
        )
    if not reason:
        raise ScheduleRecoveryRefused("A reason is required to recover a destination.")
    sched = get_schedule(schedule_id)
    if sched is None:
        raise LookupError("Schedule not found")
    if has_open_approval(sched) and _open_continuity_finding(sched) is None:
        req = sched.approval_request if isinstance(sched.approval_request, dict) else {}
        raise ScheduleRecoveryRefused(
            f"This schedule is parked on another finding ({req.get('code') or 'RUN_REFUSED'}), "
            "not a destination change. Resolve that finding instead."
        )
    endpoint = _destination_endpoint(sched)
    details: dict[str, Any] = {"dest_table": sched.dest_table}
    job_id = ""
    if action == ACTION_REBASELINE:
        try:
            result = rebaseline(endpoint, actor=actor, reason=reason, release_schedules=False)
        except RebaselineError as exc:
            raise ScheduleRecoveryRefused(str(exc)) from exc
        details.update(
            rebaseline_id=result.get("rebaseline_id"),
            destination_missing=bool(result.get("destination_missing")),
        )
        _audit(sched, action=action, actor=actor, reason=reason, details=details)
        updated = _close_and_rearm(sched, action=action, actor=actor, reason=reason)
    else:
        _audit(sched, action=action, actor=actor, reason=reason, details=details)
        updated = _close_and_rearm(sched, action=action, actor=actor, reason=reason)
        from services.schedule_runner import _run_schedule

        # A full re-sync: upsert on the key from the start (or replace without
        # one). It re-baselines continuity, so the cadence appends after it.
        job_id = str(
            _run_schedule(schedule_id, manual=True, resync_mode="full",
                          triggered_by=actor or f"recover:{action}") or ""
        )
    return {
        "schedule_id": schedule_id,
        "action": action,
        "job_id": job_id,
        "schedule": updated or get_schedule(schedule_id),
        **{k: v for k, v in details.items() if k != "dest_table"},
    }


def release_continuity_parks(
    endpoint: Any, *, actor: str, reason: str, workspace_id: str | None = None
) -> list[str]:
    """Re-arm the caller's schedules parked on a continuity finding for this table.

    Called after a route-level rebaseline (REST or MCP ``rebaseline_transfer``)
    so accepting the destination there also lets its schedules run. Only
    schedules in ``workspace_id`` whose ``dest_connector_id`` is the
    rebaselined endpoint's saved connector are released: a shared physical
    table (same host/database/table under another workspace's connector) is
    another workspace's decision. ``workspace_id=None`` releases nothing.
    Never raises: the rebaseline already happened and is audited.
    """
    released: list[str] = []
    connector_id = str(getattr(endpoint, "connector_id", "") or "").strip()
    if workspace_id is None or not connector_id:
        return released
    try:
        keys = _target_keys(endpoint)
        if not keys:
            return released
        from services.schedule_store import _load_all

        for sched in _load_all():
            if str(getattr(sched, "workspace_id", "") or "") != str(workspace_id or ""):
                continue
            if str(getattr(sched, "dest_connector_id", "") or "") != connector_id:
                continue
            if _open_continuity_finding(sched) is None:
                continue
            try:
                if not keys & _target_keys(_destination_endpoint(sched)):
                    continue
            except ScheduleRecoveryRefused:
                continue
            _audit(sched, action=ACTION_REBASELINE, actor=actor, reason=reason,
                   details={"dest_table": sched.dest_table, "via": "route_rebaseline"})
            _close_and_rearm(sched, action=ACTION_REBASELINE, actor=actor, reason=reason)
            released.append(sched.id)
    except Exception:  # noqa: BLE001
        logger.exception("Schedules parked on a rebaselined destination were not released")
    return released
