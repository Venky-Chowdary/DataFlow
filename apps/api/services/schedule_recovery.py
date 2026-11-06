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


def _close_finding(sched: Any, *, action: str, actor: str, reason: str, due_now: bool) -> Any:
    """Close an open continuity finding (if any) and re-arm the cadence.

    ``due_now`` (rebaseline): the next beat runs at once. A rebuild already
    started its run, so the cadence resumes at its next slot — a beat now
    would only race the rebuild for the claim.
    """
    from services.schedule_approvals import STATUS_APPROVED, _resolved
    from services.schedule_store import next_run_for, update_schedule

    now = datetime.now(timezone.utc)
    patch: dict[str, Any] = {
        "next_run_at": now.isoformat() if due_now else next_run_for(sched, now),
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


def _note_rebuild_on_finding(sched: Any, *, job_id: str, actor: str, reason: str) -> Any:
    """Record the rebuild on the still-open finding; the run's success closes it."""
    from services.schedule_store import get_schedule, update_schedule

    fresh = get_schedule(sched.id) or sched
    finding = _open_continuity_finding(fresh)
    if finding is None:
        return fresh
    return update_schedule(sched.id, {"approval_request": {
        **finding,
        "recovery": {"action": ACTION_REBUILD, "job_id": job_id, "actor": actor,
                     "reason": reason, "started_at": datetime.now(timezone.utc).isoformat()},
    }})


def _close_and_rearm(sched: Any, *, action: str, actor: str, reason: str) -> Any:
    return _close_finding(sched, action=action, actor=actor, reason=reason, due_now=True)


def _audit(
    sched: Any,
    *,
    action: str,
    actor: str,
    reason: str,
    details: dict[str, Any],
    outcome: str = "succeeded",
    status: int = 200,
) -> None:
    from services.audit_coverage import record_security_event
    from services.logging_config import log_context

    with log_context(workspace_id=getattr(sched, "workspace_id", "") or ""):
        record_security_event(
            "schedule.destination.recovered",
            outcome=outcome,
            actor=str(actor or "").strip() or "system",
            resource=f"schedule:{sched.id}",
            status=status,
            details={"action": action, "reason": reason, **details},
        )


def recovery_plan(sched: Any, action: str) -> dict[str, Any]:
    """What a recovery will do to the destination, in the operator's terms.

    The same disclosure for REST (``dry_run``), MCP (the staged preview) and the
    drawer's confirm dialog: whether it replaces the table, and what the next
    run writes.
    """
    from services.sync_now import full_resync_plan

    table = str(getattr(sched, "dest_table", "") or "the destination")
    watermark = str(getattr(sched, "cursor_value", "") or "").strip()
    after = f"after {sched.cursor_column} {watermark}" if watermark and sched.cursor_column else (
        "after the stored cursor" if sched.cursor_column else "from the next read"
    )
    if action == ACTION_REBUILD:
        plan = full_resync_plan({
            "sync_mode": getattr(sched, "sync_mode", ""),
            "primary_key": getattr(sched, "primary_key", ""),
            "stream_contracts": list(getattr(sched, "stream_contracts", None) or []),
        })
        destructive = bool(plan.get("destructive"))
        effect = (
            f"Replaces {table}: every row it holds now is dropped and the table is "
            "reloaded from the full source (no key to upsert on)."
            if destructive
            else f"Re-reads the full source and upserts every row into {table} on "
            f"{getattr(sched, 'primary_key', '') or 'its key'}; rows only {table} "
            "holds are kept."
        )
        return {"action": action, "destructive": destructive,
                "sync_mode": plan.get("sync_mode"), "effect": effect + (
                    " The cadence then continues incrementally.")}
    missing = None
    try:
        from services.append_continuity import destination_missing, resolve_target

        target = resolve_target(_destination_endpoint(sched))
        missing = destination_missing(target) if target is not None else None
    except Exception:  # noqa: BLE001 - the disclosure says what it knows
        missing = None
    if missing is True:
        effect = (
            f"Accepts that {table} was removed. The next run recreates {table} with "
            f"rows {after} only — earlier history is not reloaded (choose rebuild for that)."
        )
    else:
        effect = (
            f"Accepts {table} as it is now as the verified baseline; nothing is "
            f"written now, and the next run appends rows {after}."
        )
    return {"action": action, "destructive": False, "destination_missing": missing is True,
            "effect": effect}


def recover_schedule_destination(
    schedule_id: str,
    *,
    action: str,
    actor: str,
    reason: str,
    dry_run: bool = False,
    confirm_replace: bool = False,
) -> dict[str, Any]:
    """Rebaseline or rebuild a schedule's destination, then re-arm it.

    Refuses (``ScheduleRecoveryRefused``) an unknown action, a blank reason, a
    schedule parked on some other finding — a destination recovery must not
    clear a refusal it does not answer — and a rebuild that replaces the table
    without ``confirm_replace``. ``dry_run`` returns the plan and does nothing.
    A rebuild whose run cannot start (``ScheduleStartError`` / ``PermissionError``)
    leaves the finding open, is audited as failed, and re-raises.
    """
    from services.append_continuity import RebaselineError, rebaseline
    from services.schedule_store import get_schedule, has_open_approval

    action = str(action or "").strip().lower()
    reason = str(reason or "").strip()
    if action not in ACTIONS:
        raise ScheduleRecoveryRefused(
            f"Unknown recovery action {action!r}. Use one of: {', '.join(ACTIONS)}."
        )
    sched = get_schedule(schedule_id)
    if sched is None:
        raise LookupError("Schedule not found")
    if has_open_approval(sched) and _open_continuity_finding(sched) is None:
        req = sched.approval_request if isinstance(sched.approval_request, dict) else {}
        raise ScheduleRecoveryRefused(
            f"This schedule is parked on another finding ({req.get('code') or 'RUN_REFUSED'}), "
            "not a destination change. Resolve that finding instead."
        )
    plan = recovery_plan(sched, action)
    base = {"schedule_id": schedule_id, "action": action, "plan": plan}
    if dry_run:
        return {**base, "dry_run": True, "job_id": "", "schedule": sched}
    if not reason:
        raise ScheduleRecoveryRefused("A reason is required to recover a destination.")
    if plan["destructive"] and not confirm_replace:
        raise ScheduleRecoveryRefused(
            f"{plan['effect']} Send confirm_replace: true to proceed (or choose rebaseline)."
        )
    endpoint = _destination_endpoint(sched)
    details: dict[str, Any] = {"dest_table": sched.dest_table, "destructive": plan["destructive"]}
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
        updated = _close_finding(sched, action=action, actor=actor, reason=reason, due_now=True)
        _audit(sched, action=action, actor=actor, reason=reason, details=details)
    else:
        from services.schedule_runner import ScheduleStartError, _run_schedule

        # The run starts first, and the finding stays open until it SUCCEEDS:
        # the runner closes it on a completed run (close_finding_after_successful_run)
        # and sets the cadence; a run that starts and then fails leaves it
        # open (or parks a fresh finding). A start that is refused, raises, or
        # returns no job is a failed recovery: audited, finding untouched.
        def _failed(exc: BaseException, status: int) -> None:
            _audit(sched, action=action, actor=actor, reason=reason,
                   details={**details, "error": str(exc)[:300]}, outcome="failed", status=status)

        try:
            job_id = str(
                _run_schedule(schedule_id, manual=True, resync_mode="full",
                              triggered_by=actor or f"recover:{action}") or ""
            )
        except (ScheduleStartError, PermissionError) as exc:
            _failed(exc, int(getattr(exc, "http_status", 403 if isinstance(exc, PermissionError) else 409)))
            raise
        except Exception as exc:  # noqa: BLE001 - audited, then surfaced as it is
            _failed(exc, 500)
            raise
        if not job_id:
            refused = ScheduleStartError(
                "The rebuild did not start (no job was created). The finding stays "
                "open; check the schedule's connectors and try again.",
                http_status=409, code="start_refused",
            )
            _failed(refused, 409)
            raise refused
        details.update(job_id=job_id, phase="rebuild_started")
        updated = _note_rebuild_on_finding(sched, job_id=job_id, actor=actor, reason=reason)
        _audit(sched, action=action, actor=actor, reason=reason, details=details)
    return {
        **base,
        "job_id": job_id,
        "schedule": updated or get_schedule(schedule_id),
        **{k: v for k, v in details.items() if k not in ("dest_table", "job_id")},
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
