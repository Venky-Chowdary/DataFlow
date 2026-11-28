"""Deleting a connector that schedules use: one rule for REST, Studio and MCP (D432).

``DELETE /connectors/saved/{id}`` used to answer 200 for a connector a
schedule still read from or wrote to, while the MCP preview of the same
delete listed the bound schedules and asked for confirmation. The schedule
stayed listed, looked healthy, and failed only at its next run.

The rule, wherever the delete comes from:

* A connector no schedule uses is deleted as before — no extra prompt.
* A connector that schedules use is refused (409, ``connector_in_use``) unless
  the caller acknowledges exactly the schedules bound now (by id) and picks
  what happens to them: ``pause`` (disable; each shows "connection deleted"
  until re-pointed), ``repoint`` (to another saved connector of the same type)
  or ``delete_schedules``. A schedule bound after the caller looked is not
  acknowledged, so the delete is refused again with the new list.
* A bound schedule with a run in flight refuses the delete: a connector is not
  pulled out from under a running writer.
* ``repoint`` and ``delete_schedules`` change schedules, so they need
  ``schedule.manage`` (403); ``pause`` is the safe choice and does not.
* A failed connector delete never leaves schedules changed: pause / repoint
  are undone, and delete_schedules runs only once the connector is gone.
  What actually happened is audited (``connector.delete.bound_schedules``).
* ``DELETE /connectors/saved/{id}`` and ``DELETE /connectors/{id}`` (the
  platform store the scheduler also resolves) both go through
  :func:`delete_via_route`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

ACTION_PAUSE = "pause"
ACTION_REPOINT = "repoint"
ACTION_DELETE_SCHEDULES = "delete_schedules"
BOUND_SCHEDULE_ACTIONS = (ACTION_PAUSE, ACTION_REPOINT, ACTION_DELETE_SCHEDULES)
IN_USE_CODE = "connector_in_use"
AUDIT_EVENT = "connector.delete.bound_schedules"

_OPTION_TEXT = {
    ACTION_PAUSE: (
        "Pause the schedules. Each keeps its history and shows “connection deleted” "
        "until a connection is re-selected."
    ),
    ACTION_REPOINT: "Re-point the schedules to another saved connection of the same type (send repoint_to).",
    ACTION_DELETE_SCHEDULES: "Delete the schedules with the connection.",
}
#: The same choices inside one sentence of the refusal.
_OPTION_CLAUSE = (
    "pause them (each keeps its history and shows “connection deleted” until a "
    "connection is re-selected), re-point them to another saved connection of the "
    "same type, or delete them with the connection"
)
#: What the refusal's envelope tells the caller to do next.
IN_USE_ACTION = (
    "Choose pause, re-point or delete for these schedules, then send the delete again "
    "with that choice and the ids of the schedules."
)


class ConnectorInUse(Exception):
    """The delete is refused; ``payload`` is the 409 body (names, options)."""

    def __init__(self, message: str, payload: dict[str, Any]) -> None:
        super().__init__(message)
        self.payload = payload


@dataclass
class DeletePlan:
    connector_id: str
    connector_name: str
    action: str = ""
    repoint_to: str = ""
    schedules: list[dict[str, Any]] = field(default_factory=list)


def _role(sched: Any, connector_id: str) -> str:
    source = str(getattr(sched, "source_connector_id", "") or "") == connector_id
    dest = str(getattr(sched, "dest_connector_id", "") or "") == connector_id
    if source and dest:
        return "source and destination"
    return "source" if source else "destination"


def schedules_bound_to(connector_id: str, workspace_id: str = "") -> list[dict[str, Any]]:
    """Every schedule that reads from or writes to ``connector_id``.

    Scoped to the connector's workspace (and legacy unscoped rows): a schedule
    in another workspace never resolves this connector (D172), and its name
    is not this caller's to see.
    """
    from services.schedule_store import list_schedules

    cid = str(connector_id or "").strip()
    if not cid:
        return []
    out: list[dict[str, Any]] = []
    for sched in list_schedules():
        if cid not in (
            str(getattr(sched, "source_connector_id", "") or ""),
            str(getattr(sched, "dest_connector_id", "") or ""),
        ):
            continue
        owner = str(getattr(sched, "workspace_id", "") or "")
        if workspace_id and owner and owner != workspace_id:
            continue
        out.append({
            "id": sched.id,
            "name": str(sched.name or sched.id),
            "enabled": bool(sched.enabled),
            "role": _role(sched, cid),
            "running": bool(getattr(sched, "running", False)),
            "workspace_id": owner,
        })
    out.sort(key=lambda row: row["name"].lower())
    return out


def _names(rows: Iterable[dict[str, Any]], limit: int = 5) -> str:
    rows = list(rows)
    shown = ", ".join(f"“{r['name']}”" for r in rows[:limit])
    return shown + (f" and {len(rows) - limit} more" if len(rows) > limit else "")


def _refusal(conn_name: str, bound: list[dict[str, Any]], reason: str) -> ConnectorInUse:
    count = len(bound)
    message = (
        f"{count} schedule{'s' if count != 1 else ''} use{'' if count != 1 else 's'} the connection "
        f"“{conn_name}”: {_names(bound)}. {reason} To delete it, choose what happens to "
        f"{'these schedules' if count != 1 else 'this schedule'} and send that choice with "
        f"{'their ids' if count != 1 else 'its id'}: {_OPTION_CLAUSE}."
    )
    payload = {
        "code": IN_USE_CODE,
        "message": message,
        "bound_schedules": [{k: r[k] for k in ("id", "name", "enabled", "role", "running")} for r in bound],
        "options": [{"action": key, "description": text} for key, text in _OPTION_TEXT.items()],
        "acknowledge_with": {
            "bound_schedules": "|".join(BOUND_SCHEDULE_ACTIONS),
            "acknowledge_schedules": [r["id"] for r in bound],
        },
    }
    return ConnectorInUse(message, payload)


def _same_type_connector(repoint_to: str, conn: Any, workspace_id: str) -> Any:
    from services.connector_store import get_connector

    target = get_connector(repoint_to, workspace_id=workspace_id or None) if repoint_to else None
    if target is None:
        raise ValueError(
            "repoint_to must name another saved connection in this workspace; "
            f"“{repoint_to or '(empty)'}” was not found."
        )
    if target.id == conn.id:
        raise ValueError("repoint_to names the connection being deleted. Choose a different one.")
    if str(target.type or "").lower() != str(conn.type or "").lower():
        raise ValueError(
            f"repoint_to must be the same type as “{conn.name}” ({conn.type}); "
            f"“{target.name}” is {target.type}. A schedule's mapping and plan were "
            "made for this engine — re-plan it in Transfer Studio instead."
        )
    return target


def plan_connector_delete(
    conn: Any,
    *,
    workspace_id: str = "",
    action: str = "",
    acknowledged: Iterable[str] = (),
    repoint_to: str = "",
) -> DeletePlan:
    """Decide the delete of ``conn``. Raises :class:`ConnectorInUse` (409) or ``ValueError`` (422)."""
    bound = schedules_bound_to(conn.id, workspace_id or str(getattr(conn, "workspace_id", "") or ""))
    plan = DeletePlan(connector_id=conn.id, connector_name=str(conn.name or conn.id))
    if not bound:
        return plan
    name = plan.connector_name
    chosen = str(action or "").strip().lower()
    acked = {str(a).strip() for a in acknowledged if str(a).strip()}
    if not chosen and not acked:
        raise _refusal(name, bound, "Datawrap will not delete it silently.")
    if chosen not in BOUND_SCHEDULE_ACTIONS:
        raise ValueError(
            f"bound_schedules must be one of {', '.join(BOUND_SCHEDULE_ACTIONS)}; got “{action}”."
        )
    ids = {r["id"] for r in bound}
    if acked != ids:
        unseen = [r for r in bound if r["id"] not in acked]
        reason = (
            f"The acknowledgement does not cover {_names(unseen)}."
            if unseen
            else "The acknowledgement names schedules that no longer use this connection."
        )
        raise _refusal(name, bound, reason)
    busy = [r for r in bound if r["running"] and _claim_live(r["id"])]
    if busy:
        raise ConnectorInUse(
            f"A run of {_names(busy)} is in progress on the connection “{name}”. Wait for it to "
            "finish, or cancel it, then delete the connection.",
            {"code": IN_USE_CODE, "message": "", "bound_schedules": busy, "options": []},
        )
    if chosen == ACTION_REPOINT:
        plan.repoint_to = _same_type_connector(str(repoint_to or "").strip(), conn, workspace_id).id
    plan.action = chosen
    plan.schedules = bound
    return plan


def _claim_live(schedule_id: str) -> bool:
    from services.schedule_store import _is_running_stale, get_schedule

    sched = get_schedule(schedule_id)
    return bool(sched is not None and sched.running and not _is_running_stale(sched))


def may_manage_schedules(request: Any) -> bool:
    """Whether the caller holds ``schedule.manage`` (always, with authentication off)."""
    from services.rbac import Permission, principal_permissions
    from src.services import auth_service

    if not auth_service.auth_required():
        return True
    state = getattr(request, "state", None)
    user = getattr(state, "user", None) if state is not None else None
    if not user:
        return False
    role = str(getattr(state, "effective_role", "") or user.get("role") or "")
    return Permission.SCHEDULE_MANAGE in principal_permissions(user, role)


def require_schedule_permission(plan: DeletePlan, request: Any) -> None:
    """Re-pointing or deleting schedules is a schedule change: it needs ``schedule.manage``."""
    if plan.action in (ACTION_REPOINT, ACTION_DELETE_SCHEDULES) and not may_manage_schedules(request):
        verb = "Re-pointing" if plan.action == ACTION_REPOINT else "Deleting"
        raise PermissionError(
            f"{verb} the schedules that use this connection needs the schedule.manage permission. "
            "Choose pause instead, or ask a workspace admin."
        )


def delete_connector_with_schedules(
    plan: DeletePlan, *, delete: Callable[[], bool], actor: str = "", request: Any = None,
) -> dict[str, Any]:
    """Apply the plan's choice and delete the connector, in an order that never
    leaves schedules changed for a connector that was not deleted.

    ``pause`` / ``repoint`` change the schedules first (no window where an
    enabled schedule points at a vanished connector) and are undone when the
    connector delete fails. ``delete_schedules`` deletes the connector first:
    schedules are deleted only once it is gone. The audit records what happened.
    """
    if not plan.schedules:
        return {"deleted": bool(delete()), "schedules": []}
    if plan.action == ACTION_DELETE_SCHEDULES:
        deleted = bool(delete())
        results = _delete_schedules(plan, actor=actor, request=request) if deleted else []
    else:
        results = _pause_or_repoint(plan)
        try:
            deleted = bool(delete())
        except Exception:
            _undo(plan, results)
            _audit(plan, results, deleted=False, actor=actor, request=request)
            raise
        if not deleted:
            _undo(plan, results)
    _audit(plan, results, deleted=deleted, actor=actor, request=request)
    return {"deleted": deleted, "schedules": results}


def _pause_or_repoint(plan: DeletePlan) -> list[dict[str, Any]]:
    from services.schedule_store import get_schedule, update_schedule

    results: list[dict[str, Any]] = []
    for row in plan.schedules:
        sched = get_schedule(row["id"])
        if sched is None:
            continue
        done: dict[str, Any] = {"id": sched.id, "name": sched.name, "role": row["role"]}
        if plan.action == ACTION_PAUSE:
            done["was_enabled"] = bool(sched.enabled)
            if sched.enabled:
                update_schedule(sched.id, {"enabled": False})
            done["result"] = "paused"
        else:
            patch: dict[str, Any] = {}
            if sched.source_connector_id == plan.connector_id:
                patch["source_connector_id"] = plan.repoint_to
            if sched.dest_connector_id == plan.connector_id:
                patch["dest_connector_id"] = plan.repoint_to
            update_schedule(sched.id, patch)
            done.update(result="repointed", repoint_to=plan.repoint_to, fields=sorted(patch))
        results.append(done)
    return results


def _undo(plan: DeletePlan, results: list[dict[str, Any]]) -> None:
    """The connector was not deleted: put the schedules back as they were."""
    from services.schedule_store import update_schedule

    for done in results:
        if done.get("result") == "paused" and done.get("was_enabled"):
            update_schedule(done["id"], {"enabled": True})
        elif done.get("result") == "repointed":
            update_schedule(done["id"], {field: plan.connector_id for field in done.get("fields") or []})
        done["result"] = "unchanged"


def _delete_schedules(plan: DeletePlan, *, actor: str, request: Any) -> list[dict[str, Any]]:
    from services.cdc_capture_release import release_schedule_cdc_capture
    from services.schedule_audit import record_schedule_deleted
    from services.schedule_store import delete_schedule, get_schedule

    results: list[dict[str, Any]] = []
    for row in plan.schedules:
        sched = get_schedule(row["id"])
        if sched is None:
            continue
        done: dict[str, Any] = {"id": sched.id, "name": sched.name, "role": row["role"],
                                "capture_release": release_schedule_cdc_capture(sched)}
        if delete_schedule(sched.id):
            record_schedule_deleted(sched, request=request, actor=actor or None)
            done["result"] = "deleted"
        else:
            done["result"] = "not_deleted"
        results.append(done)
    return results


def _audit(plan: DeletePlan, results: list[dict[str, Any]], *, deleted: bool, actor: str,
           request: Any) -> None:
    from services.audit_coverage import record_security_event

    complete = deleted and all(r.get("result") in ("paused", "repointed", "deleted") for r in results)
    record_security_event(
        "connector.delete.bound_schedules",
        outcome="succeeded" if complete else "failed",
        request=request,
        actor=actor or None,
        resource=f"connector:{plan.connector_id}",
        status=200 if deleted else 404,
        details={
            "connector_id": plan.connector_id,
            "connector_name": plan.connector_name,
            "connector_deleted": deleted,
            "action": plan.action,
            "repoint_to": plan.repoint_to,
            "schedules": [{k: v for k, v in r.items() if k not in ("was_enabled", "fields")} for r in results],
        },
    )


def delete_via_route(
    request: Any,
    conn: Any,
    *,
    workspace_id: str,
    bound_schedules: str = "",
    acknowledge_schedules: str = "",
    repoint_to: str = "",
    delete: Callable[[], bool],
) -> dict[str, Any]:
    """One delete contract for every connector delete route (D432).

    409 (in use, not acknowledged), 422 (a choice that cannot be applied),
    403 (re-point / delete schedules without ``schedule.manage``), 404 (the
    connector was not deleted; any schedule change is undone).
    """
    from fastapi import HTTPException

    from services.safe_errors import EnvelopeHTTPException

    try:
        plan = plan_connector_delete(
            conn, workspace_id=workspace_id, action=bound_schedules,
            acknowledged=[part for part in str(acknowledge_schedules or "").split(",") if part.strip()],
            repoint_to=repoint_to,
        )
        require_schedule_permission(plan, request)
    except ConnectorInUse as exc:
        raise EnvelopeHTTPException(409, in_use_envelope(exc)) from exc
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    user = getattr(getattr(request, "state", None), "user", None) or {}
    actor = str(user.get("email") or user.get("name") or "").strip()
    outcome = delete_connector_with_schedules(plan, delete=delete, actor=actor, request=request)
    if not outcome["deleted"]:
        raise HTTPException(status_code=404, detail="Connector not found")
    out: dict[str, Any] = {"ok": True}
    if outcome["schedules"]:
        out["bound_schedules"] = {"action": plan.action, "schedules": outcome["schedules"]}
    return out


def in_use_envelope(exc: ConnectorInUse) -> dict[str, Any]:
    """The 409 body: the taxonomy envelope with an action that fits, and the payload as ``detail``."""
    import uuid

    from services.tracing import get_correlation_id

    return {
        "error": {
            "code": "platform.state_conflict",
            "title": "Connection used by schedules",
            "message": str(exc),
            "action": {"label": IN_USE_ACTION, "ui": "reload",
                       "api": "DELETE /api/v1/connectors/saved/{connector_id}"},
            "object": {},
            "retriable": False,
            "reference": get_correlation_id() or uuid.uuid4().hex,
        },
        "detail": exc.payload | {"message": str(exc)},
    }


def orphaned_sides(sched: Any) -> list[str]:
    """``source`` / ``destination`` when that connector of ``sched`` no longer exists."""
    workspace = str(getattr(sched, "workspace_id", "") or "")
    sides = (("source", getattr(sched, "source_connector_id", "")),
             ("destination", getattr(sched, "dest_connector_id", "")))
    return [side for side, cid in sides if _connector_missing(str(cid or "").strip(), workspace)]


def _connector_missing(connector_id: str, workspace_id: str) -> bool:
    """True only when the store answered and has no such connector."""
    if not connector_id:
        return False
    from services.connector_store import get_connector

    try:
        if get_connector(connector_id, workspace_id=workspace_id or None) is not None:
            return False
        return platform_connector(connector_id) is None
    except Exception:  # noqa: BLE001 - an unreadable store is not proof of deletion
        return False


def platform_connector(connector_id: str) -> dict[str, Any] | None:
    """The connector from the platform store the scheduler also resolves (``mongodb_service``)."""
    from services.mongodb_service import get_mongodb_service

    found = get_mongodb_service().get_connector(connector_id)
    return found if isinstance(found, dict) else None


def connection_deleted_reason(sides: list[str]) -> str:
    """The "connection deleted" state an orphaned schedule shows (D432 AC-4, D435 AC-3)."""
    if not sides:
        return ""
    which = " and ".join(sides)
    plural = len(sides) > 1
    return (
        f"Connection deleted: the {which} connection{'s' if plural else ''} of this schedule "
        f"{'no longer exist' if plural else 'no longer exists'}. Re-select the connection on "
        "this schedule (edit it, or open it in Transfer Studio), or restore the deleted "
        "connection, then resume."
    )
