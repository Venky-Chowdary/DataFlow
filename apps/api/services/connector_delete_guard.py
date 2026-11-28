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

import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

logger = logging.getLogger(__name__)

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
    #: Every id the connector is known by (a platform document: its ObjectId
    #: ``_id`` and its UUID ``id``); a schedule may reference either.
    aliases: frozenset[str] = frozenset()

    def references(self, value: Any) -> bool:
        return str(value or "") in (self.aliases or {self.connector_id})


def _role(sched: Any, ids: frozenset[str]) -> str:
    source = str(getattr(sched, "source_connector_id", "") or "") in ids
    dest = str(getattr(sched, "dest_connector_id", "") or "") in ids
    if source and dest:
        return "source and destination"
    return "source" if source else "destination"


def connector_aliases(conn: Any) -> frozenset[str]:
    """Its id plus any ``aliases`` it carries (a platform document's ``_id`` and ``id``)."""
    ids = {str(getattr(conn, "id", "") or "")} | {str(a) for a in getattr(conn, "aliases", ()) or ()}
    return frozenset(i for i in ids if i)


def schedules_bound_to(connector_id: Any, workspace_id: str = "") -> list[dict[str, Any]]:
    """Every schedule that reads from or writes to the connector (one id or several).

    Scoped to the connector's workspace (and legacy unscoped rows): a schedule
    in another workspace never resolves this connector (D172), and its name
    is not this caller's to see.
    """
    from services.schedule_store import list_schedules

    raw = [connector_id] if isinstance(connector_id, str) else list(connector_id or ())
    ids = frozenset(str(c).strip() for c in raw if str(c or "").strip())
    if not ids:
        return []
    out: list[dict[str, Any]] = []
    for sched in list_schedules():
        if not ids & {
            str(getattr(sched, "source_connector_id", "") or ""),
            str(getattr(sched, "dest_connector_id", "") or ""),
        }:
            continue
        owner = str(getattr(sched, "workspace_id", "") or "")
        if workspace_id and owner and owner != workspace_id:
            continue
        out.append({
            "id": sched.id,
            "name": str(sched.name or sched.id),
            "enabled": bool(sched.enabled),
            "role": _role(sched, ids),
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
    aliases = connector_aliases(conn)
    bound = schedules_bound_to(aliases, workspace_id or str(getattr(conn, "workspace_id", "") or ""))
    plan = DeletePlan(connector_id=conn.id, connector_name=str(conn.name or conn.id), aliases=aliases)
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


#: Fields a schedule change may move that are not the schedule's own state.
_NOT_STATE = frozenset({"version", "updated_at"})


def delete_connector_with_schedules(
    plan: DeletePlan, *, delete: Callable[[], bool], actor: str = "", request: Any = None,
) -> dict[str, Any]:
    """Apply the plan's choice and delete the connector, honestly, step by step.

    ``pause`` / ``repoint`` change the schedules first (no window where an
    enabled schedule points at a vanished connector). If a step fails or the
    connector delete fails, every change already made is undone exactly — the
    fields as they were before, written only where nobody changed them since.
    ``delete_schedules`` deletes the connector first; a schedule that then
    cannot be deleted is paused, so no enabled schedule outlives its
    connector. Every outcome is audited, whatever fails.

    Returns ``{"deleted", "schedules", "status", "message"}``: ``status`` is
    200, 404 (connector not deleted, schedules restored) or 500 (some
    schedule could not be put right; ``message`` names each one).
    """
    if not plan.schedules:
        return {"deleted": bool(delete()), "schedules": [], "status": 200, "message": ""}
    results: list[dict[str, Any]] = []
    deleted = False
    try:
        if plan.action == ACTION_DELETE_SCHEDULES:
            deleted = bool(delete())
            if deleted:
                results = _delete_schedules(plan, actor=actor, request=request)
        else:
            results = _pause_or_repoint(plan)
            if all(r["result"] in ("paused", "repointed") for r in results):
                deleted = _try_delete(delete, results)
            if not deleted:
                _undo(results)
    finally:
        _audit(plan, results, deleted=deleted, actor=actor, request=request)
    return {"deleted": deleted, "schedules": [_public(r) for r in results],
            **_verdict(plan, results, deleted)}


def _try_delete(delete: Callable[[], bool], results: list[dict[str, Any]]) -> bool:
    try:
        return bool(delete())
    except Exception as exc:  # noqa: BLE001 - recorded, the schedules are put back, and audited
        logger.exception("Connector delete failed after its schedules were changed")
        results.append({"id": "", "name": "", "result": "connector_delete_failed",
                        "error": type(exc).__name__})
        return False


def _snapshot(sched: Any) -> dict[str, Any]:
    return {k: v for k, v in sched.to_dict().items() if k not in _NOT_STATE}


def _change_one(plan: DeletePlan, row: dict[str, Any]) -> dict[str, Any]:
    """Pause or re-point one schedule, keeping what it was and what it became."""
    from services.schedule_store import get_schedule, update_schedule

    sched = get_schedule(row["id"])
    done: dict[str, Any] = {"id": row["id"], "name": row["name"], "role": row["role"]}
    if sched is None:
        return {**done, "result": "gone"}
    before = _snapshot(sched)
    try:
        if plan.action == ACTION_PAUSE:
            changed = update_schedule(sched.id, {"enabled": False}) if sched.enabled else sched
            result = "paused"
        else:
            patch = {f: plan.repoint_to for f in ("source_connector_id", "dest_connector_id")
                     if plan.references(getattr(sched, f, ""))}
            changed = update_schedule(sched.id, patch)
            done["repoint_to"] = plan.repoint_to
            result = "repointed"
    except Exception as exc:  # noqa: BLE001 - recorded; the caller undoes what was done
        logger.exception("Schedule %s could not be changed for a connector delete", sched.id)
        return {**done, "result": "change_failed", "error": type(exc).__name__}
    after = _snapshot(changed) if changed is not None else before
    moved = sorted(k for k in set(before) | set(after) if before.get(k) != after.get(k))
    # The undo writes only if the schedule is still exactly as this change left
    # it (every field): an operator's pause or edit in between stands.
    return {**done, "result": result, "_before": {k: before.get(k) for k in moved},
            "_after": after if moved else {}}


def _pause_or_repoint(plan: DeletePlan) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for row in plan.schedules:
        done = _change_one(plan, row)
        results.append(done)
        if done["result"] == "change_failed":
            break  # stop changing; what was done is undone by the caller
    return results


def _undo_one(done: dict[str, Any]) -> None:
    from services.schedule_store import restore_if_unchanged

    if not done.get("_before"):
        done["result"] = "unchanged" if done["result"] in ("paused", "repointed") else done["result"]
        return
    try:
        outcome = restore_if_unchanged(done["id"], done["_after"], done["_before"])
    except Exception as exc:  # noqa: BLE001 - recorded and named in the answer
        logger.exception("Schedule %s could not be restored after a failed connector delete", done["id"])
        done.update(result=f"{done['result']}_not_restored", error=type(exc).__name__)
        return
    if outcome == "restored":
        done["result"] = "restored"
    elif outcome == "changed":
        # Someone changed it after the pause (a manual pause, an edit): theirs stands.
        done["result"] = "changed_since_left_as_is"
    else:
        done["result"] = "gone"


def _undo(results: list[dict[str, Any]]) -> None:
    """The connector was not deleted: put every changed schedule back exactly."""
    for done in results:
        if done["result"] in ("paused", "repointed"):
            _undo_one(done)


def _delete_one(sched: Any, row: dict[str, Any], *, actor: str, request: Any) -> dict[str, Any]:
    from services.cdc_capture_release import release_schedule_cdc_capture
    from services.schedule_audit import record_schedule_deleted
    from services.schedule_store import delete_schedule

    done: dict[str, Any] = {"id": sched.id, "name": sched.name, "role": row["role"]}
    try:
        done["capture_release"] = release_schedule_cdc_capture(sched)
        removed = delete_schedule(sched.id)
    except Exception as exc:  # noqa: BLE001 - recorded; the schedule is paused below instead
        logger.exception("Schedule %s could not be deleted with its connector", sched.id)
        done["error"] = type(exc).__name__
        removed = False
    if removed:
        record_schedule_deleted(sched, request=request, actor=actor or None)
        return {**done, "result": "deleted"}
    return _pause_orphan(sched, done)


def _pause_orphan(sched: Any, done: dict[str, Any]) -> dict[str, Any]:
    """Its connector is gone and it could not be deleted: it must not stay enabled."""
    from services.schedule_store import update_schedule

    if not sched.enabled:
        return {**done, "result": "not_deleted_paused"}
    try:
        update_schedule(sched.id, {"enabled": False})
    except Exception as exc:  # noqa: BLE001 - recorded and named in the answer
        logger.exception("Orphaned schedule %s could not be paused", sched.id)
        return {**done, "result": "not_deleted_left_enabled", "pause_error": type(exc).__name__}
    return {**done, "result": "not_deleted_paused"}


def _delete_schedules(plan: DeletePlan, *, actor: str, request: Any) -> list[dict[str, Any]]:
    from services.schedule_store import get_schedule

    results: list[dict[str, Any]] = []
    for row in plan.schedules:
        sched = get_schedule(row["id"])
        if sched is None:
            continue
        results.append(_delete_one(sched, row, actor=actor, request=request))
    return results


_OK_RESULTS = frozenset({"paused", "repointed", "deleted"})
_ROLLED_BACK = frozenset({"restored", "unchanged", "gone", "changed_since_left_as_is"})


def _public(done: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in done.items() if not k.startswith("_")}


def _verdict(plan: DeletePlan, results: list[dict[str, Any]], deleted: bool) -> dict[str, Any]:
    """The HTTP status and the sentence that names every schedule left changed."""
    rows = [r for r in results if r.get("id")]
    raised = any(r.get("result") == "connector_delete_failed" for r in results)
    if deleted and all(r["result"] in _OK_RESULTS for r in rows):
        return {"status": 200, "message": ""}
    if not deleted and all(r["result"] in _ROLLED_BACK for r in rows):
        kept = [r for r in rows if r["result"] == "changed_since_left_as_is"]
        note = (f" {_names(kept)} changed after it was paused, so it was left as it is now."
                if kept else "")
        return {"status": 500 if raised else 404,
                "message": "The connection was not deleted. Its schedules are as they were." + note}
    parts: list[str] = []
    for r in rows:
        what = _STUCK.get(r["result"])
        if what:
            parts.append(f"schedule “{r['name']}” {what}")
    head = "The connection was deleted, but " if deleted else "The connection was not deleted, and "
    return {"status": 500, "message": head + ("; ".join(parts) if parts else "a step failed") + "."}


_STUCK = {
    "not_deleted_paused": "could not be deleted; it is paused and shows “connection deleted”",
    "not_deleted_left_enabled": "could not be deleted or paused; pause or delete it now",
    "paused_not_restored": "is still paused; resume it",
    "repointed_not_restored": "still points at the replacement connection; re-select it",
    "change_failed": "could not be changed, so the delete stopped there",
}


def _audit(plan: DeletePlan, results: list[dict[str, Any]], *, deleted: bool, actor: str,
           request: Any) -> None:
    from services.audit_coverage import record_security_event

    rows = [r for r in results if r.get("id")]
    complete = deleted and all(r.get("result") in _OK_RESULTS for r in rows)
    record_security_event(
        "connector.delete.bound_schedules",
        outcome="succeeded" if complete else "failed",
        request=request,
        actor=actor or None,
        resource=f"connector:{plan.connector_id}",
        status=200 if complete else (404 if not deleted else 500),
        details={
            "connector_id": plan.connector_id,
            "connector_name": plan.connector_name,
            "connector_deleted": deleted,
            "action": plan.action,
            "repoint_to": plan.repoint_to,
            "schedules": [_public(r) for r in results],
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
    if outcome["status"] == 404 and not outcome["message"]:
        raise HTTPException(status_code=404, detail="Connector not found")
    if outcome["status"] != 200:
        # Plain sentences built from the per-schedule outcome, never exception text.
        raise HTTPException(status_code=outcome["status"], detail={
            "message": outcome["message"], "connector_deleted": outcome["deleted"],
            "schedules": outcome["schedules"],
        })
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
    """True when the scheduler would not resolve this connector for the schedule.

    The runner's rule (``_resolve_connector_for_schedule``): the saved store,
    then the platform store, and a connector owned by another workspace counts
    as absent. Only a store that answered decides; an unreadable one does not.
    """
    if not connector_id:
        return False
    from services.connector_store import get_connector

    try:
        conn = get_connector(connector_id)
        if conn is not None:
            owner = str(conn.workspace_id or "")
        else:
            doc = platform_connector(connector_id)
            if doc is None:
                return True
            owner = str(doc.get("workspace_id") or "")
    except Exception:  # noqa: BLE001 - an unreadable store is not proof of deletion
        return False
    return connector_unavailable_to(owner, workspace_id)


def connector_unavailable_to(owner: str, schedule_workspace: str) -> bool:
    """The runner refuses a connector another workspace owns (D172)."""
    return bool(owner and schedule_workspace and owner != schedule_workspace)


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
        f"{'no longer exist' if plural else 'no longer exists'}, or "
        f"{'are' if plural else 'is'} not in this schedule's workspace. Re-select the connection on "
        "this schedule (edit it, or open it in Transfer Studio), or restore the deleted "
        "connection, then resume."
    )
