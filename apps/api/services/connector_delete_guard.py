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
* The decision is applied before the connector is removed and written to the
  audit log (``connector.delete.bound_schedules``).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable

ACTION_PAUSE = "pause"
ACTION_REPOINT = "repoint"
ACTION_DELETE_SCHEDULES = "delete_schedules"
BOUND_SCHEDULE_ACTIONS = (ACTION_PAUSE, ACTION_REPOINT, ACTION_DELETE_SCHEDULES)
IN_USE_CODE = "connector_in_use"
AUDIT_EVENT = "connector.delete.bound_schedules"

_OPTION_TEXT = {
    ACTION_PAUSE: "pause (each schedule keeps its history and shows “connection deleted” until a connection is re-selected)",
    ACTION_REPOINT: "re-point to another saved connection of the same type (repoint_to)",
    ACTION_DELETE_SCHEDULES: "delete the schedules with the connection",
}


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
        f"“{conn_name}”: {_names(bound)}. {reason} Choose what happens to "
        f"{'them' if count != 1 else 'it'} — "
        + "; ".join(f"{key}: {text}" for key, text in _OPTION_TEXT.items())
        + " — and send that choice with the ids of the schedules you acknowledge."
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


def apply_bound_schedule_action(plan: DeletePlan, *, actor: str = "", request: Any = None) -> list[dict[str, Any]]:
    """Pause, re-point or delete the bound schedules, then audit the decision."""
    if not plan.schedules:
        return []
    from services.schedule_store import delete_schedule, get_schedule, update_schedule

    results: list[dict[str, Any]] = []
    for row in plan.schedules:
        sched = get_schedule(row["id"])
        if sched is None:
            continue
        done: dict[str, Any] = {"id": sched.id, "name": sched.name, "role": row["role"]}
        if plan.action == ACTION_PAUSE:
            if sched.enabled:
                update_schedule(sched.id, {"enabled": False})
            done["result"] = "paused"
        elif plan.action == ACTION_REPOINT:
            patch: dict[str, Any] = {}
            if sched.source_connector_id == plan.connector_id:
                patch["source_connector_id"] = plan.repoint_to
            if sched.dest_connector_id == plan.connector_id:
                patch["dest_connector_id"] = plan.repoint_to
            update_schedule(sched.id, patch)
            done["result"] = "repointed"
            done["repoint_to"] = plan.repoint_to
        else:
            from services.cdc_capture_release import release_schedule_cdc_capture
            from services.schedule_audit import record_schedule_deleted

            done["capture_release"] = release_schedule_cdc_capture(sched)
            if delete_schedule(sched.id):
                record_schedule_deleted(sched, request=request, actor=actor or None)
            done["result"] = "deleted"
        results.append(done)
    _audit(plan, results, actor=actor, request=request)
    return results


def _audit(plan: DeletePlan, results: list[dict[str, Any]], *, actor: str, request: Any) -> None:
    from services.audit_coverage import record_security_event

    record_security_event(
        "connector.delete.bound_schedules",
        outcome="succeeded",
        request=request,
        actor=actor or None,
        resource=f"connector:{plan.connector_id}",
        status=200,
        details={
            "connector_id": plan.connector_id,
            "connector_name": plan.connector_name,
            "action": plan.action,
            "repoint_to": plan.repoint_to,
            "schedules": results,
        },
    )


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
        return get_connector(connector_id, workspace_id=workspace_id or None) is None
    except Exception:  # noqa: BLE001 - an unreadable store is not proof of deletion
        return False


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
