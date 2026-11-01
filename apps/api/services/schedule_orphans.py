"""Find and reconcile pipeline registrations no workspace can see (QA Snowflake D12).

A schedule is one row in the schedule store. The scheduler and the
destination-conflict check read every row; GET, list and DELETE read the rows
the caller's workspace scope covers. A row outside every workspace — saved with
no ``workspace_id`` (the MCP ``create_schedule`` → ``confirm_action`` path
before this fix), or naming a workspace that has since been deleted — keeps
running and keeps blocking new pipelines on its table while every workspace
gets 404 for it. That is the "ghost schedule" QA hit.

This module is the repair surface:

* :func:`find_orphans` lists them with the evidence for an owner. The owner
  proposed is the workspace of the destination connector (the table being
  written is that workspace's), else of the source connector. Connectors in
  two different workspaces, or none, propose nothing — a human decides.
* :func:`reconcile_orphans` is dry run by default and reports exactly what an
  apply would do. Applying adopts each orphan into its proposed workspace
  (or the one an admin names), or disables or deletes it when asked. Every
  applied change is audited; rows that are no longer orphans are skipped, so
  running it twice changes nothing the second time.
* :func:`startup_orphan_scan` runs once at API start. It only reports unless
  ``DATAFLOW_SCHEDULE_ORPHAN_REPAIR=apply``, and even then only adopts rows
  whose owner the evidence settles.

Under workspace isolation an unscoped row is invisible to every member. With
isolation off it is visible to everyone, so it is reported but flagged
``visible_to_members``; adopting it is still the admin's call.
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from typing import Any, Iterable, Mapping

logger = logging.getLogger(__name__)

REASON_UNSCOPED = "unscoped"
REASON_WORKSPACE_MISSING = "workspace_missing"

ACTION_ADOPT = "adopt"
ACTION_DISABLE = "disable"
ACTION_DELETE = "delete"
ACTION_NONE = "none"
_ACTIONS = frozenset({ACTION_ADOPT, ACTION_DISABLE, ACTION_DELETE, ACTION_NONE})

_REPAIR_ENV = "SCHEDULE_ORPHAN_REPAIR"


@dataclass
class Orphan:
    id: str
    name: str
    workspace_id: str
    reason: str
    enabled: bool
    dest_connector_id: str
    dest_table: str
    dest_schema: str
    source_connector_id: str
    source_table: str
    sync_mode: str
    last_run_at: str | None
    proposed_workspace_id: str
    evidence: dict[str, Any] = field(default_factory=dict)
    visible_to_members: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _owner_of(connector_id: str) -> tuple[bool, str]:
    if not str(connector_id or "").strip():
        return False, ""
    try:
        from services.connector_store import connector_owner_workspace

        return connector_owner_workspace(connector_id)
    except Exception:  # noqa: BLE001 — unreadable connector is no evidence
        return False, ""


def _workspace_exists(workspace_id: str) -> bool:
    from services.team_store import get_workspace

    try:
        return get_workspace(workspace_id) is not None
    except Exception:  # noqa: BLE001 — an unreadable team store proves nothing
        return True


def _proposal(sched: Any) -> tuple[str, dict[str, Any]]:
    dest_exists, dest_ws = _owner_of(getattr(sched, "dest_connector_id", ""))
    src_exists, src_ws = _owner_of(getattr(sched, "source_connector_id", ""))
    evidence = {
        "dest_connector_exists": dest_exists,
        "dest_connector_workspace_id": dest_ws,
        "source_connector_exists": src_exists,
        "source_connector_workspace_id": src_ws,
    }
    owners = {ws for ws in (dest_ws, src_ws) if ws and _workspace_exists(ws)}
    if len(owners) == 1:
        return owners.pop(), evidence
    if len(owners) > 1:
        evidence["ambiguous"] = "source and destination connectors belong to different workspaces"
    return "", evidence


def _orphan_reason(sched: Any) -> str:
    ws = str(getattr(sched, "workspace_id", "") or "").strip()
    if not ws:
        return REASON_UNSCOPED
    if not _workspace_exists(ws):
        return REASON_WORKSPACE_MISSING
    return ""


def find_orphans() -> list[Orphan]:
    """Every schedule outside every live workspace, with a proposed owner."""
    from services.schedule_store import list_schedules
    from services.team_store import require_workspace_isolation

    isolation = require_workspace_isolation()
    out: list[Orphan] = []
    for sched in list_schedules(None):
        reason = _orphan_reason(sched)
        if not reason:
            continue
        proposed, evidence = _proposal(sched)
        out.append(
            Orphan(
                id=sched.id,
                name=sched.name,
                workspace_id=str(sched.workspace_id or ""),
                reason=reason,
                enabled=bool(sched.enabled),
                dest_connector_id=sched.dest_connector_id,
                dest_table=sched.dest_table,
                dest_schema=str(getattr(sched, "dest_schema", "") or ""),
                source_connector_id=sched.source_connector_id,
                source_table=sched.source_table,
                sync_mode=sched.sync_mode,
                last_run_at=sched.last_run_at,
                proposed_workspace_id=proposed,
                evidence=evidence,
                visible_to_members=reason == REASON_UNSCOPED and not isolation,
            )
        )
    return out


def _normalize_actions(actions: Mapping[str, Any] | None) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for sid, raw in (actions or {}).items():
        spec = {"action": raw} if isinstance(raw, str) else dict(raw or {})
        action = str(spec.get("action") or "").strip().lower()
        if action not in _ACTIONS:
            raise ValueError(
                f"Unknown action {action!r} for schedule {sid}: use one of "
                f"{sorted(_ACTIONS)}."
            )
        out[str(sid)] = {
            "action": action,
            "workspace_id": str(spec.get("workspace_id") or "").strip(),
        }
    return out


def _planned(orphan: Orphan, override: Mapping[str, str] | None) -> dict[str, Any]:
    if override:
        action = override["action"]
        target = override.get("workspace_id") or (
            orphan.proposed_workspace_id if action == ACTION_ADOPT else ""
        )
    elif orphan.proposed_workspace_id:
        action, target = ACTION_ADOPT, orphan.proposed_workspace_id
    else:
        action, target = ACTION_NONE, ""
    note = ""
    if action == ACTION_ADOPT and not target:
        action, note = ACTION_NONE, "no owner workspace could be determined; name one"
    elif action == ACTION_ADOPT and not _workspace_exists(target):
        action, note = ACTION_NONE, f"workspace {target} does not exist"
    elif action == ACTION_NONE and not note:
        note = "left as is: no owner workspace could be determined"
    return {
        **orphan.to_dict(),
        "action": action,
        "target_workspace_id": target if action == ACTION_ADOPT else "",
        "note": note,
    }


def _apply_one(plan: dict[str, Any]) -> dict[str, Any]:
    from services.schedule_store import delete_schedule, get_schedule, update_schedule

    sid = plan["id"]
    current = get_schedule(sid)
    # Idempotent: a row that stopped being an orphan (adopted by an earlier
    # run, deleted, re-homed) is left alone.
    if current is None or not _orphan_reason(current):
        return {**plan, "applied": False, "note": "no longer an orphan"}
    action = plan["action"]
    if action == ACTION_ADOPT:
        update_schedule(sid, {"workspace_id": plan["target_workspace_id"]})
    elif action == ACTION_DISABLE:
        if current.enabled:
            update_schedule(sid, {"enabled": False})
    elif action == ACTION_DELETE:
        from services.cdc_capture_release import release_schedule_cdc_capture

        plan = {**plan, "capture_release": release_schedule_cdc_capture(current)}
        delete_schedule(sid)
    else:
        return {**plan, "applied": False}
    return {**plan, "applied": True}


def reconcile_orphans(
    *,
    dry_run: bool = True,
    actions: Mapping[str, Any] | None = None,
    only_ids: Iterable[str] | None = None,
    actor: str = "system",
    request: Any = None,
    via: str = "admin",
) -> dict[str, Any]:
    """Plan (and with ``dry_run=False`` apply) the repair of every orphan.

    ``actions`` maps a schedule id to ``"adopt" | "disable" | "delete" |
    "none"`` or ``{"action": "adopt", "workspace_id": "<ws>"}``; an orphan
    without one is adopted into its proposed workspace when the evidence names
    one, and left alone otherwise. ``only_ids`` limits the run.
    """
    overrides = _normalize_actions(actions)
    wanted = {str(i) for i in only_ids} if only_ids is not None else None
    orphans = [o for o in find_orphans() if wanted is None or o.id in wanted]
    plans = [_planned(o, overrides.get(o.id)) for o in orphans]
    results = plans if dry_run else [_apply_one(p) for p in plans]
    applied = [r for r in results if r.get("applied")]
    summary = {
        "dry_run": bool(dry_run),
        "orphan_count": len(orphans),
        "applied_count": len(applied),
        "results": results,
    }
    _audit_reconcile(summary, actor=actor, request=request, via=via)
    if not dry_run:
        _audit_applied(applied, actor=actor, request=request)
    return summary


def _audit_reconcile(summary: dict[str, Any], *, actor: str, request: Any, via: str) -> None:
    from services.audit_coverage import record_security_event

    record_security_event(
        "schedule.orphan.reconcile",
        outcome="succeeded",
        request=request,
        actor=actor,
        resource="schedules:orphans",
        workspace_id="",
        details={
            "via": via,
            "dry_run": summary["dry_run"],
            "orphan_count": summary["orphan_count"],
            "applied_count": summary["applied_count"],
            "plan": [
                {
                    "id": r["id"],
                    "reason": r["reason"],
                    "action": r["action"],
                    "target_workspace_id": r["target_workspace_id"],
                    "applied": bool(r.get("applied")),
                }
                for r in summary["results"]
            ],
        },
    )


def _audit_applied(applied: list[dict[str, Any]], *, actor: str, request: Any) -> None:
    """Each applied change is its own record, in the owning workspace."""
    from types import SimpleNamespace

    from services.audit_log import append_audit_event
    from services.schedule_audit import record_schedule_deleted

    for row in applied:
        if row["action"] == ACTION_DELETE:
            record_schedule_deleted(
                SimpleNamespace(**{**row, "workspace_id": row.get("workspace_id") or ""}),
                request=request,
                via="orphan_reconcile",
                actor=actor,
            )
            continue
        append_audit_event(
            action=f"schedule.orphan.{row['action']}",
            resource=f"schedule:{row['id']}",
            actor=actor,
            workspace_id=row.get("target_workspace_id") or None,
            details={
                "reason": row["reason"],
                "previous_workspace_id": row.get("workspace_id") or "",
                "target_workspace_id": row.get("target_workspace_id") or "",
                "dest_connector_id": row.get("dest_connector_id") or "",
                "dest_table": row.get("dest_table") or "",
                "evidence": row.get("evidence") or {},
            },
        )


def startup_orphan_scan() -> dict[str, Any] | None:
    """Report orphans at startup; adopt the settled ones only when asked to.

    ``DATAFLOW_SCHEDULE_ORPHAN_REPAIR``: ``off`` skips the scan, ``dry_run``
    (default) reports, ``apply`` adopts orphans whose owner the connector
    evidence settles. Nothing is disabled or deleted at startup.
    """
    from services.brand_env import getenv_brand

    mode = str(getenv_brand(_REPAIR_ENV, "dry_run") or "dry_run").strip().lower()
    if mode in {"off", "0", "false", "no"}:
        return None
    apply = mode == "apply"
    orphans = find_orphans()
    if not orphans:
        return {"dry_run": not apply, "orphan_count": 0, "applied_count": 0, "results": []}
    actions = {o.id: ACTION_NONE for o in orphans if not o.proposed_workspace_id}
    summary = reconcile_orphans(
        dry_run=not apply, actions=actions, actor="system:startup", via="startup"
    )
    logger.warning(
        "Schedule store holds %d pipeline registration(s) no workspace can see; "
        "%s. List and reconcile them at GET/POST /api/v1/schedules/admin/orphans.",
        summary["orphan_count"],
        f"adopted {summary['applied_count']}" if apply else "dry run, nothing changed",
    )
    return summary
