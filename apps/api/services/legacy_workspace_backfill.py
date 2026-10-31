"""Assign legacy unscoped resources to their owner's workspace (D154 migration).

Resources written before workspaces existed carry ``workspace_id == ""``. Once
isolation is enforced (self-serve signup on) only platform admins see them, so
the account that created them would lose its own history. This backfill gives
each one an owner workspace:

* the owner is the account named on the row (``created_by`` / ``owner_email``,
  a job's ``triggered_by``), else the single account whose legacy jobs or
  schedules use it (connectors carry no owner field), else the platform owner
  (``--owner-email`` or the bootstrap admin);
* contracts, transform projects and library recipes carry no owner either:
  they follow the one workspace of the schedules (or destination connector)
  that use them, else the platform owner's; preflight runs, transfer plans
  and shape drafts have no evidence at all and go to the platform owner;
* the workspace is the owner's personal workspace (the one signup created),
  else their earliest membership; a platform owner with no workspace gets one;
* rows that already have a workspace are never touched, so the run is
  idempotent; a dry run (the default) reports the same counts and writes
  nothing.

Audit events are deliberately left alone: every field of an audit record is
covered by its HMAC chain hash, so restamping one would read as tampering at
the next ``/audit/verify``. Unscoped audit rows stay platform-admin only.

Run it from ``scripts/backfill_legacy_workspaces.py``; it is never run at
startup.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

from services import (
    connector_store,
    preflight_run_store,
    quarantine_dlq,
    schedule_store,
    shape_draft_store,
    transfer_plan_store,
    usage_metering,
)
from services.brand_env import getenv_brand
from services.mongodb_service import get_mongodb_service
from services.team_store import create_workspace, get_workspace, normalize_email
from services.tenant_scope import ordered_memberships
from services.transform_library import get_transform_library
from services.transform_store import get_transform_store
from services.workspace_filter import WorkspaceFilter
from src.services.contract_store import get_contract_store

logger = logging.getLogger(__name__)

_OWNER_FIELDS = ("created_by", "owner_email", "owner", "triggered_by")
_UNSCOPED = WorkspaceFilter.only("")


@dataclass
class BackfillReport:
    applied: bool
    platform_owner: str
    counts: dict[str, dict[str, int]] = field(default_factory=dict)
    created_workspaces: list[str] = field(default_factory=list)
    target_workspace: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "applied": self.applied,
            "platform_owner": self.platform_owner,
            "target_workspace": self.target_workspace or None,
            "counts": self.counts,
            "created_workspaces": self.created_workspaces,
        }


def _known_account(email: str) -> bool:
    from src.services.auth_service import lookup_user

    return bool(email) and "@" in email and lookup_user(email) is not None


def _owner_named_on(row: Any) -> str:
    """The first owner-ish field on ``row`` that names a real account."""
    nested = row.get("transfer_request") if isinstance(row, dict) else None
    holders = [row] + ([nested] if isinstance(nested, dict) else [])
    for holder in holders:
        for name in _OWNER_FIELDS:
            raw = holder.get(name) if isinstance(holder, dict) else getattr(holder, name, "")
            email = normalize_email(str(raw or ""))
            if _known_account(email):
                return email
    return ""


def default_platform_owner() -> str:
    """The bootstrap admin, else the earliest stored platform admin."""
    email = normalize_email(getenv_brand("ADMIN_EMAIL", "") or "")
    if email:
        return email
    from services.user_store import list_users

    for user in list_users():
        if str(user.get("role") or "").lower() == "admin":
            return normalize_email(str(user.get("email") or ""))
    return ""


class _Workspaces:
    """Owner email -> personal workspace id, creating one only on apply.

    With ``target`` (``--workspace-id``) every row is claimed into that one
    workspace instead: the owner chooses where the history lands (D218).
    """

    def __init__(
        self, platform_owner: str, *, apply: bool, report: BackfillReport, target: str = ""
    ):
        self.platform_owner = platform_owner
        self.apply = apply
        self.report = report
        self.target = target
        self._cache: dict[str, str] = {}

    def for_owner(self, email: str) -> str:
        if self.target:
            return self.target
        email = normalize_email(email) or self.platform_owner
        if email in self._cache:
            return self._cache[email]
        memberships = ordered_memberships(email)
        if memberships:
            ws = memberships[0][0]
        elif email != self.platform_owner:
            # An owner on record with no workspace: the platform owner keeps it.
            ws = self.for_owner(self.platform_owner)
        elif self.apply:
            ws = create_workspace(name=f"{email}'s workspace", created_by=email).id
            self.report.created_workspaces.append(ws)
        else:
            ws = f"(new workspace for {email})"
        self._cache[email] = ws
        return ws


def _unique(owners: Iterable[str]) -> str:
    distinct = {o for o in owners if o}
    return distinct.pop() if len(distinct) == 1 else ""


def backfill_legacy_workspaces(
    *, apply: bool = False, owner_email: str = "", workspace_id: str = ""
) -> BackfillReport:
    """Assign every unscoped connector, job, schedule, DLQ and usage row an owner.

    ``workspace_id`` claims every unscoped row into that existing workspace
    instead of inferring an owner per row (refused when it does not exist).
    """
    target = str(workspace_id or "").strip()
    if target and get_workspace(target) is None:
        raise ValueError(f"Workspace {target!r} does not exist")
    platform_owner = normalize_email(owner_email) or default_platform_owner()
    if not platform_owner and not target:
        raise ValueError("No platform owner: pass --owner-email or set DATAWRAP_ADMIN_EMAIL")
    report = BackfillReport(applied=apply, platform_owner=platform_owner, target_workspace=target)
    workspaces = _Workspaces(platform_owner, apply=apply, report=report, target=target)
    mongo = get_mongodb_service()

    # Owner evidence first: jobs name their actor, and connectors/schedules are
    # attributed through the legacy jobs that used them.
    jobs = mongo.list_unscoped_jobs()
    job_owner = {str(j["_id"]): _owner_named_on(j) for j in jobs}
    schedules = [s for s in schedule_store.list_schedules() if _UNSCOPED.matches(s.workspace_id)]
    users_of: dict[str, list[str]] = defaultdict(list)
    for job in jobs:
        req = job.get("transfer_request") or {}
        for side in ("source", "destination"):
            cid = str((req.get(side) or {}).get("connector_id") or "")
            if cid:
                users_of[cid].append(job_owner[str(job["_id"])])
        sid = str(job.get("schedule_id") or req.get("schedule_id") or "")
        if sid:
            users_of[f"schedule:{sid}"].append(job_owner[str(job["_id"])])
    schedule_owner = {
        s.id: _owner_named_on(s) or _unique(users_of[f"schedule:{s.id}"]) for s in schedules
    }
    for s in schedules:
        for cid in (s.source_connector_id, s.dest_connector_id):
            if cid:
                users_of[cid].append(schedule_owner[s.id])

    def connector_owner(row: Any, cid: str) -> str:
        return _owner_named_on(row) or _unique(users_of[cid])

    def tally(collection: str) -> dict[str, int]:
        return report.counts.setdefault(collection, {"assigned": 0, "skipped": 0})

    def assign(
        collection: str, rows: list[tuple[str, str]], write: Callable[[str, str], Any]
    ) -> dict[str, str]:
        """Assign each ``(id, owner)`` row; returns ``id -> workspace``."""
        counts = tally(collection)
        placed: dict[str, str] = {}
        for rid, owner in rows:
            ws = workspaces.for_owner(owner)
            placed[rid] = ws
            counts["assigned"] += 1
            if apply:
                try:
                    write(rid, ws)
                except Exception as exc:  # noqa: BLE001 - one bad row must not stop the run
                    counts["assigned"] -= 1
                    counts["skipped"] += 1
                    logger.warning("backfill %s %s skipped: %s", collection, rid, exc)
        return placed

    saved = [c for c in connector_store.list_connectors() if _UNSCOPED.matches(c.workspace_id)]
    connector_ws = assign(
        "connectors",
        [(c.id, connector_owner(c, c.id)) for c in saved],
        lambda rid, ws: connector_store.update_connector(rid, {"workspace_id": ws}),
    )
    # The job store's own connector rows (the in-memory store, or Mongo docs the
    # file-backed connector store does not read).
    saved_ids = {c.id for c in saved}
    job_store_connectors = [
        c for c in mongo.list_connectors()
        if _UNSCOPED.matches(c.get("workspace_id")) and str(c["_id"]) not in saved_ids
    ]
    assign(
        "job_store_connectors",
        [(str(c["_id"]), connector_owner(c, str(c["_id"]))) for c in job_store_connectors],
        lambda rid, ws: mongo.update_connector(rid, {"workspace_id": ws}),
    )
    assign(
        "jobs",
        [(jid, owner) for jid, owner in job_owner.items()],
        lambda rid, ws: mongo.update_job_fields(rid, {"workspace_id": ws}, touch_updated_at=False),
    )
    schedule_ws = assign(
        "schedules",
        [(s.id, schedule_owner[s.id]) for s in schedules],
        lambda rid, ws: schedule_store.update_schedule(rid, {"workspace_id": ws}),
    )

    # Rows that carry no owner follow what uses them: the one workspace of the
    # schedules / connectors that reference them, else the platform owner's.
    all_schedules = schedule_store.list_schedules()
    for s in all_schedules:
        schedule_ws.setdefault(s.id, s.workspace_id)
    for c in connector_store.list_connectors():
        connector_ws.setdefault(c.id, c.workspace_id)

    def followed(evidence: Iterable[str]) -> str:
        # Resolved only when a row needs it: a run with nothing to place must
        # not create the platform owner's workspace.
        return _unique(ws for ws in evidence if ws) or workspaces.for_owner("")

    contract_used_by: dict[str, list[str]] = defaultdict(list)
    recipe_used_by: dict[str, list[str]] = defaultdict(list)
    for s in all_schedules:
        if s.contract_id:
            contract_used_by[s.contract_id].append(schedule_ws[s.id])
        if s.transform_recipe_id:
            recipe_used_by[s.transform_recipe_id].append(schedule_ws[s.id])

    def stamp_workspaces(
        collection: str, rows: list[tuple[str, str]], write: Callable[[str, str], Any]
    ) -> None:
        """Like ``assign`` for rows whose workspace is already decided."""
        counts = tally(collection)
        for rid, ws in rows:
            counts["assigned"] += 1
            if apply:
                try:
                    write(rid, ws)
                except Exception as exc:  # noqa: BLE001
                    counts["assigned"] -= 1
                    counts["skipped"] += 1
                    logger.warning("backfill %s %s skipped: %s", collection, rid, exc)

    contracts = {
        c.id: c
        for c in get_contract_store().list_contracts(limit=100_000)
        if _UNSCOPED.matches((getattr(c, "metadata", None) or {}).get("workspace_id"))
    }

    def write_contract(rid: str, ws: str) -> None:
        contract = contracts[rid]
        contract.metadata = {**(contract.metadata or {}), "workspace_id": ws}
        get_contract_store().save_contract(contract)

    stamp_workspaces(
        "contracts",
        [(cid, followed(contract_used_by[cid])) for cid in contracts],
        write_contract,
    )
    projects = {p.id: p for p in get_transform_store().list() if _UNSCOPED.matches(p.workspace_id)}

    def write_project(rid: str, ws: str) -> None:
        project = projects[rid]
        project.workspace_id = ws
        get_transform_store().save(project)

    stamp_workspaces(
        "transform_projects",
        [(pid, followed([connector_ws.get(p.destination_connector_id, "")])) for pid, p in projects.items()],
        write_project,
    )
    recipes = {r.id: r for r in get_transform_library().list() if _UNSCOPED.matches(r.workspace_id)}

    def write_recipe(rid: str, ws: str) -> None:
        recipe = recipes[rid]
        recipe.workspace_id = ws
        get_transform_library().save(recipe)

    stamp_workspaces(
        "transform_recipes",
        [(rid, followed(recipe_used_by[rid])) for rid in recipes],
        write_recipe,
    )

    # Append-only rows follow the job they belong to.
    def workspace_of_job_row(row: dict[str, Any]) -> str:
        return workspaces.for_owner(job_owner.get(str(row.get("job_id") or ""), ""))

    report.counts["quarantine_dlq"] = quarantine_dlq.assign_unscoped_workspaces(
        workspace_of_job_row, apply=apply
    )
    report.counts["usage_events"] = usage_metering.assign_unscoped_workspaces(
        workspace_of_job_row, apply=apply
    )
    # Runs, plans and drafts carry no owner at all: the platform owner keeps them.
    for name, store in (
        ("preflight_runs", preflight_run_store),
        ("transfer_plans", transfer_plan_store),
        ("shape_drafts", shape_draft_store),
    ):
        report.counts[name] = store.assign_unscoped_workspaces(
            lambda _row: workspaces.for_owner(""), apply=apply
        )
    for collection, counts in report.counts.items():
        logger.info(
            "legacy workspace backfill %s: %s assigned, %s skipped (%s)",
            collection, counts["assigned"], counts["skipped"], "applied" if apply else "dry run",
        )
    return report
