"""Assign legacy unscoped resources to their owner's workspace (D154 migration).

Resources written before workspaces existed carry ``workspace_id == ""``. Once
isolation is enforced (self-serve signup on) only platform admins see them, so
the account that created them would lose its own history. This backfill gives
each one an owner workspace:

* the owner is the account named on the row (``created_by`` / ``owner_email``,
  a job's ``triggered_by``), else the single account whose legacy jobs or
  schedules use it (connectors carry no owner field), else the platform owner
  (``--owner-email`` or the bootstrap admin);
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

from services import connector_store, quarantine_dlq, schedule_store, usage_metering
from services.brand_env import getenv_brand
from services.mongodb_service import get_mongodb_service
from services.team_store import create_workspace, normalize_email
from services.tenant_scope import ordered_memberships
from services.workspace_filter import WorkspaceFilter

logger = logging.getLogger(__name__)

_OWNER_FIELDS = ("created_by", "owner_email", "owner", "triggered_by")
_UNSCOPED = WorkspaceFilter.only("")


@dataclass
class BackfillReport:
    applied: bool
    platform_owner: str
    counts: dict[str, dict[str, int]] = field(default_factory=dict)
    created_workspaces: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "applied": self.applied,
            "platform_owner": self.platform_owner,
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
    """Owner email -> personal workspace id, creating one only on apply."""

    def __init__(self, platform_owner: str, *, apply: bool, report: BackfillReport):
        self.platform_owner = platform_owner
        self.apply = apply
        self.report = report
        self._cache: dict[str, str] = {}

    def for_owner(self, email: str) -> str:
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


def backfill_legacy_workspaces(*, apply: bool = False, owner_email: str = "") -> BackfillReport:
    """Assign every unscoped connector, job, schedule, DLQ and usage row an owner."""
    platform_owner = normalize_email(owner_email) or default_platform_owner()
    if not platform_owner:
        raise ValueError("No platform owner: pass --owner-email or set DATAWRAP_ADMIN_EMAIL")
    report = BackfillReport(applied=apply, platform_owner=platform_owner)
    workspaces = _Workspaces(platform_owner, apply=apply, report=report)
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

    def assign(collection: str, rows: list[tuple[str, str]], write: Callable[[str, str], Any]) -> None:
        counts = tally(collection)
        for rid, owner in rows:
            ws = workspaces.for_owner(owner)
            counts["assigned"] += 1
            if apply:
                try:
                    write(rid, ws)
                except Exception as exc:  # noqa: BLE001 - one bad row must not stop the run
                    counts["assigned"] -= 1
                    counts["skipped"] += 1
                    logger.warning("backfill %s %s skipped: %s", collection, rid, exc)

    saved = [c for c in connector_store.list_connectors() if _UNSCOPED.matches(c.workspace_id)]
    assign(
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
    assign(
        "schedules",
        [(s.id, schedule_owner[s.id]) for s in schedules],
        lambda rid, ws: schedule_store.update_schedule(rid, {"workspace_id": ws}),
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
    for collection, counts in report.counts.items():
        logger.info(
            "legacy workspace backfill %s: %s assigned, %s skipped (%s)",
            collection, counts["assigned"], counts["skipped"], "applied" if apply else "dry run",
        )
    return report
