"""Delete a workspace and everything scoped to it — one cascade for every caller.

``DELETE /api/v1/team/workspaces/{id}``, the sandbox teardown and the expiry
sweep (``services.sandbox_workspaces``) all end here, through
``team_store.delete_workspace`` or :func:`teardown_workspace`.

**Scope.** The cascade runs in system scope (``bound_workspace_filter(None)``)
so the admin's own current workspace cannot hide the target's rows (B1), and
therefore every step names the TARGET workspace explicitly and matches it
exactly: an unscoped legacy row or another workspace's row is never touched.

**Order** (each step idempotent — a re-run finishes what an interrupted one
left; ``dry_run`` counts every step and changes nothing):

1. refused while any job of the workspace is active (or when the job store
   cannot say): the caller cancels it, the sweep retries later;
2. the workspace is **tombstoned** (``deleting_at``): from here every write
   to it answers 409, no job can be created in it and the beat claims none
   of its schedules (S3);
3. API keys / service accounts / SCIM tokens bound to it are revoked and its
   memberships removed — nobody keeps authority mid-teardown;
4. schedules (CDC capture released through ``cdc_capture_release``, each
   deletion audited), then saved connectors with their credentials;
5. finished jobs with their checkpoints, the incremental watermarks and
   quality-profile runs only its jobs produced, quarantine / DLQ rows (Mongo
   and the JSONL fallback), uploads (file, staged object, registry row);
6. transform projects and library recipes, tenant profile (MFA / session /
   IP policy), notification channels, SCIM group mappings and managed
   memberships, "last used workspace" pointers;
7. the sandbox's own tester login, and logins that tester created which
   belong to no other workspace;
8. active jobs re-checked, then the workspace row.

**Retained on purpose** (also returned as ``retained``): the audit trail
(hash-chained evidence — rewriting it reads as tampering), usage metering
(billing evidence) and data contracts (signed evidence).
"""

from __future__ import annotations

import logging
from typing import Any, Callable

logger = logging.getLogger(__name__)

RETAINED = (
    "audit trail (hash-chained evidence)",
    "usage metering (billing evidence)",
    "data contracts (signed evidence)",
)


class WorkspaceBusy(RuntimeError):
    """The workspace has active jobs (or the job store cannot say)."""

    def __init__(self, active: int, *, started: bool = False, unknown: bool = False):
        if unknown:
            message = "The job store cannot say whether this workspace has active jobs; try again"
        elif started:
            message = (
                f"{active} job(s) became active during the teardown; the workspace is closed "
                "and the delete finishes on retry once they end"
            )
        else:
            message = f"This workspace has {active} active job(s) — cancel them first, then delete the workspace"
        super().__init__(message)
        self.active = active
        self.started = started


# ── job store ───────────────────────────────────────────────────────────────


def _active_jobs(workspace_id: str) -> int:
    """Active jobs of the workspace on whichever job store this process uses."""
    from services.job_status import TERMINAL_STATUSES
    from services.mongodb_service import get_mongodb_service

    try:
        counts = get_mongodb_service().count_jobs(workspace_id)
    except Exception as exc:  # noqa: BLE001 — cannot tell: refuse, never guess 0
        raise WorkspaceBusy(0, unknown=True) from exc
    by_status = counts.get("by_status") or {}
    return sum(int(n) for status, n in by_status.items() if status not in TERMINAL_STATUSES)


def _jobs_db():
    from services.metadata_backend import mongo_database

    return mongo_database()


def _terminal_job_ids(workspace_id: str) -> list[Any]:
    """Raw ``_id`` values (str or ObjectId) of the workspace's finished jobs."""
    from services.job_status import TERMINAL_STATUSES

    db = _jobs_db()
    if db is None:
        return []
    query = {"workspace_id": workspace_id, "status": {"$in": sorted(TERMINAL_STATUSES)}}
    return [d["_id"] for d in db["transfer_jobs"].find(query, {"_id": 1})]


def _foreign_cursor_keys(workspace_id: str) -> set[str]:
    """Cursor keys any other workspace's job or schedule still uses (never removed)."""
    keys: set[str] = set()
    db = _jobs_db()
    if db is not None:
        for doc in db["transfer_jobs"].find(
            {"workspace_id": {"$ne": workspace_id}, "cursor_key": {"$exists": True, "$ne": ""}},
            {"cursor_key": 1},
        ):
            keys.add(str(doc.get("cursor_key") or ""))
    from services.schedule_store import list_schedules

    for sched in list_schedules():
        if (sched.workspace_id or "") != workspace_id:
            key = str(getattr(sched, "cursor_key", "") or "")
            if key:
                keys.add(key)
    keys.discard("")
    return keys


def _jobs_and_derived(workspace_id: str, dry_run: bool) -> dict[str, int]:
    from services.data_quality_history import forget_job_runs
    from services.sync_cursor import forget_job_watermarks

    job_ids = _terminal_job_ids(workspace_id)
    ids = {str(j) for j in job_ids}
    out = {
        "jobs_removed": len(job_ids),
        "watermarks_removed": len(
            forget_job_watermarks(ids, keep_keys=_foreign_cursor_keys(workspace_id), dry_run=dry_run)
        ),
        "quality_profiles_touched": forget_job_runs(ids, dry_run=dry_run),
        "job_checkpoints_removed": 0,
    }
    db = _jobs_db()
    if db is not None and ids:
        query = {"job_id": {"$in": sorted(ids)}}
        coll = db["job_checkpoints"]
        out["job_checkpoints_removed"] = int(
            coll.count_documents(query) if dry_run else coll.delete_many(query).deleted_count
        )
        if not dry_run:
            db["transfer_jobs"].delete_many({"_id": {"$in": job_ids}, "workspace_id": workspace_id})
    return out


# ── authority, schedules, connectors ────────────────────────────────────────


def _api_keys(workspace_id: str, dry_run: bool) -> int:
    from services import integrations_store

    keys = [
        str(r["id"])
        for r in integrations_store.load_api_key_records()
        if str(r.get("workspace_id") or "") == workspace_id and not r.get("revoked_at")
    ]
    if dry_run:
        return len(keys)
    return sum(1 for k in keys if integrations_store.revoke_api_key(k))


def _memberships(workspace_id: str, dry_run: bool) -> int:
    from services import team_store

    if dry_run:
        return len(team_store.list_workspace_members(workspace_id))
    return team_store.delete_workspace_memberships(workspace_id)


def _schedules(workspace_id: str, actor: str, dry_run: bool) -> int:
    from services.cdc_capture_release import release_schedule_cdc_capture
    from services.schedule_audit import record_schedule_deleted
    from services.schedule_store import delete_schedule, list_schedules

    mine = [s for s in list_schedules() if (s.workspace_id or "") == workspace_id]
    if dry_run:
        return len(mine)
    removed = 0
    for sched in mine:
        # Before the connectors go: the release reads the source connector.
        release_schedule_cdc_capture(sched)
        if delete_schedule(sched.id):
            removed += 1
            record_schedule_deleted(sched, actor=actor)
    return removed


def _connectors(workspace_id: str, dry_run: bool) -> int:
    from services.connector_store import delete_connector, list_connectors

    mine = [c for c in list_connectors(workspace_id=workspace_id) if c.workspace_id == workspace_id]
    if dry_run:
        return len(mine)
    return sum(1 for c in mine if delete_connector(c.id, workspace_id, exact_workspace=True))


# ── workspace-scoped stores ─────────────────────────────────────────────────


def _transforms(workspace_id: str, dry_run: bool) -> int:
    from services.transform_library import get_transform_library
    from services.transform_store import get_transform_store

    removed = 0
    for store in (get_transform_store(), get_transform_library()):
        for item in store.list(workspace_id):
            if getattr(item, "workspace_id", "") != workspace_id:
                continue
            if dry_run or store.delete(item.id):
                removed += 1
    return removed


def _tenants(workspace_id: str, dry_run: bool) -> int:
    from services import tenant_store

    ids = [t.id for t in tenant_store.list_tenants() if t.workspace_id == workspace_id]
    if not dry_run:
        for tenant_id in ids:
            tenant_store.delete_tenant(tenant_id)
    return len(ids)


def _channels(workspace_id: str, dry_run: bool) -> int:
    from services import notification_store

    ids = [
        c.id for c in notification_store.list_channels(workspace_id=workspace_id)
        if c.workspace_id == workspace_id
    ]
    if not dry_run:
        for channel_id in ids:
            notification_store.delete_channel(channel_id)
    return len(ids)


def _dlq(workspace_id: str, dry_run: bool) -> int:
    from services.quarantine_dlq import delete_workspace_events

    return delete_workspace_events(workspace_id, dry_run=dry_run)


def _uploads(workspace_id: str, dry_run: bool) -> int:
    from services.file_parser import delete_workspace_uploads

    return delete_workspace_uploads(workspace_id, dry_run=dry_run)


def _scim(workspace_id: str, dry_run: bool) -> int:
    from services.scim_store import forget_workspace

    return forget_workspace(workspace_id, dry_run=dry_run)


def _preferences(workspace_id: str, dry_run: bool) -> int:
    from services.workspace_preference import forget_workspace

    return forget_workspace(workspace_id, dry_run=dry_run)


# ── logins ──────────────────────────────────────────────────────────────────


def _login_still_synthetic(email: str, workspace_id: str, record: dict[str, Any]) -> bool:
    """A sandbox-minted login that has not become a real account since.

    Kept (only the sandbox membership goes) when it now belongs to another
    workspace, holds a role above ``member`` or is a platform admin.
    """
    from services import team_store
    from services.tenant_scope import actor_is_unrestricted

    if any(m.get("workspace_id") != workspace_id for m in team_store.list_memberships_for_user(email)):
        return False
    if str(record.get("role") or "member") != "member":
        return False
    return not actor_is_unrestricted({"email": email, "role": record.get("role"), "auth_kind": "session"})


def _delete_login(email: str, created_at: str) -> bool:
    """Delete the exact record, then revoke its sessions — a sign-in racing the
    delete gets a session for an account that no longer exists, which the auth
    middleware refuses, and the revoke after the delete removes it too."""
    from services import auth_sessions, user_store

    removed = user_store.delete_user_if_created_at(email=email, created_at=created_at)
    auth_sessions.revoke_all_for_email(email)
    return removed


def _sandbox_logins(ws: Any, member_emails: set[str], dry_run: bool) -> dict[str, Any]:
    """The sandbox's own tester login (S1/S2), and the logins that tester created.

    The tester login is deleted only while the stored record still carries
    the ``created_at`` the sandbox recorded and it is still synthetic
    (:func:`_login_still_synthetic`); otherwise it is kept and audited as
    ``tester_login_kept``. A login the tester created is deleted only if it was
    a member here and is likewise still synthetic.
    """
    from services import user_store
    from services.audit_log import append_audit_event

    out: dict[str, Any] = {"tester_login_removed": False, "tester_created_logins_removed": 0}
    tester = str(getattr(ws, "sandbox_tester", "") or "")
    created_at = str(getattr(ws, "sandbox_tester_created_at", "") or "")
    if not tester:
        return out
    record = user_store.get_user(tester)
    if created_at and record is not None and str(record.get("created_at") or "") == created_at:
        if not _login_still_synthetic(tester, ws.id, record):
            out["tester_login_kept"] = True
            if not dry_run:
                append_audit_event(
                    action="team.sandbox.tester_login_kept",
                    resource=f"/team/sandbox-workspaces/{ws.id}",
                    actor="system:workspace-teardown",
                    level="warn",
                    outcome="succeeded",
                    workspace_id=ws.id,
                    details={"tester": tester, "reason": "the login became a real account"},
                )
        else:
            out["tester_login_removed"] = True if dry_run else _delete_login(tester, created_at)
    for user in user_store.list_users():
        email = str(user.get("email") or "")
        if str(user.get("created_by") or "") != tester or email not in member_emails:
            continue
        if not _login_still_synthetic(email, ws.id, user):
            continue
        out["tester_created_logins_removed"] += 1
        if not dry_run:
            _delete_login(email, str(user.get("created_at") or ""))
    return out


# ── the cascade ─────────────────────────────────────────────────────────────


def teardown_workspace(
    workspace_id: str, *, actor: str = "system", dry_run: bool = False
) -> dict[str, Any]:
    """Run (or, with ``dry_run``, count) the cascade. Raises :class:`WorkspaceBusy`."""
    from services.workspace_filter import bound_workspace_filter

    if not workspace_id:
        raise ValueError("The global workspace cannot be deleted")
    with bound_workspace_filter(None):
        return _cascade(workspace_id, actor=actor, dry_run=dry_run)


def _cascade(workspace_id: str, *, actor: str, dry_run: bool) -> dict[str, Any]:
    from services import team_store

    workspace = team_store.get_workspace(workspace_id)
    active = _active_jobs(workspace_id)
    if active and not dry_run:
        raise WorkspaceBusy(active)
    summary: dict[str, Any] = {
        "workspace_id": workspace_id,
        "dry_run": dry_run,
        "active_jobs": active,
        "retained": list(RETAINED),
    }
    # Authority goes first; the member list is kept — on the tombstone, so a
    # resumed teardown (memberships already removed) still has it (L4).
    member_emails = {m["email"] for m in team_store.list_workspace_members(workspace_id)}
    if not dry_run and workspace is not None:
        closed = team_store.mark_workspace_closing(workspace_id, members=sorted(member_emails))
        if closed is not None:
            member_emails |= set(closed.deleting_members)
            workspace = closed
    steps: list[tuple[str, Callable[[], int]]] = [
        ("api_keys_revoked", lambda: _api_keys(workspace_id, dry_run)),
        ("members_removed", lambda: _memberships(workspace_id, dry_run)),
        ("schedules_removed", lambda: _schedules(workspace_id, actor, dry_run)),
        ("connectors_removed", lambda: _connectors(workspace_id, dry_run)),
    ]
    for name, step in steps:
        summary[name] = step()
    summary.update(_jobs_and_derived(workspace_id, dry_run))
    for name, store_step in (
        ("quarantine_rows_removed", _dlq),
        ("uploads_removed", _uploads),
        ("transforms_removed", _transforms),
        ("tenants_removed", _tenants),
        ("channels_removed", _channels),
        ("scim_mappings_removed", _scim),
        ("preferences_cleared", _preferences),
    ):
        summary[name] = store_step(workspace_id, dry_run)
    if workspace is not None:
        summary.update(_sandbox_logins(workspace, member_emails, dry_run))
    if dry_run:
        summary["workspace_removed"] = False
        return summary
    # A job that slipped in between the first check and the tombstone.
    late = _active_jobs(workspace_id)
    if late:
        raise WorkspaceBusy(late, started=True)
    summary["workspace_removed"] = team_store._delete_workspace_rows(workspace_id)
    return summary
