"""Delete a workspace and everything scoped to it — one cascade for every caller.

``DELETE /api/v1/team/workspaces/{id}``, the sandbox teardown and the expiry
sweep (``services.sandbox_workspaces``) all end here, through
``team_store.delete_workspace``. Before this a deleted workspace left its
tenant policy, notification channels (webhook secrets), API keys (still
authenticating), saved connectors (stored credentials), schedules (still
armed — the beat kept firing them as orphans), jobs and quarantine rows behind.

Removed, in this order, each step idempotent (a re-run finishes what a failed
run left):

1. refused first — while any job of the workspace is still active (queued,
   running, paused ...): the caller cancels it, or the sweep retries later;
2. schedules (CDC capture released, deletion audited per schedule);
3. saved connectors (with their credentials);
4. finished jobs and the workspace's quarantine / DLQ rows;
5. transform projects and library recipes owned by the workspace;
6. tenant profiles (MFA / session / IP policy), notification channels;
7. API keys, service accounts and SCIM tokens bound to it — revoked, so the
   secret is still recognised and refused;
8. the workspace and its memberships.

Kept on purpose: the audit trail (hash-chained evidence — rewriting it would
read as tampering) and usage metering (billing evidence). Data contracts are
signed evidence and are kept too.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class WorkspaceBusy(RuntimeError):
    """The workspace still has active jobs; nothing was deleted."""

    def __init__(self, active: int):
        super().__init__(
            f"This workspace has {active} active job(s) — cancel them first, then delete the workspace"
        )
        self.active = active


def _jobs_collection():
    from services.metadata_backend import mongo_database

    db = mongo_database()
    return db["transfer_jobs"] if db is not None else None


def _job_query(workspace_id: str, *, terminal: bool) -> dict[str, Any]:
    from services.job_status import TERMINAL_STATUSES

    op = "$in" if terminal else "$nin"
    return {"workspace_id": workspace_id, "status": {op: sorted(TERMINAL_STATUSES)}}


def _active_jobs(workspace_id: str) -> int:
    coll = _jobs_collection()
    return int(coll.count_documents(_job_query(workspace_id, terminal=False))) if coll is not None else 0


def _schedules(workspace_id: str, actor: str) -> int:
    from services.cdc_capture_release import release_schedule_cdc_capture
    from services.schedule_audit import record_schedule_deleted
    from services.schedule_store import delete_schedule, list_schedules

    removed = 0
    for sched in list_schedules():
        if (sched.workspace_id or "") != workspace_id:
            continue
        release_schedule_cdc_capture(sched)
        if delete_schedule(sched.id):
            removed += 1
            record_schedule_deleted(sched, actor=actor)
    return removed


def _connectors(workspace_id: str) -> int:
    from services.connector_store import delete_connector, list_connectors

    return sum(
        1
        for c in list_connectors(workspace_id=workspace_id)
        if c.workspace_id == workspace_id and delete_connector(c.id, workspace_id)
    )


def _jobs_and_quarantine(workspace_id: str) -> tuple[int, int]:
    jobs = 0
    coll = _jobs_collection()
    if coll is not None:
        jobs = int(coll.delete_many(_job_query(workspace_id, terminal=True)).deleted_count)
    dlq = 0
    try:
        from services.quarantine_dlq import _dlq_coll

        dlq_coll = _dlq_coll()
        if dlq_coll is not None:
            dlq = int(dlq_coll.delete_many({"workspace_id": workspace_id}).deleted_count)
    except Exception:  # noqa: BLE001 — quarantine store optional; reported as 0
        logger.warning("Quarantine rows of workspace %s were not removed", workspace_id, exc_info=True)
    return jobs, dlq


def _transforms(workspace_id: str) -> int:
    from services.transform_library import get_transform_library
    from services.transform_store import get_transform_store

    removed = 0
    for store in (get_transform_store(), get_transform_library()):
        for item in store.list(workspace_id):
            if getattr(item, "workspace_id", "") == workspace_id and store.delete(item.id):
                removed += 1
    return removed


def _policy_channels_keys(workspace_id: str) -> tuple[int, int, int]:
    from services import integrations_store, notification_store, tenant_store

    tenants = [t.id for t in tenant_store.list_tenants() if t.workspace_id == workspace_id]
    for tenant_id in tenants:
        tenant_store.delete_tenant(tenant_id)
    channels = [c.id for c in notification_store.list_channels(workspace_id=workspace_id)]
    for channel_id in channels:
        notification_store.delete_channel(channel_id)
    keys = [
        str(r["id"])
        for r in integrations_store.load_api_key_records()
        if str(r.get("workspace_id") or "") == workspace_id
    ]
    revoked = sum(1 for k in keys if integrations_store.revoke_api_key(k))
    return len(tenants), len(channels), revoked


def teardown_workspace(workspace_id: str, *, actor: str = "system") -> dict[str, Any]:
    """Run the cascade; returns what was removed. Raises :class:`WorkspaceBusy`.

    Runs in system scope (``bound_workspace_filter(None)``) and names the
    TARGET workspace on every store call. The route runs inside the admin's
    request, whose bound filter is the workspace in the ``X-Workspace-Id``
    header (the web sends it on every call): left bound, the connector store
    saw only the admin's current workspace, so deleting workspace B from A
    removed none of B's connectors or credentials (B1, second review), and the
    CDC release could not find B's source connector to drop its slot.
    """
    from services.workspace_filter import bound_workspace_filter

    if not workspace_id:
        raise ValueError("The global workspace cannot be deleted")
    with bound_workspace_filter(None):
        return _teardown_unbound(workspace_id, actor=actor)


def _teardown_unbound(workspace_id: str, *, actor: str) -> dict[str, Any]:
    from services import team_store

    active = _active_jobs(workspace_id)
    if active:
        raise WorkspaceBusy(active)
    members = len(team_store.list_workspace_members(workspace_id))
    summary: dict[str, Any] = {"workspace_id": workspace_id, "schedules_removed": _schedules(workspace_id, actor)}
    summary["connectors_removed"] = _connectors(workspace_id)
    summary["jobs_removed"], summary["quarantine_rows_removed"] = _jobs_and_quarantine(workspace_id)
    summary["transforms_removed"] = _transforms(workspace_id)
    (
        summary["tenants_removed"],
        summary["channels_removed"],
        summary["api_keys_revoked"],
    ) = _policy_channels_keys(workspace_id)
    summary["members_removed"] = members
    summary["workspace_removed"] = team_store._delete_workspace_rows(workspace_id)
    return summary
