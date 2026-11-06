"""Scheduled job-history retention (Security 23, QA r14).

``retention_days`` (Settings -> General) was applied only when a workspace
admin ran the dry-run-first purge (``services.job_retention``). This module is
the automatic run of *exactly that purge*:

* **Off unless opted in.** It runs only while the deployment-wide General
  setting ``retention_auto_purge`` is true. That setting is off by default and
  only a platform admin changes it (the same document as ``retention_days``),
  so no deployment starts deleting history because it upgraded.
* **The setting's stated scope, nothing more.** Finished jobs (completed /
  failed / cancelled ...) in the ``transfer_jobs`` store whose finish time is
  older than ``retention_days``, per workspace — the same query, cutoff and
  two-step (dry run, then apply with its token) as the admin purge. Running,
  queued or paused jobs, jobs with no workspace (pre-scoping rows), audit
  records and quarantine are never touched: their retention has its own
  controls (``/audit/retention/purge``, the quarantine TTL).
* **Once a day, once per deployment.** A claim document in Mongo
  (``maintenance_runs``, ``_id = "job_retention"``) is advanced atomically, so
  a second scheduler replica or a restart does not run it twice.
* **Audited per run, per workspace.** ``workspace.jobs.retention.purge`` with
  ``mode: "scheduled"``, the cutoff and the count removed, actor
  ``system:retention``, stamped with the workspace so its admins see it.

The scheduler beat (``services.schedule_runner.run_schedule_loop``) calls
:func:`run_due_retention`; it never raises into the beat.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)

RUN_INTERVAL = timedelta(hours=24)
CLAIM_COLLECTION = "maintenance_runs"
CLAIM_ID = "job_retention"
ACTOR = "system:retention"
AUDIT_ACTION = "workspace.jobs.retention.purge"


def auto_purge_enabled() -> bool:
    from services.workspace_store import get_workspace_settings

    return bool(get_workspace_settings().get("retention_auto_purge"))


def _claim(db: Any, now: datetime, *, force: bool) -> bool:
    """Advance the deployment's claim when the run is due; False when it is not."""
    coll = db[CLAIM_COLLECTION]
    coll.update_one(
        {"_id": CLAIM_ID},
        {"$setOnInsert": {"next_due_at": datetime(1970, 1, 1, tzinfo=timezone.utc)}},
        upsert=True,
    )
    due = {} if force else {"next_due_at": {"$lte": now}}
    won = coll.find_one_and_update(
        {"_id": CLAIM_ID, **due},
        {"$set": {"next_due_at": now + RUN_INTERVAL, "claimed_at": now}},
    )
    return won is not None


def retention_schedule_status() -> dict[str, Any]:
    """What ``GET /workspace/settings`` says about the automatic run."""
    enabled = auto_purge_enabled()
    status: dict[str, Any] = {"scheduled": enabled, "interval_hours": int(RUN_INTERVAL.total_seconds() // 3600)}
    if not enabled:
        return status
    try:
        from services.mongodb_service import get_mongodb_service

        doc = get_mongodb_service().get_database()[CLAIM_COLLECTION].find_one({"_id": CLAIM_ID}) or {}
    except Exception as exc:  # noqa: BLE001 — status is informational
        logger.debug("retention claim unreadable: %s", exc)
        doc = {}
    for key in ("claimed_at", "next_due_at"):
        value = doc.get(key)
        status["last_run_at" if key == "claimed_at" else "next_run_at"] = (
            value.isoformat() if isinstance(value, datetime) else None
        )
    return status


def run_due_retention(*, now: datetime | None = None, force: bool = False) -> dict[str, Any] | None:
    """Run the scheduled purge if it is enabled and due. ``None`` when it did not run."""
    if not auto_purge_enabled():
        return None
    from services.job_retention import purge_expired_jobs
    from services.mongodb_service import get_mongodb_service
    from services.team_store import list_workspaces
    from services.workspace_store import get_workspace_settings

    current = now or datetime.now(timezone.utc)
    db = get_mongodb_service().get_database()
    if not _claim(db, current, force=force):
        return None
    retention_days = int(get_workspace_settings()["retention_days"])
    runs: list[dict[str, Any]] = []
    for ws in list_workspaces():
        runs.append(_purge_workspace(ws.id, retention_days, current, purge_expired_jobs))
    return {"ran_at": current.isoformat(), "retention_days": retention_days, "workspaces": runs}


def _purge_workspace(workspace_id: str, retention_days: int, now: datetime, purge) -> dict[str, Any]:
    from services.audit_log import append_audit_event

    try:
        dry = purge(workspace_id=workspace_id, retention_days=retention_days, dry_run=True, now=now)
        removed = 0
        if dry.get("would_remove"):
            applied = purge(
                workspace_id=workspace_id,
                retention_days=retention_days,
                dry_run=False,
                confirm_token=dry["confirm_token"],
                now=now,
            )
            removed = int(applied.get("removed") or 0)
        outcome, level, error = "succeeded", ("warn" if removed else "info"), ""
    except Exception as exc:  # noqa: BLE001 — one workspace must not stop the rest
        logger.exception("Scheduled job retention failed for workspace %s", workspace_id)
        dry, removed = {"cutoff": None}, 0
        outcome, level, error = "failed", "error", type(exc).__name__
    details = {
        "mode": "scheduled",
        "dry_run": False,
        "retention_days": retention_days,
        "cutoff": dry.get("cutoff"),
        "would_remove": dry.get("would_remove"),
        "removed": removed,
        "scheduled": True,
        **({"error": error} if error else {}),
    }
    append_audit_event(
        action=AUDIT_ACTION,
        resource=f"workspace:{workspace_id}/jobs",
        actor=ACTOR,
        level=level,
        outcome=outcome,
        workspace_id=workspace_id,
        details=details,
    )
    return {"workspace_id": workspace_id, "outcome": outcome, **details}
