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
  (``maintenance_runs``, ``_id = "job_retention"``, ``services.maintenance_claim``)
  is advanced atomically, so a second scheduler replica or a restart does not
  run it twice. Without that database the setting refuses to turn on, and a
  run finds no store, skips and warns once.
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
CLAIM_ID = "job_retention"
ACTOR = "system:retention"
AUDIT_ACTION = "workspace.jobs.retention.purge"


def auto_purge_enabled() -> bool:
    """Off whenever the shared settings store cannot be read (fails closed)."""
    from services.workspace_store import safety_flag

    return safety_flag("retention_auto_purge")


_warned_no_store = False


class RetentionNeedsDurableStore(ValueError):
    """Scheduled retention needs the shared control-plane store (MongoDB)."""


def assert_can_enable() -> None:
    """Refuse turning the schedule on where no durable store holds the job history."""
    from services.maintenance_claim import durable_store

    if durable_store() is None:
        raise RetentionNeedsDurableStore(
            "Automatic job purge needs the shared metadata database (MongoDB): this "
            "deployment keeps jobs without it, so nothing could run or be claimed once a day. "
            "Use the admin purge instead."
        )


def retention_schedule_status() -> dict[str, Any]:
    """What ``GET /workspace/settings`` says about the automatic run."""
    enabled = auto_purge_enabled()
    status: dict[str, Any] = {"scheduled": enabled, "interval_hours": int(RUN_INTERVAL.total_seconds() // 3600)}
    if not enabled:
        return status
    from services.maintenance_claim import claim_status

    try:
        doc = claim_status(CLAIM_ID)
    except Exception as exc:  # noqa: BLE001 — status is informational
        logger.debug("retention claim unreadable: %s", exc)
        doc = {}
    if not doc:
        status["warning"] = "no shared metadata database: the automatic purge cannot run"
    for key in ("claimed_at", "next_due_at"):
        value = doc.get(key)
        status["last_run_at" if key == "claimed_at" else "next_run_at"] = (
            value.isoformat() if isinstance(value, datetime) else None
        )
    return status


def run_due_retention(*, now: datetime | None = None, force: bool = False) -> dict[str, Any] | None:
    """Run the scheduled purge if it is enabled and due. ``None`` when it did not run."""
    global _warned_no_store
    from services.job_retention import purge_expired_jobs
    from services.maintenance_claim import claim_run
    from services.team_store import list_workspaces
    from services.workspace_store import SettingsUnavailable, get_workspace_settings

    # One read of the shared settings for the flag and the window; a store that
    # is configured but unreachable skips the run — never a node-local copy.
    try:
        settings = get_workspace_settings()
    except SettingsUnavailable as exc:
        logger.warning("Scheduled job retention skipped: %s", exc)
        return None
    if not settings.get("retention_auto_purge"):
        return None
    current = now or datetime.now(timezone.utc)
    claimed = claim_run(CLAIM_ID, RUN_INTERVAL, now=current, force=force)
    if claimed is None:
        # File-backed deployment (or the database is down): degrade cleanly,
        # say so once per process — GET /settings carries the warning too.
        if not _warned_no_store:
            logger.warning("Scheduled job retention is on but no shared metadata database is reachable; skipped")
            _warned_no_store = True
        return None
    _warned_no_store = False
    if not claimed:
        return None
    retention_days = int(settings["retention_days"])
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
