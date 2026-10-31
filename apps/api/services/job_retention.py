"""Job history retention purge (QA enterprise #4, round 8).

``retention_days`` on General settings used to be stored and never applied
(the page said so). A workspace admin can now purge finished jobs older than
that many days from the job store, for their own workspace only:

* a **dry run** comes first: it reports how many jobs would go (with a few
  ids) and returns a short-lived ``confirm_token`` bound to the workspace and
  the cutoff;
* the purge applies only with that token, and deletes only terminal jobs
  (completed / failed / cancelled ...) whose finish time is before the cutoff
  — a running, queued or paused job is never touched;
* both steps are audited (``workspace.jobs.retention.purge``).

Nothing runs on a schedule: jobs are removed only when an admin applies a
purge. Only the ``transfer_jobs`` documents are removed.
"""

from __future__ import annotations

import hashlib
import hmac
import time
from datetime import datetime, timedelta, timezone
from typing import Any

TOKEN_TTL_SEC = 15 * 60
_SAMPLE = 10


class RetentionTokenInvalid(ValueError):
    pass


def _secret() -> bytes:
    from services.audit_log import _platform_hmac_secret

    return _platform_hmac_secret()


def _token(workspace_id: str, cutoff: str, issued: int) -> str:
    msg = f"job-retention|{workspace_id}|{cutoff}|{issued}".encode()
    return f"{issued}.{hmac.new(_secret(), msg, hashlib.sha256).hexdigest()[:32]}"


def _check_token(token: str, workspace_id: str, cutoff: str, now: int) -> None:
    try:
        issued_s, _sig = token.split(".", 1)
        issued = int(issued_s)
    except (ValueError, AttributeError) as exc:
        raise RetentionTokenInvalid("Run a dry run first; its confirm_token is required") from exc
    if now - issued > TOKEN_TTL_SEC:
        raise RetentionTokenInvalid("The dry run expired; run it again")
    if not hmac.compare_digest(_token(workspace_id, cutoff, issued), token):
        raise RetentionTokenInvalid("confirm_token does not match this workspace and retention window")


def _query(workspace_id: str, cutoff_dt: datetime) -> dict[str, Any]:
    from services.job_status import TERMINAL_STATUSES

    cutoff_iso = cutoff_dt.isoformat()
    finished_before = {"$or": [
        {"completed_at": {"$lt": cutoff_dt}},
        {"completed_at": {"$lt": cutoff_iso}},
        {"completed_at": None, "created_at": {"$lt": cutoff_dt}},
        {"completed_at": None, "created_at": {"$lt": cutoff_iso}},
    ]}
    return {"$and": [
        {"workspace_id": workspace_id},
        {"status": {"$in": sorted(TERMINAL_STATUSES)}},
        finished_before,
    ]}


def purge_expired_jobs(
    *,
    workspace_id: str,
    retention_days: int,
    dry_run: bool = True,
    confirm_token: str = "",
    now: datetime | None = None,
) -> dict[str, Any]:
    from services.mongodb_service import get_mongodb_service

    if not workspace_id:
        raise ValueError("A job purge needs a workspace")
    current = now or datetime.now(timezone.utc)
    # Day granularity so a dry run and its apply agree on the window.
    cutoff_dt = (current - timedelta(days=int(retention_days))).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    cutoff = cutoff_dt.isoformat()
    coll = get_mongodb_service().get_database()["transfer_jobs"]
    query = _query(workspace_id, cutoff_dt)
    result: dict[str, Any] = {
        "workspace_id": workspace_id,
        "retention_days": int(retention_days),
        "cutoff": cutoff,
        "dry_run": dry_run,
        "scheduled": False,
    }
    if dry_run:
        ids = [str(d["_id"]) for d in coll.find(query, {"_id": 1}).limit(_SAMPLE)]
        result["would_remove"] = coll.count_documents(query)
        result["sample_job_ids"] = ids
        result["confirm_token"] = _token(workspace_id, cutoff, int(time.time()))
        result["confirm_expires_in_sec"] = TOKEN_TTL_SEC
        return result
    _check_token(confirm_token, workspace_id, cutoff, int(time.time()))
    result["removed"] = int(coll.delete_many(query).deleted_count)
    return result
