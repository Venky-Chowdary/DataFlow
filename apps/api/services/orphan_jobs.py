"""Settle transfer jobs whose worker died (service restart or worker kill).

A job is an orphan when every owner signal is gone at once:

* status is one a live worker owns (``pending``/``queued``/``running``/``retrying``);
* no worker holds its lease (the lease heartbeats every half TTL while the
  transfer runs, and from submit while it waits for a pool thread);
* this process is not running it;
* no fleet queue row is waiting for or holding a claim (``reclaim_stale_claims``
  owns those);
* the document has not been written for at least the grace period.

An orphan is resumed from its checkpoint up to ``JOB_ORPHAN_MAX_RECLAIMS``
times. One that asked to be cancelled is recorded cancelled. One with no
request to resume, a file source whose bytes are gone, or no reclaims left is
recorded failed with the rows it had already committed. Rows a dead worker
committed are not deleted; the message says so. Every status write is
conditional on the status that was read, so two replicas sweeping together
cannot both settle one job.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from services.brand_env import getenv_brand

_logger = logging.getLogger(__name__)

ORPHAN_ERROR_CODE = "job_orphaned"

_sweeper_stop: threading.Event | None = None
_sweeper_thread: threading.Thread | None = None


@dataclass(frozen=True)
class OrphanDisposition:
    job_id: str
    action: str
    reason: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


def _int_env(name: str, default: int, *, floor: int) -> int:
    try:
        return max(floor, int(getenv_brand(name, str(default)) or default))
    except ValueError:
        return default


def orphan_grace_seconds() -> int:
    from services.lease_heartbeat import lease_ttl_seconds

    return _int_env("JOB_ORPHAN_GRACE_SEC", lease_ttl_seconds(), floor=0)


def orphan_max_reclaims() -> int:
    return _int_env("JOB_ORPHAN_MAX_RECLAIMS", 2, floor=0)


def _as_utc(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _idle_seconds(job: dict[str, Any], now: datetime) -> float | None:
    for key in ("updated_at", "started_at", "created_at"):
        stamp = _as_utc(job.get(key))
        if stamp is not None:
            return max(0.0, (now - stamp).total_seconds())
    return None


def _committed_rows(job: dict[str, Any]) -> int:
    candidates = [job.get("records_processed")]
    checkpoint = job.get("checkpoint")
    if isinstance(checkpoint, dict):
        candidates.append(checkpoint.get("rows_processed"))
    best = 0
    for raw in candidates:
        try:
            best = max(best, int(raw or 0))
        except (TypeError, ValueError):
            continue
    return best


def owner_signal(
    job: dict[str, Any],
    *,
    now: datetime,
    grace_seconds: int,
    lease_held: bool,
    local: bool,
    queue_status: str | None,
) -> str | None:
    """Why the job still has an owner, or None when it is an orphan."""
    from services.job_status import UNFINISHED_JOB_STATUSES

    status = str(job.get("status") or "")
    if status not in UNFINISHED_JOB_STATUSES:
        return f"status_{status or 'unknown'}"
    if local:
        return "running_in_this_process"
    if lease_held:
        return "lease_held"
    if queue_status in {"queued", "claimed"}:
        return f"queue_{queue_status}"
    idle = _idle_seconds(job, now)
    if idle is None or idle < grace_seconds:
        return "within_grace"
    return None


def _interrupted_message(job: dict[str, Any], why: str) -> str:
    rows = _committed_rows(job)
    phase = str(job.get("phase") or job.get("status") or "running")
    committed = (
        f"{rows} row(s) were committed before it stopped and were not rolled back."
        if rows
        else "No committed rows were recorded for this job."
    )
    return (
        "Interrupted: the worker running this job stopped during "
        f"{phase} (service restart or worker kill) and it was not resumed — "
        f"{why}. {committed} Start the transfer again to finish it."
    )


def _settle(
    mongo: Any,
    job: dict[str, Any],
    status: str,
    message: str,
    **fields: Any,
) -> bool:
    job_id = str(job.get("_id") or job.get("id") or "")
    prior = str(job.get("status") or "")
    try:
        return bool(
            mongo.update_job_status(
                job_id,
                status,
                only_from_status=[prior],
                message=message,
                **fields,
            )
        )
    except Exception as exc:  # noqa: BLE001
        _logger.warning("orphan settle %s -> %s failed: %s", job_id, status, exc)
        return False


def _request_for(job: dict[str, Any]) -> tuple[Any | None, str]:
    payload = job.get("transfer_request")
    if not isinstance(payload, dict) or not payload:
        return None, "the job has no stored transfer request to resume from"
    try:
        from src.transfer.models import transfer_request_from_dict

        request = transfer_request_from_dict(payload)
    except Exception as exc:  # noqa: BLE001
        return None, f"the stored transfer request could not be read ({exc})"
    try:
        from services.transfer_file_staging import (
            file_source_bytes_available,
            hydrate_file_source,
        )

        hydrate_file_source(request)
        if getattr(request.source, "kind", "") == "file" and not file_source_bytes_available(
            request
        ):
            return None, "the uploaded file is no longer staged; re-upload it"
    except Exception as exc:  # noqa: BLE001
        _logger.warning(
            "Orphan resume hydrate failed for %s: %s", job.get("_id"), exc
        )
    return request, ""


def _default_resubmit(job_id: str, request: Any, resume: bool) -> None:
    from src.transfer.background import run_transfer_async

    run_transfer_async(job_id, request, resume=resume)


def settle_orphan(
    mongo: Any,
    job: dict[str, Any],
    *,
    max_reclaims: int,
    resubmit: Callable[[str, Any, bool], None],
) -> OrphanDisposition:
    job_id = str(job.get("_id") or job.get("id") or "")
    phase = str(job.get("phase") or "").strip().lower()
    failed_at = phase if phase and phase not in {"queued", "pending", "claimed"} else "load"

    if bool(job.get("cancel_requested")):
        message = (
            "Cancelled: the worker stopped before it observed the cancel request. "
            + (
                f"{_committed_rows(job)} row(s) it had committed were not rolled back."
                if _committed_rows(job)
                else "No committed rows were recorded."
            )
        )
        ok = _settle(mongo, job, "cancelled", message, phase="cancelled", error=message)
        return OrphanDisposition(job_id, "cancelled" if ok else "skipped", "cancel_requested")

    reclaims = 0
    try:
        reclaims = max(0, int(job.get("orphan_reclaims") or 0))
    except (TypeError, ValueError):
        reclaims = 0

    request, why = _request_for(job)
    if request is not None and reclaims >= max_reclaims:
        why = (
            f"its worker stopped {reclaims + 1} time(s); the limit is "
            f"{max_reclaims} automatic resume(s)"
        )
        request = None
    if request is None:
        message = _interrupted_message(job, why)
        ok = _settle(
            mongo,
            job,
            "failed",
            message,
            phase="failed",
            error=message,
            error_code=ORPHAN_ERROR_CODE,
            failed_at_phase=failed_at,
            progress_pct=0,
        )
        return OrphanDisposition(job_id, "failed" if ok else "skipped", why)

    from services.execution_engine_contract import resolve_reclaim_resume

    resume = bool(resolve_reclaim_resume(job))
    attempt = reclaims + 1
    message = (
        f"Resuming after the worker stopped (automatic resume {attempt} of "
        f"{max_reclaims}"
        + (", from the last checkpoint)" if resume else ", from the start)")
    )
    if not _settle(
        mongo,
        job,
        "pending",
        message,
        phase="queued",
        orphan_reclaims=attempt,
        orphan_reclaimed_at=datetime.now(timezone.utc).isoformat(),
    ):
        return OrphanDisposition(job_id, "skipped", "status_changed")
    try:
        resubmit(job_id, request, resume)
    except Exception as exc:  # noqa: BLE001
        failed = _interrupted_message(job, f"resubmitting it failed ({exc})")
        _settle(
            mongo,
            {**job, "status": "pending"},
            "failed",
            failed,
            phase="failed",
            error=failed,
            error_code=ORPHAN_ERROR_CODE,
            failed_at_phase=failed_at,
        )
        return OrphanDisposition(job_id, "failed", "resubmit_failed")
    return OrphanDisposition(job_id, "resumed", "checkpoint" if resume else "fresh_start")


def sweep_orphan_jobs(
    *,
    mongo: Any | None = None,
    lease_store: Any | None = None,
    now: datetime | None = None,
    grace_seconds: int | None = None,
    max_reclaims: int | None = None,
    local_ids: frozenset[str] | None = None,
    queue_status: Callable[[str], str | None] | None = None,
    resubmit: Callable[[str, Any, bool], None] | None = None,
    limit: int = 500,
) -> list[dict[str, str]]:
    """Settle every orphaned job once. Returns the dispositions taken."""
    if mongo is None:
        from services.mongodb_service import get_mongodb_service

        mongo = get_mongodb_service()
    if lease_store is None:
        from services.worker_leases import get_worker_lease_store

        lease_store = get_worker_lease_store()
    if local_ids is None:
        from services.transfer_scheduler import local_job_ids

        local_ids = local_job_ids()
    if queue_status is None:
        from services.worker_fleet import queue_row_status

        queue_status = queue_row_status
    clock = now or datetime.now(timezone.utc)
    grace = orphan_grace_seconds() if grace_seconds is None else max(0, int(grace_seconds))
    cap = orphan_max_reclaims() if max_reclaims is None else max(0, int(max_reclaims))
    submit = resubmit or _default_resubmit

    try:
        jobs = mongo.list_unfinished_jobs(limit=limit)
    except Exception as exc:  # noqa: BLE001
        _logger.warning("orphan sweep could not list jobs: %s", exc)
        return []

    out: list[dict[str, str]] = []
    for job in jobs:
        job_id = str(job.get("_id") or job.get("id") or "")
        if not job_id:
            continue
        try:
            held = bool(lease_store.is_held(job_id))
        except Exception as exc:  # noqa: BLE001
            _logger.debug("lease check failed for %s: %s", job_id, exc)
            held = True
        owner = owner_signal(
            job,
            now=clock,
            grace_seconds=grace,
            lease_held=held,
            local=job_id in local_ids,
            queue_status=queue_status(job_id),
        )
        if owner is not None:
            continue
        try:
            disposition = settle_orphan(mongo, job, max_reclaims=cap, resubmit=submit)
        except Exception as exc:  # noqa: BLE001
            _logger.exception("orphan settle failed for %s", job_id)
            disposition = OrphanDisposition(job_id, "skipped", f"error: {exc}")
        if disposition.action != "skipped":
            _logger.warning(
                "Orphaned job %s %s (%s)",
                job_id,
                disposition.action,
                disposition.reason,
            )
        out.append(disposition.to_dict())
    return out


def start_orphan_sweeper(*, interval_seconds: float | None = None) -> bool:
    """Run :func:`sweep_orphan_jobs` on a daemon thread until stopped."""
    global _sweeper_stop, _sweeper_thread
    if _sweeper_thread is not None and _sweeper_thread.is_alive():
        return True
    try:
        interval = float(
            interval_seconds
            if interval_seconds is not None
            else (getenv_brand("JOB_ORPHAN_SWEEP_SEC", "30") or "30")
        )
    except ValueError:
        interval = 30.0
    interval = max(5.0, interval)
    stop = threading.Event()
    _sweeper_stop = stop

    def _run() -> None:
        while not stop.is_set():
            try:
                sweep_orphan_jobs()
            except Exception:  # noqa: BLE001
                _logger.exception("orphan sweep failed")
            stop.wait(interval)

    _sweeper_thread = threading.Thread(target=_run, name="df-orphan-sweep", daemon=True)
    _sweeper_thread.start()
    return True


def stop_orphan_sweeper() -> None:
    global _sweeper_stop, _sweeper_thread
    if _sweeper_stop is not None:
        _sweeper_stop.set()
    _sweeper_stop = None
    _sweeper_thread = None
