"""Background scheduler — runs due pipeline syncs."""

from __future__ import annotations

import asyncio
import logging
import os
import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from services.schedule_store import due_schedules

logger = logging.getLogger(__name__)

_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="schedule-runner")
CHECK_INTERVAL_SECONDS = 60
LOCK_TTL_SECONDS = int(os.getenv("DATAFLOW_SCHEDULER_LOCK_TTL", "300"))


def _scheduler_instance_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}"


def _lock_expiry() -> datetime:
    return datetime.now(timezone.utc) + timedelta(seconds=LOCK_TTL_SECONDS)


def _mongo_backend():
    try:
        from services.mongodb_service import get_mongodb_service
    except ImportError:
        from services.mongodb_service import get_mongodb_service
    try:
        svc = get_mongodb_service()
    except Exception:
        return None
    if type(svc).__name__ == "MemoryMongoDBService":
        return None
    return svc if getattr(svc, "client", None) is not None else None


def _acquire_scheduler_lock() -> bool:
    """Try to acquire a short-lived distributed lock for this scheduler beat.

    When a real MongoDB is shared across replicas this prevents duplicate runs.
    When multi-replica coordination is required and Mongo is unavailable, fail
    closed (return False). Single-instance / memory mode may proceed without a
    shared lock.
    """
    from services.worker_leases import requires_distributed_backend

    svc = _mongo_backend()
    if not svc:
        if requires_distributed_backend():
            logger.error("Scheduler lock unavailable; refuse beat (multi-replica fail-closed)")
            return False
        return True
    db = svc.get_database()
    now = datetime.now(timezone.utc)
    instance = _scheduler_instance_id()
    try:
        result = db["schedule_locks"].find_one_and_update(
            {
                "_id": "global_scheduler_lock",
                "$or": [
                    {"expires_at": {"$lte": now}},
                    {"expires_at": None},
                    {"instance": instance},
                ],
            },
            {
                "$set": {
                    "instance": instance,
                    "acquired_at": now,
                    "expires_at": _lock_expiry(),
                },
                "$setOnInsert": {"_id": "global_scheduler_lock"},
            },
            upsert=True,
            return_document=True,
        )
        return bool(result and result.get("instance") == instance)
    except Exception as exc:
        name = type(exc).__name__
        if "DuplicateKey" in name:
            return False
        logger.exception("Failed to acquire scheduler lock")
        if requires_distributed_backend():
            return False
        return False


def _release_scheduler_lock() -> None:
    svc = _mongo_backend()
    if not svc:
        return
    try:
        svc.get_database()["schedule_locks"].delete_one(
            {"_id": "global_scheduler_lock", "instance": _scheduler_instance_id()}
        )
    except Exception:
        logger.exception("Failed to release scheduler lock")


def _resolve_connector(connector_id: str) -> dict | None:
    """Load connector from file store (UUID) with MongoDB platform fallback."""
    import sys
    from pathlib import Path

    api_root = Path(__file__).resolve().parents[2]
    if str(api_root) not in sys.path:
        sys.path.insert(0, str(api_root))

    try:
        from services.connector_store import get_connector

        conn = get_connector(connector_id)
        if conn:
            data = conn.to_dict()
            data["_id"] = conn.id
            data["id"] = conn.id
            data["type"] = conn.type
            return data
    except Exception:
        pass

    try:
        from services.mongodb_service import get_mongodb_service

        return get_mongodb_service().get_connector(connector_id)
    except Exception:
        return None


def _endpoint_from_connector(conn: dict, table: str):
    from src.transfer.models import EndpointConfig

    is_mongo = conn.get("type") == "mongodb"
    connector_id = str(conn.get("_id") or conn.get("id") or "")
    return EndpointConfig(
        kind="database",
        format=conn.get("type", ""),
        connector_id=connector_id or None,
        host=conn.get("host", ""),
        port=int(conn.get("port", 0) or 0),
        database=conn.get("database", ""),
        schema=conn.get("schema", "public"),
        table=table if not is_mongo else "",
        collection=table if is_mongo else "",
        username=conn.get("username", ""),
        password=conn.get("password", ""),
        connection_string=conn.get("connection_string", ""),
        warehouse=conn.get("warehouse", ""),
    )


def _normalize_sync_mode(sync_mode: str, primary_key: str) -> str:
    """Map the schedule's coarse sync_mode onto the engine's contract vocabulary."""
    mode = (sync_mode or "full_refresh_overwrite").lower()
    if mode == "incremental":
        return "incremental_deduped" if primary_key else "incremental_append"
    if mode in ("scd2", "mirror"):
        return mode
    return mode


def build_schedule_request(sched, src: dict, dst: dict):
    """Build a :class:`TransferRequest` from a persisted schedule.

    Threads the per-schedule sync mode, validation mode, mappings, and any
    watermark/primary-key contract so scheduled runs can be incremental/CDC
    instead of always full-refresh. Backward compatible: schedules created before
    these fields existed fall back to full_refresh_overwrite / strict.
    """
    from src.transfer.models import TransferRequest

    source = _endpoint_from_connector(src, sched.source_table)
    destination = _endpoint_from_connector(dst, sched.dest_table)

    effective_mode = _normalize_sync_mode(sched.sync_mode, sched.primary_key)
    stream_contracts = list(sched.stream_contracts or [])
    if not stream_contracts and effective_mode not in ("full_refresh_overwrite", "full_refresh_append"):
        stream_contracts = [{
            "selected": True,
            "name": sched.source_table,
            "stream": sched.source_table,
            "sync_mode": effective_mode,
            "cursor_field": sched.cursor_column,
            "primary_key": sched.primary_key,
            "schema_policy": sched.schema_policy,
            "validation_mode": sched.validation_mode,
        }]

    contract_id = (getattr(sched, "contract_id", None) or "").strip()
    require_signed = bool(getattr(sched, "require_signed_contract", False))
    if contract_id or require_signed:
        from services.schedule_store import assert_signed_contract

        assert_signed_contract(contract_id, require_signed=require_signed)

    return TransferRequest(
        source=source,
        destination=destination,
        mappings=list(sched.mappings or []),
        skip_preflight=False,
        sync_mode=effective_mode,
        schema_policy=sched.schema_policy or "manual_review",
        validation_mode=sched.validation_mode or "strict",
        backfill_new_fields=bool(sched.backfill_new_fields),
        stream_contracts=stream_contracts,
        workspace_id=sched.workspace_id or "",
        contract_id=contract_id,
        enforce_contract=bool(contract_id),
        require_signed_contract=require_signed,
    )


def _run_entry(job_id: str, status: str, attempt: int, started_at: datetime, job_doc: dict | None) -> dict:
    doc = job_doc or {}
    finished = datetime.now(timezone.utc)
    return {
        "job_id": job_id,
        "status": status,
        "attempt": attempt,
        "started_at": started_at.isoformat(),
        "finished_at": finished.isoformat(),
        "duration_seconds": round((finished - started_at).total_seconds(), 3),
        "records_transferred": int(doc.get("records_processed", 0) or 0),
        "rejected_rows": int(doc.get("rejected_rows", 0) or 0),
        "coerced_null_rows": int(doc.get("coerced_null_rows", 0) or 0),
        "error": (doc.get("error") or "")[:500],
    }


def _notify_schedule(sched, job_id: str, status: str, job_doc: dict | None) -> None:
    """Deliver success/failure notifications honoring per-schedule preferences."""
    success = _is_success(status)
    if success and not sched.notify_on_success:
        return
    if not success and not sched.notify_on_failure:
        return
    try:
        from services.notification_service import (
            build_job_payload,
            log_job_notifications,
            notify_workspace,
        )
        from services.platform_config import public_url, web_url

        doc = job_doc or {}
        payload = build_job_payload(
            job_id=job_id,
            status=status,
            source=sched.source_table or sched.source_connector_id,
            destination=sched.dest_table or sched.dest_connector_id,
            records_transferred=int(doc.get("records_processed", 0) or 0),
            rejected_rows=int(doc.get("rejected_rows", 0) or 0),
            error=doc.get("error") or "",
            retry_url=f"/api/v1/connectors/jobs/{job_id}/resume",
            workspace_id=sched.workspace_id or "",
            base_url=public_url(),
            web_url=web_url(),
        )
        results = notify_workspace(sched.workspace_id or "", payload)
        log_job_notifications(job_id, results)
    except Exception:
        logger.exception("Failed to send schedule notification for %s", sched.id)


_SUCCESS_STATUSES = frozenset({"completed", "completed_with_quarantine", "success"})


def _is_success(status: str | None) -> bool:
    return (status or "") in _SUCCESS_STATUSES


def _should_retry(status: str | None, attempt: int, max_retries: int) -> bool:
    return (not _is_success(status)) and attempt < max_retries


def _job_doc(job_id: str) -> dict | None:
    try:
        from services.mongodb_service import get_mongodb_service

        return get_mongodb_service().get_job(job_id)
    except Exception:
        return None


def _finalize_run(schedule_id: str, job_id: str, attempt: int, started_at: datetime) -> None:
    """Handle a finished scheduled run: retry on failure, else record + notify."""
    from services.schedule_store import get_schedule, mark_schedule_run, record_run_history

    sched = get_schedule(schedule_id)
    if not sched:
        return
    job_doc = _job_doc(job_id)
    status = (job_doc or {}).get("status") or "failed"
    entry = _run_entry(job_id, status, attempt, started_at, job_doc)

    if _should_retry(status, attempt, sched.max_retries):
        record_run_history(schedule_id, {**entry, "retry_scheduled": True})
        delay = max(0, sched.retry_backoff_seconds) * (attempt + 1)
        logger.warning(
            "Schedule %s attempt %s failed; retrying in %ss", schedule_id, attempt + 1, delay
        )
        timer = threading.Timer(
            delay, lambda: _dispatch_transfer(schedule_id, attempt=attempt + 1)
        )
        timer.daemon = True
        timer.start()
        return

    cursor_value = None
    if _is_success(status) and sched.cursor_column:
        cursor_value = (job_doc or {}).get("cursor_value")
    mark_schedule_run(
        schedule_id, job_id, status=status, run_entry=entry, cursor_value=cursor_value
    )
    _notify_schedule(sched, job_id, status, job_doc)


def _dispatch_transfer(schedule_id: str, attempt: int = 0) -> str | None:
    """Build and submit the transfer for a schedule attempt (used for retries too)."""
    from services.schedule_store import get_schedule
    from src.transfer.background import run_transfer_async
    from src.transfer.engine import get_transfer_engine

    sched = get_schedule(schedule_id)
    if not sched or not sched.enabled:
        return None
    src = _resolve_connector(sched.source_connector_id)
    dst = _resolve_connector(sched.dest_connector_id)
    if not src or not dst:
        logger.warning("Schedule %s skipped — connector missing", schedule_id)
        return None

    try:
        request = build_schedule_request(sched, src, dst)
    except ValueError as exc:
        logger.error("Schedule %s blocked by contract policy: %s", schedule_id, exc)
        mark_schedule_run(
            schedule_id,
            "",
            status="failed",
            run_entry={
                "job_id": "",
                "status": "failed",
                "attempt": attempt,
                "started_at": datetime.now(timezone.utc).isoformat(),
                "finished_at": datetime.now(timezone.utc).isoformat(),
                "duration_seconds": 0,
                "records_transferred": 0,
                "rejected_rows": 0,
                "coerced_null_rows": 0,
                "error": str(exc)[:500],
            },
        )
        return None
    engine = get_transfer_engine()
    job_id = engine._create_pending_job(request)
    started_at = datetime.now(timezone.utc)
    future = run_transfer_async(job_id, request)
    future.add_done_callback(
        lambda _f, sid=schedule_id, jid=job_id, a=attempt, ts=started_at: _finalize_run(sid, jid, a, ts)
    )
    logger.info("Schedule %s started job %s (attempt %s)", schedule_id, job_id, attempt + 1)
    return job_id


def _run_schedule(schedule_id: str) -> str | None:
    from services.schedule_store import get_schedule, mark_schedule_running

    sched = get_schedule(schedule_id)
    if not sched or not sched.enabled:
        return None

    # Concurrency guard: refuse to start when this schedule (or another schedule
    # for the same source→dest connector pair) already has a live run in flight.
    if mark_schedule_running(schedule_id, _scheduler_instance_id()) is None:
        logger.info("Schedule %s skipped — a run is already in progress", schedule_id)
        return None

    job_id = _dispatch_transfer(schedule_id, attempt=0)
    if job_id is None:
        # Nothing was dispatched (missing connector); release the running guard.
        from services.schedule_store import clear_schedule_running

        clear_schedule_running(schedule_id)
    return job_id


def _run_due_schedules() -> int:
    if not _acquire_scheduler_lock():
        logger.debug("Scheduler lock held by another instance; skipping this beat")
        return 0
    try:
        started = 0
        for sched in due_schedules():
            try:
                if _run_schedule(sched.id):
                    started += 1
            except Exception:
                logger.exception("Failed to run schedule %s", sched.id)
        return started
    finally:
        _release_scheduler_lock()


def _clear_stale_running_schedules() -> None:
    """On startup, clear running flags left by a previous crashed instance."""
    from services.schedule_store import (
        PipelineSchedule,
        _is_running_stale,
        _load_all,
        _save_all,
    )

    schedules = _load_all()
    changed = False
    for i, s in enumerate(schedules):
        if s.running and _is_running_stale(s):
            schedules[i] = PipelineSchedule.from_dict({
                **s.to_dict(),
                "running": False,
                "running_instance": "",
                "running_started_at": None,
            })
            changed = True
    if changed:
        _save_all(schedules)


async def run_schedule_loop() -> None:
    """Poll for due schedules and enqueue transfers."""
    logger.info("Pipeline scheduler started (interval=%ss)", CHECK_INTERVAL_SECONDS)
    await asyncio.get_event_loop().run_in_executor(_executor, _clear_stale_running_schedules)
    while True:
        try:
            count = await asyncio.get_event_loop().run_in_executor(_executor, _run_due_schedules)
            if count:
                logger.info("Scheduler started %s pipeline run(s)", count)
        except Exception:
            logger.exception("Schedule loop error")
        await asyncio.sleep(CHECK_INTERVAL_SECONDS)
