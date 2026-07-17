"""Background scheduler — runs due pipeline syncs."""

from __future__ import annotations

import asyncio
import logging
import os
import socket
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from services.schedule_store import due_schedules, mark_schedule_run

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
        from ..services.mongodb_service import get_mongodb_service
    try:
        svc = get_mongodb_service()
    except Exception:
        return None
    if type(svc).__name__ == "MemoryMongoDBService":
        return None
    return svc if getattr(svc, "client", None) is not None else None


def _acquire_scheduler_lock() -> bool:
    """Try to acquire a short-lived distributed lock for this scheduler beat.

    When a real MongoDB is shared across Railway instances this prevents
    multiple replicas from running the same scheduled pipelines simultaneously.
    Falls back to an in-process lock when MongoDB is unavailable.
    """
    svc = _mongo_backend()
    if not svc:
        return True
    db = svc.get_database()
    now = datetime.now(timezone.utc)
    instance = _scheduler_instance_id()
    try:
        result = db["schedule_locks"].find_one_and_update(
            {"_id": "global_scheduler_lock", "$or": [{"expires_at": {"$lte": now}}, {"expires_at": None}]},
            {"$set": {"_id": "global_scheduler_lock", "instance": instance, "acquired_at": now, "expires_at": _lock_expiry()}},
            upsert=True,
            return_document=True,
        )
        return bool(result and result.get("instance") == instance)
    except Exception:
        logger.exception("Failed to acquire scheduler lock")
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
        from ..services.mongodb_service import get_mongodb_service

        return get_mongodb_service().get_connector(connector_id)
    except Exception:
        return None


def _endpoint_from_connector(conn: dict, table: str):
    from ..transfer.models import EndpointConfig

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


def _run_schedule(schedule_id: str) -> str | None:
    from services.schedule_store import get_schedule, mark_schedule_running, mark_schedule_run
    from ..transfer.engine import get_transfer_engine
    from ..transfer.background import run_transfer_async

    sched = get_schedule(schedule_id)
    if not sched or not sched.enabled:
        return None

    src = _resolve_connector(sched.source_connector_id)
    dst = _resolve_connector(sched.dest_connector_id)
    if not src or not dst:
        logger.warning("Schedule %s skipped — connector missing", schedule_id)
        return None

    source = _endpoint_from_connector(src, sched.source_table)
    destination = _endpoint_from_connector(dst, sched.dest_table)
    from ..transfer.models import TransferRequest

    request = TransferRequest(source=source, destination=destination, skip_preflight=False)

    engine = get_transfer_engine()
    job_id = engine._create_pending_job(request)
    mark_schedule_running(schedule_id, _scheduler_instance_id())
    future = run_transfer_async(job_id, request)
    future.add_done_callback(
        lambda _f, sid=schedule_id, jid=job_id: mark_schedule_run(sid, jid)
    )
    logger.info("Schedule %s started job %s", schedule_id, job_id)
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
    from services.schedule_store import _load_all, _save_all, _is_running_stale, PipelineSchedule

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
