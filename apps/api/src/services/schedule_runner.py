"""Background scheduler — runs due pipeline syncs."""

from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from services.schedule_store import due_schedules, mark_schedule_run

logger = logging.getLogger(__name__)

_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="schedule-runner")
CHECK_INTERVAL_SECONDS = 60


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
    from services.schedule_store import get_schedule
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
    run_transfer_async(job_id, request)
    mark_schedule_run(schedule_id, job_id)
    logger.info("Schedule %s started job %s", schedule_id, job_id)
    return job_id


def _run_due_schedules() -> int:
    started = 0
    for sched in due_schedules():
        try:
            if _run_schedule(sched.id):
                started += 1
        except Exception:
            logger.exception("Failed to run schedule %s", sched.id)
    return started


async def run_schedule_loop() -> None:
    """Poll for due schedules and enqueue transfers."""
    logger.info("Pipeline scheduler started (interval=%ss)", CHECK_INTERVAL_SECONDS)
    while True:
        try:
            count = await asyncio.get_event_loop().run_in_executor(_executor, _run_due_schedules)
            if count:
                logger.info("Scheduler started %s pipeline run(s)", count)
        except Exception:
            logger.exception("Schedule loop error")
        await asyncio.sleep(CHECK_INTERVAL_SECONDS)
