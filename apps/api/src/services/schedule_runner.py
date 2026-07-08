"""Background scheduler — runs due pipeline syncs."""

from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from services.schedule_store import due_schedules, mark_schedule_run

logger = logging.getLogger(__name__)

_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="schedule-runner")
CHECK_INTERVAL_SECONDS = 60


def _endpoint_from_connector(conn: dict, table: str):
    from ..transfer.models import EndpointConfig

    is_mongo = conn.get("type") == "mongodb"
    return EndpointConfig(
        kind="database",
        format=conn.get("type", ""),
        connector_id=conn["_id"],
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
    from ..services.mongodb_service import get_mongodb_service
    from ..transfer.engine import get_transfer_engine
    from ..transfer.models import TransferRequest
    from ..transfer.background import run_transfer_async

    sched = get_schedule(schedule_id)
    if not sched or not sched.enabled:
        return None

    mongo = get_mongodb_service()
    src = mongo.get_connector(sched.source_connector_id)
    dst = mongo.get_connector(sched.dest_connector_id)
    if not src or not dst:
        logger.warning("Schedule %s skipped — connector missing", schedule_id)
        return None

    source = _endpoint_from_connector(src, sched.source_table)
    destination = _endpoint_from_connector(dst, sched.dest_table)
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
