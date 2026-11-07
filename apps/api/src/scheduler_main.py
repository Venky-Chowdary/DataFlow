#!/usr/bin/env python3
"""Pipeline cadence process. Same image as the API, different command.

    python -m src.scheduler_main

Enqueues due pipelines onto ``transfer_job_queue``. Does not serve HTTP and
does not execute a transfer. A second replica may stand by on the Mongo
scheduler lock; it must not grow its own executor.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys

from services.brand_env import getenv_brand


def _force_scheduler_env() -> None:
    """This command is the scheduler, even if a parent shell left API env behind."""
    os.environ["DATAFLOW_PROCESS_ROLE"] = "scheduler"
    os.environ["DATAFLOW_SCHEDULE_LOOP"] = "1"
    os.environ["DATAFLOW_API_CLAIM_LOOP"] = "0"
    # A due pipeline calls run_transfer_async. Fleet on means enqueue, not a
    # thread-pool execute inside the scheduler.
    os.environ["DATAFLOW_WORKER_FLEET"] = "1"


def main() -> int:
    _force_scheduler_env()
    try:
        from services.logging_config import configure_logging

        configure_logging()
    except Exception:  # pragma: no cover - logging must never block the beat
        logging.basicConfig(
            level=getenv_brand("LOG_LEVEL", "INFO"),
            format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        )
    log = logging.getLogger("dataflow.scheduler")

    from services.process_role import topology_errors

    errors = topology_errors()
    if errors:
        for msg in errors:
            log.error("topology: %s", msg)
        return 2

    from services.schedule_runner import run_schedule_loop
    from services.schedule_store import import_file_schedules_into_mongo

    try:
        imported = import_file_schedules_into_mongo()
        if imported:
            log.info("Imported %s pipeline schedule(s) from schedules.json", imported)
    except Exception:
        log.exception("Schedule file import failed; the loop will retry")

    log.info("Datawrap scheduler starting (cadence only, no transfer execution)")
    try:
        asyncio.run(run_schedule_loop())
    except KeyboardInterrupt:
        log.info("Scheduler interrupted — shutting down")
    return 0


if __name__ == "__main__":
    sys.exit(main())
