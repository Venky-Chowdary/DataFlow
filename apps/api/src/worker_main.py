#!/usr/bin/env python3
"""Dedicated transfer worker process for Railway (data plane).

Run with the same API image, different start command:

    DATAFLOW_WORKER_FLEET=1 python -m src.worker_main

The API service enqueues job ids into Mongo ``transfer_job_queue``; this process
claims them under a worker lease and executes ``UniversalTransferEngine``.
"""

from __future__ import annotations

import logging
import os
import sys


def main() -> int:
    logging.basicConfig(
        level=os.getenv("DATAFLOW_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    log = logging.getLogger("dataflow.worker")

    # Ensure fleet mode is on for worker processes.
    os.environ.setdefault("DATAFLOW_WORKER_FLEET", "1")

    from services.worker_fleet import fleet_enabled, run_fleet_loop
    from src.transfer.background import run_fleet_job

    if not fleet_enabled():
        log.error("DATAFLOW_WORKER_FLEET must be enabled for the worker process")
        return 2

    log.info(
        "DataFlow worker starting (worker_id=%s)",
        os.getenv("HOSTNAME") or os.getenv("RAILWAY_REPLICA_ID") or "local",
    )
    try:
        run_fleet_loop(run_fleet_job, poll_seconds=float(os.getenv("DATAFLOW_WORKER_POLL", "2")))
    except KeyboardInterrupt:
        log.info("Worker interrupted — shutting down")
    return 0


if __name__ == "__main__":
    sys.exit(main())
