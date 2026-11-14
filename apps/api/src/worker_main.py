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
import signal
import sys
import threading

from services.brand_env import getenv_brand, setenv_brand


def _force_worker_env() -> None:
    """This command claims jobs. It does not own cadence or the API claim loop.

    ``WORKER_MODE`` is left alone so the chart can run one batch Deployment
    and one CDC Deployment from the same module.
    """
    # Both names: a DATAWRAP_ value left in a shared env block must not win
    # over the entrypoint (the reader prefers DATAWRAP_).
    setenv_brand("PROCESS_ROLE", "worker")
    setenv_brand("WORKER_FLEET", "1")
    setenv_brand("SCHEDULE_LOOP", "0")
    setenv_brand("API_CLAIM_LOOP", "0")


def install_stop_signals(
    stop: threading.Event, log: logging.Logger, *, what: str, grace_seconds: float
) -> None:
    """SIGTERM / SIGINT set ``stop``; the loop then drains and returns.

    Kubernetes sends SIGTERM and waits ``terminationGracePeriodSeconds`` before
    SIGKILL. A second signal, or a loop that has not returned a few seconds
    after its grace period, ends the process at once: the leases it still
    holds expire and the jobs are reclaimed, as after any crash.
    """

    def _hard_exit() -> None:
        log.error("%s did not stop %.0fs after the drain period — exiting", what, _HARD_EXIT_MARGIN)
        logging.shutdown()
        os._exit(1)

    def _on_signal(signum: int, _frame: object) -> None:
        if stop.is_set():
            log.warning("%s received signal %s again — exiting now", what, signum)
            logging.shutdown()
            os._exit(1)
        log.info(
            "%s received signal %s — no new work; draining for up to %.0fs", what, signum, grace_seconds
        )
        stop.set()
        timer = threading.Timer(grace_seconds + _HARD_EXIT_MARGIN, _hard_exit)
        timer.daemon = True
        timer.start()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, _on_signal)
        except (ValueError, OSError):  # not the main thread (embedded use): nothing to install
            log.debug("signal %s handler not installed", sig)


#: After the drain period, how long the loop gets to return before the process exits.
_HARD_EXIT_MARGIN = 5.0


def main() -> int:
    _force_worker_env()
    # Same structured configuration as the API process, so a job that starts in
    # the API and executes in the worker fleet produces one correlatable log
    # stream instead of two differently-shaped ones.
    try:
        from services.logging_config import configure_logging

        configure_logging()
    except Exception:  # pragma: no cover - never let logging stop the worker
        logging.basicConfig(
            level=getenv_brand("LOG_LEVEL", "INFO"),
            format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        )
    log = logging.getLogger("dataflow.worker")

    from services.process_role import topology_errors, worker_workloads
    from services.worker_fleet import fleet_enabled, run_fleet_loop
    from src.transfer.background import run_fleet_job

    errors = topology_errors()
    if errors:
        for msg in errors:
            log.error("topology: %s", msg)
        return 2

    if not fleet_enabled():
        log.error("DATAFLOW_WORKER_FLEET must be enabled for the worker process")
        return 2

    log.info(
        "Datawrap worker starting (worker_id=%s workloads=%s)",
        os.getenv("HOSTNAME") or os.getenv("RAILWAY_REPLICA_ID") or "local",
        ",".join(sorted(worker_workloads())),
    )
    from services.worker_fleet import fleet_inflight_job_ids, worker_drain_seconds

    stop = threading.Event()
    grace = worker_drain_seconds()
    install_stop_signals(stop, log, what="worker", grace_seconds=grace)
    # /health for probes and an authenticated /metrics, when a port is configured (M-7).
    try:
        from src.metrics_app import start_metrics_server

        start_metrics_server(
            role="worker",
            health=lambda: {"draining": stop.is_set(), "inflight": len(fleet_inflight_job_ids())},
        )
    except Exception:  # noqa: BLE001 - metrics must never stop a worker from claiming
        log.exception("Metrics endpoint failed to start; the worker continues without it")
    try:
        run_fleet_loop(
            run_fleet_job,
            poll_seconds=float(getenv_brand("WORKER_POLL", "2")),
            stop_event=stop,
            drain_seconds=grace,
        )
    except KeyboardInterrupt:
        log.info("Worker interrupted — shutting down")
    left = sorted(fleet_inflight_job_ids())
    if left:
        # A writer that did not reach a batch boundary in the grace period.
        # Its lease is still held and expires once this process is gone, so
        # another worker reclaims it then — never while this one might write.
        log.warning(
            "Worker exiting with %d transfer(s) still running after the %.0fs drain: %s",
            len(left), grace, ", ".join(left),
        )
        logging.shutdown()
        os._exit(0)
    log.info("Worker drained — exiting")
    return 0


if __name__ == "__main__":
    sys.exit(main())
