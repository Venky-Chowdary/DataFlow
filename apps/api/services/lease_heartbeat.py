"""Keep a worker lease alive for the life of the transfer that holds it.

The fleet claim loop used to acquire a 60s lease and never extend it. After
two minutes ``reclaim_stale_claims`` requeued the same job, and the second
start overwrite-dropped the table the first writer was still filling. The
scheduler path already heartbeats. This is that same algorithm, so a batch
that runs longer than the lease TTL stays one writer.
"""

from __future__ import annotations

import logging
import threading
from typing import Callable

from services.brand_env import getenv_brand
from services.worker_leases import WorkerLeaseStore

_logger = logging.getLogger(__name__)


def lease_ttl_seconds() -> int:
    try:
        return max(15, int(getenv_brand("WORKER_LEASE_TTL", "60") or "60"))
    except ValueError:
        return 60


def start_lease_heartbeat(
    store: WorkerLeaseStore,
    job_id: str,
    *,
    ttl_seconds: int | None = None,
    interval_seconds: float | None = None,
    on_lost: Callable[[], None] | None = None,
) -> Callable[[], None]:
    """Extend ``job_id``'s lease until the returned stop function runs.

    ``on_lost`` runs once when a heartbeat is refused (the lease expired or
    another worker took the fence). The writer must treat that as cancel.
    """
    ttl = int(ttl_seconds if ttl_seconds is not None else lease_ttl_seconds())
    interval = float(interval_seconds if interval_seconds is not None else max(5, ttl // 2))
    stop_event = threading.Event()

    def _beat() -> None:
        # Extend once before sleeping. The claim TTL is the same length as
        # this interval when the interval is half the TTL, so waiting first
        # lets reclaim start a second writer before the first beat lands.
        while not stop_event.is_set():
            if store.heartbeat(job_id, ttl_seconds=ttl):
                if stop_event.wait(interval):
                    break
                continue
            _logger.warning("Lease heartbeat failed for job %s", job_id)
            if on_lost is not None:
                try:
                    on_lost()
                except Exception:  # noqa: BLE001 - lease loss must still stop the beater
                    _logger.exception("Lease-loss hook failed for job %s", job_id)
            break

    thread = threading.Thread(target=_beat, name=f"df-lease-{job_id}", daemon=True)
    thread.start()

    def _stop() -> None:
        stop_event.set()
        thread.join(timeout=max(1.0, interval))

    return _stop
