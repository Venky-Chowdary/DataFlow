"""Phase F5 — scheduler mode SSOT (local vs distributed claim queue).

Modes
-----
* ``local`` — API ThreadPoolExecutor + worker_leases only (single-replica / demo).
* ``claim`` — enqueue to Mongo ``transfer_job_queue``; workers (or API claim loop)
  pull under leases. Multi-replica safe.
* ``auto`` — ``claim`` when ``requires_distributed_backend()``, else ``local``.

``DATAFLOW_WORKER_FLEET=1/0`` remains a forced override for claim/local.
"""

from __future__ import annotations

from services.brand_env import getenv_brand
from services.worker_leases import requires_distributed_backend


def scheduler_mode() -> str:
    """Return ``local`` or ``claim`` (never ``auto`` — resolved)."""
    fleet = (getenv_brand("WORKER_FLEET", "") or "").strip().lower()
    if fleet in ("0", "false", "no", "off"):
        return "local"
    if fleet in ("1", "true", "yes", "on"):
        return "claim"

    mode = (getenv_brand("SCHEDULER_MODE", "auto") or "auto").strip().lower()
    if mode in ("local", "inprocess", "legacy", "api"):
        return "local"
    if mode in ("claim", "fleet", "distributed", "queue"):
        return "claim"
    # auto
    return "claim" if requires_distributed_backend() else "local"


def claim_queue_enabled() -> bool:
    """True when transfers must enqueue to the durable job queue."""
    return scheduler_mode() == "claim"


def api_claim_loop_enabled() -> bool:
    """Whether API process should also pull from the claim queue.

    Default ON in claim mode so multi-replica API deployments do not strand
    jobs when a dedicated Worker service is not yet deployed. Set
    ``DATAFLOW_API_CLAIM_LOOP=0`` when only ``src.worker_main`` should execute.
    """
    if not claim_queue_enabled():
        return False
    raw = (getenv_brand("API_CLAIM_LOOP", "1") or "1").strip().lower()
    if raw in ("0", "false", "no", "off"):
        return False
    return True
