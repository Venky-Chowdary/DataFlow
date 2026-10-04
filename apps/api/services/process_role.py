"""Which process this image is allowed to be.

Same image, different command. The control-plane API enqueues. The scheduler
owns cadence. Workers execute. Mapping, preflight, and the writer stay
in-process in the worker that already calls them — this module only decides
who may call them, and where acks live.
"""

from __future__ import annotations

from services.brand_env import getenv_brand
from services.scheduler_mode import api_claim_loop_enabled, claim_queue_enabled, scheduler_mode

_ROLES = frozenset({"api", "scheduler", "worker"})
_WORKLOADS = frozenset({"cdc", "batch"})


def _raw(name: str) -> str:
    return (getenv_brand(name, "") or "").strip()


def _truthy(raw: str) -> bool:
    return raw.lower() in ("1", "true", "yes", "on")


def _explicit_bit(name: str) -> str | None:
    """``"1"`` / ``"0"`` when the operator set the flag, else None (auto)."""
    raw = getenv_brand(name)
    if raw is None or str(raw).strip() == "":
        return None
    return "1" if _truthy(str(raw)) else "0"


def multi_replica() -> bool:
    return _truthy(_raw("MULTI_REPLICA"))


def process_role() -> str:
    """``api``, ``scheduler``, or ``worker``. Unknown values stay ``api``."""
    role = _raw("PROCESS_ROLE").lower() or "api"
    if role not in _ROLES:
        return "api"
    return role


def schedule_loop_enabled() -> bool:
    """Whether this process may run the pipeline cadence loop.

    ``SCHEDULE_LOOP=0/1`` is an override. Auto keeps the loop off the worker
    and on for the API and the scheduler. Multi-replica API pods must set
    the override to 0; :func:`topology_errors` refuses to boot otherwise.
    """
    override = _explicit_bit("SCHEDULE_LOOP")
    if override == "0":
        return False
    if override == "1":
        return True
    return process_role() != "worker"


def api_executes_transfers() -> bool:
    """True when this process may run a transfer (local pool or API claim loop).

    The scheduler never executes. A worker executes only inside ``worker_main``,
    not through the API pool. An API in claim mode executes only while its
    claim loop is on. With the loop off it enqueues and returns.
    """
    if process_role() in ("scheduler", "worker"):
        return False
    if not claim_queue_enabled():
        return True
    return api_claim_loop_enabled()


def orphan_resume_enabled() -> bool:
    """Re-enqueue orphans on API start.

    In claim mode ``run_transfer_async`` only upserts the queue row, so the
    scan stays on the API and does not start a local executor. Set
    ``API_ORPHAN_RESUME=0`` to skip it. Workers and the scheduler do not scan.
    """
    override = _explicit_bit("API_ORPHAN_RESUME")
    if override is not None:
        return override == "1"
    if process_role() != "api":
        return False
    return True


def ack_backend() -> str:
    """``file`` (single process) or ``mongo`` (shared confirm ledger).

    Multi-replica with the variable unset resolves to mongo so a new process
    does not silently open a pod-local file. :func:`topology_errors` still
    requires the operator to set ``ACK_BACKEND=mongo`` explicitly.
    """
    raw = _raw("ACK_BACKEND").lower()
    if raw in ("file", "mongo"):
        return raw
    if multi_replica():
        return "mongo"
    return "file"


def worker_workloads() -> frozenset[str]:
    """Workloads this worker may claim.

    Empty or ``cdc,batch`` claims both (one process, today's behavior).
    ``batch`` skips CDC. ``cdc`` claims only CDC. An unknown token claims
    both so a typo cannot drain the queue into a worker that drops every job.
    """
    raw = _raw("WORKER_MODE").lower()
    if not raw:
        return frozenset(_WORKLOADS)
    parts = {part.strip() for part in raw.split(",") if part.strip()}
    known = parts & _WORKLOADS
    if not known or known == _WORKLOADS:
        return frozenset(_WORKLOADS)
    return frozenset(known)


def workload_for_sync_mode(mode: str | None) -> str:
    """Queue workload for a transfer. CDC (including ``cdc_incremental``) is ``cdc``."""
    from services.sync_cursor import normalize_sync_mode

    if normalize_sync_mode(mode) == "cdc":
        return "cdc"
    return "batch"


def topology_errors() -> list[str]:
    """Fatal process-split mistakes when more than one replica is intended.

    Development and single-process installs leave this empty. Production
    config checks stay in :func:`services.platform_config.validate_production_config`.
    """
    if not multi_replica():
        return []

    errors: list[str] = []
    if scheduler_mode() == "local":
        errors.append(
            "DATAFLOW_MULTI_REPLICA=1 with scheduler_mode=local runs a thread pool "
            "on every API replica (DATAFLOW_WORKER_FLEET=0). Set DATAFLOW_WORKER_FLEET=1 "
            "so replicas enqueue onto transfer_job_queue."
        )

    role = process_role()
    if role == "api" and schedule_loop_enabled():
        errors.append(
            "API process must set DATAFLOW_SCHEDULE_LOOP=0 when DATAFLOW_MULTI_REPLICA=1. "
            "The scheduler process owns cadence."
        )
    if role == "api" and api_claim_loop_enabled():
        errors.append(
            "API process must set DATAFLOW_API_CLAIM_LOOP=0 when DATAFLOW_MULTI_REPLICA=1 "
            "so only worker processes execute transfers."
        )
    if role == "worker" and schedule_loop_enabled():
        errors.append(
            "Worker process must set DATAFLOW_SCHEDULE_LOOP=0. Cadence belongs to the scheduler."
        )
    if role == "scheduler" and not schedule_loop_enabled():
        errors.append(
            "Scheduler process has DATAFLOW_SCHEDULE_LOOP=0, so no process fires due pipelines."
        )

    explicit_ack = _raw("ACK_BACKEND").lower()
    if explicit_ack != "mongo":
        errors.append(
            "Set DATAFLOW_ACK_BACKEND=mongo when DATAFLOW_MULTI_REPLICA=1. "
            "A file ack ledger is pod-local, so a confirm staged on one API replica "
            "is invisible to the replica that receives the click."
        )
    return errors
