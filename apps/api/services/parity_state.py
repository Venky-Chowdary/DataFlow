"""Parity Studio: the per-job state a person changes, stored on the job document.

Everything a person decides — approving, dismissing or accepting a fix, the
re-check rounds, the explanation they asked for, sign-off approvals and the
signature — lives under the job's ``parity`` field, written through the job
store's ``update_job_fields``. ``version`` goes up by one on every change, so a
client can tell a stale report from the current one. Writes for one job are
serialized in-process; every change also writes an audit-chain event.
"""

from __future__ import annotations

import copy
import logging
import threading
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_REQUIRED_ROLES: tuple[str, ...] = ("engineering", "data_owner", "compliance")
#: Applied write-path rounds per job; after this, write fixes are disabled.
MAX_APPLIED_ROUNDS = 3

_LOCKS: dict[str, threading.Lock] = {}
_LOCKS_GUARD = threading.Lock()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def job_store() -> Any:
    """The job store (patched in tests that run the engine on a fake store)."""
    from services.mongodb_service import get_mongodb_service

    return get_mongodb_service()


def job_lock(job_id: str) -> threading.Lock:
    with _LOCKS_GUARD:
        lock = _LOCKS.get(job_id)
        if lock is None:
            lock = _LOCKS[job_id] = threading.Lock()
        return lock


def empty_state() -> dict[str, Any]:
    return {
        "version": 0,
        "fixes": {},
        "acceptances": {},
        "rounds": [],
        "resolved": {},
        "approvals": [],
        "required_roles": list(DEFAULT_REQUIRED_ROLES),
        "explanation": None,
        "signed": None,
        "summary": None,
    }


def state_of(job: dict[str, Any]) -> dict[str, Any]:
    """A deep copy of the job's parity state with every field present."""
    stored = job.get("parity") if isinstance(job.get("parity"), dict) else {}
    state = empty_state()
    state.update(copy.deepcopy(stored))
    for key in ("fixes", "acceptances", "resolved"):
        if not isinstance(state.get(key), dict):
            state[key] = {}
    for key in ("rounds", "approvals"):
        if not isinstance(state.get(key), list):
            state[key] = []
    if not state.get("required_roles"):
        state["required_roles"] = list(DEFAULT_REQUIRED_ROLES)
    return state


def save_state(job_id: str, job: dict[str, Any], state: dict[str, Any], *, bump: bool = True) -> dict[str, Any]:
    """Persist ``state`` on the job (bumping ``version``) and return the updated job."""
    if bump:
        state["version"] = int(state.get("version") or 0) + 1
    if not job_store().update_job_fields(job_id, {"parity": state}, touch_updated_at=False):
        raise RuntimeError("the job store did not accept the parity state")
    updated = dict(job)
    updated["parity"] = state
    return updated


def audit(
    action: str,
    *,
    job: dict[str, Any],
    job_id: str,
    actor: str,
    details: dict[str, Any],
    outcome: str = "succeeded",
) -> None:
    """One audit-chain event per Parity Studio decision (``job.parity.*``)."""
    from services.audit_log import append_audit_event

    try:
        append_audit_event(
            action=action,
            resource=f"job:{job_id}",
            actor=actor or "anonymous",
            level="info",
            details=details,
            workspace_id=str(job.get("workspace_id") or "") or None,
            outcome=outcome,
        )
    except Exception as exc:  # noqa: BLE001 - any store fault; the decision stands
        # The decision is already stored; a failed audit append is reported,
        # not allowed to reverse it silently.
        logger.error("parity audit %s failed (%s)", action, type(exc).__name__, exc_info=exc)
