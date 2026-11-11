"""Parity Studio: the per-job state a person changes, stored on the job document.

Everything a person decides — approving, dismissing or accepting a fix, the
re-check rounds, the explanation they asked for, sign-off approvals and the
signature — lives under the job's ``parity`` field. ``version`` goes up by one
on every change, so a client can tell a stale report from the current one.

Every write is a compare-and-swap on ``parity.version`` through the job
store's ``update_job_fields(..., expected=...)`` — one atomic ``update_one``
on MongoDB, the same check under the store's lock in memory — because the API
runs as several replicas and a process lock serializes nothing across them.
:func:`update_state` re-reads and re-applies a change when another writer got
there first, or answers 409 when the caller pinned the version it decided on.
Every change also writes an audit-chain event.
"""

from __future__ import annotations

import copy
import logging
from datetime import datetime, timezone
from typing import Any, Callable, TypeVar

logger = logging.getLogger(__name__)

DEFAULT_REQUIRED_ROLES: tuple[str, ...] = ("engineering", "data_owner", "compliance")
#: Applied write-path rounds per job; after this, write fixes are disabled.
MAX_APPLIED_ROUNDS = 3
#: Read-modify-write attempts before a contended change gives up with 409.
CAS_ATTEMPTS = 4

T = TypeVar("T")


class ParityError(Exception):
    status_code = 400


class ParityNotFound(ParityError):
    status_code = 404


class ParityConflict(ParityError):
    status_code = 409


class ParityStale(ParityConflict):
    """The parity state changed since the caller read it (another replica or person)."""


class ParityInvalid(ParityError):
    status_code = 422


class ParityForbidden(ParityError):
    status_code = 403


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def job_store() -> Any:
    """The job store (patched in tests that run the engine on a fake store)."""
    from services.mongodb_service import get_mongodb_service

    return get_mongodb_service()


def load_job(job_id: str) -> dict[str, Any]:
    job = job_store().get_job(job_id)
    if not job:
        raise ParityNotFound("Job not found")
    return job


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


def version_of(job: dict[str, Any]) -> int:
    stored = job.get("parity") if isinstance(job.get("parity"), dict) else {}
    try:
        return int(stored.get("version") or 0)
    except (TypeError, ValueError):
        return 0


def version_guard(version: int) -> dict[str, Any]:
    """The ``expected`` spec for "the state is still at ``version``".

    A job nobody decided anything on has no ``parity.version`` yet (or 0).
    """
    return {"parity.version": int(version)} if version else {"parity.version": (0, None)}


def stale(current: int, expected: int) -> ParityStale:
    return ParityStale(
        f"The parity report changed since it was read (version {expected}, now {current}). "
        "Reload it and decide again."
    )


def check_version(job: dict[str, Any], expected_version: int | None) -> None:
    """409 when the caller decided on another version than the stored one."""
    if expected_version is not None and version_of(job) != int(expected_version):
        raise stale(version_of(job), int(expected_version))


def write_state(job_id: str, state: dict[str, Any], *, expected_version: int) -> None:
    """Compare-and-swap ``state`` onto the job: written only while still at ``expected_version``.

    Bumps ``state["version"]``. Raises :class:`ParityStale` when another writer
    changed the state first (nothing is written), :class:`ParityNotFound` when
    the job is gone.
    """
    state["version"] = int(expected_version) + 1
    ok = job_store().update_job_fields(
        job_id, {"parity": state}, touch_updated_at=False, expected=version_guard(expected_version)
    )
    if not ok:
        job = job_store().get_job(job_id)
        if not job:
            raise ParityNotFound("Job not found")
        raise stale(version_of(job), int(expected_version))


def update_state(
    job_id: str,
    change: Callable[[dict[str, Any], dict[str, Any]], T],
    *,
    expected_version: int | None = None,
    attempts: int = CAS_ATTEMPTS,
) -> tuple[dict[str, Any], dict[str, Any], T]:
    """Read the job, apply ``change(job, state)``, compare-and-swap it back.

    ``change`` mutates the state in place and runs on the freshest state of
    each attempt, so every check it makes (a fix is still proposed, the round
    limit, a role signs once) holds at the moment of the write. With
    ``expected_version`` the caller decided on that version: any other one is
    a 409 and nothing is retried. Without it a lost race is retried on the
    re-read state. Returns ``(job with the new state, state, change's value)``.
    """
    last: ParityStale | None = None
    for _attempt in range(max(1, attempts)):
        job = load_job(job_id)
        check_version(job, expected_version)
        state = state_of(job)
        current = int(state.get("version") or 0)
        result = change(job, state)
        try:
            write_state(job_id, state, expected_version=current)
        except ParityStale as exc:
            if expected_version is not None:
                raise
            last = exc
            continue
        return {**job, "parity": state}, state, result
    raise last or ParityStale("The parity report kept changing; reload it and try again.")


def store_cache(job_id: str, fields: dict[str, Any], *, version: int) -> bool:
    """Write derived ``parity.*`` cache fields (no version bump) only while the
    state is still at ``version``: a reader computing from an older snapshot
    never overwrites what a decision wrote since. Returns whether it was written.
    """
    if not fields:
        return False
    return bool(job_store().update_job_fields(
        job_id, {f"parity.{k}": v for k, v in fields.items()}, touch_updated_at=False,
        expected=version_guard(version),
    ))


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
