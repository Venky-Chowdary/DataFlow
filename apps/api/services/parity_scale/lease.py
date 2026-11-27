"""Who runs a job's verification: one lease on the job, renewed while the scan works.

A verification runs on a background worker. A worker that dies (restart,
crash, lost replica) must not leave the job "running" forever: the Trust
Report would say "Verification running" and keep sign-off blocked, the
schedule would wait on every tick and the watermark would stay parked.

So the worker that runs a scan holds a lease on the job document
(``parity_scale_lease``: a token, its owner and an expiry) taken with a
compare-and-swap, and renews it after every slice of work. Readers judge a
"running" block by the lease: with no live lease the verification has
**stopped** — it reads as an error with a plain reason, re-verify may take it
over, and the sweeper (:func:`services.parity_scale.verification.sweep_once`)
resumes it from its checkpoint when it can. Two replicas never scan the same
job: only the lease holder writes the block.
"""

from __future__ import annotations

import os
import socket
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

LEASE_FIELD = "parity_scale_lease"
#: A worker renews every few seconds; one statement may take up to its own
#: timeout (120 s) and a slot wait renews too, so ten minutes of silence is
#: a worker that is gone.
LEASE_TTL_S = 600
OWNER = f"{socket.gethostname()}:{os.getpid()}"
STOPPED_REASON = (
    "Verification stopped before it finished (the worker restarted or lost contact). "
    "Re-verify the table to compare every row."
)


class LeaseLost(RuntimeError):
    """Another worker holds the job's verification now: this one stops writing."""


def _service():
    from services.mongodb_service import get_mongodb_service

    return get_mongodb_service()


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def lease_of(job: dict[str, Any] | None) -> dict[str, Any]:
    raw = (job or {}).get(LEASE_FIELD)
    return raw if isinstance(raw, dict) else {}


def lease_live(job: dict[str, Any] | None, *, now: datetime | None = None) -> bool:
    lease = lease_of(job)
    expires = _parse(lease.get("expires_at")) if lease.get("token") else None
    return expires is not None and expires > (now or _now())


def _record(token: str, owner: str, claimed_at: str | None = None) -> dict[str, Any]:
    now = _now()
    return {
        "token": token,
        "owner": owner,
        "claimed_at": claimed_at or now.isoformat(),
        "heartbeat_at": now.isoformat(),
        "expires_at": (now + timedelta(seconds=LEASE_TTL_S)).isoformat(),
        # The same instant as a number, for the sweeper's indexed query.
        "expires_epoch": (now + timedelta(seconds=LEASE_TTL_S)).timestamp(),
    }


def claim(job_id: str) -> str | None:
    """Take the job's verification lease; the token, or ``None`` when another worker holds a live one."""
    job = _service().get_job(job_id) or {}
    current = lease_of(job)
    if lease_live(job) and current.get("owner") != OWNER:
        return None
    token = uuid.uuid4().hex
    ok = _service().update_job_fields(
        job_id, {LEASE_FIELD: _record(token, OWNER)}, touch_updated_at=False,
        expected={f"{LEASE_FIELD}.token": current.get("token") or None},
    )
    return token if ok else None


def renew(job_id: str, token: str) -> None:
    """Extend the lease; :class:`LeaseLost` when another worker took it over."""
    job = _service().get_job(job_id) or {}
    current = lease_of(job)
    ok = current.get("token") == token and _service().update_job_fields(
        job_id, {LEASE_FIELD: _record(token, OWNER, current.get("claimed_at"))}, touch_updated_at=False,
        expected={f"{LEASE_FIELD}.token": token},
    )
    if not ok:
        raise LeaseLost(f"the verification of job {job_id} is held by another worker now")


def write_if_held(job_id: str, token: str, fields: dict[str, Any]) -> None:
    """Write ``fields`` only while this worker still holds the lease (one compare-and-swap)."""
    if not _service().update_job_fields(job_id, fields, touch_updated_at=False,
                                        expected={f"{LEASE_FIELD}.token": token}):
        raise LeaseLost(f"the verification of job {job_id} is held by another worker now")


def release(job_id: str, token: str) -> None:
    _service().update_job_fields(job_id, {LEASE_FIELD: None}, touch_updated_at=False,
                                 expected={f"{LEASE_FIELD}.token": token})
