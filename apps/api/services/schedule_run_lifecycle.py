"""One run-lifecycle state machine for scheduled runs.

Every scheduled attempt — cadence beat, retry, Run now — is one *run record*
in the schedule's ``run_history``. The record is written before any work
starts and every transition is persisted on it::

    pending ──► running ──► completed | completed_with_* | failed | cancelled
       │
       └──────► failed (refused / crashed before a job existed)
       └──────► skipped (overlap, pending approval, append replay)

Rules:

* A terminal record never changes again (a late callback cannot rewrite it).
* Every terminal record carries ``reason`` (never empty) and ``reason_code``.
* A schedule's displayed status is derived from its records
  (:func:`derive_status`), not from the stored ``last_status`` mirror.
* An open record whose owner is gone — the job's lease and heartbeat expired,
  or the scheduler died before a job existed — is failed by
  :func:`sweep_orphan_runs` with the reason "worker lost".
* Skips coalesce: an identical skip on consecutive beats counts occurrences
  on one record instead of burying real runs.

Backfill-safe: records written before this module existed have no ``run_id``,
``reason`` or ``transitions``; :func:`run_view` fills them on read and
:func:`merge_run_entry` adopts them when a writer touches the same job.
"""

from __future__ import annotations

import contextvars
import logging
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterator

logger = logging.getLogger(__name__)

PENDING = "pending"
RUNNING = "running"
FAILED = "failed"
CANCELLED = "cancelled"
SKIPPED = "skipped"
OPEN_STATUSES = frozenset({PENDING, RUNNING})

WORKER_LOST_CODE = "worker_lost"
WORKER_LOST_REASON = "worker lost"

TRIGGER_CADENCE = "cadence"
TRIGGER_RETRY = "retry"
TRIGGER_MANUAL = "manual"

#: An identical skip seen again within this window is not rewritten.
_SKIP_REFRESH = timedelta(seconds=60)

_CURRENT_RUN: contextvars.ContextVar[tuple[str, str] | None] = contextvars.ContextVar(
    "schedule_current_run", default=None
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse(value: Any) -> datetime | None:
    from services.schedule_store import _parse_ts

    return _parse_ts(value)


def new_run_id() -> str:
    return uuid.uuid4().hex


def is_open(status: Any) -> bool:
    return str(status or "").strip().lower() in OPEN_STATUSES


def _is_success(status: Any) -> bool:
    from services.job_status import is_completed

    return is_completed(status)


def current_run_id(schedule_id: str) -> str:
    """The run this thread is driving for ``schedule_id``, or ``""``."""
    bound = _CURRENT_RUN.get()
    if bound and bound[0] == schedule_id:
        return bound[1]
    return ""


@contextmanager
def bound_run(schedule_id: str, run_id: str) -> Iterator[str]:
    token = _CURRENT_RUN.set((schedule_id, run_id))
    try:
        yield run_id
    finally:
        _CURRENT_RUN.reset(token)


# --- record shape ------------------------------------------------------------


def default_reason(entry: dict[str, Any]) -> str:
    """The reason a terminal record states when its writer gave none."""
    status = str(entry.get("status") or "").strip().lower()
    for key in ("reason", "error", "retry_refused"):
        text = str(entry.get(key) or "").strip()
        if text:
            return text
    if _is_success(status):
        rows = int(entry.get("records_transferred") or 0)
        return f"Completed: {rows:,} row(s) transferred."
    if status == CANCELLED:
        return "Cancelled."
    if status == SKIPPED:
        return "Skipped."
    if is_open(status):
        return ""
    return f"The run ended as {status or 'unknown'} without an error message."


def _default_code(entry: dict[str, Any]) -> str:
    status = str(entry.get("status") or "").strip().lower()
    if _is_success(status):
        return "completed"
    fc = entry.get("failure_class")
    if isinstance(fc, dict) and fc.get("kind"):
        return f"job_failed_{fc['kind']}"
    if status == CANCELLED:
        return "cancelled"
    if is_open(status):
        return ""
    return "job_failed" if entry.get("job_id") else "start_failed"


def run_view(entry: dict[str, Any], index: int = 0) -> dict[str, Any]:
    """A record as readers see it; legacy rows get an id, a reason and a code."""
    row = dict(entry)
    row.setdefault("job_id", "")
    row["run_id"] = str(row.get("run_id") or "") or (
        f"legacy-{row['job_id']}" if row["job_id"] else f"legacy-{index}"
    )
    row.setdefault("trigger", TRIGGER_RETRY if int(row.get("attempt") or 0) else TRIGGER_CADENCE)
    if not is_open(row.get("status")):
        row["reason"] = default_reason(row)
        row["reason_code"] = str(row.get("reason_code") or "") or _default_code(row)
    else:
        row.setdefault("reason", "")
        row.setdefault("reason_code", "")
    if not isinstance(row.get("transitions"), list):
        row["transitions"] = [
            {"status": row.get("status"), "at": row.get("finished_at") or row.get("started_at")}
        ]
    return row


def _transition(record: dict[str, Any], status: str, reason: str = "") -> None:
    moves = list(record.get("transitions") or [])
    if moves and moves[-1].get("status") == status:
        return
    step: dict[str, Any] = {"status": status, "at": _now()}
    if reason:
        step["reason"] = reason[:500]
    moves.append(step)
    record["transitions"] = moves


def _match(history: list[dict[str, Any]], entry: dict[str, Any]) -> int:
    """Index of the record ``entry`` updates, or -1 for a new record."""
    run_id = str(entry.get("run_id") or "")
    if run_id:
        for i in range(len(history) - 1, -1, -1):
            if str(history[i].get("run_id") or "") == run_id:
                return i
    job_id = str(entry.get("job_id") or "")
    if job_id:
        for i in range(len(history) - 1, -1, -1):
            row = history[i]
            if str(row.get("job_id") or "") == job_id and is_open(row.get("status")):
                return i
    return -1


def _finished_job(history: list[dict[str, Any]], entry: dict[str, Any]) -> bool:
    job_id = str(entry.get("job_id") or "")
    return bool(job_id) and any(
        str(r.get("job_id") or "") == job_id and r.get("run_id") and not is_open(r.get("status"))
        for r in history
    )


def run_finished(sched: Any, job_id: str) -> bool:
    """True when the run record for ``job_id`` is already terminal."""
    return _finished_job(list(getattr(sched, "run_history", None) or []), {"job_id": job_id})


def merge_run_entry(history: list[dict[str, Any]], entry: dict[str, Any]) -> list[dict[str, Any]]:
    """Apply one transition to ``history`` and return the new list.

    Writers that predate the lifecycle (``mark_schedule_run``,
    ``schedule_retry``, ``record_run_history``) pass their entry here, so a
    finished run updates its own record instead of appending a second one.
    """
    out = [dict(r) for r in history]
    incoming = dict(entry)
    status = str(incoming.get("status") or "").strip().lower() or FAILED
    incoming["status"] = status
    idx = _match(out, incoming)
    if idx < 0 and _finished_job(out, incoming):
        # The job's run already ended (lost callback + beat both finalized it).
        return out
    if idx >= 0:
        record = out[idx]
        if not is_open(record.get("status")):
            # Terminal is final. A second finalize (lost callback + beat) or a
            # late writer must not rewrite what the operator already saw.
            return out
        for key, value in incoming.items():
            if key in ("run_id", "transitions", "created_at", "trigger"):
                continue
            if key == "started_at" and record.get("started_at"):
                continue
            record[key] = value
    else:
        record = dict(incoming)
        record["run_id"] = str(record.get("run_id") or "") or new_run_id()
        record.setdefault("trigger", TRIGGER_RETRY if int(record.get("attempt") or 0) else TRIGGER_CADENCE)
        record.setdefault("created_at", _now())
        record.setdefault("started_at", record["created_at"])
        out.append(record)
    record["heartbeat_at"] = _now()
    if not is_open(status):
        record["reason"] = default_reason(record)
        record["reason_code"] = str(record.get("reason_code") or "") or _default_code(record)
        record.setdefault("finished_at", _now())
    _transition(record, status, record.get("reason", "") if not is_open(status) else "")
    return out


def trim_history(history: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    """Keep the newest ``limit`` records, never dropping an open one."""
    if len(history) <= limit:
        return history
    keep_open = [r for r in history[:-limit] if is_open(r.get("status"))]
    return keep_open + history[-limit:]


# --- persistence -------------------------------------------------------------


def _mutate(
    schedule_id: str,
    change: Callable[[Any], dict[str, Any] | None],
) -> Any:
    """Load, apply ``change(sched) -> patch`` and save, under the claim lock."""
    from services.schedule_store import (
        _CLAIM_LOCK,
        PipelineSchedule,
        _load_all,
        _mongo_backend,
        _save_all,
    )

    with _CLAIM_LOCK:
        rows = _load_all()
        for i, sched in enumerate(rows):
            if sched.id != schedule_id:
                continue
            patch = change(sched)
            if not patch:
                return sched
            updated = PipelineSchedule.from_dict({**sched.to_dict(), **patch})
            rows[i] = updated
            # Per-document store: write this schedule only (a version-CAS
            # delta). The file store rewrites the whole snapshot.
            _save_all([updated] if _mongo_backend() is not None else rows)
            return updated
    return None


def _history_patch(sched: Any, entry: dict[str, Any], *, mirror: bool = True) -> dict[str, Any]:
    from services.schedule_store import RUN_HISTORY_LIMIT

    history = trim_history(merge_run_entry(list(sched.run_history or []), entry), RUN_HISTORY_LIMIT)
    patch: dict[str, Any] = {"run_history": history}
    status = str(entry.get("status") or "")
    if mirror and status != SKIPPED:
        # ``last_status`` is a legacy mirror for old readers; nothing displays it.
        patch["last_status"] = status
    return patch


def begin_run(schedule_id: str, *, trigger: str, attempt: int = 0) -> str:
    """Persist a ``pending`` record before any work starts. Returns its id."""
    run_id = new_run_id()
    now = _now()
    entry = {
        "run_id": run_id,
        "status": PENDING,
        "trigger": trigger,
        "attempt": int(attempt or 0),
        "job_id": "",
        "created_at": now,
        "started_at": now,
        "reason": "",
        "reason_code": "",
    }
    try:
        from services.schedule_runner import _scheduler_instance_id

        entry["instance"] = _scheduler_instance_id()
    except Exception:  # noqa: BLE001
        logger.debug("scheduler instance id unavailable", exc_info=True)
    _mutate(schedule_id, lambda s: _history_patch(s, entry, mirror=False))
    return run_id


def mark_running(schedule_id: str, run_id: str, job_id: str) -> None:
    """The job exists and was submitted: ``pending -> running``."""
    entry = {"run_id": run_id, "status": RUNNING, "job_id": str(job_id or "")}
    if not run_id:
        entry.pop("run_id")
        entry["trigger"] = TRIGGER_CADENCE
    _mutate(schedule_id, lambda s: _history_patch(s, entry, mirror=False))


def finish_run(
    schedule_id: str,
    run_id: str,
    status: str,
    *,
    reason: str,
    reason_code: str,
    **fields: Any,
) -> Any:
    """Move an open record to a terminal state (no cadence change)."""
    entry = {
        **fields,
        "run_id": run_id,
        "status": status,
        "reason": str(reason or "")[:500],
        "reason_code": reason_code,
    }
    if status == SKIPPED:
        return _mutate(schedule_id, lambda s: _skip_patch(s, entry))
    return _mutate(schedule_id, lambda s: _history_patch(s, entry))


def record_skip(
    schedule_id: str,
    reason_code: str,
    reason: str,
    *,
    sched: Any = None,
    **fields: Any,
) -> None:
    """A beat that did not start a run, with why. Identical skips coalesce."""
    if sched is not None and _skip_is_fresh(sched, reason_code, reason):
        return
    entry = {
        **fields,
        "status": SKIPPED,
        "reason": str(reason or "")[:500],
        "reason_code": reason_code,
        "trigger": fields.get("trigger") or TRIGGER_CADENCE,
    }
    try:
        _mutate(schedule_id, lambda s: _skip_patch(s, entry))
    except Exception:  # noqa: BLE001 — a skip record must never stop the beat
        logger.exception("Schedule %s skip not recorded", schedule_id)


def _latest_skip_to_coalesce(history: list[dict[str, Any]], code: str, reason: str) -> int:
    for i in range(len(history) - 1, -1, -1):
        row = history[i]
        if str(row.get("status") or "") != SKIPPED:
            return -1  # a real run came after it
        if str(row.get("reason_code") or "") == code and str(row.get("reason") or "") == reason:
            return i
        return -1
    return -1


def _skip_is_fresh(sched: Any, code: str, reason: str) -> bool:
    history = list(getattr(sched, "run_history", None) or [])
    idx = _latest_skip_to_coalesce(history, code, str(reason or "")[:500])
    if idx < 0:
        return False
    seen = _parse(history[idx].get("last_seen_at") or history[idx].get("finished_at"))
    return seen is not None and datetime.now(timezone.utc) - seen < _SKIP_REFRESH


def _skip_patch(sched: Any, entry: dict[str, Any]) -> dict[str, Any] | None:
    from services.schedule_store import RUN_HISTORY_LIMIT

    history = [dict(r) for r in (sched.run_history or [])]
    run_id = str(entry.get("run_id") or "")
    code = str(entry.get("reason_code") or "")
    reason = str(entry.get("reason") or "")
    if run_id:
        # A pending record of this attempt becomes the skip, unless an identical
        # skip is already the newest record: then the pending one is folded in.
        idx = _match(history, {"run_id": run_id})
        if idx >= 0 and not is_open(history[idx].get("status")):
            return None
        prior = _latest_skip_to_coalesce(
            [r for r in history if str(r.get("run_id") or "") != run_id], code, reason
        )
        if prior >= 0 and idx >= 0:
            history.pop(idx)
            return _bump_skip(history, code, reason, RUN_HISTORY_LIMIT)
        return {"run_history": trim_history(merge_run_entry(history, entry), RUN_HISTORY_LIMIT)}
    if _latest_skip_to_coalesce(history, code, reason) >= 0:
        return _bump_skip(history, code, reason, RUN_HISTORY_LIMIT)
    merged = merge_run_entry(history, entry)
    merged[-1]["occurrences"] = 1
    merged[-1]["last_seen_at"] = merged[-1].get("finished_at")
    return {"run_history": trim_history(merged, RUN_HISTORY_LIMIT)}


def _bump_skip(history: list[dict[str, Any]], code: str, reason: str, limit: int) -> dict[str, Any]:
    idx = _latest_skip_to_coalesce(history, code, reason)
    row = history[idx]
    row["occurrences"] = int(row.get("occurrences") or 1) + 1
    row["last_seen_at"] = _now()
    return {"run_history": trim_history(history, limit)}


# --- what readers display ----------------------------------------------------


_RUN_STATUSES_LEGACY = frozenset(
    {"completed", "completed_with_quarantine", "completed_with_warnings", "failed",
     "cancelled", "partial", "running", "pending", "success", "succeeded", "error"}
)


def derive_status(sched: Any) -> dict[str, Any]:
    """The schedule's displayed status, from its latest run record.

    ``last_status`` is the latest run's status (open or terminal) and
    ``last_status_reason`` that run's reason. ``current_run`` is the open
    record, if any; ``last_skip`` is the newest skip that came after it.
    A document with no records falls back to its stored ``last_status``.
    """
    history = [run_view(r, i) for i, r in enumerate(getattr(sched, "run_history", None) or [])]
    latest: dict[str, Any] | None = None
    last_skip: dict[str, Any] | None = None
    for row in reversed(history):
        if row.get("status") == SKIPPED:
            if latest is None and last_skip is None:
                last_skip = row
            continue
        latest = row
        break
    current = next((r for r in reversed(history) if is_open(r.get("status"))), None)
    if last_skip is not None and latest is not None and not is_open(latest.get("status")):
        skip_seen = _parse(last_skip.get("last_seen_at") or last_skip.get("finished_at"))
        done = _parse(latest.get("finished_at"))
        if skip_seen is not None and done is not None and skip_seen < done:
            last_skip = None  # it was about a run that has since finished
    if latest is not None:
        status = str(latest.get("status") or "")
        reason = str(latest.get("reason") or "")
        code = str(latest.get("reason_code") or "")
        run_id = str(latest.get("run_id") or "")
    else:
        stored = str(getattr(sched, "last_status", "") or "")
        status = stored if (stored in _RUN_STATUSES_LEGACY or not history) else ""
        reason, code, run_id = "", "", ""
    return {
        "last_status": status or None,
        "last_status_reason": reason,
        "last_status_code": code,
        "last_run_id": run_id,
        "current_run": _brief(current),
        "last_skip": _brief(last_skip),
    }


def _brief(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    keys = ("run_id", "status", "trigger", "attempt", "job_id", "started_at",
            "finished_at", "reason", "reason_code", "occurrences", "last_seen_at")
    return {k: row.get(k) for k in keys if k in row}


def recent_runs(sched: Any, limit: int = 5) -> list[dict[str, Any]]:
    history = [run_view(r, i) for i, r in enumerate(getattr(sched, "run_history", None) or [])]
    return [_brief(r) or {} for r in reversed(history)][: max(1, limit)]


# --- the orphan sweeper ------------------------------------------------------


def worker_lost_reason(detail: str) -> str:
    return f"{WORKER_LOST_REASON.capitalize()}: {detail}"


def settle_lost_job(job_id: str, job: dict[str, Any] | None) -> tuple[str, str]:
    """Fail a job whose worker is gone. Returns ``(status, reason)`` it ends in.

    Conditional on the status that was read: if a sweeper resumed the job or
    the worker finished it meanwhile, that newer status wins and is returned.
    """
    from services.mongodb_service import get_mongodb_service

    if not job:
        return FAILED, worker_lost_reason(
            f"job {job_id} no longer exists; the run cannot have finished."
        )
    prior = str(job.get("status") or "")
    rows = int(job.get("records_processed") or 0)
    phase = str(job.get("phase") or prior or "running")
    reason = worker_lost_reason(
        f"the worker running job {job_id} stopped during {phase} (its lease and "
        f"heartbeat expired). {rows:,} row(s) were committed before it stopped "
        "and were not rolled back."
    )
    mongo = get_mongodb_service()
    try:
        ok = bool(mongo.update_job_status(
            job_id, FAILED, only_from_status=[prior], phase="failed",
            error=reason, message=reason, error_code=WORKER_LOST_CODE,
            operator_command=True,
        ))
    except Exception:  # noqa: BLE001
        logger.exception("Job %s could not be settled as worker lost", job_id)
        ok = True  # the run is still lost; the job sweeper settles the doc
    if ok:
        return FAILED, reason
    fresh = mongo.get_job(job_id) or {}
    return str(fresh.get("status") or FAILED), str(fresh.get("error") or reason)


def sweep_orphan_runs() -> list[dict[str, str]]:
    """Finish every open run record whose owner is gone. Returns what it did."""
    from services.schedule_store import _dispatching_here, _job_is_live, _load_all

    out: list[dict[str, str]] = []
    for sched in _load_all():
        for row in list(sched.run_history or []):
            if not is_open(row.get("status")):
                continue
            try:
                action = _sweep_one(sched, row, _job_is_live, _dispatching_here)
            except Exception:  # noqa: BLE001 — one schedule must not stop the sweep
                logger.exception("Run sweep failed for schedule %s", sched.id)
                continue
            if action:
                out.append({"schedule_id": sched.id, "run_id": str(row.get("run_id") or ""), "action": action})
    return out


def _sweep_one(sched: Any, row: dict[str, Any], job_is_live: Callable, dispatching_here: Callable) -> str:
    from services.schedule_store import CLAIM_MAX_RUNTIME, _unbound_claim_grace

    run_id = str(row.get("run_id") or "")
    job_id = str(row.get("job_id") or "")
    now = datetime.now(timezone.utc)
    seen = _parse(row.get("heartbeat_at") or row.get("started_at") or row.get("created_at"))
    age = (now - seen) if seen else timedelta.max
    if job_id:
        live = job_is_live(job_id)
        if live is True or (live is None and age <= CLAIM_MAX_RUNTIME):
            return ""
        from services.schedule_runner import _finalize_run

        started = _parse(row.get("started_at")) or now
        _finalize_run(sched.id, job_id, int(row.get("attempt") or 0), started)
        from services.schedule_store import get_schedule

        fresh = get_schedule(sched.id)
        still = [r for r in (fresh.run_history if fresh else []) if str(r.get("run_id") or "") == run_id]
        if still and is_open(still[0].get("status")):
            # Finalize declined (it had already recorded this job): close the record.
            from services.mongodb_service import get_mongodb_service

            status, reason = settle_lost_job(job_id, get_mongodb_service().get_job(job_id))
            finish_run(sched.id, run_id, status if not is_open(status) else FAILED,
                       reason=reason, reason_code=WORKER_LOST_CODE, job_id=job_id)
        return "finalized"
    if dispatching_here(sched.id) or age <= _unbound_claim_grace():
        return ""
    reason = worker_lost_reason(
        "the scheduler stopped before this run created a job (no work was started)."
    )
    _fail_unstarted(sched, run_id, reason)
    return "worker_lost"


def _fail_unstarted(sched: Any, run_id: str, reason: str) -> None:
    from services.schedule_store import clear_schedule_running, get_schedule

    finish_run(sched.id, run_id, FAILED, reason=reason, reason_code=WORKER_LOST_CODE)
    fresh = get_schedule(sched.id)
    if fresh is not None and fresh.running and not str(fresh.running_job_id or "").strip():
        clear_schedule_running(sched.id)
    try:
        from services.schedule_runner import _notify_schedule

        if fresh is not None:
            _notify_schedule(fresh, "", FAILED, None, reason=reason)
    except Exception:  # noqa: BLE001
        logger.exception("Worker-lost notification failed for %s", sched.id)
