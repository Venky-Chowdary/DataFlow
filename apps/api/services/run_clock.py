"""The run's start time: fixed once per job, stamped on the job, replayed on resume.

The Transform engine is a pure function of its rows: it has no clock, so a
preview, Validate, the write and Gate-8's recomputation all produce the same
values. A ``run_timestamp`` step ("Run start time" on the Map step) needs one
clock value per run, and it must be the same value on every row, every page,
every resumed pass and in the proof. This module is that value.

* :func:`run_clock_scope` opens for the life of a tracked job (beside the
  hash_pii key scope). It does nothing until a recipe asks for the clock.
* The first ask reads the job record: a job that already has ``run_clock``
  (a resumed pass) replays it; otherwise the value is fixed now, in whole UTC
  seconds, and stamped on the job record before any row uses it.
* Outside a job (Transform preview, Validate) there is no run start yet:
  :func:`current_run_started_at` returns ``""`` and the engine uses a design
  clock it labels ``preview``.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Callable, Iterator

logger = logging.getLogger(__name__)

#: Job-record field holding ``{"started_at": iso, "source": "run"}``.
JOB_FIELD = "run_clock"


class _Clock:
    def __init__(self, job_id: str, store: Callable[[], Any]) -> None:
        self.job_id = job_id
        self.store = store
        self.value = ""
        self.lock = Lock()

    def resolve(self) -> str:
        with self.lock:
            if not self.value:
                self.value = self._recorded() or self._fix()
            return self.value

    def _recorded(self) -> str:
        try:
            job = self.store().get_job(self.job_id) or {}
        except Exception:  # noqa: BLE001 - an unreadable record is "none recorded"
            logger.warning("could not read run_clock for %s", self.job_id, exc_info=True)
            return ""
        return str((job.get(JOB_FIELD) or {}).get("started_at") or "")

    def _fix(self) -> str:
        value = now_iso()
        # Stamped before any row uses it: a resume must find the same value.
        self.store().update_job_fields(
            self.job_id,
            {JOB_FIELD: {"started_at": value, "source": "run"}},
            touch_updated_at=False,
        )
        return value


_CLOCK: ContextVar[_Clock | None] = ContextVar("dataflow_run_clock", default=None)


def now_iso() -> str:
    """Now, in whole UTC seconds (every destination's timestamp holds it exactly)."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def as_instant(value: str) -> datetime:
    """The stored ISO text as an aware UTC datetime."""
    moment = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc)


@contextmanager
def run_clock_scope(job_id: str, *, store: Callable[[], Any]) -> Iterator[None]:
    """Make the job's run start available to every recipe built inside the run."""
    token = _CLOCK.set(_Clock(job_id, store))
    try:
        yield
    finally:
        _CLOCK.reset(token)


def current_run_started_at() -> str:
    """The run's start (ISO, UTC) inside a job; ``""`` at design time."""
    clock = _CLOCK.get()
    return clock.resolve() if clock is not None else ""


def recorded_run_started_at(record: Any) -> str:
    """The value a finished run used, from its job record or its summary."""
    if not isinstance(record, dict):
        return ""
    for key in (JOB_FIELD, "shape_run_clock"):
        value = (record.get(key) or {}).get("started_at") if isinstance(record.get(key), dict) else ""
        if value:
            return str(value)
    return ""
