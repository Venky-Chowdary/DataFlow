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
  seconds, and stamped on the job record with a compare-and-swap before any
  row uses it (two replicas racing both use the one that won).
* It fails closed: a job record that cannot be read, or a stamp that cannot
  be recorded, raises :class:`RunClockUnavailable` and the run refuses.
* Outside a job (Transform preview, Validate) there is no run start yet:
  :func:`current_run_started_at` returns ``""`` and the engine uses a design
  clock it labels ``preview``.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Callable, Iterator

from services.shape_models import ShapeError


#: Job-record field holding ``{"started_at": iso, "source": "run"}``.
JOB_FIELD = "run_clock"


class RunClockUnavailable(ShapeError):
    """The run's start could not be read or recorded: the run refuses.

    Minting a value the job record does not hold would let a resumed pass
    (or Gate-8) see a different time from the rows already written.
    """


def _recorded(job: Any) -> str:
    clock = job.get(JOB_FIELD) if isinstance(job, dict) else None
    return str(clock.get("started_at") or "") if isinstance(clock, dict) else ""


class _Clock:
    def __init__(self, job_id: str, store: Callable[[], Any]) -> None:
        self.job_id = job_id
        self.store = store
        self.value = ""
        self.lock = Lock()

    def resolve(self) -> str:
        with self.lock:
            if not self.value:
                self.value = self._settle()
            return self.value

    def _read(self, store: Any) -> dict[str, Any]:
        try:
            job = store.get_job(self.job_id)
        except Exception as exc:  # noqa: BLE001 - surfaced as the run's refusal
            raise RunClockUnavailable(
                f"the run's start time could not be read from job {self.job_id} ({type(exc).__name__}); "
                "the run stops rather than stamp a different time"
            ) from exc
        if not isinstance(job, dict):
            raise RunClockUnavailable(
                f"job {self.job_id} could not be read, so its run start time is unknown; "
                "the run stops rather than stamp a different time"
            )
        return job

    def _settle(self) -> str:
        store = self.store()
        recorded = _recorded(self._read(store))
        if recorded:
            return recorded  # a resumed pass replays the run's own value
        value = now_iso()
        # Compare-and-swap: only the first replica to stamp the job wins.
        if store.update_job_fields(
            self.job_id,
            {JOB_FIELD: {"started_at": value, "source": "run"}},
            touch_updated_at=False,
            expected={JOB_FIELD: None},
        ):
            return value
        recorded = _recorded(self._read(store))
        if recorded:
            return recorded  # another replica stamped it first: use that one
        raise RunClockUnavailable(
            f"the run's start time could not be recorded on job {self.job_id}; "
            "the run stops rather than stamp rows with a time a resume could not repeat"
        )


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
