"""Cutover-window estimation from measured checkpoint throughput.

A migration is scheduled inside an outage window, so "how long will this take"
is a planning question with a wrong answer that costs money: promise four hours
for a nine-hour load and the business reopens on a half-migrated database.

Every number here is derived from checkpoints the engine already persisted —
``(rows_processed, at)`` pairs — never from row counts multiplied by a guessed
rate. When no throughput has been observed, or the population size is unknown,
the estimate reports ``available=False`` with the reason instead of a number an
operator would plan against.

Two bases, in preference order:

``observed_checkpoints``
    The running job's own trailing throughput. This is the only basis that
    reflects the destination's current load, so it wins whenever it exists.
``prior_runs``
    Throughput measured on completed runs of the same source→destination
    route, used before a run starts and before its first checkpoints land.

Estimates are reported as a p50/p90 pair from the observed rate distribution
rather than a point: throughput varies with batch, index maintenance and lock
contention, and a single number implies a precision the data does not carry.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Sequence

MIN_INTERVALS = 2
TRAILING_INTERVALS = 20
MAX_MARKS = TRAILING_INTERVALS + 1
_TERMINAL_OK = frozenset({"completed", "succeeded", "success"})


@dataclass(frozen=True)
class RuntimeEstimate:
    """What is left to do, how fast it is actually going, and how sure we are."""

    available: bool
    reason: str = ""
    basis: str = ""
    rows_total: int | None = None
    rows_done: int = 0
    rows_remaining: int | None = None
    rows_per_second_p50: float | None = None
    rows_per_second_p10: float | None = None
    remaining_seconds_p50: float | None = None
    remaining_seconds_p90: float | None = None
    finishes_at_p50: str = ""
    finishes_at_p90: str = ""
    intervals_observed: int = 0
    runs_observed: int = 0
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _parse_ts(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _as_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _series(checkpoints: Iterable[Any]) -> list[tuple[datetime, int]]:
    """Timestamped cumulative row marks, oldest first, monotonic in both axes."""
    points: list[tuple[datetime, int]] = []
    for entry in checkpoints or []:
        if not isinstance(entry, dict):
            continue
        at = _parse_ts(entry.get("at") or entry.get("created_at"))
        rows = _as_int(entry.get("rows"))
        if at is None or rows is None or rows < 0:
            continue
        points.append((at, rows))
    points.sort(key=lambda p: p[0])
    return points


def _rates(points: Sequence[tuple[datetime, int]]) -> list[float]:
    """Rows/second per interval; a restart resets the counter, so cut there.

    A resumed job re-checkpoints from a lower ``rows`` mark. Treating that drop
    as negative throughput would poison the median, and treating it as elapsed
    time with zero rows would understate the rate — the interval is dropped.
    """
    rates: list[float] = []
    for (t0, r0), (t1, r1) in zip(points, points[1:]):
        seconds = (t1 - t0).total_seconds()
        if seconds <= 0 or r1 < r0:
            continue
        rates.append((r1 - r0) / seconds)
    return [r for r in rates if r > 0]


def _percentile(values: Sequence[float], pct: float) -> float:
    """Nearest-rank percentile — no interpolation across a tiny sample."""
    ordered = sorted(values)
    if not ordered:
        return 0.0
    rank = max(1, min(len(ordered), int(round(pct / 100.0 * len(ordered)))))
    return ordered[rank - 1]


def _unavailable(reason: str, *, rows_total: int | None, rows_done: int) -> RuntimeEstimate:
    return RuntimeEstimate(
        available=False,
        reason=reason,
        rows_total=rows_total,
        rows_done=rows_done,
        rows_remaining=None,
    )


def _project(
    *,
    basis: str,
    rates: Sequence[float],
    rows_total: int,
    rows_done: int,
    now: datetime,
    runs_observed: int = 0,
    notes: Sequence[str] = (),
) -> RuntimeEstimate:
    remaining = max(rows_total - rows_done, 0)
    p50 = _percentile(rates, 50)
    p10 = _percentile(rates, 10)
    slow = p10 if p10 > 0 else p50
    secs_p50 = remaining / p50 if p50 > 0 else None
    secs_p90 = remaining / slow if slow > 0 else None
    return RuntimeEstimate(
        available=True,
        basis=basis,
        rows_total=rows_total,
        rows_done=rows_done,
        rows_remaining=remaining,
        rows_per_second_p50=round(p50, 3),
        rows_per_second_p10=round(slow, 3),
        remaining_seconds_p50=round(secs_p50, 1) if secs_p50 is not None else None,
        remaining_seconds_p90=round(secs_p90, 1) if secs_p90 is not None else None,
        finishes_at_p50=_at(now, secs_p50),
        finishes_at_p90=_at(now, secs_p90),
        intervals_observed=len(rates),
        runs_observed=runs_observed,
        notes=list(notes),
    )


def _at(now: datetime, seconds: float | None) -> str:
    if seconds is None:
        return ""
    return (now + timedelta(seconds=seconds)).isoformat()


def estimate_running_job(job: Any, *, now: datetime | None = None) -> RuntimeEstimate:
    """ETA for a job in flight, from its own checkpoints only."""
    now = now or datetime.now(timezone.utc)
    checkpoints = _field(job, "checkpoints") or []
    rows_done = _as_int(_field(job, "rows_processed")) or 0
    rows_total = _as_int(_field(job, "total_rows"))
    points = _series(checkpoints)
    if rows_total is None or rows_total <= 0:
        return _unavailable(
            "Population size is not known for this run, so no completion time "
            "can be projected. Row counts are still reported as they land.",
            rows_total=None,
            rows_done=rows_done,
        )
    rates = _rates(points)[-TRAILING_INTERVALS:]
    if len(rates) < MIN_INTERVALS:
        return _unavailable(
            f"Throughput needs {MIN_INTERVALS} completed checkpoint intervals "
            f"before an estimate is meaningful; {len(rates)} observed so far.",
            rows_total=rows_total,
            rows_done=rows_done,
        )
    notes: list[str] = []
    if rows_done > rows_total:
        notes.append(
            "More rows are processed than the population declared — the "
            "remaining count is clamped to zero, not negative."
        )
    return _project(
        basis="observed_checkpoints",
        rates=rates,
        rows_total=rows_total,
        rows_done=rows_done,
        now=now,
        notes=notes,
    )


def estimate_before_run(
    history: Iterable[Any],
    *,
    source: str,
    destination: str,
    rows_total: int | None,
    now: datetime | None = None,
) -> RuntimeEstimate:
    """Cutover window for a run that has not started, from this route's history.

    Only completed runs of the same route count. Throughput is taken from each
    prior run's checkpoint intervals rather than its wall-clock span, so queue
    time and post-write verification do not deflate the movement rate.
    """
    now = now or datetime.now(timezone.utc)
    if rows_total is None or rows_total <= 0:
        return _unavailable(
            "Source row count is unmeasured, so the cutover window cannot be "
            "projected. Run a preflight count first.",
            rows_total=rows_total,
            rows_done=0,
        )
    rates: list[float] = []
    runs = 0
    for record in history or []:
        if str(_field(record, "status") or "").strip().casefold() not in _TERMINAL_OK:
            continue
        if not _same_route(record, source, destination):
            continue
        observed = _rates(_series(_field(record, "checkpoints") or []))
        if observed:
            rates.extend(observed)
            runs += 1
    if runs == 0 or len(rates) < MIN_INTERVALS:
        return _unavailable(
            f"No completed run of {source} → {destination} has enough measured "
            "throughput yet. The first run of a route is unestimated rather "
            "than guessed from row counts.",
            rows_total=rows_total,
            rows_done=0,
        )
    return _project(
        basis="prior_runs",
        rates=rates,
        rows_total=rows_total,
        rows_done=0,
        now=now,
        runs_observed=runs,
        notes=[
            "Projected from throughput measured on prior runs of this route; "
            "destination load, indexes and row width on the day still apply."
        ],
    )


def append_throughput_mark(
    marks: Any, rows_processed: Any, *, now: datetime | None = None
) -> list[dict[str, Any]] | None:
    """Add one ``(rows, at)`` mark, keeping only the trailing window.

    Returns ``None`` when the row count is not a usable number, so a caller can
    leave the stored marks untouched rather than record a mark it cannot read
    back.
    """
    rows = _as_int(rows_processed)
    if rows is None or rows < 0:
        return None
    kept = [m for m in (marks or []) if isinstance(m, dict)][-(MAX_MARKS - 1) :]
    stamp = (now or datetime.now(timezone.utc)).isoformat()
    return [*kept, {"rows": rows, "at": stamp}]


def estimate_for_job_doc(doc: Any, *, now: datetime | None = None) -> dict[str, Any]:
    """Estimate for a control-plane job document, in its own field names."""
    job = {
        "rows_processed": _field(doc, "records_processed"),
        "total_rows": _field(doc, "total_rows"),
        "checkpoints": _field(doc, "throughput_marks") or [],
    }
    return estimate_running_job(job, now=now).to_dict()


def _same_route(record: Any, source: str, destination: str) -> bool:
    return (
        str(_field(record, "source") or "").strip().casefold()
        == str(source or "").strip().casefold()
        and str(_field(record, "destination") or "").strip().casefold()
        == str(destination or "").strip().casefold()
    )


def _field(obj: Any, name: str) -> Any:
    if isinstance(obj, dict):
        return obj.get(name)
    return obj.__dict__.get(name) if hasattr(obj, "__dict__") else None
