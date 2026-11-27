"""Review: next_run walked one minute at a time over a 4-year horizon.

"0 0 29 2 *" took ~21 s of CPU (Casablanca 22.6 s) — on the scheduler's save
and beat paths and on the form's preview route. next_run now jumps over the
calendar fields and expands the hour x minute grid only on matching days.
These tests pin (1) speed: the worst expressions resolve in < 50 ms;
(2) the same answers: a randomized cron x zone x start corpus (DST gaps and
overlaps, Sunday 0/7, Lord Howe's 30-minute shift) is checked against the old
minute-by-minute walk, run here as the oracle over a small horizon.
apps/web/src/lib/__fixtures__/cron_next_runs.json (recorded with the old walk
over the full horizon) is checked by test_cron_preview_fixture.py.
"""

from __future__ import annotations

import random
import time
from datetime import datetime, timedelta, timezone

import pytest

from services.cron_schedule import (
    CronError,
    CronExpression,
    CronNeverFiresError,
    count_matches,
    next_run,
    resolve_timezone,
)

NOW = datetime(2026, 11, 21, 10, 0, tzinfo=timezone.utc)


def _oracle(expr: str, after: datetime, tz: str, minutes: int) -> datetime | None:
    """The old definition: test every minute after ``after`` (horizon ``minutes``)."""
    cron = CronExpression(expr)
    zone = resolve_timezone(tz)
    candidate = (after.astimezone(timezone.utc) + timedelta(minutes=1)).replace(second=0, microsecond=0)
    for _ in range(minutes):
        if cron.matches(candidate.astimezone(zone)):
            return candidate
        candidate += timedelta(minutes=1)
    return None


@pytest.mark.parametrize(("expr", "zone"), [
    ("0 0 29 2 *", "UTC"),
    ("0 0 29 2 *", "Africa/Casablanca"),
    ("59 23 31 12 *", "Australia/Lord_Howe"),
    ("0 0 29 2 1", "Australia/Lord_Howe"),
    ("* * * * *", "America/New_York"),
])
def test_the_slow_expressions_resolve_in_milliseconds(expr, zone):
    next_run(expr, NOW, zone)  # warm the zone cache
    start = time.perf_counter()
    for _ in range(10):
        next_run(expr, NOW, zone)
    assert (time.perf_counter() - start) / 10 < 0.05, expr


def test_an_impossible_date_is_a_clear_refusal_fast():
    start = time.perf_counter()
    with pytest.raises(CronNeverFiresError, match="can never fire"):
        next_run("0 0 31 2 *", NOW, "UTC")
    assert time.perf_counter() - start < 0.05


ZONES = ["UTC", "America/New_York", "Europe/London", "Australia/Lord_Howe", "Asia/Kolkata",
         "Africa/Casablanca", "America/Santiago", "Pacific/Chatham", "Europe/Amsterdam", "Asia/Kathmandu"]
# DST changes in those zones, and ordinary instants.
ANCHORS = ["2026-11-01T04:30:00", "2026-11-01T05:40:00", "2026-03-08T06:10:00", "2026-03-29T00:20:00",
           "2026-10-25T00:05:00", "2026-04-05T14:30:00", "2026-10-03T15:10:00", "2026-04-04T14:40:00",
           "2026-09-26T13:30:00", "2026-04-04T23:10:00", "2026-12-31T23:30:00", "2026-06-15T09:07:00",
           # Amsterdam's offset had seconds (+00:19:32) until 1937: never a whole-minute day.
           "1930-06-01T10:00:00"]


def _corpus(seed: int = 20261127, size: int = 500) -> list[tuple[str, str, datetime]]:
    rng = random.Random(seed)

    def field(lo: int, hi: int) -> str:
        roll = rng.random()
        if roll < 0.35:
            return "*"
        if roll < 0.55:
            return f"*/{rng.choice([2, 3, 5, 10, 15, 20, 30])}" if hi >= 30 else f"*/{rng.randint(1, max(1, hi // 2))}"
        if roll < 0.75:
            return str(rng.randint(lo, hi))
        a = rng.randint(lo, hi)
        return f"{a}-{rng.randint(a, hi)}"

    cases = []
    for _ in range(size):
        dow = rng.choice(["*", "*", "0", "7", "0,7", "1-5", field(0, 6)])
        expr = " ".join([field(0, 59), rng.choice(["*", "0", "1", "2", "3", "23", field(0, 23)]),
                         rng.choice(["*", "*", "1", "15", field(1, 31)]), "*", dow])
        after = datetime.fromisoformat(rng.choice(ANCHORS)).replace(tzinfo=timezone.utc)
        cases.append((expr, rng.choice(ZONES), after))
    return cases


def test_agrees_with_the_minute_by_minute_definition():
    horizon = 4 * 24 * 60  # the oracle's window: four days of minutes
    checked = 0
    for expr, zone, after in _corpus():
        want = _oracle(expr, after, zone, horizon)
        try:
            got = next_run(expr, after, zone)
        except CronError:
            got = None
        if want is not None:
            assert got == want, (expr, zone, after.isoformat(), got, want)
            checked += 1
        else:
            assert got is None or got >= after + timedelta(minutes=horizon), (expr, zone, after.isoformat(), got)
    assert checked > 300, "the corpus exercises the matching path"


def _oracle_count(expr: str, after: datetime, until: datetime, tz: str) -> int:
    """The old count: every minute in (after, until] that matches."""
    cron = CronExpression(expr)
    zone = resolve_timezone(tz)
    t = (after + timedelta(minutes=1)).replace(second=0, microsecond=0)
    n = 0
    while t <= until:
        n += cron.matches(t.astimezone(zone))
        t += timedelta(minutes=1)
    return n


def test_counts_agree_with_the_minute_by_minute_definition():
    for expr, zone, after in _corpus(size=200):
        until = after + timedelta(hours=40, minutes=17)
        assert count_matches(expr, after, until, zone, limit=10_000) == _oracle_count(expr, after, until, zone), (
            expr, zone, after.isoformat())


@pytest.mark.parametrize("zone", ["UTC", "America/New_York", "Australia/Lord_Howe", "Asia/Kathmandu"])
@pytest.mark.parametrize("expr", ["* * * * *", "*/5 * * * *", "*/7 9-17 * * 1-5"])
def test_dense_expressions_resolve_in_under_a_millisecond(expr, zone):
    """Review round 4 #1: the day expansion cost ~28 ms per call on "* * * * *", inside the claim lock."""
    next_run(expr, NOW, zone)
    start = time.perf_counter()
    for i in range(200):
        next_run(expr, NOW + timedelta(minutes=13 * i), zone)
    assert (time.perf_counter() - start) / 200 < 0.001, expr
