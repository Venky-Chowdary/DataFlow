"""Review round 4 #1: count_missed_windows ran inside the scheduler's claim lock
and called next_run once per missed instant (up to 1000). A "* * * * *"
schedule overdue three days took 28 s, "*/5" 4.3 s. It now counts a local day
at a time; the answer is the one the old next_run loop gave.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

import pytest

from services.cron_schedule import CronError, next_run
from services.schedule_store import MISSED_WINDOW_SCAN_LIMIT, count_missed_windows

NOW = datetime(2026, 11, 27, 12, 0, 30, tzinfo=timezone.utc)


def _old_count(cron: str, tz: str, due: datetime, now: datetime) -> int:
    """The previous implementation: one next_run per missed instant."""
    missed, cursor = 0, due
    for _ in range(MISSED_WINDOW_SCAN_LIMIT):
        try:
            cursor = next_run(cron, cursor, tz)
        except CronError:
            return missed
        if cursor > now:
            return missed
        missed += 1
    return missed


@pytest.mark.parametrize(("cron", "want"), [("* * * * *", MISSED_WINDOW_SCAN_LIMIT), ("*/5 * * * *", 864)])
@pytest.mark.parametrize("zone", ["UTC", "America/New_York", "Australia/Lord_Howe"])
def test_three_days_overdue_on_a_dense_cron_counts_in_milliseconds(cron, want, zone):
    due = (NOW - timedelta(days=3)).replace(second=0)
    count_missed_windows(cron=cron, interval="", tz=zone, next_run_at=due.isoformat(), now=NOW)
    start = time.perf_counter()
    got = count_missed_windows(cron=cron, interval="", tz=zone, next_run_at=due.isoformat(), now=NOW)
    assert time.perf_counter() - start < 0.05
    assert got == want


@pytest.mark.parametrize(("cron", "zone", "due"), [
    ("*/5 * * * *", "Australia/Lord_Howe", "2026-04-04T13:07:00+00:00"),  # across the 30-minute fall-back
    ("*/20 * * * *", "Australia/Lord_Howe", "2026-10-03T14:50:00+00:00"),  # across the spring-forward
    ("30 1 * * *", "America/New_York", "2026-10-30T00:00:00+00:00"),  # the repeated 01:30
    ("0 9 * * 1-5", "Europe/London", "2026-10-01T09:00:00+00:00"),
    ("15 */2 * * *", "Asia/Kathmandu", "2026-11-20T03:12:45+00:00"),
    ("0 0 31 2 *", "UTC", "2026-11-01T00:00:00+00:00"),  # never fires
])
def test_matches_the_old_next_run_loop(cron, zone, due):
    now = datetime.fromisoformat(due) + timedelta(days=2, hours=5, minutes=31, seconds=10)
    got = count_missed_windows(cron=cron, interval="", tz=zone, next_run_at=due, now=now)
    assert got == _old_count(cron, zone, datetime.fromisoformat(due), now)


def test_an_unknown_zone_counts_nothing():
    assert count_missed_windows(cron="* * * * *", interval="", tz="Mars/Olympus",
                                next_run_at="2026-11-26T00:00:00+00:00", now=NOW) == 0
