"""Pilot ground truth — rolling windows are elapsed time, on DST days too (review 2, C2a).

"The last 24 hours" was computed as wall-clock arithmetic on a zone-aware
local time, so on the day New York's clocks go back it covered 25 real hours
and on the day they go forward 23, and the wrong count was stated as grounded.
The span is now cut from the UTC instant; only the label is in the workspace zone.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from tests.helpers import pilot_truth as pt


def _failed_between(start: datetime, end: datetime) -> int:
    return int(pt.mongo().count_job_matches(status="failed", date_from=start, date_to=end) or 0)


# ---- rolling windows are real elapsed time, on DST days too (review 2, C2a) ---
#
# "The last 24 hours" was computed as wall-clock arithmetic on a local time, so
# on the day clocks go back it covered 25 real hours, and on the day they go
# forward 23. The span is now taken from the UTC instant; only the label is local.

NY = "America/New_York"
#: 2026 DST in New York: clocks go forward Sun 8 Mar 02:00 EST, back Sun 1 Nov 02:00 EDT.
DST_CASES = [
    # (now, question, minutes ago of jobs seeded, expected count)
    ("dst-end noon", datetime(2026, 11, 1, 17, 0, tzinfo=timezone.utc), "how many jobs failed in the last 24 hours",
     [60, 24 * 60 + 30], 1),
    ("dst-end 03:00 EST", datetime(2026, 11, 1, 8, 0, tzinfo=timezone.utc), "how many jobs failed in the last 3 hours",
     [60, 3 * 60 + 30], 1),
    ("dst-start noon", datetime(2026, 3, 8, 16, 0, tzinfo=timezone.utc), "how many jobs failed in the last 24 hours",
     [23 * 60 + 30], 1),
    ("dst-start 04:00 EDT", datetime(2026, 3, 8, 8, 0, tzinfo=timezone.utc), "how many jobs failed in the last 3 hours",
     [2 * 60 + 30, 3 * 60 + 30], 1),
    ("dst-end last day", datetime(2026, 11, 1, 17, 0, tzinfo=timezone.utc), "how many jobs failed in the last day",
     [24 * 60 - 10, 24 * 60 + 30], 1),
]


@pytest.mark.parametrize("label,now,question,ages,expected", DST_CASES, ids=[c[0] for c in DST_CASES])
def test_rolling_windows_are_elapsed_time_on_dst_days(monkeypatch, label, now, question, ages, expected):
    pt.reset_workspace(monkeypatch)
    pt.freeze_clock(monkeypatch, now, NY)
    for minutes in ages:
        pt.seed_job("failed", at=now - timedelta(minutes=minutes))
    span = {"24 hours": timedelta(hours=24), "3 hours": timedelta(hours=3), "last day": timedelta(days=1)}
    real = next(v for k, v in span.items() if k in question)
    assert _failed_between(now - real, now) == expected, "fixture: truth is the real elapsed span"
    response = pt.ask(pt.agent(), question)
    assert pt.bold_ints(response.answer)[:1] == [expected], response.answer
    assert NY in response.answer, "the window is labelled in the workspace zone"
    assert pt.grounded(response)


def test_a_rolling_window_resolves_to_utc_instants_on_dst_days():
    from src.ai.copilot.job_window import named_window

    for now in (datetime(2026, 11, 1, 17, 0, tzinfo=timezone.utc), datetime(2026, 3, 8, 16, 0, tzinfo=timezone.utc)):
        for key, span in (("last_24_hours", timedelta(hours=24)), ("last_3_hours", timedelta(hours=3)),
                          ("last_2_days", timedelta(days=2)), ("last_1_weeks", timedelta(weeks=1)),
                          ("last_90_minutes", timedelta(minutes=90))):
            window = named_window(key, now=now, zone=NY)
            assert window.end == now and window.end - window.start == span, (key, now, window)
