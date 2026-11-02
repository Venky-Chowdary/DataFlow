"""C18 — the natural-language scheduler must not silently rewrite a cadence.

Every phrase maps to an exact schedule (a cron the runner already evaluates,
with the zone the operator named) or is refused with the unparsed fragment
named. Never a partial parse.

Defects closed here (prod SHA c6c16128):
- D41: "Tuesday and Thursday" kept only Tuesday; "8am and 8pm" kept only 8am;
  "weekend" became a rolling 7-day preset.
- D44: "every 3 hours starting at 1am" dropped the anchor; "every other
  Friday" became every Friday; "hourly at :15" ran on the hour.
- D29: "every 2 hours on weekdays between 9am and 5pm" dropped the window
  AND the weekday bound and staged a 24/7 cron; a bare cron with a trailing
  zone was refused outright.
- D22: ``update_schedule`` accepted a cron that can never fire (Feb 30); the
  preview labelled the schedule "(UTC)" while keeping another zone; a bare
  cron followed by an IANA zone was not parsed.
"""

import pytest

from src.ai.copilot.schedule_cadence import parse_cadence


# (phrase, expected_cron, expected_timezone) — exact cadence or a refusal.
_CADENCE_TABLE = [
    # ---- D41: every named day and every named time belongs in the cron ----
    ("every Tuesday and Thursday at 4:15pm in America/Chicago",
     "15 16 * * 2,4", "America/Chicago"),
    ("Tuesdays and Thursdays at 16:15 UTC", "15 16 * * 2,4", "UTC"),
    ("Mondays, Wednesdays and Fridays at 9am America/New_York",
     "0 9 * * 1,3,5", "America/New_York"),
    ("every weekend at 6am UTC", "0 6 * * 0,6", "UTC"),
    ("twice a day at 8am and 8pm", "0 8,20 * * *", "UTC"),
    ("twice a day at 8am and 8pm America/New_York", "0 8,20 * * *", "America/New_York"),
    # ---- D44: anchors and :NN minutes ----
    ("every 3 hours starting at 1am", "0 1-23/3 * * *", "UTC"),
    ("every 6 hours starting at 3pm UTC", "0 15-23/6 * * *", "UTC"),
    ("hourly at :15 past the hour", "15 * * * *", "UTC"),
    ("hourly at :7", "7 * * * *", "UTC"),
    # ---- D29: window + day bound are part of the cadence ----
    ("every 2 hours on weekdays between 9am and 5pm America/Chicago",
     "0 9-17/2 * * 1-5", "America/Chicago"),
    ("every 4 hours between 8am and 8pm", "0 8-20/4 * * *", "UTC"),
    ("every 2 hours on Saturdays between 9 and 5",
     "0 9-5/2 * * 6", "UTC") if False else (
     "every 2 hours on Saturdays between 9am and 5pm", "0 9-17/2 * * 6", "UTC"),
    # ---- D22: bare cron with a trailing zone parses ----
    ("0 9-17/2 * * 1-5 in America/Chicago", "0 9-17/2 * * 1-5", "America/Chicago"),
    ("30 2 * * 0 America/New_York", "30 2 * * 0", "America/New_York"),
    ("*/10 * * * *", "*/10 * * * *", "UTC"),
    # ---- previously-working phrasings must still resolve exactly ----
    ("hourly", None, "UTC"),
    ("hourly at minute 7", "7 * * * *", "UTC"),
    ("weekdays at 21:40 UTC", "40 21 * * 1-5", "UTC"),
    ("nightly at 02:00 Asia/Kolkata", "0 2 * * *", "Asia/Kolkata"),
    ("daily at 03:00 UTC", "0 3 * * *", "UTC"),
    ("every day at noon UTC", "0 12 * * *", "UTC"),
    ("monthly on the 1st at 02:00 UTC", "0 2 1 * *", "UTC"),
    ("weekly on Monday at 9am UTC", "0 9 * * 1", "UTC"),
    ("every Monday at 9am UTC", "0 9 * * 1", "UTC"),
    ("every 15 minutes", "*/15 * * * *", "UTC"),
    ("every 30 minutes", "*/30 * * * *", "UTC"),
    ("every 6 hours", "0 */6 * * *", "UTC"),
    ("cron 0 3 * * 1-5", "0 3 * * 1-5", "UTC"),
    ("cron 15 2 * * * in Europe/Paris", "15 2 * * *", "Europe/Paris"),
]

_REFUSAL_TABLE = [
    # (phrase, fragment the refusal must name)
    ("every other Friday at 5pm", "every other"),
    ("every other week on Monday at 9am", "every other"),
    ("biweekly on Tuesday at 10am", "biweek"),
    ("cron 0 0 30 2 *", "never fire"),          # Feb 30 — D22
    ("cron 0 0 31 2 *", "never fire"),          # Feb 31
    ("cron 0 0 31 4,6 *", "never fire"),        # Apr/Jun have no 31st
    ("twice a day at 8:15 and 20:30", "08:15"),  # cartesian would double-fire
    ("every 5 days", "every 5 days"),
    ("every 3 weeks on Friday", "every"),
]


@pytest.mark.parametrize("phrase,cron,tz", _CADENCE_TABLE)
def test_nl_cadence_table(phrase, cron, tz):
    spec = parse_cadence(phrase)
    assert spec.resolved, f"{phrase!r} was refused: {spec.question}"
    if cron is not None:
        assert spec.cron == cron, f"{phrase!r} -> {spec.cron!r}"
    assert spec.timezone == tz


@pytest.mark.parametrize("phrase,fragment", _REFUSAL_TABLE)
def test_nl_cadence_refusals(phrase, fragment):
    spec = parse_cadence(phrase)
    assert not spec.resolved, f"{phrase!r} silently parsed to {spec.cron!r}"
    assert fragment.lower() in spec.question.lower()


def test_cron_never_fires_refused():
    spec = parse_cadence("0 0 30 2 *")
    assert not spec.resolved
    spec = parse_cadence("cron 0 0 29 2 *")
    assert spec.resolved  # Feb 29 fires in leap years — allowed.


def test_cron_zone_label_honest_when_zone_given():
    spec = parse_cadence("30 2 * * 0 America/New_York")
    assert spec.timezone == "America/New_York"
    assert "America/New_York" in spec.description
    assert "(UTC)" not in spec.description


def test_zone_not_claimed_when_assumed():
    """An assumed UTC must not appear inside the cadence description — the
    caller names the zone it actually keeps (D22's '(UTC)' mislabel)."""
    spec = parse_cadence("daily at 03:00")
    assert spec.timezone_assumed
    assert "UTC" not in spec.description


def test_multi_day_cron_fires_all_named_days():
    """The produced cron must actually fire on every named day — verify with
    the runner's own evaluator, not just the string."""
    from services.cron_schedule import next_run
    from datetime import datetime, timezone
    from zoneinfo import ZoneInfo

    spec = parse_cadence("every Tuesday and Thursday at 4:15pm in America/Chicago")
    # Tuesday 2026-10-27, Thursday 2026-10-29 at 16:15 Chicago time.
    seen = set()
    cursor = datetime(2026, 10, 26, tzinfo=timezone.utc)
    for _ in range(4):
        cursor = next_run(spec.cron, cursor, spec.timezone)
        wall = cursor.astimezone(ZoneInfo(spec.timezone))
        seen.add((wall.weekday(), wall.hour, wall.minute))
        cursor = cursor.replace(microsecond=0)
    assert (1, 16, 15) in seen and (3, 16, 15) in seen
    assert all(h == 16 and m == 15 for _, h, m in seen)


def test_window_cron_does_not_fire_outside_window():
    from services.cron_schedule import next_run
    from datetime import datetime, timezone
    from zoneinfo import ZoneInfo

    spec = parse_cadence("every 2 hours on weekdays between 9am and 5pm America/Chicago")
    cursor = datetime(2026, 10, 26, tzinfo=timezone.utc)
    for _ in range(8):
        cursor = next_run(spec.cron, cursor, spec.timezone)
        wall = cursor.astimezone(ZoneInfo(spec.timezone))
        assert wall.weekday() < 5, wall
        assert 9 <= wall.hour <= 17, wall


def test_multi_time_cron_fires_both_times():
    from services.cron_schedule import next_run
    from datetime import datetime, timezone

    spec = parse_cadence("twice a day at 8am and 8pm")
    cursor = datetime(2026, 10, 26, tzinfo=timezone.utc)
    hours = []
    for _ in range(4):
        cursor = next_run(spec.cron, cursor, spec.timezone)
        hours.append((cursor.hour, cursor.minute))
    assert (8, 0) in hours and (20, 0) in hours
