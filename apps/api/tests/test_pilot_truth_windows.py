"""Pilot ground truth — every time phrase and qualifier is applied or refused by name.

Review C2/M1/M4: a phrase the window parser did not consume ("this hour", "in
2025", "over the weekend") or a qualifier no filter applied ("mysql", "CDC",
"on Local Postgres") was dropped, and the whole-history number was stated as
the answer. Each question here is either answered with the store's own count
for exactly the window it names, or refused with the phrase quoted — never
answered with the unfiltered number.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from tests.helpers import pilot_truth as pt

NOW = datetime(2026, 11, 11, 15, 0, tzinfo=timezone.utc)  # a Wednesday
TODAY = datetime(2026, 11, 11, tzinfo=timezone.utc)


@pytest.fixture
def ledger(monkeypatch):
    """Failed: 2 today, 3 yesterday, 4 on Sun 8 Nov, 5 on 20 Oct, 6 forty days ago; 7 completed today."""
    pt.reset_workspace(monkeypatch)
    pt.freeze_clock(monkeypatch, NOW, "UTC")
    for i in range(2):
        pt.seed_job("failed", at=NOW - timedelta(hours=1 + i))
    for i in range(3):
        pt.seed_job("failed", at=NOW - timedelta(days=1, hours=i))
    for i in range(4):
        pt.seed_job("failed", at=NOW - timedelta(days=3, hours=i))
    for i in range(5):
        pt.seed_job("failed", at=datetime(2026, 10, 20, 10, i, tzinfo=timezone.utc))
    for i in range(6):
        pt.seed_job("failed", at=NOW - timedelta(days=40, hours=i))
    for i in range(7):
        pt.seed_job("completed", at=NOW - timedelta(hours=2, minutes=i), source_type="mysql", sync_mode="cdc")
    return pt.agent()


def _failed_between(start: datetime, end: datetime) -> int:
    return int(pt.mongo().count_job_matches(status="failed", date_from=start, date_to=end) or 0)


def _refused(response, phrase: str) -> None:
    answer = response.answer
    assert phrase.lower() in answer.lower(), f"refusal does not name {phrase!r}: {answer!r}"
    assert "can't filter" in answer.lower() or "can’t filter" in answer.lower(), answer
    assert not pt.grounded(response), "a refusal is not a grounded answer"
    assert 20 not in pt.bold_ints(answer), f"whole-history failed count stated: {answer!r}"


@pytest.mark.parametrize("question,phrase", [
    ("how many jobs failed this hour", "this hour"),
    ("how many jobs failed between the 3rd and 5th", "between the 3rd and 5th"),
    ("how many jobs failed on 11/08", "11/08"),
    ("how many jobs failed in 2025", "2025"),
    ("how many jobs failed week to date", "week to date"),
    ("how many jobs failed over the weekend", "weekend"),
    ("how many jobs failed in the last 0 days", "last 0 days"),
    ("how many jobs will fail tomorrow", "tomorrow"),
    # M1: partially applied phrases refuse too.
    ("how many jobs failed the day before yesterday", "day before yesterday"),
    ("how many jobs failed before yesterday", "before yesterday"),
    ("how many jobs failed since last week", "since last week"),
    ("how many jobs failed yesterday and today", "yesterday and today"),
    ("how many jobs failed after 3pm today", "after 3pm today"),
    # M4: no silent whole-history answer.
    ("did last night's job finish", "last night"),
    ("is the job I started 5 minutes ago still running", "5 minutes ago"),
    ("what is the status of my last transfer this morning", "this morning"),
])
def test_unapplied_time_phrase_is_refused_by_name(ledger, question, phrase):
    _refused(pt.ask(ledger, question), phrase)


@pytest.mark.parametrize("question,phrase", [
    ("how many failed mysql jobs today", "mysql"),
    ("how many cdc jobs failed today", "cdc"),
    ("how many of my jobs were CDC", "cdc"),
    ("list failed jobs on Local Postgres", "postgres"),
])
def test_unapplied_qualifier_is_refused_by_name(ledger, question, phrase):
    _refused(pt.ask(ledger, question), phrase)


@pytest.mark.parametrize("question,span", [
    ("how many jobs failed in the last ten days", timedelta(days=10)),
    ("how many jobs failed in the last 48h", timedelta(hours=48)),
    ("how many jobs failed in the last 48 hours", timedelta(hours=48)),
    ("how many jobs failed in the past 24 hours", timedelta(hours=24)),
    ("how many jobs failed in the last week", timedelta(weeks=1)),
    ("how many jobs failed in the past week", timedelta(weeks=1)),
    ("how many jobs failed in the last 30 minutes", timedelta(minutes=30)),
])
def test_rolling_windows_count_exactly(ledger, question, span):
    response = pt.ask(ledger, question)
    truth = _failed_between(NOW - span, NOW)
    stated = pt.bold_ints(response.answer)
    assert stated and stated[0] == truth, f"{question!r}: expected {truth}, got {response.answer!r}"
    assert pt.grounded(response)


def test_iso_day_is_applied_not_routed_to_a_data_read(ledger):
    response = pt.ask(ledger, "how many jobs failed on 2026-11-08")
    truth = _failed_between(datetime(2026, 11, 8, tzinfo=timezone.utc), datetime(2026, 11, 9, tzinfo=timezone.utc))
    assert truth == 4
    assert pt.bold_ints(response.answer)[:1] == [truth], response.answer
    assert "aggregate_data" not in {t.get("name") for t in response.tools_used or []}


def test_recently_states_the_window_it_used(ledger):
    response = pt.ask(ledger, "have any jobs failed recently?")
    truth = _failed_between(NOW - timedelta(days=7), NOW)
    assert "last 7 days" in response.answer, response.answer
    assert pt.bold_ints(response.answer)[:1] == [truth], response.answer


def test_window_upper_bound_is_exclusive_to_the_instant(monkeypatch):
    """A job at 00:00:00 today is today's, one a second earlier is yesterday's."""
    pt.reset_workspace(monkeypatch)
    pt.freeze_clock(monkeypatch, NOW, "UTC")
    pt.seed_job("failed", at=TODAY)  # exactly the end of yesterday's window
    pt.seed_job("failed", at=TODAY - timedelta(seconds=1))  # last second of yesterday
    pt.seed_job("failed", at=TODAY - timedelta(days=1))  # exactly the start of yesterday
    pt.seed_job("failed", at=TODAY - timedelta(days=1, seconds=1))  # the day before
    agent = pt.agent()
    yesterday = pt.ask(agent, "how many jobs failed yesterday")
    assert pt.bold_ints(yesterday.answer)[:1] == [2], yesterday.answer
    today = pt.ask(agent, "how many jobs failed today")
    assert pt.bold_ints(today.answer)[:1] == [1], today.answer
