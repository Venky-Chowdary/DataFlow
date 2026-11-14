"""Pilot ground truth — a word around a date changes the window, or the question is refused (review 2, C2b).

"how many jobs failed by 2026-11-08" was answered for 8 November alone: the
date was read as "on that day" and "by" was dropped. The same for through,
starting, prior to, post, "… onwards", and fuzzy spans ("the last hour and a
half" read as the last hour). A date is applied only in the forms that are
exact — on <date>, since <date>, from <date> to <date>, between <date> and
<date> — each an exact ``[start, end)`` that the answer names. Any other word
around a date, and any fuzzy span, is refused by name.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from tests.helpers import pilot_truth as pt

NOW = datetime(2026, 11, 11, 15, 0, tzinfo=timezone.utc)
#: Failed jobs per UTC day: 5 on 1 Oct, 3 on 8 Nov, 2 on 9 Nov, 4 on 10 Nov.
DAYS = {datetime(2026, 10, 1, 9, tzinfo=timezone.utc): 5, datetime(2026, 11, 8, 9, tzinfo=timezone.utc): 3,
        datetime(2026, 11, 9, 9, tzinfo=timezone.utc): 2, datetime(2026, 11, 10, 9, tzinfo=timezone.utc): 4}


@pytest.fixture
def ledger(monkeypatch):
    pt.reset_workspace(monkeypatch)
    pt.freeze_clock(monkeypatch, NOW, "UTC")
    for day, n in DAYS.items():
        for i in range(n):
            pt.seed_job("failed", at=day.replace(minute=i))
    pt.seed_job("failed", at=NOW.replace(hour=14))  # one today
    return pt.agent()


def _between(start: datetime, end: datetime) -> int:
    return int(pt.mongo().count_job_matches(status="failed", date_from=start, date_to=end) or 0)


def _day(d: int, month: int = 11) -> datetime:
    return datetime(2026, month, d, tzinfo=timezone.utc)


@pytest.mark.parametrize("question,phrase", [
    ("how many jobs failed by 2026-11-08", "by 2026-11-08"),
    ("how many jobs failed through 2026-11-09", "through 2026-11-09"),
    ("how many jobs failed thru 2026-11-09", "thru 2026-11-09"),
    ("how many jobs failed starting 2026-11-09", "starting 2026-11-09"),
    ("how many jobs failed as of 2026-11-09", "as of 2026-11-09"),
    ("how many jobs failed prior to 2026-11-09", "prior to 2026-11-09"),
    ("how many jobs failed pre 2026-11-09", "pre 2026-11-09"),
    ("how many jobs failed pre-2026-11-09", "pre-2026-11-09"),
    ("how many jobs failed post 2026-11-08", "post 2026-11-08"),
    ("how many jobs failed 2026-11-08 onwards", "2026-11-08 onwards"),
    ("how many jobs failed from 2026-11-08 onwards", "2026-11-08 onwards"),
    ("how many jobs failed up to 2026-11-09", "up to 2026-11-09"),
    ("how many jobs failed until 2026-11-09", "until 2026-11-09"),
    ("how many jobs failed before 2026-11-09", "before 2026-11-09"),
    ("how many jobs failed after 2026-11-08", "after 2026-11-08"),
    ("how many jobs failed from 2026-11-08 until 2026-11-09", "until 2026-11-09"),
    ("how many jobs failed between 2026-11-08", "between 2026-11-08"),
    # Fuzzy spans: no exact window stands for them.
    ("how many jobs failed in the last hour and a half", "hour and a half"),
    ("how many jobs failed in the past day or two", "day or two"),
    ("how many jobs failed daily", "daily"),
    ("how many jobs failed weekly", "weekly"),
    ("how many jobs failed per day", "per day"),
])
def test_a_word_around_a_date_or_a_fuzzy_span_is_refused_by_name(ledger, question, phrase):
    response = pt.ask(ledger, question)
    answer = response.answer
    assert phrase.lower() in answer.lower(), f"the refusal must name {phrase!r}: {answer!r}"
    assert "can't" in answer.lower(), answer
    assert not pt.bold_ints(answer), f"a refused question states no count: {answer!r}"
    assert not pt.grounded(response)


@pytest.mark.parametrize("question,start,end,named", [
    ("how many jobs failed on 2026-11-08", _day(8), _day(9), "2026-11-08"),
    ("how many jobs failed since 2026-11-09", _day(9), NOW, "2026-11-09"),
    ("how many jobs failed from 2026-11-08 to 2026-11-09", _day(8), _day(10), "2026-11-08"),
    ("how many jobs failed between 2026-11-08 and 2026-11-09", _day(8), _day(10), "2026-11-09"),
    ("how many jobs failed from 2026-11-08 through 2026-11-09", _day(8), _day(10), "2026-11-09"),
])
def test_an_exact_date_form_is_applied_as_start_end_and_labelled(ledger, question, start, end, named):
    response = pt.ask(ledger, question)
    truth = _between(start, end)
    assert truth not in (0, _between(_day(1, 10), NOW)), "fixture: the window must change the count"
    assert pt.bold_ints(response.answer)[:1] == [truth], response.answer
    assert named in response.answer, "the answer names the window it applied"
    assert pt.grounded(response)
