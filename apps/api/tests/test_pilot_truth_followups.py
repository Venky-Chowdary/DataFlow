"""Pilot ground truth — an elliptical follow-up never produces a wrong number (review M3).

"and yesterday?" after "how many jobs failed today" was refused as off-subject,
"and completed?" dropped the window the conversation was in, and "how many of
those were CDC?" was answered with the whole ledger. A follow-up that can be
resolved exactly carries the previous status / window; one that cannot is
refused by name. "summarize that" says it restates the earlier answer.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from tests.helpers import pilot_truth as pt

NOW = datetime(2026, 11, 11, 15, 0, tzinfo=timezone.utc)
TODAY = datetime(2026, 11, 11, tzinfo=timezone.utc)


@pytest.fixture
def chat(monkeypatch):
    pt.reset_workspace(monkeypatch)
    pt.freeze_clock(monkeypatch, NOW, "UTC")
    for i in range(2):
        pt.seed_job("failed", at=NOW - timedelta(hours=1 + i))
    for i in range(3):
        pt.seed_job("failed", at=NOW - timedelta(days=1, hours=i))
    for i in range(4):
        pt.seed_job("failed", at=NOW - timedelta(days=8, hours=i))
    for i in range(5):
        pt.seed_job("completed", at=NOW - timedelta(hours=2, minutes=i))
    for i in range(6):
        pt.seed_job("completed", at=NOW - timedelta(days=20, minutes=i))
    agent = pt.agent()
    history: list[dict] = []

    def say(question: str):
        response = pt.ask(agent, question, list(history), pilot_session_id="followups")
        history.extend([{"role": "user", "content": question}, {"role": "assistant", "content": response.answer}])
        return response

    return say


def _count(status: str, start: datetime, end: datetime) -> int:
    return int(pt.mongo().count_job_matches(status=status, date_from=start, date_to=end) or 0)


def test_and_yesterday_carries_the_status(chat):
    chat("how many jobs failed today")
    response = chat("and yesterday?")
    truth = _count("failed", TODAY - timedelta(days=1), TODAY)
    assert truth == 3
    assert pt.bold_ints(response.answer)[:1] == [truth], response.answer
    assert "yesterday" in response.answer


def test_what_about_last_week_carries_the_status(chat):
    chat("how many jobs failed today")
    response = chat("what about last week?")
    monday = TODAY - timedelta(days=TODAY.weekday())
    truth = _count("failed", monday - timedelta(days=7), monday)
    assert pt.bold_ints(response.answer)[:1] == [truth], response.answer


def test_and_completed_carries_the_window(chat):
    chat("how many jobs failed today")
    response = chat("and completed?")
    truth = _count("completed", TODAY, TODAY + timedelta(days=1))
    assert truth == 5
    assert pt.bold_ints(response.answer)[:1] == [truth], response.answer
    assert "today" in response.answer


def test_a_qualifier_on_those_is_refused_by_name(chat):
    # Round 2 (review 2, C2c): CDC is a sync mode the store filters by, so it is
    # applied (below); a qualifier nothing applies is the one refused by name.
    chat("how many jobs failed today")
    response = chat("how many of those were nightly?")
    assert "nightly" in response.answer.lower() and "can't filter" in response.answer.lower(), response.answer
    assert pt.bold_ints(response.answer) == []
    assert not pt.grounded(response)


def test_a_sync_mode_on_those_is_applied_with_the_earlier_filters(chat):
    """Review 2, C2c / H4: "of those were CDC" is the earlier question narrowed, not the ledger."""
    chat("how many jobs failed today")
    response = chat("how many of those were CDC?")
    truth = int(pt.mongo().count_job_matches(status="failed", sync_mode="cdc", date_from=TODAY,
                                             date_to=TODAY + timedelta(days=1)) or 0)
    stated = pt.bold_ints(response.answer)
    assert stated[:1] == [truth] or (truth == 0 and response.answer.startswith("None of")), response.answer
    assert 20 not in stated and "cdc" in response.answer.lower(), response.answer
    assert "today" in response.answer


def test_summarize_that_says_it_restates_the_earlier_answer(chat):
    chat("how many jobs failed today")
    response = chat("summarize that")
    assert "restat" in response.answer.lower() and "not re-read" in response.answer.lower(), response.answer
    assert not pt.grounded(response)


def test_a_follow_up_to_a_follow_up_keeps_the_conversation(chat):
    chat("how many jobs failed today")
    chat("and yesterday?")
    response = chat("and completed?")
    truth = _count("completed", TODAY - timedelta(days=1), TODAY)
    assert truth == 0
    assert response.answer.startswith("None of") and "completed" in response.answer, response.answer
    assert "yesterday" in response.answer
    # A qualifier nothing applies is refused (CDC is applied since review 2, C2c).
    cdc = chat("how many of those were nightly?")
    assert "nightly" in cdc.answer.lower() and "can't filter" in cdc.answer.lower(), cdc.answer
