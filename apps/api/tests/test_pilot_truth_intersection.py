"""Pilot ground truth — a status and a verification verdict are applied together (review 2, H4).

"how many completed jobs failed verification" was answered with every job
that failed verification (the status was dropped whenever a verdict was
asked), and "how many of those failed verification?" after "how many jobs
completed yesterday" replaced the earlier question instead of narrowing it.
Both filters now reach the store together, and an "of those" follow-up keeps
every filter the earlier question applied.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from tests.helpers import pilot_truth as pt

NOW = datetime(2026, 11, 11, 15, 0, tzinfo=timezone.utc)
TODAY = datetime(2026, 11, 11, tzinfo=timezone.utc)
YESTERDAY = {"date_from": TODAY - timedelta(days=1), "date_to": TODAY}
FAILED_CHECK = {"passed": False, "rows_differ": 3}


@pytest.fixture
def ledger(monkeypatch):
    pt.reset_workspace(monkeypatch)
    pt.freeze_clock(monkeypatch, NOW, "UTC")
    y = NOW - timedelta(days=1)

    def job(status, at, **extra):
        pt.seed_job(status, at=at, destination_collection="dw", **extra)

    # Yesterday: completed 4 (1 failed verification, 1 passed), failed 3 (1 failed verification).
    job("completed", y, reconciliation=FAILED_CHECK, source_name="orders")
    job("completed", y - timedelta(minutes=1), reconciliation={"passed": True})
    job("completed", y - timedelta(minutes=2))
    job("completed", y - timedelta(minutes=3), source_name="orders")
    job("failed", y - timedelta(minutes=4), reconciliation=FAILED_CHECK)
    job("failed", y - timedelta(minutes=5), source_name="orders")
    job("failed", y - timedelta(minutes=6))
    # Today: 3 completed that failed verification, 1 partial that failed it, 1 failed that passed it.
    for i in range(3):
        job("completed", NOW - timedelta(hours=1, minutes=i), reconciliation=FAILED_CHECK)
    job("partial", NOW - timedelta(hours=2), reconciliation=FAILED_CHECK)
    # A failed run whose verification passed (it was verified before it failed).
    job("failed", NOW - timedelta(hours=3), reconciliation={"passed": True})
    return pt.agent()


def _truth(**filters) -> int:
    return int(pt.mongo().count_job_matches(**filters) or 0)


@pytest.mark.parametrize("question,filters", [
    ("how many completed jobs failed verification", {"status": "completed", "verification": "failed"}),
    ("how many failed jobs failed verification", {"status": "failed", "verification": "failed"}),
    ("how many jobs completed but failed verification", {"status": "completed", "verification": "failed"}),
    ("how many jobs completed and passed verification", {"status": "completed", "verification": "passed"}),
])
def test_a_status_and_a_verdict_are_applied_together(ledger, question, filters):
    truth = _truth(**filters)
    assert truth != _truth(verification=filters["verification"]), "fixture: the status must change the count"
    response = pt.ask(ledger, question)
    assert pt.bold_ints(response.answer)[:1] == [truth], response.answer
    assert filters["status"] in response.answer and "verification" in response.answer, response.answer
    assert pt.grounded(response)


def test_an_exact_status_with_a_verdict_is_refused_by_name(ledger):
    response = pt.ask(ledger, "how many partial jobs failed verification")
    assert "partial" in response.answer and "can't" in response.answer, response.answer
    assert pt.bold_ints(response.answer) == [] and not pt.grounded(response)


@pytest.fixture
def chat(ledger):
    history: list[dict] = []

    def say(question: str):
        response = pt.ask(ledger, question, list(history), pilot_session_id="intersection")
        history.extend([{"role": "user", "content": question}, {"role": "assistant", "content": response.answer}])
        return response

    return say


@pytest.mark.parametrize("first,follow,filters", [
    ("how many jobs completed yesterday", "how many of those failed verification?",
     {"status": "completed", "verification": "failed", **YESTERDAY}),
    ("how many jobs failed yesterday", "how many of those failed verification?",
     {"status": "failed", "verification": "failed", **YESTERDAY}),
    ("how many jobs failed verification yesterday", "how many of those completed?",
     {"status": "completed", "verification": "failed", **YESTERDAY}),
    ("how many orders jobs failed", "and yesterday?", {"status": "failed", "search": "orders", **YESTERDAY}),
    ("how many orders jobs completed yesterday", "how many of those failed verification?",
     {"status": "completed", "verification": "failed", "search": "orders", **YESTERDAY}),
])
def test_of_those_narrows_the_earlier_question(chat, first, follow, filters):
    chat(first)
    response = chat(follow)
    truth = _truth(**filters)
    broader = _truth(**{k: v for k, v in filters.items() if k not in ("status", "search")})
    assert truth != broader, "fixture: the earlier filters must change the count"
    stated = pt.bold_ints(response.answer)
    assert stated[:1] == [truth] or (truth == 0 and response.answer.startswith("None of")), response.answer
    assert "yesterday" in response.answer, response.answer


def test_of_those_with_a_contradicting_status_is_refused(chat):
    chat("how many jobs completed yesterday")
    response = chat("how many of those were running?")
    assert pt.bold_ints(response.answer) == [] and not pt.grounded(response), response.answer
