"""Pilot ground truth — answer from a tool, or decline (audit H7).

A question about a live number (duration, success rate, rows moved, next run,
CDC lag) is either answered with the value a tool read, or declined. It is
never answered with a neighbouring job list or a documentation paragraph that
is then marked grounded because a tool happened to run.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from tests.helpers import pilot_truth as pt

NOW = pt.FROZEN_NOW


@pytest.fixture
def ledger(monkeypatch):
    pt.reset_workspace(monkeypatch)
    pt.freeze_clock(monkeypatch, NOW, "UTC")
    for i in range(3):
        pt.seed_job("failed", at=NOW - timedelta(days=3, minutes=i))
    for i in range(9):
        pt.seed_job("completed", at=NOW - timedelta(minutes=90 - i), records_processed=1000 + i)
    pt.seed_job("cancelled", at=NOW - timedelta(minutes=70))
    started = NOW - timedelta(minutes=47)
    last = pt.seed_job(
        "completed_with_quarantine", at=started, started_at=started,
        completed_at=started + timedelta(minutes=17), records_processed=958, rejected_rows=42,
    )
    return {"last": last}


def test_last_job_duration_is_computed_from_its_timestamps(ledger):
    response = pt.ask(pt.agent(), "how long did my last job take")
    assert ledger["last"] in response.answer
    assert "**17m 0s**" in response.answer
    assert pt.listed_job_ids(response.answer) == [ledger["last"]], "a job list is not a duration"
    assert pt.grounded(response)


def test_named_job_duration_is_stated(ledger):
    response = pt.ask(pt.agent(), f"how long did job {ledger['last']} take")
    assert "**17m 0s**" in response.answer
    assert pt.grounded(response)


def test_duration_of_an_unfinished_job_is_declined(ledger):
    running = pt.seed_job("running", at=NOW - timedelta(minutes=1))
    response = pt.ask(pt.agent(), "how long did my last job take")
    assert running in response.answer
    assert "no completion time" in response.answer
    assert "took **" not in response.answer, "a duration was stated for a job that has not finished"


def test_success_rate_is_computed_from_the_ledger_counts(ledger):
    by_status = pt.job_truth()["by_status"]
    completed = by_status["completed"] + by_status["completed_with_quarantine"]
    finished = completed + by_status["failed"]
    response = pt.ask(pt.agent(), "what is the success rate of my jobs")

    assert pt.bold_ints(response.answer)[:2] == [completed, finished] == [10, 13]
    assert f"**{100 * completed / finished:.1f}%**" in response.answer
    assert [t["name"] for t in response.tools_used] == ["list_jobs"]
    assert pt.grounded(response)


def test_rows_moved_in_a_window_is_the_sum_over_that_window(ledger):
    window = {"date_from": datetime(2026, 11, 11, tzinfo=timezone.utc),
              "date_to": datetime(2026, 11, 12, tzinfo=timezone.utc)}
    rows = sum(int(j.get("records_processed") or 0) for j in pt.mongo().list_jobs(limit=100, **window))
    response = pt.ask(pt.agent(), "how many rows did I move today")
    assert rows > 0 and pt.bold_ints(response.answer)[0] == rows
    assert "today (Wed 11 Nov 2026, UTC)" in response.answer


def test_rows_moved_is_declined_when_the_window_cannot_be_read_whole(ledger):
    for i in range(120):
        pt.seed_job("completed", at=NOW - timedelta(seconds=30 + i), records_processed=7)
    response = pt.ask(pt.agent(), "how many rows did I move today")
    assert "can't total rows" in response.answer
    assert pt.bold_ints(response.answer) == [pt.job_truth(
        date_from=datetime(2026, 11, 11, tzinfo=timezone.utc),
        date_to=datetime(2026, 11, 12, tzinfo=timezone.utc))["total"]]


@pytest.mark.parametrize("question", ["what is my CDC lag", "is my cdc pipeline behind", "how far behind is our CDC right now"])
def test_live_cdc_lag_is_declined_not_answered_from_docs(ledger, question):
    pt.seed_job("running", at=NOW - timedelta(minutes=2), sync_mode="cdc", operation="cdc")
    response = pt.ask(pt.agent(), question)
    assert "can't read live CDC lag" in response.answer
    assert not pt.grounded(response)
    assert response.confidence <= 0.4
    assert response.sources == []


def test_what_cdc_lag_means_is_still_a_documentation_answer(ledger):
    response = pt.ask(pt.agent(), "what is CDC lag")
    assert "can't read live CDC lag" not in response.answer


def test_next_run_of_a_named_pipeline_is_read_from_the_schedule(ledger):
    import services.schedule_store as schedule_store

    pt.seed_schedules(30)
    target = next(s for s in schedule_store.list_schedules() if s.name == "nightly-02")
    response = pt.ask(pt.agent(), "when does nightly-02 run next")
    assert [t["name"] for t in response.tools_used] == ["get_schedule"]
    assert target.next_run_at in response.answer
    assert pt.grounded(response)


def test_next_run_of_a_paused_pipeline_says_it_has_none(ledger):
    pt.seed_schedules(30)
    response = pt.ask(pt.agent(), "when does nightly-03 run next")
    assert [t["name"] for t in response.tools_used] == ["get_schedule"]
    assert "paused" in response.answer.lower()
    assert "no next run" in response.answer.lower()


def test_a_tool_that_ran_but_was_not_used_does_not_make_the_turn_grounded():
    from src.ai.copilot.agent import CopilotResponse
    from src.ai.copilot.pilot_agent import carries_evidence

    used = CopilotResponse(answer="x", intent="i", confidence=0.9, method="m",
                           tools_used=[{"name": "list_jobs", "success": True, "summary": "3 jobs"}])
    unused = CopilotResponse(answer="x", intent="i", confidence=0.9, method="m",
                             tools_used=[{"name": "list_jobs", "success": True, "summary": "3 jobs", "used": False}])
    assert carries_evidence(used) is True
    assert carries_evidence(unused) is False
