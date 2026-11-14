"""Pilot ground truth — pipeline health is computed over every pipeline (review H3).

Failed-last-run, parked-on-approval and the soonest next run were read from
the first 20 rows of the pipelines page, so a workspace whose failing or parked
pipelines sat beyond row 20 was called healthy, and "next due" was the newest
pipeline's next run rather than the soonest one. "How many pipelines failed
their last run" was answered from the job ledger.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from tests.helpers import pilot_truth as pt


@pytest.fixture
def pipelines(monkeypatch):
    """30 enabled pipelines: 6 failed last run and 1 parked, all beyond row 20."""
    import services.schedule_store as ss

    pt.reset_workspace(monkeypatch)
    rows = pt.seed_schedules(30, enabled_every=1)
    for s in rows[22:28]:
        s.last_status = "failed"
    rows[29].approval_request = {"status": "open", "finding": "schema drift"}
    ss._save_all(rows)
    return pt.agent()


@pytest.mark.parametrize("question", ["why are my schedules not running", "are my pipelines healthy"])
def test_health_counts_failed_and_parked_beyond_the_first_page(pipelines, question):
    response = pt.ask(pipelines, question)
    stated = pt.bold_ints(response.answer)
    assert 6 in stated, f"failed-last-run pipelines not counted: {response.answer!r}"
    assert 1 in stated and "parked" in response.answer, response.answer
    assert pt.grounded(response)


def test_parked_question_names_the_parked_pipeline(pipelines):
    response = pt.ask(pipelines, "which pipelines are parked on approval")
    assert "nightly-29" in response.answer, response.answer


def test_failed_last_run_is_answered_from_the_pipelines(pipelines):
    response = pt.ask(pipelines, "how many pipelines failed their last run")
    assert "No transfer jobs" not in response.answer
    assert pt.bold_ints(response.answer)[:1] == [6], response.answer


def test_next_due_is_the_soonest_run_not_the_newest_pipeline(monkeypatch):
    import services.schedule_store as ss

    pt.reset_workspace(monkeypatch)
    rows = pt.seed_schedules(6, enabled_every=1)
    soonest = datetime.now(timezone.utc) + timedelta(minutes=2)
    rows[4].next_run_at = soonest.isoformat()
    ss._save_all(rows)
    response = pt.ask(pt.agent(), "why are my schedules not running")
    # The lead's "Next due" — not the row list, which shows every pipeline's next run.
    due = response.answer.split("Next due:", 1)[-1].splitlines()[0] if "Next due:" in response.answer else ""
    assert "nightly-04" in due and soonest.isoformat() in due, response.answer
