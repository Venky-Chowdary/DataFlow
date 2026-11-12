"""Pilot ground truth — the four mutants review 2 found surviving are killed.

Each case pins a guard that the mutation pass could switch off without any
test noticing:

* ``free_text_not_declared`` (``note_tool_result`` a no-op) and
  ``free_text_keys_narrow`` (``FREE_TEXT_KEYS = {"error"}``) are killed in
  ``test_pilot_truth_egress`` (declared-at-run-time and every-key cases);
* ``narrate_counts`` (``_UNNARRATED_TOOLS`` emptied) is killed by the count
  answer with a same-number narrator in ``test_pilot_truth_narration``;
* ``decline_grounded`` (``is_decline`` always False) is killed here: a turn
  that declines a jobs question is not grounded, and the check is made
  through the module, so switching it off is seen.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from tests.helpers import pilot_truth as pt

NOW = datetime(2026, 11, 11, 15, 0, tzinfo=timezone.utc)


@pytest.fixture
def ledger(monkeypatch):
    pt.reset_workspace(monkeypatch)
    pt.freeze_clock(monkeypatch, NOW, "UTC")
    for i in range(3):
        pt.seed_job("completed", at=NOW - timedelta(hours=1, minutes=i), records_processed=10)
    return pt.agent()


@pytest.mark.parametrize("question,decline", [
    ("how long do my jobs take", "I don't have a duration summary"),
])
def test_a_declined_jobs_answer_is_not_grounded(ledger, question, decline):
    response = pt.ask(ledger, question)
    assert decline in response.answer, response.answer
    assert not pt.grounded(response), "a decline was reported as a grounded answer"


def test_an_unfinished_last_job_duration_is_a_decline_not_grounded(ledger):
    running = pt.seed_job("running", at=NOW - timedelta(minutes=1))
    response = pt.ask(ledger, "how long did my last job take")
    assert running in response.answer and "no completion time" in response.answer, response.answer
    assert not pt.grounded(response)


def test_every_decline_sentence_is_recognised():
    from src.ai.copilot import job_narration

    for sentence in job_narration._DECLINES:
        assert job_narration.is_decline(f"Something. {sentence}. More.")
    assert not job_narration.is_decline("**3** of your **5** job(s) failed:")
