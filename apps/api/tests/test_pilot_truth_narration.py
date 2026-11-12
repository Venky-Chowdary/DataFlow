"""Pilot ground truth — a narrator cannot change the facts of a grounded answer (review H5).

In hybrid mode a narrator rewrote "**3** of … failed" as "**13** of the **3**
job(s) … failed" and appended an invented cause, and the result came back as
``openai_polish``, 0.96, grounded. Until a full answer verifier exists:
deterministic count and status answers are not narrated at all, and any
narration that is kept must state exactly the draft's numbers.
"""

from __future__ import annotations

import pytest

from tests.helpers import pilot_truth as pt


class _Narrator:
    name = "openai"

    def __init__(self, rewrite):
        self.rewrite = rewrite
        self.calls = 0

    def is_available(self) -> bool:
        return True

    def generate(self, prompt, system="", max_tokens=1024):
        from src.ai.llm.provider import LLMResponse

        self.calls += 1
        draft = prompt.split("Draft answer:\n", 1)[-1].strip()
        return LLMResponse(content=self.rewrite(draft), success=True, provider="openai")


@pytest.fixture
def hybrid(monkeypatch, tmp_path):
    from src.ai.llm import provider

    pt.reset_workspace(monkeypatch)
    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "hybrid")
    pt.seed_sqlite_connector(tmp_path)
    for i in range(3):
        pt.seed_job("failed", minutes_ago=5 + i)  # no error text: C1 would skip narration anyway
    for i in range(4):
        pt.seed_job("completed", minutes_ago=50 + i)

    def use(rewrite):
        narrator = _Narrator(rewrite)
        monkeypatch.setattr(provider, "pick_narration_provider", lambda: (narrator, "openai_polish"))
        return narrator

    return use


@pytest.mark.parametrize("question", ["how many jobs failed today", "show me my failed jobs", "how many jobs do I have"])
def test_count_and_status_answers_are_not_narrated(hybrid, question):
    hybrid(lambda d: d.replace("**3** of", "**13** of")
           + "\n\nThe failures were caused by an expired Snowflake credential; re-running fixes all 13.")
    response = pt.ask(pt.agent(), question)
    assert response.method != "openai_polish", response.answer
    # (Job ids are hex and may contain "13": the stated numbers are what count.)
    assert 13 not in pt.bold_ints(response.answer) and "Snowflake credential" not in response.answer
    truth = pt.job_truth()
    assert set(pt.bold_ints(response.answer)) <= {truth["total"], truth["by_status"].get("failed", 0),
                                                  truth["by_status"].get("completed", 0), 7, 3, 4}


def test_kept_narration_with_changed_numbers_is_discarded(hybrid):
    narrator = hybrid(lambda d: d + "\n\nAll 12 columns are populated in 99% of rows.")
    response = pt.ask(pt.agent(), "what columns are in customers on CRM SQLite")
    assert narrator.calls, "a schema answer is still narrated"
    assert "99%" not in response.answer and response.method != "openai_polish", response.answer


def test_kept_narration_with_the_same_numbers_is_used(hybrid):
    narrator = hybrid(lambda d: "In short: " + d)
    response = pt.ask(pt.agent(), "what columns are in customers on CRM SQLite")
    assert narrator.calls
    assert response.method == "openai_polish" and response.answer.startswith("In short: ")
    assert pt.grounded(response)
