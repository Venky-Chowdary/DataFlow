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


#: A documentation answer: since review 2 (H1) the only kind still narrated.
DOC = "what decimal precision does mysql support"


def test_kept_narration_with_changed_numbers_is_discarded(hybrid):
    narrator = hybrid(lambda d: d + "\n\nAll 12 columns are populated in 99% of rows.")
    response = pt.ask(pt.agent(), DOC)
    assert narrator.calls, "a documentation answer is still narrated"
    assert "99%" not in response.answer and response.method != "openai_polish", response.answer


def test_kept_narration_with_the_same_numbers_is_used(hybrid):
    narrator = hybrid(lambda d: "In short: " + d)
    response = pt.ask(pt.agent(), DOC)
    assert narrator.calls
    assert response.method == "openai_polish" and response.answer.startswith("In short: ")
    assert pt.grounded(response)


# ---- review 2, H1: a narrator cannot change meaning while keeping the numbers --
#
# The guard compared only the multiset of numbers, so a narrator that swapped
# two numbers between nouns, swapped column types, appended an invented cause
# or relabelled "Maximum" as "Minimum" was returned as ``openai_polish``, 0.96,
# grounded. Until the answer verifier exists, no tool-derived answer is
# narrated at all; a documentation answer is narrated only when every number
# keeps the words next to it.


def _swap_mysql(draft: str) -> str:
    import re

    # The reviewer's rewrite (b9): the two numbers that belong to mysql trade places.
    return re.sub(r"mysql (\d+) digits with scale up to (\d+)",
                  lambda m: f"mysql {m.group(2)} digits with scale up to {m.group(1)}", draft)


def test_a_doc_narration_that_moves_a_number_to_another_noun_is_discarded(hybrid):
    narrator = hybrid(_swap_mysql)
    response = pt.ask(pt.agent(), DOC)
    assert narrator.calls, "a documentation answer is narrated"
    assert response.method != "openai_polish", response.answer
    assert "mysql 38 digits with scale up to 30" in response.answer, response.answer


@pytest.mark.parametrize("label,question,rewrite,env", [
    ("schema types swapped", "what columns are in customers on CRM SQLite",
     lambda d: d.replace("INTEGER", "TMP").replace("TEXT", "INTEGER").replace("TMP", "TEXT"), {}),
    ("connection test with an invented cause", "test the connection to CRM SQLite",
     lambda d: d + " It was unreachable earlier because its credential expired; rotate the password "
                   "in Connectors and re-run every pipeline that uses it.", {}),
    ("connection test negated", "test the connection to CRM SQLite",
     lambda d: d.replace("is reachable", "is not reachable"), {}),
    ("cells allowed: query cells swapped", "run select id, amount from customers on CRM SQLite",
     lambda d: d.replace("| 1 | 10.0 |", "| 1 | 20.0 |").replace("| 2 | 20.0 |", "| 2 | 10.0 |"),
     {"DATAFLOW_AI_METADATA_ONLY": "false"}),
    ("cells allowed: maximum relabelled", "what is the max amount in customers on CRM SQLite",
     lambda d: d.replace("Maximum", "Minimum"), {"DATAFLOW_AI_METADATA_ONLY": "false"}),
    ("count answer, same numbers", "how many jobs failed", lambda d: "In short: " + d, {}),
    ("status answer, same numbers", "show me my failed jobs", lambda d: "Summary: " + d, {}),
])
def test_a_tool_derived_answer_is_never_narrated(hybrid, monkeypatch, label, question, rewrite, env):
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    narrator = hybrid(rewrite)
    response = pt.ask(pt.agent(), question)
    assert narrator.calls == 0, f"{label}: a tool-derived answer was sent to the narrator"
    assert response.method != "openai_polish", response.answer
    assert "Minimum" not in response.answer and "credential expired" not in response.answer


class _Native:
    """A native tool loop that answers with a confident, tool-backed count."""

    def __init__(self):
        self.calls = 0

    def __call__(self, message, history, system, data_context):
        from src.ai.copilot.agent import CopilotResponse

        self.calls += 1
        return CopilotResponse(answer="You have **42** failed rows.", intent="data", confidence=0.99,
                               method="openai_native", tools_used=[{"name": "run_query", "success": True}])


def test_a_count_question_never_goes_through_the_native_loop(hybrid, monkeypatch):
    from src.ai.copilot.agent import CopilotResponse
    from src.ai.copilot.pilot_agent import DataPilotAgent

    hybrid(lambda d: d)
    native = _Native()
    agent = pt.agent()
    monkeypatch.setattr(DataPilotAgent, "_first_available_native_agent", lambda self: native)
    failed_local = CopilotResponse(
        answer="I couldn't count that: the read timed out.", intent="data", confidence=0.5,
        method="pilot_local_engine", tools_used=[{"name": "aggregate_data", "success": False, "error": "timeout"}],
    )
    monkeypatch.setattr(DataPilotAgent, "_local_agent", lambda self, *a, **k: failed_local)
    response = pt.ask(agent, "how many rows in orders failed the check")
    assert native.calls == 0, "a count question raced the native loop"
    assert "42" not in response.answer and not pt.grounded(response)
