"""Engine-instance procedures and two-subject questions.

Two shapes the single-subject audit never exercised:

* "how do I connect to Snowflake" is the add-connector procedure with an
  engine as the instance. The per-engine procedure card is generated from the
  registry plus the connector catalog, so it exists for exactly the engines
  marked transfer-ready and the answer opens on New connection instead of on
  the engine's capability cards.
* "what is quarantine and how does reconcile work" names two subjects. Both
  the evidence window and the composed answer owe each subject a sentence,
  and a "what is" clause is owed a definitional one.

100% on this fixture only.
"""

from __future__ import annotations

import pytest

from src.ai.rag.product_docs import compose_product_answer, retrieve_product_answer
from src.ai.rag.product_facts import _connect_engine_sections


def _answer(question: str) -> str:
    answer = retrieve_product_answer(question, limit=5)
    return " ".join((compose_product_answer(answer) or "").split()).lower()


def _lead(question: str) -> str:
    return _answer(question).split(". ")[0]


def test_engine_procedures_are_generated_for_transfer_ready_engines_only() -> None:
    titles = {s.section_title for s in _connect_engine_sections()}
    assert "Procedure: connect a PostgreSQL database" in titles
    assert "Procedure: connect a Snowflake database" in titles
    assert "Procedure: connect a SQL Server database" in titles
    assert not any("Databricks" in t for t in titles), "planned engine must not get a procedure"


@pytest.mark.parametrize(
    "question,engine",
    [
        ("how do i connect to snowflake", "snowflake"),
        ("how do i connect to bigquery", "bigquery"),
        ("how do I add a mysql connector", "mysql"),
        ("how do i set up sql server", "sql server"),
        ("how do I connect a postgres database", "postgresql"),
    ],
)
def test_connect_an_engine_opens_on_new_connection(question: str, engine: str) -> None:
    lead = _lead(question)
    assert "new connection" in lead and engine in lead, lead


def test_snowflake_key_pair_guidance_stays_secondary() -> None:
    body = _answer("how do i connect to snowflake")
    assert body.index("new connection") < body.index("key-pair")


def test_planned_engine_is_not_given_a_procedure() -> None:
    body = _answer("can i connect databricks")
    assert "not a transfer-ready driver" in body
    assert "new connection" not in body


@pytest.mark.parametrize(
    "question,first,second",
    [
        ("what is quarantine and how does reconcile work", "quarantine", "checksum"),
        ("explain schema drift and the sync modes", "schema drift", "sync mode"),
        ("what is a pipeline and how do i pause one", "pipeline", "pause"),
    ],
)
def test_two_subject_questions_answer_both(question: str, first: str, second: str) -> None:
    body = _answer(question)
    assert first in body and second in body, body


def test_a_what_is_clause_gets_a_definition_first() -> None:
    assert _lead("what is a pipeline and how do i pause one").startswith("a **pipeline** is")


def test_single_subject_answers_are_unchanged_by_the_subject_debt() -> None:
    lead = _lead("is a green test on connectors enough to skip validation")
    assert "does **not** skip preflight" in lead, lead
    lead = _lead("do you use openai by default")
    assert "local engine" in lead, lead


def test_a_settled_negative_is_not_followed_by_how_to_do_it() -> None:
    """Browser QA: the Databricks refusal was followed by warehouse connect steps
    and a dispatch list that named databricks as an engine."""
    body = _answer("can i connect databricks")
    assert "configured the same way" not in body, body
    assert "pick the type under" not in body, body
    assert "not transfer-ready and cannot be connected: databricks" in body, body


def test_a_procedure_does_not_borrow_sibling_capability_cards() -> None:
    """Browser QA: the PostgreSQL procedure ended with the Aurora/Cloud SQL card's
    "Connect the instance as MySQL or PostgreSQL"."""
    body = _answer("how do I connect a postgres database")
    assert "connect the instance as mysql" not in body, body
    assert "cloud sql" not in body, body


def test_pause_wording_does_not_say_activate_turns_it_off() -> None:
    body = _answer("how do i pause a pipeline")
    assert "pause a saved pipeline from pipelines to turn it off" in body, body
    assert "activate turns it back on" in body, body
