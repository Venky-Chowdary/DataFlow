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


def test_source_chips_are_only_the_sections_the_answer_cites() -> None:
    """Browser QA: the PostgreSQL body was clean but the source chips still showed
    the Aurora and Cloud SQL cards the composer had set aside."""
    from src.ai.copilot.tools import get_pilot_tools

    result = get_pilot_tools().execute(
        "explain_product", {"query": "how do I connect a postgres database"}
    )
    assert result.success, result.error
    titles = [str(s["title"]) for s in result.output["sources"]]
    assert titles, result.output
    assert all("Procedure: connect a PostgreSQL" in t for t in titles), titles


# ── Quality wave: topic shares, procedure completion, tail cohesion ───────────


def test_question_topics_groups_terms_by_clause() -> None:
    from src.ai.rag.evidence_policy import is_subject_term
    from src.ai.rag.query_analysis import analyze_query, question_topics

    def topics(q: str) -> list[list[str]]:
        a = analyze_query(q)
        return question_topics(a.text, [t for t in a.terms if is_subject_term(t)])

    assert topics("explain preflight gates and sync modes") == [
        ["preflight", "gate"],
        ["sync", "mode"],
    ]
    assert len(topics("how do I connect to snowflake")) == 1
    assert len(topics("what is quarantine and what is reconcile")) == 2


def test_each_joined_topic_gets_its_share_of_the_answer() -> None:
    """"Explain preflight gates and sync modes" spent five sentences on sync
    modes and one imperative on gates; the gate half was three fragments of
    "it requires a cursor field; preflight refuses the run" from the sync-mode
    passage."""
    body = _answer("explain preflight gates and sync modes")
    assert "validate" in body and "gate" in body, body
    assert "sync mode upsert" in body, body
    assert "it requires a cursor field" not in body, body
    gate_sentences = [s for s in body.split(". ") if "gate" in s or "validate" in s]
    assert len(gate_sentences) >= 2, body


def test_second_topic_is_retrieved_from_its_own_section() -> None:
    """"Checksum MATCH" — the reconcile passage — sat below fusion depth and the
    answer's reconcile half was one step of the first-transfer walkthrough."""
    result = retrieve_product_answer("what is quarantine and what is reconcile")
    titles = {h.chunk.section_title for h in result.hits}
    assert "Checksum MATCH" in titles, titles
    body = compose_product_answer(result).lower()
    assert "row counts and content hashes" in body, body
    assert body.startswith("quarantine is"), body


def test_a_procedure_answer_keeps_its_later_steps() -> None:
    body = _answer("how do I connect to snowflake")
    assert "click new connection and pick the snowflake driver" in body, body
    assert "click test, and save" in body, body


def test_a_definition_is_not_followed_by_an_unrelated_sections_sentence() -> None:
    """"What is BYOK" ended on the audit-log retention sentence and "very large
    decimals" on the array-carriage rule — both from a passage retrieved on a
    common word, sharing no word with the question or the lead."""
    assert "audit log" not in _answer("what is BYOK")
    assert "g4 column mappings" not in _answer("what is the g7 gate").split(". ")[0]


def test_empty_string_and_null_are_distinct_values() -> None:
    body = _answer("how do you handle empty strings versus null")
    assert "empty string and a sql null are different values" in body, body
    assert "empty_string_as_null_cells" in body, body


def test_cdc_engines_come_from_the_capability_registry() -> None:
    from services.connector_capability_registry import CAPABILITY_REGISTRY, get_connector_capability

    expected = sorted(
        k
        for k in CAPABILITY_REGISTRY
        if get_connector_capability(k).get("supports_cdc")
        and get_connector_capability(k).get("transfer_ready")
    )
    body = _answer("which engines support CDC")
    assert f"{len(expected)} of them: " + ", ".join(expected) in body, body
    assert "at-least-once" in body, body
