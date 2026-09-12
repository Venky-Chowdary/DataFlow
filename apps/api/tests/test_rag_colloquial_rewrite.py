"""Chat-style wording must reach the same evidence as the docs heading.

Pilot is extractive, not generative. The rewrite is how different operator
phrasings — slang, filler, 'what about mysql' — hit the sentence the engine
already wrote. It must never invent a subject that was not in the question
or the prior CDC turn.
"""

from __future__ import annotations

from src.ai.copilot.followup import resolve_knowledge_engine_followup
from src.ai.rag.product_docs import compose_product_answer, retrieve_product_answer
from src.ai.rag.query_analysis import analyze_query, rewrite_operator_question


def test_rewrite_strips_chat_filler_and_expands_slang() -> None:
    assert rewrite_operator_question(
        "hey, gotta have logical wal for pg cdc right?"
    ).lower().startswith("do i need wal_level logical")
    assert "what is" in rewrite_operator_question("whats g3").lower()
    assert "export yaml" in rewrite_operator_question(
        "can viewers download the pipeline yaml"
    ).lower()
    assert rewrite_operator_question("how do I cook rice tonight") == (
        "how do I cook rice tonight"
    )


def test_slang_wal_question_leads_with_wal_level() -> None:
    body = " ".join(
        (
            compose_product_answer(
                retrieve_product_answer(
                    "gotta have logical wal for pg cdc right?", limit=4
                )
            )
            or ""
        ).split()
    )
    lead = body.split(". ")[0].lower()
    assert "wal_level" in lead
    assert "logical" in lead


def test_plain_english_quarantine_still_leads() -> None:
    body = " ".join(
        (
            compose_product_answer(
                retrieve_product_answer(
                    "explain like im 5: where do bad rows go", limit=4
                )
            )
            or ""
        ).split()
    )
    assert "quarantine" in body.split(". ")[0].lower()


def test_engine_followup_only_after_a_cdc_answer() -> None:
    history = [
        {
            "role": "assistant",
            "content": "Yes — Postgres CDC needs wal_level=logical.",
        }
    ]
    assert (
        resolve_knowledge_engine_followup("what about mysql?", history)
        == "do I need binlog_format ROW for mysql CDC"
    )
    assert resolve_knowledge_engine_followup("what about mysql?", []) is None
    assert (
        resolve_knowledge_engine_followup(
            "what about mysql?",
            [{"role": "assistant", "content": "You have 2 jobs."}],
        )
        is None
    )


def test_rewrite_does_not_create_terms_for_an_off_subject_question() -> None:
    analysis = analyze_query("hey whats the capital of France lol")
    assert "wal_level" not in analysis.expansions
    assert "cdc" not in analysis.expansions
