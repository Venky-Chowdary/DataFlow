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
    assert rewrite_operator_question(
        "do I gotta have replica identity full or nah"
    ).lower() == "do i need replica identity full"
    assert rewrite_operator_question("G3?").lower() == "what is g3"
    assert rewrite_operator_question(
        "i was wondering where bad rows go"
    ).lower() == "where do bad rows end up"
    assert rewrite_operator_question(
        "can you tell me if viewers can export yaml"
    ).lower() == "can a viewer export yaml"
    assert rewrite_operator_question(
        "wait so do I need logical decoding for postgres cdc"
    ).lower().startswith("do i need wal_level logical")
    assert rewrite_operator_question("rice?") == "rice?"
    assert "same cdc change twice" in rewrite_operator_question(
        "what if I replay the same change"
    ).lower()
    assert "rejected" in rewrite_operator_question(
        "can I replay rejected rows"
    ).lower()


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
    assert (
        resolve_knowledge_engine_followup("same question but for mysql", history)
        == "do I need binlog_format ROW for mysql CDC"
    )


def test_rewrite_does_not_create_terms_for_an_off_subject_question() -> None:
    analysis = analyze_query("hey whats the capital of France lol")
    assert "wal_level" not in analysis.expansions
    assert "cdc" not in analysis.expansions


def _lead(question: str) -> str:
    body = " ".join(
        (
            compose_product_answer(retrieve_product_answer(question, limit=4))
            or ""
        ).split()
    )
    return body.split(". ")[0].lower()


def test_chat_wrappers_still_lead_on_the_documented_subject() -> None:
    assert "g3" in _lead("G3?")
    assert "quarantine" in _lead("i was wondering where bad rows go")
    assert "viewer" in _lead("can you tell me if viewers can export yaml")
    assert "wal_level" in _lead(
        "wait so do I need logical decoding for postgres cdc"
    )
    assert "_df_lsn" in _lead("is _df_lsn how you skip dupes")
    assert "_df_lsn" in _lead("what if I replay the same change")
