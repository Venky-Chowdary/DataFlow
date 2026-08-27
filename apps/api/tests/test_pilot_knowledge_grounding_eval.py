"""Measured knowledge-grounding fixture for Datawrap Pilot.

This is the accuracy contract for the chatbot knowledge path — not a marketing
score. ``100%`` here means every case in THIS fixture passed. It does not mean
every possible English question.

Algorithm under test (one owner each):
* Product answers come from ``product_doc_search`` (cited) or an explicit FAQ regex.
* Embedding nearest-neighbours are never spoken as knowledge.
* ``unsupported_question`` is the single refusal owner.
* Dialogue acts do not reclassify off-topic shorts as workspace just because
  history exists.
* History text is ``content`` or ``text`` — one reader.
* ``map_columns`` is not invoked; RAG does not invent a mapping confidence.
"""

from __future__ import annotations

import pytest

from src.ai.copilot.dialogue_acts import classify_dialogue_act
from src.ai.copilot.followup import last_assistant_content
from src.ai.copilot.pilot_agent import DataPilotAgent, carries_evidence
from src.ai.copilot.tools import DataPilotTools
from src.ai.rag.product_docs import product_doc_search

DOCUMENTED = [
    "What does quarantine mean in Datawrap?",
    "what are the preflight gates",
    "how do I add a connector",
    "what is a data contract",
]

OFF_TOPIC = [
    "how do I cook rice",
    "what is the weather in Paris",
    "who won the world cup in 1998",
]

# Product-ish vocabulary, no Help page that can vouch for the ask.
ON_VOCAB_NO_DOC = [
    "what semantic patterns match subscriber_id column naming",
    "explain the ontology shard for telecom subscriber identifiers",
]


def _pass(rows: list[tuple[str, bool, str]]) -> None:
    failed = [(name, why) for name, ok, why in rows if not ok]
    assert not failed, (
        f"{len(rows) - len(failed)}/{len(rows)} passed on this fixture; "
        f"failed: {failed}"
    )


def test_documented_questions_are_cited_not_vector():
    tools = DataPilotTools()
    rows = []
    for q in DOCUMENTED:
        hits = product_doc_search(q)
        tr = tools._search_knowledge(q)
        o = tr.output or {}
        ok = (
            bool(hits)
            and tr.success
            and o.get("grounded") is True
            and o.get("source") == "product_documentation"
            and bool(o.get("sources"))
            and "650+" not in (o.get("answer") or "")
        )
        rows.append((q, ok, f"source={o.get('source')} grounded={o.get('grounded')}"))
    _pass(rows)


def test_off_topic_and_on_vocab_no_doc_refuse_without_vector_narration():
    tools = DataPilotTools()
    rows = []
    for q in OFF_TOPIC + ON_VOCAB_NO_DOC:
        tr = tools._search_knowledge(q)
        o = tr.output or {}
        answer = (o.get("answer") or "").lower()
        ok = (
            tr.success
            and o.get("grounded") is False
            and o.get("source") == "unsupported_question"
            and o.get("count") == 0
            and "will not answer it from guesswork" in answer
            and "here's what matches" not in answer
            and "synonym group" not in answer
        )
        rows.append((q, ok, f"source={o.get('source')} answer={answer[:80]!r}"))
    _pass(rows)


def test_explain_product_does_not_prepend_uncited_faq_over_docs():
    tools = DataPilotTools()
    tr = tools._explain_product("what are the preflight gates")
    o = tr.output or {}
    assert o.get("grounded") is True
    assert o.get("source") == "product_documentation"
    assert o.get("sources")
    # Curated G1–G9 blurb must not sit above the cited Help text.
    assert not (o.get("answer") or "").startswith("Preflight has **9 gates**")


def test_pilot_chat_knowledge_fixture_pass_rate():
    """End-to-end: documented cited, off-topic refused at 0.2, no invented counts."""
    agent = DataPilotAgent()
    rows: list[tuple[str, bool, str]] = []

    for q in DOCUMENTED:
        resp = agent.chat(q)
        src_kinds = {str(s.get("type") or "") for s in (resp.sources or [])}
        ok = (
            carries_evidence(resp)
            and "product_doc" in src_kinds
            and "650+" not in (resp.answer or "")
            and "will not answer it from guesswork" not in (resp.answer or "")
        )
        rows.append((f"doc:{q}", ok, f"conf={resp.confidence} src={src_kinds}"))

    hist = [{"role": "assistant", "text": "You have **2** recent jobs. **1** failed."}]
    for q in OFF_TOPIC:
        resp = agent.chat(q, history=hist)
        ok = (
            resp.confidence == pytest.approx(0.2, abs=0.01)
            and carries_evidence(resp) is False
            and resp.sources == []
            and "will not answer it from guesswork" in (resp.answer or "")
            and classify_dialogue_act(q, history=hist) == "general"
        )
        rows.append((f"off:{q}", ok, f"conf={resp.confidence} act={classify_dialogue_act(q, history=hist)}"))

    # History ``text`` field must still feed coreference readers.
    rows.append((
        "history_text_ssot",
        last_assistant_content(hist) == "You have **2** recent jobs. **1** failed.",
        last_assistant_content(hist),
    ))

    passed = sum(1 for _, ok, _ in rows if ok)
    # Named-fixture floor — 100% means this list only.
    assert passed == len(rows), (
        f"{passed}/{len(rows)} on knowledge-grounding fixture; "
        f"failed={[n for n, ok, _ in rows if not ok]}"
    )


def test_search_knowledge_never_returns_vector_knowledge_source():
    tools = DataPilotTools()
    for q in DOCUMENTED + OFF_TOPIC + ON_VOCAB_NO_DOC:
        src = (tools._search_knowledge(q).output or {}).get("source")
        assert src in {"product_documentation", "unsupported_question"}, (q, src)
