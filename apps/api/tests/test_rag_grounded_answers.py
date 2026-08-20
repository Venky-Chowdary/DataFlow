"""Chatbot/RAG grounding contract.

The failure these lock down: a configured provider made the assistant *look* like
working RAG while the answer rested on nothing. Any provider reply was accepted as
grounded, the retrieved passages never reached the caller, and the local provider —
which emits a column-analysis JSON document whatever it is asked — answered prose
questions. So an operator got a confident paragraph, `sources: []`, and no way to
tell a documented answer from a guess.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.ai.llm.provider import (  # noqa: E402
    DataTransferLocalProvider,
    DataTransferOpenAIProvider,
    LLMResponse,
)
from src.ai.rag.generator import UNGROUNDED_CONFIDENCE, DataTransferRAGGenerator  # noqa: E402
from src.ai.rag.lexical_index import Bm25Index, content_terms, normalize, tokenize  # noqa: E402
from src.ai.rag.product_docs import (  # noqa: E402
    HELP_CORPUS_PATH,
    compose_documented_answer,
    load_product_doc_chunks,
    nearest_articles,
    product_doc_search,
)
from src.ai.rag.retriever import RetrievalResult  # noqa: E402

DOCUMENTED_QUESTIONS = [
    "What does quarantine mean in Datawrap?",
    "how do I use the MCP server",
    "what are the preflight gates",
    "how do I add a connector",
    "what is a data contract",
    "how do I prove the row counts matched after a transfer",
    "how do I schedule a pipeline every hour",
]

OFF_TOPIC_QUESTIONS = [
    "how do I cook rice",
    "what is the weather in Paris",
    "who won the world cup in 1998",
    "zzzz qqqq wwww",
]


# --- corpus -----------------------------------------------------------------

def test_help_corpus_is_present_and_self_consistent():
    payload = json.loads(HELP_CORPUS_PATH.read_text(encoding="utf-8"))
    chunks = load_product_doc_chunks()
    assert payload["chunk_count"] == len(payload["chunks"])
    assert len(chunks) == payload["chunk_count"]
    assert chunks, "an empty corpus means Pilot can answer nothing from documentation"
    for chunk in chunks:
        assert chunk.id and chunk.doc_title and chunk.section_title and chunk.text
        assert chunk.href.startswith("#/help/")


# --- lexical index ----------------------------------------------------------

def test_short_names_normalize_onto_the_documented_spelling():
    assert normalize("postgres") == "postgresql"
    assert normalize("mongo") == "mongodb"
    assert tokenize("Postgres tables") == ["postgresql", "table"]
    assert tokenize("batches") == tokenize("batch")
    assert tokenize("policies") == tokenize("policy")
    assert "full_refresh" in tokenize("use full_refresh here")


def test_stopwords_carry_no_signal_and_domain_words_do():
    terms = content_terms("How do I fix a failing gate?")
    assert "how" not in terms and "do" not in terms
    assert "gate" in terms and "fix" in terms


def test_grounding_counts_terms_the_corpus_cannot_answer():
    index = Bm25Index([
        ("a", "Quarantine holds rejected rows with column, value and reason."),
        ("b", "Schedules run a pipeline on a cadence."),
    ])
    covered = index.search("quarantine rejected rows")
    assert covered and covered[0].grounding == pytest.approx(1.0)

    partial = index.search("quarantine kubernetes helm chart")
    assert partial, "a partial match must still rank, so the floor can reject it"
    assert partial[0].grounding < 0.5


# --- retrieval --------------------------------------------------------------

@pytest.mark.parametrize("question", DOCUMENTED_QUESTIONS)
def test_documented_question_retrieves_citable_evidence(question: str):
    hits = product_doc_search(question)
    assert hits, question
    top = hits[0].as_source()
    assert top["type"] == "product_doc"
    assert top["doc"] and top["section"] and top["text"]
    assert top["href"].startswith("#/help/")
    assert 0.0 < float(top["grounding"]) <= 1.0
    assert top["matched_terms"]


@pytest.mark.parametrize("question", OFF_TOPIC_QUESTIONS)
def test_off_topic_question_retrieves_no_evidence(question: str):
    assert product_doc_search(question) == []


def test_composed_answer_quotes_the_sections_and_names_them():
    hits = product_doc_search("what does quarantine mean")
    answer = compose_documented_answer(hits)
    assert "Source:" in answer
    assert hits[0].chunk.citation in answer
    body = hits[0].chunk.text.strip().splitlines()[-1][:40]
    assert body in answer


def test_nearest_articles_are_leads_not_evidence():
    leads = nearest_articles("how do I cook rice")
    assert all(isinstance(t, str) for t in leads)
    assert product_doc_search("how do I cook rice") == []


# --- generator: provider behaviour -----------------------------------------

class _FakeChain:
    """Stands in for the provider chain with a scripted prose reply."""

    def __init__(self, reply: str | None = None, *, fail: bool = False, raise_exc: bool = False):
        self.reply = reply
        self.fail = fail
        self.raise_exc = raise_exc
        self.prompts: list[str] = []

    def generate_prose(self, prompt: str, system: str = "") -> LLMResponse:
        self.prompts.append(prompt)
        if self.raise_exc:
            raise RuntimeError("provider exploded")
        if self.fail:
            return LLMResponse(content="", success=False, provider="openai")
        return LLMResponse(content=self.reply or "", success=True, provider="openai")


def _retrieval(question: str) -> RetrievalResult:
    return RetrievalResult(
        query=question,
        documents=[],
        canonical_form=None,
        matched_pattern=None,
        synonym_matches=[],
        confidence=0.0,
        product_docs=product_doc_search(question),
    )


def _generator(chain) -> DataTransferRAGGenerator:
    gen = DataTransferRAGGenerator()
    gen._llm = chain
    return gen


QUESTION = "What does quarantine mean in Datawrap?"


def test_no_provider_answers_from_the_documentation_with_sources():
    result = _generator(None).generate_natural_language_response(QUESTION, _retrieval(QUESTION))
    assert result.method == "product_doc"
    assert result.grounded is True
    assert result.sources and all(s["type"] == "product_doc" for s in result.sources)
    assert 0.5 <= result.confidence <= 0.95


def test_provider_that_ignores_the_prompt_is_not_published_as_grounded():
    chain = _FakeChain("STUB_ANSWER: grounded reply from the stub provider.")
    result = _generator(chain).generate_natural_language_response(QUESTION, _retrieval(QUESTION))
    assert chain.prompts, "the passages must be sent to the provider"
    assert "quarantine" in chain.prompts[0].lower()
    assert result.method == "product_doc"
    assert "STUB_ANSWER" not in result.answer
    assert result.sources


def test_structured_document_is_never_served_as_an_answer():
    chain = _FakeChain(json.dumps({"analysis": [], "reasoning": ["Column 'x' → 'unknown'"]}))
    result = _generator(chain).generate_natural_language_response(QUESTION, _retrieval(QUESTION))
    assert result.method == "product_doc"
    assert "analysis" not in result.answer


def test_provider_narration_that_keeps_the_evidence_is_accepted():
    hits = product_doc_search(QUESTION)
    narration = (
        "Quarantine isolates rejected rows with their column, value and reason "
        "instead of dropping them, and you replay them after the fix. "
        + " ".join(hits[0].matched_terms)
    )
    result = _generator(_FakeChain(narration)).generate_natural_language_response(
        QUESTION, _retrieval(QUESTION)
    )
    assert result.method == "product_doc_llm"
    assert result.grounded is True
    assert result.sources
    assert result.answer == narration


@pytest.mark.parametrize("chain", [_FakeChain(fail=True), _FakeChain(raise_exc=True), _FakeChain("")])
def test_provider_failure_falls_back_to_the_documented_text(chain):
    result = _generator(chain).generate_natural_language_response(QUESTION, _retrieval(QUESTION))
    assert result.method == "product_doc"
    assert result.grounded is True
    assert result.sources


@pytest.mark.parametrize("question", OFF_TOPIC_QUESTIONS)
def test_uncovered_question_is_refused_at_low_confidence(question: str):
    chain = _FakeChain("Sure! Here is a confident answer about anything you asked.")
    result = _generator(chain).generate_natural_language_response(question, _retrieval(question))
    assert result.method == "ungrounded"
    assert result.grounded is False
    assert result.sources == []
    assert result.confidence == UNGROUNDED_CONFIDENCE
    assert "does not cover" in result.answer
    assert not chain.prompts, "nothing was retrieved, so no provider call may be made"


# --- provider chain ---------------------------------------------------------

def test_local_provider_is_excluded_from_prose_answers():
    from src.ai.llm.fallback import DataTransferFallbackChain

    assert DataTransferLocalProvider().speaks_prose is False
    assert DataTransferOpenAIProvider.speaks_prose is True

    chain = DataTransferFallbackChain()
    chain.providers = [DataTransferLocalProvider()]
    assert chain.generate_prose("explain quarantine").success is False
    # the same provider still serves the structured column path
    assert chain.generate("analyze column 'email'").success is True


# --- API surface ------------------------------------------------------------

def test_query_natural_language_carries_sources_and_grounded_flag():
    from src.ai import query_natural_language

    documented = query_natural_language(QUESTION)
    assert documented["grounded"] is True
    assert documented["sources"]
    assert documented["method"].startswith("product_doc")

    refused = query_natural_language("how do I cook rice")
    assert refused["grounded"] is False
    assert refused["sources"] == []
    assert refused["confidence"] == UNGROUNDED_CONFIDENCE


def test_rag_query_endpoint_publishes_the_evidence():
    import asyncio

    from src.routers.ai_router import RAGQueryRequest, api_rag_query

    response = asyncio.run(api_rag_query(RAGQueryRequest(query=QUESTION)))
    assert response.grounded is True
    assert response.sources
    assert response.sources[0]["href"].startswith("#/help/")

    refused = asyncio.run(api_rag_query(RAGQueryRequest(query="how do I cook rice")))
    assert refused.grounded is False
    assert refused.sources == []
    assert refused.confidence == UNGROUNDED_CONFIDENCE


@pytest.mark.parametrize(
    "question",
    [
        "what does quarantine mean",
        "how do I use the MCP server",
        "how do I prove the row counts matched",
        "what are the preflight gates",
    ],
)
def test_pilot_chat_answers_documented_questions_with_citations(question: str):
    from src.ai.copilot.pilot_agent import DataPilotAgent

    reply = DataPilotAgent().chat(question)
    assert reply.sources, question
    assert reply.sources[0]["href"].startswith("#/help/")
    assert "Source:" in reply.answer
    assert reply.confidence >= 0.9


@pytest.mark.parametrize("question", ["how do I cook rice", "who won the world cup in 1998"])
def test_pilot_chat_does_not_sound_certain_without_evidence(question: str):
    from src.ai.copilot.pilot_agent import DataPilotAgent

    reply = DataPilotAgent().chat(question)
    assert reply.sources == []
    assert reply.confidence <= 0.4, (question, reply.confidence, reply.answer[:200])


def test_pilot_chat_cites_documentation_for_a_curated_definition():
    """The curated definition is a lead, not a substitute for a traceable page."""
    from src.ai.copilot.pilot_agent import DataPilotAgent

    reply = DataPilotAgent().chat("what is append mode")
    assert "Append" in reply.answer
    assert reply.sources and reply.sources[0]["href"].startswith("#/help/")


@pytest.mark.parametrize(
    ("question", "is_aggregate"),
    [
        ("what does quarantine mean in Datawrap?", False),
        ("what does this column mean", False),
        ("what does upsert mean", False),
        ("what is the mean of amount in orders on Local Postgres", True),
        ("what is the mean salary in employees", True),
        ("average salary in employees", True),
        ("how many rows in airports", True),
    ],
)
def test_definitional_mean_is_not_parsed_as_an_average(question: str, is_aggregate: bool):
    from src.ai.copilot.tools import infer_tools_from_message

    planned = {name for name, _ in infer_tools_from_message(question)}
    assert ("aggregate_data" in planned) is is_aggregate, (question, planned)


def test_polish_keeps_facts_only_when_the_rewrite_carries_them():
    from src.ai.rag.evidence import keeps_draft_facts

    draft = "Quarantine held 1,204 rows from `orders` on job pf_2f9a1c for a failed cast."
    good = (
        "1,204 rows from `orders` were quarantined on job pf_2f9a1c because a cast failed."
    )
    assert keeps_draft_facts(draft, good)
    assert not keeps_draft_facts(draft, "Sure! Here is a friendly summary of your data.")
    # A dropped count is a wrong answer even though the prose reads plausibly.
    assert not keeps_draft_facts(
        draft, "Some rows from `orders` were quarantined on job pf_2f9a1c after a cast failed."
    )
    assert not keeps_draft_facts(draft, "")
    # Spelling a small count out is a rewrite, not a lost fact.
    assert keeps_draft_facts("3 jobs listed.", "You have three transfer jobs listed.")
    assert not keeps_draft_facts("3 jobs listed.", "You have several transfer jobs listed.")


def test_provider_that_ignores_the_prompt_cannot_replace_a_grounded_answer(monkeypatch):
    from src.ai.copilot import pilot_agent as pa
    from src.ai.llm.provider import LLMResponse

    class _Ignoring:
        def generate(self, prompt: str, system: str = "", max_tokens: int = 0) -> LLMResponse:
            return LLMResponse(
                content="Absolutely! I'd be delighted to help with whatever you need today.",
                success=True,
                provider="openai",
            )

    monkeypatch.setattr(
        "src.ai.llm.provider.pick_narration_provider",
        lambda: (_Ignoring(), "openai_polish"),
    )
    agent = pa.DataPilotAgent()
    local = pa.CopilotResponse(
        answer="Quarantine held 1,204 rows from `orders`.",
        intent="product_help",
        confidence=0.9,
        method="pilot_local_engine",
        tools_used=[{"name": "search_knowledge", "success": True, "summary": "3 hits"}],
        sources=[{"title": "Quarantine & replay", "href": "#/help/quarantine"}],
    )
    kept = agent._polish_with_llm("what does quarantine mean", [], local, system="")
    assert kept.answer == local.answer
    assert kept.method == "pilot_local_engine"


def test_narration_never_raises_confidence(monkeypatch):
    from src.ai.copilot import pilot_agent as pa
    from src.ai.llm.provider import LLMResponse

    class _Faithful:
        def generate(self, prompt: str, system: str = "", max_tokens: int = 0) -> LLMResponse:
            return LLMResponse(
                content="Quarantine held 1,204 rows from `orders` after a failed cast.",
                success=True,
                provider="openai",
            )

    monkeypatch.setattr(
        "src.ai.llm.provider.pick_narration_provider",
        lambda: (_Faithful(), "openai_polish"),
    )
    local = pa.CopilotResponse(
        answer="Quarantine held 1,204 rows from `orders`.",
        intent="product_help",
        confidence=0.7,
        method="pilot_local_engine",
        tools_used=[{"name": "search_knowledge", "success": True, "summary": "3 hits"}],
        sources=[{"title": "Quarantine & replay", "href": "#/help/quarantine"}],
    )
    polished = pa.DataPilotAgent()._polish_with_llm("q", [], local, system="")
    assert polished.method == "openai_polish"
    assert polished.confidence == 0.7


def test_unsupported_question_is_refused_by_the_product_tool():
    from src.ai.copilot.tools import get_pilot_tools

    out = get_pilot_tools()._explain_product("how do I cook rice").output
    assert out["intent"] == "unsupported"
    assert out["grounded"] is False
    assert out["sources"] == []
    assert "will not answer it from guesswork" in out["answer"]


def test_knowledge_search_serves_cited_documentation_first():
    from src.ai.copilot.tools import get_pilot_tools

    out = get_pilot_tools()._search_knowledge("how do I inspect quarantine").output
    assert out["grounded"] is True
    assert out["source"] == "product_documentation"
    assert out["sources"] and out["answer"]
    assert all(h["type"] == "product_doc" for h in out["hits"])


def test_copilot_explain_product_answers_from_documentation():
    from src.ai.copilot.tools import get_pilot_tools

    result = get_pilot_tools()._explain_product("how do I use the MCP server")
    assert result.success
    assert result.output["source"] == "product_documentation"
    assert result.output["grounded"] is True
    assert result.output["sources"]
    assert "MCP" in result.output["answer"]


def test_curated_definition_is_delivered_with_documentation_citations():
    """A definition the operator cannot trace to a page is not an answer."""
    from src.ai.copilot.tools import get_pilot_tools

    out = get_pilot_tools()._explain_product("What is append mode?").output
    assert out["source"] == "product_documentation"
    assert out["grounded"] is True
    assert out["sources"]
    # The precise curated definition survives as the lead paragraph.
    assert "**Append** adds source rows" in out["answer"]


def test_a_refusal_is_never_handed_to_a_provider_to_rewrite(monkeypatch):
    from src.ai.copilot import pilot_agent as pa
    from src.ai.llm.provider import LLMResponse

    class _Helpful:
        def generate(self, prompt: str, system: str = "", max_tokens: int = 0) -> LLMResponse:
            return LLMResponse(
                content="To cook rice, rinse it, then simmer two parts water to one part rice.",
                success=True,
                provider="openai",
            )

    called: list[str] = []
    monkeypatch.setattr(
        "src.ai.llm.provider.pick_narration_provider",
        lambda: (called.append("picked") or (_Helpful(), "openai_polish")),
    )
    local = pa.CopilotResponse(
        answer="That is outside what the Datawrap documentation covers.",
        intent="unsupported",
        confidence=0.2,
        method="pilot_local_engine",
        tools_used=[{"name": "search_knowledge", "success": True, "summary": "no hits"}],
    )
    kept = pa.DataPilotAgent()._polish_with_llm("how do I cook rice", [], local, system="")
    assert kept is local
    assert called == []


def test_copilot_chat_reports_grounded_only_when_evidence_exists():
    from fastapi.testclient import TestClient

    from src.main import app

    client = TestClient(app)
    documented = client.post(
        "/api/v1/copilot/chat", json={"message": "What does quarantine mean in Datawrap?"}
    ).json()
    assert documented["grounded"] is True
    assert documented["sources"]

    refused = client.post("/api/v1/copilot/chat", json={"message": "how do I cook rice"}).json()
    assert refused["grounded"] is False
    assert refused["sources"] == []
    assert refused["confidence"] <= 0.3
