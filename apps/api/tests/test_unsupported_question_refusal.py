"""An off-subject question must be refused, not answered from nearest neighbours.

The browser run asked Pilot "how do I cook rice". The curated FAQ refused it, but the
knowledge tool did not apply the same subject gate — embedding search returns its
nearest neighbours for any input, so the operator got three confident product
fragments ("paste a job id", "your dataset has 11 columns") narrated as an answer with
`grounded: false` and `sources: []`. Both paths now read one refusal.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.ai.copilot.pilot_agent import DataPilotAgent, carries_evidence  # noqa: E402
from src.ai.copilot.tools import get_pilot_tools  # noqa: E402
from src.ai.copilot.unsupported_question import (  # noqa: E402
    is_answerable_subject,
    unsupported_question_output,
)

OFF_SUBJECT = [
    "how do I cook rice",
    "who is the president",
    "what is the weather in Paris",
    "who won the world cup in 1998",
    "zzzz qqqq wwww",
]

ON_SUBJECT = [
    "what is append mode",
    "how do I inspect quarantine",
    "what are the preflight gates",
    "how do I use the MCP server",
]


@pytest.mark.parametrize("query", OFF_SUBJECT)
def test_knowledge_search_refuses_before_retrieval(query: str):
    out = get_pilot_tools()._search_knowledge(query).output
    assert out["intent"] == "unsupported"
    assert out["source"] == "unsupported_question"
    assert out["grounded"] is False
    assert out["sources"] == []
    assert out["hits"] == []
    assert "will not answer it from guesswork" in out["answer"]


@pytest.mark.parametrize("query", ON_SUBJECT)
def test_documented_questions_still_answer_with_citations(query: str):
    out = get_pilot_tools()._search_knowledge(query).output
    assert out["source"] == "product_documentation"
    assert out["grounded"] is True
    assert out["sources"], query
    assert all(s.get("href", "").startswith("#/help/") for s in out["sources"])


@pytest.mark.parametrize(
    "query",
    [
        "why did job 65f1a2b3c4d5e6f7a8b9c0d1 fail",
        "what happened in `orders_2024`",
        "show me pf_a1b2c3d4",
    ],
)
def test_a_named_workspace_object_is_not_treated_as_off_subject(query: str):
    """A pasted id is the operator's own data — the docs cannot vouch for its wording."""
    assert is_answerable_subject(query) is True


def test_both_paths_share_one_refusal():
    curated = get_pilot_tools()._explain_product("how do I cook rice").output
    knowledge = get_pilot_tools()._search_knowledge("how do I cook rice").output
    assert curated["answer"] == knowledge["answer"]
    assert curated["source"] == knowledge["source"] == "unsupported_question"


def test_the_refusal_offers_a_route_the_workspace_can_open():
    # `help` is the marketing page; the authenticated router cannot navigate to it.
    routes = [a["route"] for a in unsupported_question_output("how do I cook rice")["actions"]]
    assert routes == ["docs"]


def test_pilot_refuses_off_subject_with_low_confidence_and_no_sources():
    resp = DataPilotAgent().chat("how do I cook rice")
    assert resp.sources == []
    assert carries_evidence(resp) is False
    assert resp.confidence == pytest.approx(0.2, abs=0.01)
    assert "will not answer it from guesswork" in resp.answer


def test_prior_product_turns_do_not_make_an_off_subject_question_answerable():
    agent = DataPilotAgent()
    agent.chat("what is append mode")
    resp = agent.chat("how do I cook rice")
    assert resp.sources == []
    assert carries_evidence(resp) is False
    assert resp.confidence == pytest.approx(0.2, abs=0.01)
