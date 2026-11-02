"""Wave 34 — Railway-class release hardening for Datawrap Pilot.

Pins the release-critical gaps that amnesia / soft transfer NL and inflated
local-provider marketing used to leave open:

1. Transfer NL accepts polite / trailing / \"all\" phrasings.
2. Hybrid Anthropic path commits working memory.
3. Empty RAG returns an explicit refuse hint.
4. Local provider capabilities copy stays honest.
5. Prompt corpus stays ≥1000 cases.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


@pytest.mark.parametrize(
    "message",
    [
        "please transfer all orders from Local Postgres to Warehouse",
        "transfer orders from Local Postgres to Warehouse now",
        "can you move orders from pg to wh?",
        "transfer orders from Local Postgres to Warehouse please",
        "copy all customers from Local Postgres to Warehouse",
    ],
)
def test_natural_transfer_phrasing_parses(message: str):
    from src.ai.copilot.tools import parse_transfer_intent

    got = parse_transfer_intent(message)
    assert got is not None, message
    assert got["source_table"]
    assert got["source_connector_name"]
    assert got["dest_connector_name"]


def test_search_knowledge_empty_shape_from_tool(monkeypatch):
    from src.ai.copilot.tools import DataPilotTools

    class _Empty:
        documents = []

    def fake_retrieve(query, n_results=8):
        return _Empty()

    class _Pipeline:
        class ingestion:
            @staticmethod
            def ensure_knowledge_loaded():
                return None

        class retriever:
            retrieve = staticmethod(fake_retrieve)

    monkeypatch.setattr(
        "src.ai.rag.pipeline.get_rag_pipeline",
        lambda: _Pipeline(),
    )
    tr = DataPilotTools()._search_knowledge("unmapped nonsense query zzqq")
    assert tr.success
    assert tr.output["count"] == 0
    assert tr.output.get("empty") is True
    assert tr.output.get("grounded") is False
    assert tr.output.get("source") == "unsupported_question"
    assert "will not" in (tr.output.get("answer") or "").lower()
    assert "No grounded" in (tr.output.get("hint") or "")


def test_empty_knowledge_compose_uses_hint():
    from src.ai.copilot.pilot_agent import DataPilotAgent, PilotTurn
    from src.ai.copilot.tools import ToolResult

    agent = DataPilotAgent()
    turn = PilotTurn()
    turn.tool_results.append(
        ToolResult(
            name="search_knowledge",
            success=True,
            output={
                "query": "xyzzy",
                "hits": [],
                "count": 0,
                "empty": True,
                "hint": "No grounded product knowledge matched. Ask about a saved connector.",
            },
        )
    )
    answer = agent._compose_local_answer("xyzzy", "knowledge", turn, None, {})
    assert "No grounded product knowledge" in answer


def test_local_capabilities_copy_is_honest():
    from src.ai.llm.provider import MODEL_CAPABILITY_MATRIX

    local = next(p for p in MODEL_CAPABILITY_MATRIX if p["provider"] == "local")
    best = local["best_for"].lower()
    assert "brain" not in best
    assert "deterministic" in best or "local tool" in best


def test_anthropic_loop_commits_memory(monkeypatch):
    """Cloud tool loop must persist focus so follow-ups are not amnesiac."""
    from src.ai.copilot.pilot_agent import DataPilotAgent
    from src.ai.copilot.tools import ToolResult
    from src.ai.copilot.working_memory import get_working_memory

    agent = DataPilotAgent()
    session = "wave34-hybrid-session"
    memory = get_working_memory()

    calls = {"n": 0}

    def fake_generate_agent(**kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            return {
                "success": True,
                "content": "",
                "tool_calls": [{
                    "id": "tc1",
                    "name": "aggregate_data",
                    "input": {
                        "connector_name": "PilotSQLite",
                        "table": "orders",
                        "metric": "count",
                    },
                }],
            }
        return {
            "success": True,
            "content": "There are 5 orders.",
            "tool_calls": [],
        }

    class _FakeAnthropic:
        def generate_agent(self, **kwargs):
            return fake_generate_agent(**kwargs)

    monkeypatch.setattr(agent, "_anthropic", _FakeAnthropic())

    def fake_execute(name, args):
        return ToolResult(
            name=name,
            success=True,
            output={
                "metric": "count",
                "value": 5,
                "table": "orders",
                "connector_name": "PilotSQLite",
            },
        )

    monkeypatch.setattr(agent.tools, "execute", fake_execute)

    committed = {"ok": False}

    def fake_commit(planned, turn, data_context):
        committed["ok"] = True
        get_working_memory().update_focus(
            session,
            table="orders",
            connector_name="PilotSQLite",
            metric="count",
        )

    monkeypatch.setattr(agent, "_commit_memory", fake_commit)
    monkeypatch.setattr(agent, "_run_local_recovery", lambda *a, **k: None)

    resp = agent._anthropic_agent_loop(
        "how many orders",
        [],
        "system",
        {"pilot_session_id": session},
    )
    assert resp is not None
    assert committed["ok"] is True
    focus = memory.get_focus(session)
    assert focus is not None
    assert focus.table == "orders"


def test_corpus_still_meets_floor():
    from src.ai.copilot.prompt_corpus import corpus_stats

    stats = corpus_stats()
    assert stats["total"] >= 1000, stats
