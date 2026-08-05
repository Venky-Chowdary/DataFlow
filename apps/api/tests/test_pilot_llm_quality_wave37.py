"""Wave 37 — LLM-first hybrid scoring + engine resolve."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.ai.copilot.agent import CopilotResponse
from src.ai.copilot.pilot_agent import _resolve_pilot_engine, _score_response


def test_resolve_engine_local_when_forced(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "local")
    assert _resolve_pilot_engine() == "local"


def test_score_prefers_grounded_llm_over_local():
    local = CopilotResponse(
        answer="Count is 5",
        intent="aggregate",
        confidence=0.9,
        method="pilot_local_engine",
        tools_used=[{"name": "aggregate_data", "success": True, "summary": "5"}],
    )
    llm = CopilotResponse(
        answer="There are **5** orders on Local Postgres matching your filter.",
        intent="aggregate",
        confidence=0.92,
        method="openai_agent",
        tools_used=[{"name": "aggregate_data", "success": True, "summary": "5"}],
    )
    assert _score_response(llm) > _score_response(local)


def test_score_rejects_ungrounded_llm():
    local = CopilotResponse(
        answer="I'm not sure how to do that yet.",
        intent="unknown",
        confidence=0.5,
        method="pilot_local_engine",
        tools_used=[],
    )
    llm = CopilotResponse(
        answer=(
            "Sure! Here's a long fluent answer about phone synonyms and telecom "
            "schemas that invents facts."
        ),
        intent="knowledge",
        confidence=0.95,
        method="anthropic_agent",
        tools_used=[],
    )
    assert _score_response(local) > _score_response(llm)


def test_filter_followup_prefers_filter_result(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "local")
    from src.ai.copilot.pilot_agent import DataPilotAgent
    from src.ai.copilot.working_memory import get_working_memory

    session = "wave37-filter"
    memory = get_working_memory()
    memory.update_focus(
        session,
        table="orders",
        connector_name="PilotSQLite",
        result_id="pr_wave37",
        columns=["id", "amount", "status"],
    )
    agent = DataPilotAgent()
    planned = agent._plan_with_memory(
        "filter where amount > 10",
        {"pilot_session_id": session, "last_result_id": "pr_wave37"},
    )
    names = [n for n, _ in planned]
    assert "analyze_result" not in names or "filter_result" in names
    assert "filter_result" in names or "aggregate_data" in names
