"""Wave 39 — single grounded execution, pending typo keep, capabilities schema."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_typo_keeps_pending_clarification(monkeypatch):
    """When candidates are offered, a mistyped name must not clear the slot."""
    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "local")
    from src.ai.copilot.pilot_agent import DataPilotAgent
    from src.ai.copilot.working_memory import PendingSlot, get_working_memory

    agent = DataPilotAgent()
    sid = "wave39-typo"
    memory = get_working_memory()
    memory.remember_pending(
        sid,
        PendingSlot(
            tool="sample_connector_object",
            missing="connector_name",
            args={"table": "users"},
            candidates=["Local Postgres", "Warehouse"],
            question="Which connector has users?",
        ),
    )
    resp = agent.chat("Loacl Postgress", data_context={"pilot_session_id": sid})
    still = memory.get_pending(sid)
    assert still is not None
    assert still.missing == "connector_name"
    assert "didn't match" in (resp.answer or "").lower() or "which connector" in (resp.answer or "").lower()
    assert not any(t.get("name") == "sample_connector_object" for t in (resp.tools_used or []))


def test_fresh_intent_still_clears_pending(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "local")
    from src.ai.copilot.pilot_agent import DataPilotAgent

    agent = DataPilotAgent()
    sid = "wave39-fresh"
    agent.chat(
        "can you get users data from postgres",
        data_context={"pilot_session_id": sid},
    )
    resp = agent.chat("take me to jobs", data_context={"pilot_session_id": sid})
    names = [t["name"] for t in (resp.tools_used or [])]
    assert "navigate" in names


def test_hybrid_skips_native_race_when_local_grounded(monkeypatch):
    """Local success must not kick a second mutating tool loop."""
    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "hybrid")
    from src.ai.copilot import pilot_agent as pa
    from src.ai.copilot.agent import CopilotResponse
    from src.ai.copilot.pilot_agent import DataPilotAgent

    agent = DataPilotAgent()
    local = CopilotResponse(
        answer="You have 3 jobs.",
        intent="jobs",
        confidence=0.95,
        method="pilot_local_engine",
        tools_used=[{"name": "list_jobs", "success": True, "summary": "3 jobs"}],
    )
    monkeypatch.setattr(agent, "_local_agent", lambda *a, **k: local)
    monkeypatch.setattr(agent, "_polish_with_llm", lambda *a, **k: local)
    monkeypatch.setattr(agent, "_anthropic_agent_loop", MagicMock(side_effect=AssertionError("race")))
    monkeypatch.setattr(agent, "_openai_agent", MagicMock(side_effect=AssertionError("race")))
    monkeypatch.setattr(agent, "_ollama_agent", MagicMock(side_effect=AssertionError("race")))
    monkeypatch.setattr(pa, "_resolve_pilot_engine", lambda: "hybrid")

    class _Anth:
        def is_available(self):
            return True

    agent._anthropic = _Anth()
    out = agent.chat("show my jobs", history=[], data_context={"pilot_session_id": "wave39-race"})
    assert out.tools_used
    assert out.method == "pilot_local_engine"


def test_models_schema_includes_pilot_engine():
    from src.routers.copilot_router import ModelCapabilitiesResponse

    fields = ModelCapabilitiesResponse.model_fields
    assert "pilot_engine" in fields


def test_system_prompt_includes_focus(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "local")
    from src.ai.copilot.pilot_agent import DataPilotAgent
    from src.ai.copilot.working_memory import get_working_memory

    agent = DataPilotAgent()
    sid = "wave39-focus"
    get_working_memory().update_focus(
        sid,
        connector_name="Local Postgres",
        table="airports",
    )
    ctx = agent.context_builder.build({"pilot_session_id": sid}, "hi")
    prompt = agent._build_system_prompt(ctx, {"pilot_session_id": sid})
    assert "Local Postgres" in prompt
    assert "airports" in prompt


def test_looks_like_fresh_intent():
    from src.ai.copilot.followup import looks_like_fresh_intent

    assert looks_like_fresh_intent("take me to jobs")
    assert looks_like_fresh_intent("explain mapping assurance")
    assert not looks_like_fresh_intent("Local Postgres")
    assert not looks_like_fresh_intent("xyzzy")
