"""Wave 38 — pending-steal fix + LLM polish path."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_pending_does_not_steal_navigate(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "local")
    from src.ai.copilot.pilot_agent import DataPilotAgent

    agent = DataPilotAgent()
    sid = "wave38-steal"
    agent.chat(
        "can you get users data from postgres",
        data_context={"pilot_session_id": sid},
    )
    resp = agent.chat("take me to jobs", data_context={"pilot_session_id": sid})
    names = [t["name"] for t in (resp.tools_used or [])]
    assert "navigate" in names
    assert "sample_connector_object" not in names
    assert "jobs" in (resp.answer or "").lower() or "→" in str(resp.tools_used)


def test_pending_does_not_steal_explain(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "local")
    from src.ai.copilot.pilot_agent import DataPilotAgent

    agent = DataPilotAgent()
    sid = "wave38-steal-explain"
    agent.chat(
        "can you get users data from postgres",
        data_context={"pilot_session_id": sid},
    )
    resp = agent.chat(
        "explain how mapping assurance works",
        data_context={"pilot_session_id": sid},
    )
    names = [t["name"] for t in (resp.tools_used or [])]
    assert "explain_mapping_assurance" in names
    assert "synonym group" not in (resp.answer or "").lower()


def test_resolve_pending_rejects_take_me_to():
    from src.ai.copilot.followup import resolve_pending_answer
    from src.ai.copilot.working_memory import PendingSlot

    pending = PendingSlot(
        tool="sample_connector_object",
        missing="connector_name",
        args={"table": "users"},
        candidates=[],
        question="Which connector?",
    )
    assert resolve_pending_answer("take me to jobs", pending) is None
    assert resolve_pending_answer("Local Postgres", pending) is not None


def test_polish_scores_above_local():
    """When hybrid is opted in, polished prose may still be preferred; local stays strong."""
    from src.ai.copilot.agent import CopilotResponse
    from src.ai.copilot.pilot_agent import _score_response

    local = CopilotResponse(
        answer="Here are your recent transfer jobs.",
        intent="jobs",
        confidence=0.9,
        method="pilot_local_engine",
        tools_used=[{"name": "list_jobs", "success": True, "summary": "7 jobs"}],
    )
    polished = CopilotResponse(
        answer="You have 7 recent transfer jobs. The latest completed successfully.",
        intent="jobs",
        confidence=0.93,
        method="openai_polish",
        tools_used=[{"name": "list_jobs", "success": True, "summary": "7 jobs"}],
    )
    # Local primary: grounded local must stay competitive with optional polish.
    assert abs(_score_response(local) - _score_response(polished)) < 0.5
