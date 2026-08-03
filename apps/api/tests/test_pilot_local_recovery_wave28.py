"""Wave 28 — local Pilot recovery, suggestions, list-vs-navigate honesty."""

from __future__ import annotations

from src.ai.copilot.pilot_agent import DataPilotAgent
from src.ai.copilot.tools import infer_tools_from_message


def test_show_my_jobs_lists_without_navigate():
    planned = infer_tools_from_message("show my jobs")
    names = [n for n, _ in planned]
    assert "list_jobs" in names
    assert "navigate" not in names


def test_show_my_connectors_lists_without_navigate():
    planned = infer_tools_from_message("show my connectors")
    names = [n for n, _ in planned]
    assert "list_connectors" in names
    assert "navigate" not in names


def test_missing_connector_recovers_with_list_and_clarification():
    agent = DataPilotAgent()
    resp = agent.chat("how many rows in orders on Local Postgres?")
    assert resp.method == "pilot_local_engine"
    tool_names = [t.get("name") for t in (resp.tools_used or [])]
    assert "aggregate_data" in tool_names
    assert "list_connectors" in tool_names
    answer = (resp.answer or "").lower()
    assert "connector" in answer
    assert resp.needs_clarification or "no saved connectors" in answer or "which" in answer


def test_suggest_improvements_without_dataset_is_actionable():
    agent = DataPilotAgent()
    resp = agent.chat("suggest improvements for my data")
    assert resp.method == "pilot_local_engine"
    answer = (resp.answer or "").lower()
    assert "quality" in answer or "dataset" in answer or "upload" in answer
    assert "0 columns):" not in (resp.answer or "")  # old greenwash empty profile


def test_fix_bad_data_stages_studio_confirm():
    agent = DataPilotAgent()
    resp = agent.chat("fix bad data")
    assert any(a.get("type") == "studio" for a in (resp.pending_actions or []))
    assert "confirm" in (resp.answer or "").lower()


def test_prune_keeps_list_over_navigate():
    # Content list phrases must not also force a screen navigate.
    planned = infer_tools_from_message("show my jobs")
    assert [n for n, _ in planned] == ["list_jobs"]
    planned2 = infer_tools_from_message("take me to jobs")
    assert any(n == "navigate" and a.get("screen") == "jobs" for n, a in planned2)
