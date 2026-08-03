"""Wave 35 — Railway-class refuse gate, remediate honesty, inventory NL."""

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
        "remove the Warehouse connector",
        "schedule this transfer nightly",
        "export orders to csv",
        "delete the Local Postgres connector",
        "create a new nightly schedule for orders",
    ],
)
def test_unsupported_mutations_do_not_rag(message: str):
    from src.ai.copilot.tools import (
        _looks_like_unsupported_mutation,
        infer_tools_from_message,
    )

    assert _looks_like_unsupported_mutation(message.lower())
    names = {n for n, _ in infer_tools_from_message(message)}
    assert "search_knowledge" not in names


@pytest.mark.parametrize(
    ("message", "tool"),
    [
        ("jobs that failed", "list_jobs"),
        ("list tables", "list_connectors"),
        ("what tables do I have", "list_connectors"),
        ("heal quarantine", "remediate_validation"),
        ("repair bad rows", "remediate_validation"),
        ("fix the mapping", "remediate_validation"),
        ("review the mappings", "remediate_validation"),
    ],
)
def test_wave35_nl_routes(message: str, tool: str):
    from src.ai.copilot.tools import infer_tools_from_message

    names = {n for n, _ in infer_tools_from_message(message)}
    assert tool in names, (message, names)


def test_remediate_compose_is_honest():
    from src.ai.copilot.pilot_agent import DataPilotAgent, PilotTurn
    from src.ai.copilot.tools import ToolResult

    agent = DataPilotAgent()
    turn = PilotTurn()
    turn.pending_actions.append({"id": "studio:x", "type": "studio"})
    turn.tool_results.append(
        ToolResult(
            name="remediate_validation",
            success=True,
            output={"label": "Fix bad data…", "kind": "open_bad_data_fix"},
        )
    )
    turn.tool_results.append(
        ToolResult(name="navigate", success=True, output={"screen": "transfer"}),
    )
    answer = agent._compose_local_answer("fix bad data", "remediate", turn, None, {})
    assert "does not rewrite" in answer.lower() or "opens" in answer.lower()
    assert "opening **transfer studio** for you" not in answer.lower()
    assert "after you confirm" in answer.lower()


def test_refuse_reply_for_remove_connector():
    from src.ai.copilot.pilot_agent import DataPilotAgent

    agent = DataPilotAgent()
    resp = agent.chat("remove the Warehouse connector")
    assert "search_knowledge" not in {t.name for t in (resp.tools_used or [])}
    lower = (resp.answer or "").lower()
    assert "delete" in lower or "ui" in lower or "not sure" in lower or "can't" in lower or "cannot" in lower


def test_aggregate_insight_includes_result_id():
    from src.ai.copilot.pilot_agent import DataPilotAgent, PilotTurn
    from src.ai.copilot.tools import ToolResult

    agent = DataPilotAgent()
    turn = PilotTurn()
    turn.tool_results.append(
        ToolResult(
            name="aggregate_data",
            success=True,
            output={
                "metric": "count",
                "value": 5,
                "table": "orders",
                "result_id": "pr_wave35agg",
                "columns": ["status"],
            },
        )
    )
    insight = agent._data_insight_from_turn(turn)
    assert insight is not None
    assert insight.get("last_result_id") == "pr_wave35agg"
