"""A start_transfer refused before any plan shows its reason, not a None plan.

The chat rendered ``**None**.`None` → **None**.`None` — sync `None``` under a
refusal whose tool output was ``{}`` (no plan was built). A blocked plan with
real gate evidence still renders that evidence.
"""

from __future__ import annotations


def _answer(output):
    from src.ai.copilot.pilot_agent import DataPilotAgent, PilotTurn
    from src.ai.copilot.tools import ToolResult

    turn = PilotTurn()
    turn.tool_results.append(ToolResult(
        name="start_transfer", success=False, output=output,
        error="Append into an existing table is refused because id is already a primary key.",
    ))
    return DataPilotAgent()._compose_local_answer("copy orders", "transfer", turn, None, {})


def test_a_refusal_with_no_plan_names_the_reason_and_no_none_fields():
    answer = _answer({})
    assert "primary key" in answer
    assert "None" not in answer
