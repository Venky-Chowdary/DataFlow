"""Capability asks must not become workspace object lookups.

Compose already answers destination and unique-key questions from generated
cards. Chat still planned ``list_connector_objects`` / ``aggregate_data`` off
phrases like ``tables in Redshift`` and ``unique key collision``, then led
the reply with ``no connector matched``.
"""

from __future__ import annotations

import os

os.environ.setdefault("DATAFLOW_PILOT_ENGINE", "local")

import pytest

from src.ai.copilot.pilot_agent import DataPilotAgent, PilotTurn
from src.ai.copilot.tools import ToolResult, infer_tools_from_message


def _names(question: str) -> list[str]:
    return [name for name, _ in infer_tools_from_message(question)]


@pytest.mark.parametrize(
    "question",
    [
        "can I land tables in Redshift",
        "what happens on a unique key collision",
        "what if two source rows have the same key",
    ],
)
def test_capability_asks_do_not_plan_named_object_lookups(question: str) -> None:
    names = _names(question)
    assert "explain_product" in names, names
    assert "list_connector_objects" not in names, names
    assert "aggregate_data" not in names, names


@pytest.mark.parametrize(
    "question",
    [
        "can you get tables from PostgresVenkat",
        "list tables on sales",
        "how many tables on sales",
    ],
)
def test_live_inventory_asks_still_plan_object_lookups(question: str) -> None:
    names = _names(question)
    assert "list_connector_objects" in names or "list_connectors" in names, names


def test_product_answer_is_not_prefixed_with_connector_miss() -> None:
    agent = DataPilotAgent()
    turn = PilotTurn()
    turn.tool_results.append(
        ToolResult(
            name="explain_product",
            success=True,
            output={
                "answer": (
                    "Amazon Redshift is not a transfer-ready destination on this host. "
                    "Catalog tiles are not a live write path."
                )
            },
        )
    )
    turn.tool_results.append(
        ToolResult(
            name="list_connector_objects",
            success=False,
            error='No connector matched "redshift".',
        )
    )
    turn.needs_clarification = 'No connector matched "redshift".'
    answer = agent._compose_local_answer(
        "can I land tables in Redshift", "knowledge", turn, None, {}
    )
    low = (answer or "").lower()
    assert "not a transfer-ready" in low
    assert "no connector matched" not in low
