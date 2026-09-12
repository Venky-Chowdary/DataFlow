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
from src.ai.copilot.tools import (
    ToolResult,
    _looks_like_unsupported_mutation,
    infer_tools_from_message,
)


def _names(question: str) -> list[str]:
    return [name for name, _ in infer_tools_from_message(question)]


@pytest.mark.parametrize(
    "question",
    [
        "can I land tables in Redshift",
        "what happens on a unique key collision",
        "what if two source rows have the same key",
        "is Slack a connector",
        "do you support Microsoft Teams as a destination",
        "if I pause CDC do I lose the slot",
        "does pausing CDC drop the replication slot",
        "can I pause CDC",
        "can I land in ADLS",
        "can I write to Microsoft Fabric",
        "do you support Oracle XStream",
        "do you support Cloud SQL",
        "do you support Azure SQL",
        "can I write to Google Pub/Sub",
        "do you support SharePoint as a destination",
        "can I assume an AWS IAM role",
        "can I set the replication slot name",
        "can I use incremental by updated_at",
        "do you support SCD type 2",
        "can I write to Excel Online",
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


@pytest.mark.parametrize(
    "question",
    [
        "if I pause CDC do I lose the slot",
        "does pausing CDC drop the replication slot",
        "can I pause CDC",
        "how do I pause CDC",
    ],
)
def test_pause_cdc_does_not_recommend_a_sync_mode(question: str) -> None:
    names = _names(question)
    assert "explain_product" in names, names
    assert "recommend_sync_mode" not in names, names


@pytest.mark.parametrize(
    "question",
    [
        "can I use incremental by updated_at",
        "can I set the replication slot name",
        "do you support SCD type 2",
    ],
)
def test_cursor_mode_asks_do_not_recommend_a_sync_mode(question: str) -> None:
    names = _names(question)
    assert "explain_product" in names, names
    assert "recommend_sync_mode" not in names, names


def test_excel_online_is_not_a_file_export_mutation() -> None:
    assert _looks_like_unsupported_mutation("can I write to excel online") is False
    assert _looks_like_unsupported_mutation("export rows to excel") is True


def test_should_i_use_cdc_still_recommends_a_mode() -> None:
    names = _names("should I use CDC for a nightly load")
    assert "recommend_sync_mode" in names, names


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
            output={},
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
