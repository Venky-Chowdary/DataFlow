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
        "do you support OneDrive as a destination",
        "do you support Looker as a destination",
        "do you support Azure Database for PostgreSQL",
        "do you support Google Cloud Dataflow",
        "do you support GKE as a destination",
        "do you support Outlook as a destination",
        "do you support Azure PostgreSQL Flexible Server",
        "do you support Firebase",
        "do you support Azure Table Storage",
        "do you support Splunk as a destination",
        "do you support Tableau as a destination",
        "do you support Azure Data Lake Gen2",
        "do you support Amazon DynamoDB",
        "do you support Elasticsearch",
        "can I use a service principal for Azure SQL",
        "do you support SQL Server on Azure VMs",
        "can I use Aurora as a source",
        "do you support Qlik as a destination",
        "do you support Cloud Armor",
        "do you support Azure DevOps",
        "do you support Gmail as a source",
        "do you support SQL Server Integration Services",
        "do you support Amazon RDS for PostgreSQL",
        "do you support Informatica as a destination",
        "do you support Azure HDInsight",
        "do you support Classroom as a source",
        "do you support Google Chat as a destination",
        "do you support Azure SignalR",
        "do you support Amazon RDS for SQL Server",
        "do you support Azure Logic Apps",
        "do you support Copilot Studio as a destination",
        "do you support Cloud DNS",
        "do you support Natural Language API",
        "do you support Azure Database for MariaDB",
        "do you support Azure Relay",
        "do you support Windows 365 as a destination",
        "do you support API Gateway",
        "do you support Azure Test Plans",
        "do you support Azure Dedicated SQL Pool",
        "do you support Cloud CDN",
        "do you support Binary Authorization",
        "do you support Azure Maps",
        "do you support Retail API",
        "do you support Bing Ads",
        "do you support Config Connector",
        "do you support GitHub Copilot as a destination",
        "do you support Iceberg",
        "do you support SFTP",
        "do you support Kafka as a destination",
        "do you support MySQL",
        "do you support Redis",
        "do you support Google Photos as a source",
        "do you support Vision AI as a destination",
        "do you support Earth Engine",
        "do you support BigQuery BI Engine",
        "do you support Azure Confidential Ledger",
        "do you support Microsoft Clarity",
        "do you support Anthos",
        "do you support Azure IoT Hub",
        "do you support Merchant Center",
        "do you support Translation API as a destination",
        "do you support Recommendations AI",
        "do you support Google Optimize",
        "do you support Confidential VM",
        "do you support Azure Stack Hub",
        "can I write to Translation API",
        "do you support Microsoft Copilot as a destination",
        "do you support Cloud Deploy",
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
