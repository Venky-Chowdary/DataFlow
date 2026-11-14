"""CLM-9, CLM-10, CLM-14: Pilot states capability facts from the enforced tables.

* CLM-9: the capabilities summary omits the route matrix, so the renderer
  read ``len(live_combinations)`` and said "0 live routes".
* CLM-10: "Can I run CDC from DynamoDB?" was answered "Yes"; no DynamoDB
  change reader exists and Validate refuses it.
* CLM-14: unsupported combinations got a paragraph instead of "No".
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="module")
def ask():
    sys.path.insert(0, str(ROOT / "scripts"))
    from pilot_answer_audit import isolated_stores

    with isolated_stores():
        from src.ai.copilot.pilot_agent import DataPilotAgent

        agent = DataPilotAgent()

        def _ask(question: str) -> str:
            return agent.chat(question, [], data_context={"pilot_session_id": "clm"}).answer

        yield _ask


def test_route_count_is_the_live_combination_count(ask) -> None:
    from src.transfer.registry import get_capabilities

    count = len(get_capabilities()["live_combinations"])
    assert count > 0
    answer = ask("Is Databricks supported?")
    assert f"**{count:,} live routes**" in answer
    assert "**0 live routes**" not in answer


def test_dynamodb_is_not_a_cdc_source(ask) -> None:
    answer = ask("Can I run CDC from DynamoDB?")
    assert answer.startswith("No — DynamoDB has no change-capture reader"), answer[:200]
    from services.preflight_cursor_gate import CDC_CAPABLE_SOURCES

    assert "dynamodb" not in CDC_CAPABLE_SOURCES


def test_cdc_source_list_is_the_one_validate_enforces(ask) -> None:
    answer = ask("Which sources support CDC?")
    head = answer.split("Kafka, DynamoDB")[0]
    for engine in ("postgresql", "mysql", "mongodb", "oracle", "sqlserver"):
        assert engine in head
    for not_a_source in ("kafka", "qdrant", "pgvector", "dynamodb"):
        assert not_a_source not in head.lower()


@pytest.mark.parametrize(
    ("question", "verdict"),
    [
        ("Can I mirror into MongoDB?", "No — Mirror into MongoDB is not offered"),
        ("Can I upsert into Kafka?", "No — upsert (Incremental deduped) into Kafka is not offered"),
        ("Can I read from Weaviate?", "No — Weaviate cannot be read by a transfer"),
        ("Is Shopify supported?", "No — Shopify is Planned"),
        ("Can I use SCD Type 2 into Snowflake?", "Yes — Snowflake accepts SCD Type 2."),
    ],
)
def test_unsupported_combinations_get_a_plain_verdict(ask, question: str, verdict: str) -> None:
    assert ask(question).startswith(verdict), ask(question)[:200]


def test_no_dynamodb_streams_cdc_claim_ships() -> None:
    catalog = json.loads((ROOT / "data" / "connector_catalog.json").read_text(encoding="utf-8"))
    for entry in catalog["connectors"]:
        assert "Streams CDC" not in str(entry.get("description") or ""), entry["id"]
    page = (ROOT.parents[1] / "apps" / "web" / "src" / "pages" / "TransferPage.tsx").read_text(encoding="utf-8")
    assert "Streams CDC" not in page
