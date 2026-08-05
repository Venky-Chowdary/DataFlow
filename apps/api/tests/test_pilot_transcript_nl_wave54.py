"""Regression: Pilot NL phrases from the bad production transcript.

Must plan the right tools (or FAQ) — never dead-end RAG / synonym dumps /
"I'm not sure how to do PostgresVenkat".
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def _names(planned):
    return [n for n, _ in planned]


def test_schema_types_faq_not_empty_rag():
    from src.ai.copilot.tools import DataPilotTools, infer_tools_from_message

    for q in (
        "Tell me everything about schema types",
        "tell me about schema types",
        "explain the type system",
    ):
        planned = infer_tools_from_message(q)
        assert "explain_product" in _names(planned), (q, planned)
        assert "search_knowledge" not in _names(planned), (q, planned)

    faq = DataPilotTools()._explain_product("Tell me everything about schema types")
    assert faq.success
    assert "logical types" in (faq.output or {}).get("answer", "").lower()


def test_how_many_tables_available_is_inventory():
    from src.ai.copilot.tools import infer_tools_from_message

    for q in (
        "how many tables availabale in PostgresVenkat",
        "how many tables available in PostgresVenkat",
        "can you give me tables available in MySQL connectionCrown",
        "list tables in PostgresVenkat",
    ):
        planned = infer_tools_from_message(q)
        assert "list_connector_objects" in _names(planned), (q, planned)
        assert "aggregate_data" not in _names(planned), (q, planned)
        assert "sample_connector_object" not in _names(planned), (q, planned)
        args = next(a for n, a in planned if n == "list_connector_objects")
        cname = str(args.get("connector_name") or "").lower()
        if "mysql" in q.lower():
            assert "connectioncrown" in cname.replace(" ", "") or "crown" in cname, (q, args)
        else:
            assert "postgresvenkat" in cname.replace(" ", ""), (q, args)


def test_mysql_connectioncrown_name_preserved():
    from src.ai.copilot.tools import _clean_connector_phrase

    assert _clean_connector_phrase("MySQL connectionCrown") == "MySQL connectionCrown"
    assert _clean_connector_phrase("postgres connection Venkat").lower() == "venkat"


def test_bare_connector_lists_tables():
    from src.ai.copilot.tools import infer_tools_from_message

    saved = [{"id": "1", "name": "PostgresVenkat", "type": "postgresql"}]
    with patch("services.connector_store.list_connectors", return_value=saved):
        planned = infer_tools_from_message("PostgresVenkat")
    assert "list_connector_objects" in _names(planned), planned
    assert planned[0][1].get("connector_name") == "PostgresVenkat"


def test_show_the_data_from_table():
    from src.ai.copilot.tools import infer_tools_from_message

    planned = infer_tools_from_message("show the data from countries")
    assert "sample_connector_object" in _names(planned), planned
    args = next(a for n, a in planned if n == "sample_connector_object")
    assert args.get("table") == "countries"


def test_sample_typo_still_plans_sample():
    from src.ai.copilot.tools import infer_tools_from_message

    planned = infer_tools_from_message("sample countires on PostgresVenkat")
    assert "sample_connector_object" in _names(planned), planned
    args = next(a for n, a in planned if n == "sample_connector_object")
    assert args.get("table") == "countires"  # fuzzy resolve happens at execute
    assert "postgresvenkat" in str(args.get("connector_name") or "").lower()


def test_clarification_slot_for_missing_table():
    from src.ai.copilot.followup import clarification_slot

    slot = clarification_slot(
        "sample_connector_object",
        {"table": "users", "connector_name": "PostgresVenkat"},
        "No table `users` on **PostgresVenkat**. Which table? `airports`, `countries`.",
    )
    assert slot is not None
    assert slot.missing == "table"


def test_hr_does_not_dump_synonym_rag():
    from src.ai.copilot.tools import infer_tools_from_message

    planned = infer_tools_from_message("Tell me everything about hr")
    assert "search_knowledge" not in _names(planned), planned
