"""Wave 50 — colloquial ops: typos, short nav, soft transfer, FAQ, samples."""

from __future__ import annotations

import os

os.environ.setdefault("DATAFLOW_PILOT_ENGINE", "local")

from src.ai.copilot.tools import infer_tools_from_message, parse_transfer_intent


def _names(q: str) -> list[str]:
    return [n for n, _ in infer_tools_from_message(q)]


def _args(q: str, tool: str) -> dict:
    for name, args in infer_tools_from_message(q):
        if name == tool:
            return args or {}
    return {}


def test_typo_cnt_connectorz_and_dbs():
    assert "aggregate_data" in _names("cnt of orders")
    assert _args("cnt of orders", "aggregate_data").get("table") == "orders"
    assert "list_connectors" in _names("whats my connectorz")
    assert "list_connectors" in _names("can u list my dbs")


def test_short_nav_go_pipelines_contracts_mcp():
    assert _args("go pipelines", "navigate").get("screen") == "schedules"
    assert _args("contracts screen", "navigate").get("screen") == "contracts"
    assert _args("mcp tools", "navigate").get("screen") == "mcp"


def test_local_primary_faq_phrases():
    assert "explain_product" in _names("dont use openai")
    assert "explain_product" in _names("are you local only")
    assert "explain_product" in _names("don't use openai")


def test_fire_and_execute_schedule_names():
    assert "run_schedule_now" in _names("fire Nightly Orders")
    assert _args("fire Nightly Orders", "run_schedule_now").get("name") == "nightly orders"
    assert "run_schedule_now" in _names("execute Nightly")


def test_soft_transfer_without_source_is_plan_only():
    intent = parse_transfer_intent("transfer orders to warehouse")
    assert intent is not None
    assert intent.get("plan_only") is True
    assert intent.get("source_table") == "orders"
    assert "plan_transfer" in _names("trasfer orders to warehouse")
    # Full from→to remains a mutation path (not plan_only by default).
    full = parse_transfer_intent("transfer orders from Local Postgres to Warehouse")
    assert full is not None
    assert full.get("plan_only") is False
    assert "start_transfer" in _names("transfer orders from Local Postgres to Warehouse")


def test_sample_preview_profile_and_map():
    assert "sample_connector_object" in _names("preview first 5 of orders on warehouse")
    assert _args("preview first 5 of orders on warehouse", "sample_connector_object").get("table") == "orders"
    assert "sample_connector_object" in _names("profile customers on sales")
    assert _args("profile customers on sales", "sample_connector_object").get("analyze") is True
    mapped = _args("map customers to dim_customer", "map_connector_schemas")
    assert mapped.get("source_table") == "customers"
    assert mapped.get("dest_table") == "dim_customer"


def test_write_mode_cdc_quality_and_table_inventory():
    assert "recommend_sync_mode" in _names("what write mode should i use")
    assert "recommend_sync_mode" in _names("enable change data capture on sales.orders")
    assert "plan_transfer_route" in _names("start cdc from orders")
    assert "recommend_sync_mode" not in _names("start cdc from orders")
    assert "profile_quality_rules" in _names("suggest quality for orders")
    assert _names("how many tables on sales") == ["list_connector_objects"]
    assert _names("how mny tbls on sales") == ["list_connector_objects"]
