"""Wave 52 — to←from transfers, Local FAQ false positive, named show, last job."""

from __future__ import annotations

import os

os.environ.setdefault("DATAFLOW_PILOT_ENGINE", "local")

from src.ai.copilot.connector_create import wants_create_connector
from src.ai.copilot.followup import looks_like_elliptical_edit
from src.ai.copilot.tools import (
    _looks_like_product_howto,
    infer_tools_from_message,
    parse_transfer_intent,
)


def _names(q: str) -> list[str]:
    return [n for n, _ in infer_tools_from_message(q)]


def _args(q: str, tool: str) -> dict:
    for name, args in infer_tools_from_message(q):
        if name == tool:
            return args or {}
    return {}


def test_push_to_dest_from_source_is_transfer_not_faq():
    msg = "push orders to warehouse from Local Postgres"
    assert not _looks_like_product_howto(msg.lower())
    intent = parse_transfer_intent(msg)
    assert intent is not None
    assert intent["source_table"] == "orders"
    assert "local" in intent["source_connector_name"].lower()
    assert "warehouse" in intent["dest_connector_name"].lower()
    assert "start_transfer" in _names(msg)
    # Regression: Local Postgres must not trip local-primary FAQ.
    assert "explain_product" not in _names(msg)
    assert "aggregate_data" in _names("count of orders on Local Postgres")
    assert "explain_product" not in _names("count of orders on Local Postgres")
    assert "explain_product" in _names("are you local only")


def test_show_named_pipeline_and_details_suffix():
    assert "get_schedule" in _names("show Nightly Orders")
    assert _args("show Nightly Orders", "get_schedule").get("name") == "nightly orders"
    assert "get_schedule" in _names("Nightly Orders details")


def test_what_columns_does_table_have():
    args = _args("what columns does orders have on sales", "introspect_connector_schema")
    assert args.get("table") == "orders"
    assert args.get("connector_name") == "sales"


def test_last_job_and_transfer_status():
    assert "list_jobs" in _names("open my last job")
    assert "list_jobs" in _names("status of my last transfer")
    assert "list_jobs" in _names("get job details")


def test_save_this_postgres_connector_credentials():
    msg = (
        "save this postgres connector host db user app password x "
        "database sales named SalesDB"
    )
    assert wants_create_connector(msg)
    assert "create_connector" in _names(msg)


def test_make_it_upsert_and_elliptical_guards():
    assert "recommend_sync_mode" in _names("make it upsert")
    assert "recommend_sync_mode" in _names("switch to cdc")
    assert looks_like_elliptical_edit("group by country instead")
    assert looks_like_elliptical_edit("same but for returns")
    assert looks_like_elliptical_edit("filter to paid")
    assert looks_like_elliptical_edit("drop the group by")
    assert looks_like_elliptical_edit("no grouping")
    # Fresh aggregate must not look elliptical just because it starts with count.
    assert not looks_like_elliptical_edit("count of orders on Local Postgres")
