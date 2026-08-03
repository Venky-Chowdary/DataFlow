"""Wave 51 — schedule manage, connector health, size/has-col, create NL."""

from __future__ import annotations

import os

os.environ.setdefault("DATAFLOW_PILOT_ENGINE", "local")

from src.ai.copilot.connector_create import wants_create_connector
from src.ai.copilot.tools import infer_tools_from_message


def _names(q: str) -> list[str]:
    return [n for n, _ in infer_tools_from_message(q)]


def _args(q: str, tool: str) -> dict:
    for name, args in infer_tools_from_message(q):
        if name == tool:
            return args or {}
    return {}


def test_do_the_nightly_one_now():
    assert "run_schedule_now" in _names("do the nightly one now")
    assert "nightly" in (_args("do the nightly one now", "run_schedule_now").get("name") or "")


def test_pause_resume_open_named_pipeline():
    assert "open_schedule" in _names("pause Nightly Orders")
    assert _args("pause Nightly Orders", "open_schedule").get("name") == "nightly orders"
    assert "open_schedule" in _names("stop the Nightly Orders schedule")
    assert "open_schedule" in _names("resume Nightly Orders")
    assert "open_schedule" in _names("clone Nightly Orders")
    # CDC enable must not be treated as schedule manage.
    assert "recommend_sync_mode" in _names("enable change data capture on sales.orders")
    assert "open_schedule" not in _names("enable change data capture on sales.orders")


def test_get_and_details_named_pipeline():
    assert "get_schedule" in _names("get Nightly Orders pipeline")
    assert _args("get Nightly Orders pipeline", "get_schedule").get("name") == "nightly orders"
    assert "get_schedule" in _names("details about Nightly Orders")


def test_connector_health_lists_objects():
    assert _args("validate sales", "list_connector_objects").get("connector_name") == "sales"
    assert _args("check if sales is healthy", "list_connector_objects").get("connector_name") == "sales"
    assert _args("is sales connected", "list_connector_objects").get("connector_name") == "sales"
    assert _args("test connection to sales", "list_connector_objects").get("connector_name") == "sales"
    assert _args("ping sales connector", "list_connector_objects").get("connector_name") == "sales"


def test_how_big_and_has_column():
    args = _args("how big is orders on sales", "aggregate_data")
    assert args.get("metric") == "count"
    assert args.get("table") == "orders"
    assert args.get("connector_name") == "sales"
    intro = _args("does orders have updated_at on sales", "introspect_connector_schema")
    assert intro.get("table") == "orders"
    assert intro.get("connector_name") == "sales"


def test_last_transfer_fail_heal_compare_create():
    assert "list_jobs" in _names("why did the last transfer fail")
    assert _args("heal the quarantine", "remediate_validation").get("kind") == "quarantine_and_rerun"
    cmp = _args("compare dataset a and dataset b", "compare_datasets")
    assert cmp.get("dataset_a") == "a"
    assert cmp.get("dataset_b") == "b"
    msg = "add mysql named warehouse host localhost user root password secret database app"
    assert wants_create_connector(msg)
    assert "create_connector" in _names(msg)
