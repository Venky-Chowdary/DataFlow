"""Destination introspect reports missing tables as auto-create (not a hard error)."""

from __future__ import annotations

from unittest.mock import patch

from src.transfer.endpoint_intelligence import _attach_db_sample
from src.transfer.models import EndpointConfig


def test_missing_sql_table_sets_table_exists_false_and_auto_create():
    out: dict = {
        "kind": "database",
        "format": "snowflake",
        "connected": True,
        "objects": [],
        "columns": [],
        "schema": {},
        "row_estimate": 0,
        "auto_create": [],
        "message": "ok",
    }
    endpoint = EndpointConfig(
        kind="database",
        format="snowflake",
        database="DATAFLOW",
        schema="PUBLIC",
        table="brand_new_orders",
    )
    with patch(
        "src.transfer.endpoint_intelligence.resolve_connector_config",
        return_value={"type": "snowflake", "database": "DATAFLOW", "schema": "PUBLIC"},
    ), patch(
        "src.transfer.endpoint_intelligence._introspect_table_schema",
        return_value={},
    ):
        _attach_db_sample(out, endpoint)

    assert out["table_exists"] is False
    assert out["columns"] == []
    assert any("CREATE TABLE" in item for item in out["auto_create"])
    assert "created automatically" in out["message"].lower() or "not found" in out["message"].lower()
