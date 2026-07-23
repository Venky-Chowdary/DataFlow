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


def test_listed_mysql_table_not_marked_missing_when_columns_empty():
    """Probe listed ``jobs`` but INFORMATION_SCHEMA returned no columns — still exists."""
    out: dict = {
        "kind": "database",
        "format": "mysql",
        "connected": True,
        "objects": [{"name": "jobs", "type": "table"}, {"name": "users", "type": "table"}],
        "columns": [],
        "schema": {},
        "row_estimate": 0,
        "auto_create": [],
        "message": "MySQL connected",
        "table_exists": True,  # set by _mark_table_listed_if_present
    }
    endpoint = EndpointConfig(
        kind="database",
        format="mysql",
        database="railway",
        table="jobs",
        extra={"introspect_purpose": "destination"},
    )
    with patch(
        "src.transfer.endpoint_intelligence.resolve_connector_config",
        return_value={"type": "mysql", "database": "railway", "host": "h", "port": 3306},
    ), patch(
        "src.transfer.endpoint_intelligence._introspect_table_schema",
        return_value={},
    ):
        _attach_db_sample(out, endpoint)

    assert out["table_exists"] is True
    assert out["auto_create"] == []
    assert "not found" not in out["message"].lower()
    assert "exists" in out["message"].lower()


def test_case_insensitive_object_name_match():
    from src.transfer.endpoint_intelligence import _object_name_match

    assert _object_name_match(["Jobs", "users"], "jobs") == "Jobs"
    assert _object_name_match(["jobs"], "JOBS") == "jobs"
    assert _object_name_match(["(no tables)"], "jobs") is None


def test_missing_sql_table_source_purpose_does_not_promise_create():
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
        table="csv",
        extra={"introspect_purpose": "source"},
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
    assert out["auto_create"] == []
    assert "first write" not in out["message"].lower()
    assert "not found on this source" in out["message"].lower()
