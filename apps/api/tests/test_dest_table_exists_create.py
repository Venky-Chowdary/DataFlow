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


def test_qualified_object_name_match_bare_vs_schema():
    from src.transfer.endpoint_intelligence import _object_name_match

    assert _object_name_match(["public.jobs", "public.users"], "jobs") == "public.jobs"
    assert _object_name_match(["jobs"], "public.jobs") == "jobs"
    # Ambiguous leaf across schemas → no match (operator must qualify).
    assert _object_name_match(["a.jobs", "b.jobs"], "jobs") is None


def test_mongo_empty_collection_still_exists():
    """Empty Mongo collection must not report table_exists=False (create-new false positive)."""
    from unittest.mock import MagicMock

    out: dict = {
        "kind": "database",
        "format": "mongodb",
        "connected": True,
        "objects": [{"name": "empty_coll", "type": "collection"}],
        "columns": [],
        "schema": {},
        "row_estimate": 0,
        "auto_create": [],
        "message": "ok",
    }
    endpoint = EndpointConfig(
        kind="database",
        format="mongodb",
        database="test",
        collection="empty_coll",
    )
    coll = MagicMock()
    coll.find.return_value.max_time_ms.return_value.limit.return_value = iter([])
    coll.estimated_document_count.return_value = 0
    db = MagicMock()
    db.__getitem__.return_value = coll
    client = MagicMock()
    client.__getitem__.return_value = db

    with patch(
        "src.transfer.endpoint_intelligence.resolve_connector_config",
        return_value={"type": "mongodb", "database": "test"},
    ), patch(
        "src.transfer.endpoint_intelligence._mongo_client",
        return_value=client,
    ), patch(
        "src.transfer.endpoint_intelligence.mongodb_connection_string",
        return_value="mongodb://localhost",
    ):
        _attach_db_sample(out, endpoint)

    assert out["table_exists"] is True
    assert out["columns"] == []
    assert out["auto_create"] == []


def test_probe_exception_does_not_claim_create_new():
    out: dict = {
        "kind": "database",
        "format": "mysql",
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
        format="mysql",
        database="railway",
        table="jobs",
    )
    with patch(
        "src.transfer.endpoint_intelligence.resolve_connector_config",
        side_effect=RuntimeError("boom"),
    ):
        _attach_db_sample(out, endpoint)

    assert out["table_exists"] is None
    assert out["auto_create"] == []


def test_elasticsearch_empty_index_still_exists():
    from unittest.mock import MagicMock

    out: dict = {
        "kind": "database",
        "format": "elasticsearch",
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
        format="elasticsearch",
        database="orders",
        table="orders",
    )
    empty_batch = MagicMock()
    empty_batch.headers = []
    empty_batch.rows = []
    empty_batch.total_rows = 0
    client = MagicMock()
    client.indices.exists.return_value = True

    with patch(
        "src.transfer.endpoint_intelligence.resolve_connector_config",
        return_value={"type": "elasticsearch", "host": "localhost", "port": 9200},
    ), patch(
        "connectors.elasticsearch_reader._client",
        return_value=client,
    ), patch(
        "connectors.elasticsearch_reader.read_index_batch",
        return_value=(empty_batch, None),
    ):
        _attach_db_sample(out, endpoint)

    assert out["table_exists"] is True
    assert out["auto_create"] == []


def test_dynamodb_auth_error_does_not_claim_missing():
    out: dict = {
        "kind": "database",
        "format": "dynamodb",
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
        format="dynamodb",
        database="orders",
        table="orders",
    )

    class AccessDenied(Exception):
        response = {"Error": {"Code": "AccessDeniedException", "Message": "nope"}}

    with patch(
        "src.transfer.endpoint_intelligence.resolve_connector_config",
        return_value={"type": "dynamodb", "database": "orders"},
    ), patch(
        "connectors.dynamodb_reader.describe_table_schema",
        side_effect=AccessDenied("denied"),
    ), patch(
        "connectors.dynamodb_reader.read_all_paginated",
        side_effect=AccessDenied("denied"),
    ):
        _attach_db_sample(out, endpoint)

    assert out["table_exists"] is None


def test_dynamo_numeric_types_serialize_as_decimal():
    from decimal import Decimal

    from connectors.dynamodb_writer import _to_dynamo_value

    assert _to_dynamo_value("42", "INTEGER") == Decimal("42")
    assert _to_dynamo_value("1.5", "FLOAT") == Decimal("1.5")
    assert _to_dynamo_value("9.99", "DECIMAL") == Decimal("9.99")


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
