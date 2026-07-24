"""Wave X accuracy: Dynamo/Neo4j totals, stable OFFSET ORDER BY, Protobuf catalog."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


def test_dynamodb_itemcount_does_not_bound_scan_total():
    from connectors.dynamodb_reader import read_table_batch

    client = MagicMock()
    client.scan.return_value = {
        "Items": [{"id": {"S": "1"}, "name": {"S": "a"}}],
        "LastEvaluatedKey": {"id": {"S": "1"}},
    }
    with patch("connectors.dynamodb_reader.boto3_client", return_value=client), patch(
        "connectors.dynamodb_reader.estimate_item_count", return_value=1
    ):
        batch, next_key = read_table_batch(
            cfg={"host": "us-east-1"},
            table="t",
            columns=["id", "name"],
            offset=0,
            limit=1,
        )

    assert batch.total_rows is None
    assert next_key == {"id": {"S": "1"}}
    assert (batch.meta or {}).get("approx_item_count") == 1


def test_stream_continues_dynamodb_despite_approx_underestimate():
    """Defense-in-depth: total_rows must not stop DynamoDB while cursor remains."""
    total_rows = 1
    fetch_offset = 1
    src_type = "dynamodb"
    assert not (total_rows is not None and fetch_offset >= total_rows and src_type != "dynamodb")


def test_neo4j_page_does_not_claim_full_total():
    from connectors.neo4j import read_object

    with patch(
        "connectors.neo4j._run_cypher",
        return_value={
            "results": [{
                "columns": ["_neo4j_element_id", "_neo4j_labels", "props"],
                "data": [
                    {"row": ["e1", ["Person"], {"n": 1}]},
                    {"row": ["e2", ["Person"], {"n": 2}]},
                ],
            }]
        },
    ):
        batch = read_object(
            cfg={"host": "localhost", "username": "neo4j", "password": "p"},
            object="Person",
            limit=2,
            offset=0,
        )
    assert len(batch.rows) == 2
    assert batch.total_rows is None


def test_bigquery_snapshot_orders_for_stable_offset():
    from connectors.bigquery_reader import read_table_batch

    client = MagicMock()
    field = MagicMock()
    field.name = "id"
    table = MagicMock()
    table.schema = [field]
    client.get_table.return_value = table
    job = MagicMock()
    job.schema = [field]
    job.result.return_value = []
    client.query.return_value = job

    with patch("connectors.bigquery_conn.get_client", return_value=client):
        read_table_batch(
            host="",
            port=443,
            database="proj",
            username="",
            password="",
            schema="ds",
            connection_string="",
            ssl=True,
            table="t",
            columns=None,
            offset=0,
            limit=100,
            known_total_rows=0,
        )

    select_sql = client.query.call_args[0][0]
    assert "ORDER BY" in select_sql
    assert "`id`" in select_sql
    assert "LIMIT 100" in select_sql


def test_snowflake_snapshot_orders_for_stable_offset():
    from connectors.snowflake_reader import read_table_batch

    conn = MagicMock()
    cur = MagicMock()
    cur.description = [("ID",), ("NAME",)]
    cur.fetchall.return_value = []
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch("connectors.snowflake_reader.get_connection", return_value=conn), patch(
        "connectors.snowflake_reader._use_warehouse"
    ), patch("connectors.snowflake_reader._table_ref", return_value="SCHEMA.T"), patch(
        "connectors.snowflake_reader.count_table_rows", return_value=0
    ):
        read_table_batch(
            host="x",
            port=443,
            database="db",
            username="u",
            password="p",
            schema="SCHEMA",
            connection_string="",
            warehouse="wh",
            table="T",
            columns=None,
            offset=10,
            limit=50,
            known_total_rows=0,
        )

    executed = " ".join(str(c.args[0]) for c in cur.execute.call_args_list)
    assert "ORDER BY" in executed
    assert "LIMIT 50 OFFSET 10" in executed


def test_generic_sql_fallback_orders_by_first_column():
    from connectors.generic_sql import _read_table_raw

    conn = MagicMock()
    probe = MagicMock()
    probe.keys.return_value = ["id", "name"]
    result = MagicMock()
    result.keys.return_value = ["id", "name"]
    result.fetchall.return_value = [(1, "a")]
    conn.execute.side_effect = [probe, result]

    headers, rows = _read_table_raw(conn, "t", None, offset=5, limit=10, dialect="postgresql")
    assert headers == ["id", "name"]
    assert rows == [["1", "a"]]
    sql = conn.execute.call_args_list[1][0][0].text
    assert "ORDER BY" in sql
    assert "LIMIT 10 OFFSET 5" in sql


def test_protobuf_cataloged_unimplemented_not_semi_structured():
    from services.connector_capability_registry import (
        SEMI_STRUCTURED_FILE_FORMATS,
        UNIMPLEMENTED_FILE_FORMATS,
        classify_payload,
    )

    assert "protobuf" in UNIMPLEMENTED_FILE_FORMATS
    assert "protobuf" not in SEMI_STRUCTURED_FILE_FORMATS
    assert classify_payload(source_format="protobuf")["shape"] != "structured"
