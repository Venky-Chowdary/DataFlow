"""Wave N accuracy: composite cursors, Kafka decimal schema, bucket/create honesty."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest


def test_kafka_registry_decimal_schema_is_exact_string_not_number():
    from connectors.kafka_writer import _json_schema_property_for_logical

    decimal_schema = _json_schema_property_for_logical("DECIMAL(18,4)")
    assert decimal_schema["type"] == ["string", "null"]
    assert "contentMediaType" in decimal_schema
    float_schema = _json_schema_property_for_logical("FLOAT")
    assert float_schema["type"] == ["number", "null"]


def test_kafka_json_default_emits_decimal_as_string():
    from services.value_serializer import json_default

    assert json_default(Decimal("123.450")) == "123.450"


def test_snowflake_cursor_sql_uses_composite_tiebreak():
    from connectors.snowflake_reader import read_table_cursor_batch

    cur = MagicMock()
    cur.description = [("UPDATED_AT",), ("ID",), ("NAME",)]
    cur.fetchall.return_value = [("2024-01-01", "b", "x")]
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = False

    with patch("connectors.snowflake_reader.normalize_account", return_value="acct"), patch(
        "connectors.snowflake_reader.get_connection", return_value=conn
    ), patch("connectors.snowflake_reader._use_warehouse"), patch(
        "connectors.snowflake_reader._table_ref", return_value='"PUBLIC"."ORDERS"'
    ):
        batch = read_table_cursor_batch(
            host="acct",
            port=443,
            database="DB",
            username="u",
            password="p",
            schema="PUBLIC",
            connection_string="",
            warehouse="WH",
            table="ORDERS",
            cursor_column="UPDATED_AT",
            cursor_after="2024-01-01|a",
            columns=["UPDATED_AT", "ID", "NAME"],
            limit=10,
            cursor_primary_key="ID",
        )

    sql = cur.execute.call_args[0][0]
    params = cur.execute.call_args[0][1]
    assert "(UPDATED_AT" in sql.upper().replace('"', "") or "UPDATED_AT" in sql
    assert ">" in sql
    assert params[0] == "2024-01-01"
    assert params[1] == "a"
    assert batch.rows


def test_bigquery_cursor_builds_composite_predicate():
    from connectors.bigquery_reader import read_table_cursor_batch

    job = MagicMock()
    job.schema = [MagicMock(name="updated_at"), MagicMock(name="id")]
    # field.name attribute
    job.schema[0].name = "updated_at"
    job.schema[1].name = "id"
    row = MagicMock()
    row.values.return_value = ["2024-01-01", "b"]
    job.result.return_value = [row]
    client = MagicMock()
    client.query.return_value = job

    with patch("connectors.bigquery_conn.get_client", return_value=client), patch(
        "google.cloud.bigquery.ScalarQueryParameter",
        side_effect=lambda name, typ, value: {"name": name, "type": typ, "value": value},
    ), patch(
        "google.cloud.bigquery.QueryJobConfig",
        side_effect=lambda **kwargs: kwargs,
    ):
        batch = read_table_cursor_batch(
            host="proj",
            port=443,
            database="proj",
            username="",
            password="",
            schema="ds",
            connection_string="",
            ssl=False,
            table="orders",
            cursor_column="updated_at",
            cursor_after="2024-01-01|a",
            columns=["updated_at", "id"],
            limit=50,
            cursor_primary_key="id",
        )

    query = client.query.call_args[0][0]
    assert "@cursor" in query
    assert "@pk" in query
    assert "ORDER BY" in query
    assert batch.rows[0][1] == "b"


def test_s3_ensure_bucket_403_does_not_create():
    from botocore.exceptions import ClientError

    from connectors.s3_writer import _ensure_bucket

    client = MagicMock()
    err = ClientError(
        {"Error": {"Code": "403", "Message": "Forbidden"}, "ResponseMetadata": {"HTTPStatusCode": 403}},
        "HeadBucket",
    )
    client.head_bucket.side_effect = err
    with pytest.raises(RuntimeError, match="Cannot verify S3 bucket"):
        _ensure_bucket(client, "secret-bucket", {"host": "s3.amazonaws.com"})
    client.create_bucket.assert_not_called()


def test_s3_ensure_bucket_404_creates():
    from botocore.exceptions import ClientError

    from connectors.s3_writer import _ensure_bucket

    client = MagicMock()
    err = ClientError(
        {"Error": {"Code": "404", "Message": "Not Found"}, "ResponseMetadata": {"HTTPStatusCode": 404}},
        "HeadBucket",
    )
    client.head_bucket.side_effect = err
    _ensure_bucket(client, "new-bucket", {"host": "localhost", "port": 4566, "endpoint_url": "http://localhost:4566"})
    assert client.create_bucket.called


def test_pgvector_create_table_false_skips_ddl_and_fails_closed():
    from connectors.pgvector_writer import write_mapped_rows

    cur = MagicMock()
    cur.fetchone.return_value = (None,)
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = False

    with patch("connectors.pgvector_writer.importlib.util.find_spec", return_value=object()), patch(
        "connectors.pgvector_writer.vectorize_records",
        return_value=[{"content": "hi", "embedding": [0.1, 0.2], "source_id": "1", "chunk_index": 0}],
    ), patch(
        "connectors.pgvector_writer.get_connection",
        return_value=conn,
    ), patch(
        "connectors.pgvector_writer._exec_schema_table"
    ) as exec_ddl:
        result = write_mapped_rows(
            host="localhost",
            port=5432,
            database="dataflow",
            username="u",
            password="p",
            schema="public",
            connection_string="",
            ssl=False,
            table_name="chunks",
            headers=["id", "content"],
            data_rows=[["1", "hi"]],
            mappings=[],
            column_types={},
            create_table=False,
            embedding_model="hash/32",
        )

    exec_ddl.assert_not_called()
    assert result.ok is False
    assert "create_table is disabled" in (result.error or "")


def test_mongo_composite_cursor_query_shape():
    from connectors.mongodb_reader import read_collection_cursor_batch

    coll = MagicMock()
    coll.count_documents.return_value = 1
    find_cur = MagicMock()
    find_cur.sort.return_value.limit.return_value = iter(
        [{"_id": "b", "updated_at": "2024-01-01", "name": "x"}]
    )
    coll.find.return_value = find_cur
    db = MagicMock()
    db.__getitem__.return_value = coll
    client = MagicMock()
    client.__getitem__.return_value = db

    with patch("connectors.mongodb_reader._mongo_client", return_value=client), patch(
        "connectors.mongodb_reader._connection_string", return_value="mongodb://localhost"
    ), patch(
        "connectors.mongodb_reader.expand_mongo_documents",
        side_effect=lambda docs, cfg=None: docs,
    ):
        batch = read_collection_cursor_batch(
            cfg={},
            database="db",
            collection="orders",
            cursor_column="updated_at",
            cursor_after="2024-01-01|a",
            cursor_type="STRING",
            limit=10,
            cursor_primary_key="_id",
        )

    query = coll.find.call_args[0][0]
    assert "$or" in query
    assert any("$gt" in branch.get("updated_at", {}) for branch in query["$or"] if isinstance(branch.get("updated_at"), dict))
    assert batch.rows
