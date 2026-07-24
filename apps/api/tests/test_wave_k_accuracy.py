"""Wave K accuracy: CDC nulls, Redis/Dynamo/Mongo identity, ES insert create, Kafka schema, Avro stream."""

from __future__ import annotations

import io
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_cdc_matrix_preserves_sql_null_and_missing():
    from src.transfer.cdc_transfer import _records_to_matrix
    from services.value_serializer import DF_MISSING_SENTINEL, SQL_NULL_SENTINEL

    rows = _records_to_matrix(
        [{"id": 1, "note": None}, {"id": 2}],
        ["id", "note", "extra"],
    )
    assert rows[0][1] == SQL_NULL_SENTINEL
    assert rows[0][2] == DF_MISSING_SENTINEL
    assert rows[1][1] == DF_MISSING_SENTINEL


def test_redis_refuses_batch_index_identity():
    from connectors.redis_writer import _resolve_redis_key_id

    key, col = _resolve_redis_key_id(
        {"name": "x"},  # natural id column present but empty/missing
        ["name", "id"],
        conflict_columns=[],
        row_index=0,
    )
    assert key is None
    assert col == "id"


def test_kafka_json_schema_types_from_logical():
    from connectors.kafka_writer import _json_schema_property_for_logical

    assert "integer" in str(_json_schema_property_for_logical("INTEGER")["type"])
    assert "string" in str(_json_schema_property_for_logical("DECIMAL(12,2)")["type"])
    assert "number" in str(_json_schema_property_for_logical("FLOAT")["type"])
    assert "boolean" in str(_json_schema_property_for_logical("BOOLEAN")["type"])
    assert _json_schema_property_for_logical("ARRAY<TEXT>")["type"] == "array"


def test_es_insert_uses_create_op_type():
    from connectors.elasticsearch_writer import write_mapped_rows

    captured: list[dict] = []

    class FakeIndices:
        def exists(self, index):
            return True

        def create(self, **kwargs):
            return None

        def refresh(self, index):
            return None

    class FakeClient:
        indices = FakeIndices()

        def close(self):
            return None

    def fake_bulk(client, actions, raise_on_error=False):
        captured.extend(list(actions))
        return len(actions), []

    with patch("connectors.elasticsearch_writer._client", return_value=FakeClient()), patch(
        "elasticsearch.helpers.bulk", fake_bulk
    ):
        result = write_mapped_rows(
            host="localhost",
            port=9200,
            database="orders",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            table_name="orders",
            headers=["id", "amount"],
            data_rows=[["1", "10"]],
            mappings=[{"source": "id", "target": "id"}, {"source": "amount", "target": "amount"}],
            column_types={"id": "TEXT", "amount": "INTEGER"},
            conflict_columns=["id"],
            write_mode="insert",
            create_table=False,
        )
    assert result.ok
    assert captured and captured[0].get("_op_type") == "create"
    assert captured[0].get("_id") == "1"


def test_es_mapping_nested_and_identity():
    from services.schema_introspect import _es_mapping_type

    assert _es_mapping_type("nested") == "ARRAY<JSON>"
    assert _es_mapping_type("geo_point") == "GEOGRAPHY"
    assert _es_mapping_type("date_nanos") == "TIMESTAMPTZ"


def test_avro_streaming_batches():
    fastavro = pytest.importorskip("fastavro")
    from src.transfer.file_stream import _batch_iterator_for_type, peek_file_source

    schema = {
        "type": "record",
        "name": "R",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "opt", "type": ["null", "string"], "default": None},
        ],
    }
    buf = io.BytesIO()
    fastavro.writer(buf, schema, [{"id": 1, "opt": "a"}, {"id": 2, "opt": None}])
    raw = buf.getvalue()

    headers, schema_map, total, sample = peek_file_source(raw, "t.avro")
    assert total == 2
    assert "id" in headers
    assert schema_map.get("id") == "INTEGER"

    batches = list(_batch_iterator_for_type("avro", raw, 1))
    flat = [r for b in batches for r in b]
    assert len(flat) == 2
    assert flat[0]["id"] == 1


def test_dynamo_fail_policy_on_transform_errors():
    from connectors.dynamodb_writer import write_mapped_rows

    with patch(
        "connectors.dynamodb_writer.build_mapped_rows_with_details",
        return_value=([], ["bad type"], [{"row": 1, "reason": "bad type"}]),
    ), patch("connectors.dynamodb_writer.boto3_client"):
        result = write_mapped_rows(
            host="",
            port=0,
            database="t",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            table_name="t",
            headers=["id"],
            data_rows=[["1"]],
            mappings=[{"source": "id", "target": "id"}],
            column_types={"id": "TEXT"},
            error_policy="fail",
            create_table=False,
        )
    assert result.ok is False
    assert "Transform" in (result.error or "")
