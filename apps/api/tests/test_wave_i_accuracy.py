"""Wave I accuracy: Kafka Avro registry, Avro writer schema, Influx/Couchbase, ADLS, vector honesty."""

from __future__ import annotations

import io
import json
import struct
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_avro_schema_logical_types():
    from services.avro_schema import avro_type_to_logical, schema_map_from_avro

    schema = {
        "type": "record",
        "name": "Order",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "amt", "type": {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2}},
            {"name": "ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
            {"name": "tags", "type": {"type": "array", "items": "string"}},
            {"name": "opt", "type": ["null", "string"], "default": None},
        ],
    }
    mapped = schema_map_from_avro(schema)
    assert mapped["id"] == "INTEGER"
    assert mapped["amt"] == "DECIMAL(12,2)"
    assert mapped["ts"] == "TIMESTAMPTZ"
    assert mapped["tags"].startswith("ARRAY<")
    assert mapped["opt"] == "TEXT"
    assert avro_type_to_logical(["null", "int"]) == "INTEGER"


def test_confluent_avro_decode_and_refuse_protobuf(monkeypatch):
    fastavro = pytest.importorskip("fastavro")
    from connectors import confluent_schema_registry as csr

    schema = {
        "type": "record",
        "name": "Evt",
        "fields": [{"name": "n", "type": "string"}],
    }
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, {"n": "hello"})
    body = buf.getvalue()
    framed = bytes([0]) + struct.pack(">I", 7) + body

    monkeypatch.setattr(
        csr,
        "fetch_schema",
        lambda url, sid, **kw: {"schemaType": "AVRO", "schema": json.dumps(schema)},
    )
    out = csr.decode_kafka_value(framed, registry_url="http://registry")
    assert out == {"n": "hello"}

    monkeypatch.setattr(
        csr,
        "fetch_schema",
        lambda url, sid, **kw: {"schemaType": "PROTOBUF", "schema": "syntax='proto3';"},
    )
    with pytest.raises(csr.SchemaRegistryError, match="Protobuf"):
        csr.decode_kafka_value(framed, registry_url="http://registry")


def test_influx_unions_all_series_and_tags():
    from connectors.influxdb import _extract_rows

    body = {
        "results": [
            {
                "series": [
                    {
                        "name": "cpu",
                        "tags": {"host": "a"},
                        "columns": ["time", "value"],
                        "values": [["t1", 1]],
                    },
                    {
                        "name": "cpu",
                        "tags": {"host": "b"},
                        "columns": ["time", "value"],
                        "values": [["t2", 2]],
                    },
                ]
            }
        ]
    }
    headers, rows = _extract_rows(body)
    assert "host" in headers
    assert len(rows) == 2
    host_idx = headers.index("host")
    hosts = {r[host_idx] for r in rows}
    assert hosts == {"a", "b"}


def test_couchbase_unions_keys_and_preserves_missing():
    from connectors.couchbase import _extract_rows
    from services.value_serializer import DF_MISSING_SENTINEL, SQL_NULL_SENTINEL

    body = {
        "results": [
            {"doc": {"id": "1", "a": "x"}},
            {"doc": {"id": "2", "b": None}},
        ]
    }
    headers, rows = _extract_rows(body)
    assert set(headers) == {"a", "b", "id"}
    # Second doc missing "a"
    a_idx = headers.index("a")
    b_idx = headers.index("b")
    assert rows[1][a_idx] == DF_MISSING_SENTINEL
    assert rows[1][b_idx] == SQL_NULL_SENTINEL
    assert rows[0][b_idx] == DF_MISSING_SENTINEL


def test_qdrant_points_refuse_zero_and_stable_id():
    from connectors.qdrant_writer import build_qdrant_points

    points, rejected = build_qdrant_points(
        [{"id": "", "source_id": "s1", "chunk_index": 0, "content": "hello", "embedding": None}],
        dimension=3,
    )
    assert points == []
    assert rejected

    a, ra = build_qdrant_points(
        [{"source_id": "s1", "chunk_index": 0, "content": "hello", "embedding": [0.1, 0.2, 0.3]}],
        dimension=3,
    )
    b, rb = build_qdrant_points(
        [{"source_id": "s1", "chunk_index": 0, "content": "hello", "embedding": [0.1, 0.2, 0.3]}],
        dimension=3,
    )
    assert ra == [] and rb == []
    assert a[0]["id"] == b[0]["id"]


def test_kafka_infer_prefers_registry_schema(monkeypatch):
    from connectors import kafka_reader as kr

    monkeypatch.setattr(
        "connectors.confluent_schema_registry.fetch_latest_subject_schema",
        lambda url, subject, **kw: {
            "schemaType": "AVRO",
            "schema": json.dumps({
                "type": "record",
                "name": "T",
                "fields": [
                    {"name": "order_id", "type": "long"},
                    {"name": "amt", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}},
                ],
            }),
        },
    )

    def _boom(*_a, **_k):
        raise AssertionError("should not poll samples when registry schema resolves")

    monkeypatch.setattr(kr, "read_topic_batch", _boom)
    schema, native, warning = kr.infer_topic_schema(
        {"schema_registry_url": "http://registry", "host": "localhost"},
        "orders",
    )
    assert schema["order_id"] == "INTEGER"
    assert schema["amt"] == "DECIMAL(10,2)"
    assert "Registry" in warning


def test_adls_introspect_wired():
    from services import object_store_introspect as osi

    assert hasattr(osi, "introspect_adls_object")
