"""Wave L accuracy: Kafka registry types, Redis SCAN buffer, composite cursors, SaaS pagination."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_kafka_writer_resolves_logical_types_for_registry():
    from connectors.kafka_writer import _json_schema_property_for_logical
    from connectors.writer_common import resolve_target_columns

    cols, logical = resolve_target_columns(
        [{"source": "id", "target": "id"}, {"source": "amt", "target": "amt"}],
        {"id": "INTEGER", "amt": "DECIMAL(12,2)"},
        preserve_case=True,
    )
    assert cols == ["id", "amt"]
    assert "integer" in str(_json_schema_property_for_logical(logical[0])["type"])
    # Decimal wire is exact text (json_default) — Registry must not claim IEEE number.
    assert "string" in str(_json_schema_property_for_logical(logical[1])["type"])
    assert _json_schema_property_for_logical(logical[1]).get("contentMediaType") == "application/x-decimal"


def test_composite_cursor_compare_and_max():
    from services.sync_cursor import compare_cursor_values, max_cursor_value

    assert compare_cursor_values("2020-01-01|a", "2020-01-01|b") < 0
    assert compare_cursor_values("2020-01-02|a", "2020-01-01|z") > 0
    best = max_cursor_value(
        [["2020-01-01", "b"], ["2020-01-01", "a"], ["2019-12-31", "z"]],
        ["updated_at", "id"],
        "updated_at",
        "id",
    )
    assert best == "2020-01-01|b"


def test_redis_scan_buffers_overflow_keys():
    from connectors.redis_reader import RedisScanState, read_keys_batch

    state = RedisScanState()
    client = MagicMock()
    # First SCAN returns 3 keys with COUNT hint; we only want limit=2.
    client.scan.return_value = (0, [b"k1", b"k2", b"k3"])
    client.type.return_value = b"string"
    client.get.return_value = b"v"

    with patch("connectors.redis_reader._redis_client", return_value=client):
        batch, new_state = read_keys_batch(cfg={}, pattern="*", limit=2, scan_state=state)

    assert len(batch.rows) == 2
    assert new_state.pending_keys == ["k3"]
    assert new_state.scan_complete is True
    assert new_state.exhausted is False

    with patch("connectors.redis_reader._redis_client", return_value=client):
        batch2, final = read_keys_batch(cfg={}, pattern="*", limit=2, scan_state=new_state)
    assert len(batch2.rows) == 1
    assert final.exhausted is True
    assert not final.pending_keys


def test_rest_wraps_scalar_arrays():
    from connectors.rest_api import _extract_records

    rows = _extract_records([1, 2, "x"])
    assert rows == [{"value": 1}, {"value": 2}, {"value": "x"}]


def test_generic_sql_cursor_accepts_primary_key_param():
    import inspect

    from connectors.generic_sql import read_table_cursor_batch

    assert "cursor_primary_key" in inspect.signature(read_table_cursor_batch).parameters


def test_milvus_long_id_digested_not_truncated():
    from connectors.milvus_writer import build_milvus_entities

    long_id = "x" * 100
    entities, rejected = build_milvus_entities(
        [{"id": long_id, "content": "c", "embedding": [0.1, 0.2, 0.3], "metadata": {}}],
        dimension=3,
    )
    assert rejected == []
    assert len(entities[0]["id"]) == 64
    assert entities[0]["id"] != long_id[:64]
