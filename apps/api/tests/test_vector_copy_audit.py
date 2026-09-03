"""Unit tests for vector identity-COPY audit fixes (Milvus PK filter, Weaviate batch)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_milvus_common import _milvus_all_filter, _milvus_pk_info  # noqa: E402
from services.copy_weaviate_common import _weaviate_assert_batch_ok  # noqa: E402


def test_milvus_pk_info_int_primary_key():
    describe = {
        "fields": [
            {"fieldName": "pk", "dataType": "Int64", "isPrimary": True},
            {"fieldName": "vector", "dataType": "FloatVector"},
        ]
    }
    name, dtype = _milvus_pk_info(describe)
    assert name == "pk"
    assert "INT" in dtype.upper()


def test_milvus_pk_info_varchar_primary_key():
    describe = {
        "fields": [
            {"fieldName": "doc_id", "dataType": "VarChar", "primaryKey": True},
        ]
    }
    name, dtype = _milvus_pk_info(describe)
    assert name == "doc_id"
    assert dtype == "VarChar"


def test_milvus_all_filter_uses_pk_field_name(monkeypatch):
    describe = {
        "fields": [
            {"fieldName": "entity_key", "dataType": "Int64", "isPrimary": True},
        ]
    }

    def _fake_describe(_cfg, _collection):
        return describe

    monkeypatch.setattr(
        "services.copy_milvus_common._milvus_describe_raw",
        _fake_describe,
    )
    assert _milvus_all_filter({}, "coll") == "entity_key >= 0"


def test_milvus_query_window_respects_limit_plus_offset():
    from connectors.milvus_writer import _MILVUS_QUERY_WINDOW

    offset = 15400
    limit = min(1000, 50000 - offset, _MILVUS_QUERY_WINDOW - 1 - offset)
    assert offset + limit < _MILVUS_QUERY_WINDOW


def test_weaviate_batch_ok_accepts_clean_response():
    batch = [{"class": "C", "id": "1"}]
    response = [{"id": "1", "result": {"status": "SUCCESS"}}]
    _weaviate_assert_batch_ok(batch, response)


def test_weaviate_batch_rejects_per_object_failure():
    batch = [{"class": "C", "id": "1"}, {"class": "C", "id": "2"}]
    response = [
        {"id": "1", "result": {"status": "SUCCESS"}},
        {"id": "2", "result": {"status": "FAILED", "errors": {"error": "bad vector"}}},
    ]
    with pytest.raises(ValueError, match="rejected 1 batch"):
        _weaviate_assert_batch_ok(batch, response)


def test_weaviate_batch_rejects_incomplete_ack():
    batch = [{"class": "C", "id": "1"}, {"class": "C", "id": "2"}]
    with pytest.raises(ValueError, match="incomplete per-object"):
        _weaviate_assert_batch_ok(batch, [{"id": "1"}])
