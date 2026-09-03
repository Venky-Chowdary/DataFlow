"""Unit tests for Milvus identity-COPY audit fixes."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_fast_path import FastPathUnavailable  # noqa: E402
from services.copy_milvus_common import (  # noqa: E402
    _milvus_all_filter,
    _milvus_assert_schema_matches,
    _milvus_assert_upsert_ok,
    _milvus_pk_info,
    _milvus_upsert_ack_count,
)


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


def test_milvus_large_collection_declines():
    from services.copy_milvus_common import milvus_query_offset_cap_exceeded
    from connectors.milvus_writer import _MILVUS_QUERY_WINDOW

    assert milvus_query_offset_cap_exceeded(_MILVUS_QUERY_WINDOW) is True
    assert milvus_query_offset_cap_exceeded(_MILVUS_QUERY_WINDOW - 1) is False


def test_milvus_milvus_copy_declines_large_collection(monkeypatch):
    from services.copy_milvus_milvus import copy_milvus_to_milvus

    monkeypatch.delenv("DATAFLOW_MILVUS_MILVUS_COPY", raising=False)
    monkeypatch.setattr(
        "services.copy_milvus_milvus.milvus_collection_exists",
        lambda _cfg, _coll: True,
    )
    monkeypatch.setattr(
        "services.copy_milvus_milvus.milvus_entity_count",
        lambda _cfg, _coll: 20000,
    )
    with pytest.raises(FastPathUnavailable, match="PK-segmented pagination"):
        copy_milvus_to_milvus(
            source_cfg={"host": "127.0.0.1", "port": 19530},
            source_table="src_coll",
            dest_cfg={"host": "127.0.0.1", "port": 19530},
            dest_table="dest_coll",
            pairs=[("id", "id")],
            milvus_ddls=["varchar"],
            replace_destination=True,
        )


def test_milvus_upsert_ack_count_from_data():
    assert _milvus_upsert_ack_count({"code": 0, "data": {"upsertCount": 7}}) == 7
    assert _milvus_upsert_ack_count({"data": {"insertCount": 3}}) == 3
    assert _milvus_upsert_ack_count({"data": {"upsertIds": ["a", "b"]}}) == 2
    assert _milvus_upsert_ack_count({"code": 0}) is None


def test_milvus_upsert_ok_accepts_matching_count():
    _milvus_assert_upsert_ok({"code": 0, "data": {"upsertCount": 2}}, 200, 2)


def test_milvus_upsert_rejects_partial_count():
    with pytest.raises(ValueError, match="upsertCount 1 != batch 2"):
        _milvus_assert_upsert_ok({"code": 0, "data": {"upsertCount": 1}}, 200, 2)


def test_milvus_upsert_rejects_missing_count():
    with pytest.raises(ValueError, match="missing upsertCount"):
        _milvus_assert_upsert_ok({"code": 0, "data": {}}, 200, 2)


def test_milvus_schema_mismatch_on_exists(monkeypatch):
    src = {
        "fields": [
            {"fieldName": "id", "dataType": "VarChar"},
            {"fieldName": "vector", "dataType": "FloatVector"},
        ]
    }
    dest = {
        "fields": [
            {"fieldName": "id", "dataType": "Int64"},
            {"fieldName": "vector", "dataType": "FloatVector"},
        ]
    }

    def _describe(cfg, _coll):
        return dest if cfg.get("_dest") else src

    monkeypatch.setattr(
        "services.copy_milvus_common._milvus_describe_raw",
        _describe,
    )
    with pytest.raises(FastPathUnavailable, match="schema does not match"):
        _milvus_assert_schema_matches(
            dest_cfg={"_dest": True},
            dest_collection="d",
            source_cfg={},
            source_collection="s",
        )


def test_milvus_skip_complete_when_counts_match(monkeypatch):
    from services.copy_milvus_milvus import copy_milvus_to_milvus

    monkeypatch.delenv("DATAFLOW_MILVUS_MILVUS_COPY", raising=False)
    monkeypatch.setattr(
        "services.copy_milvus_milvus.milvus_collection_exists",
        lambda _cfg, _coll: True,
    )

    def _count(_cfg, coll):
        return 40

    monkeypatch.setattr(
        "services.copy_milvus_milvus.milvus_entity_count",
        _count,
    )
    result = copy_milvus_to_milvus(
        source_cfg={"host": "127.0.0.1", "port": 19530},
        source_table="src_coll",
        dest_cfg={"host": "127.0.0.1", "port": 19530},
        dest_table="dest_coll",
        pairs=[("id", "id")],
        milvus_ddls=["varchar"],
        replace_destination=False,
    )
    assert result.source_snapshot.get("copy_split") == "skip"


def test_milvus_occupied_mismatch_declines(monkeypatch):
    from services.copy_milvus_milvus import copy_milvus_to_milvus

    monkeypatch.delenv("DATAFLOW_MILVUS_MILVUS_COPY", raising=False)
    monkeypatch.setattr(
        "services.copy_milvus_milvus.milvus_collection_exists",
        lambda _cfg, _coll: True,
    )

    def _count(_cfg, coll):
        return 2 if "dest" in coll else 80

    monkeypatch.setattr(
        "services.copy_milvus_milvus.milvus_entity_count",
        _count,
    )
    with pytest.raises(FastPathUnavailable, match="occupied Milvus dest"):
        copy_milvus_to_milvus(
            source_cfg={"host": "127.0.0.1", "port": 19530},
            source_table="src_coll",
            dest_cfg={"host": "127.0.0.1", "port": 19530},
            dest_table="dest_coll",
            pairs=[("id", "id")],
            milvus_ddls=["varchar"],
            replace_destination=False,
        )
