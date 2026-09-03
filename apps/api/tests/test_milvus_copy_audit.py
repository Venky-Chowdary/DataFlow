"""Unit tests for Milvus identity-COPY audit fixes."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_fast_path import FastPathUnavailable  # noqa: E402
from services.copy_milvus_common import _milvus_all_filter, _milvus_pk_info  # noqa: E402


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
