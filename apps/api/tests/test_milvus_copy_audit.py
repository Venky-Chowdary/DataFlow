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
    assert _milvus_all_filter({}, "coll") == "entity_key >= -9223372036854775808"


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


def test_milvus_large_collection_uses_pk_pagination(monkeypatch):
    from services.copy_milvus_milvus import copy_milvus_to_milvus

    monkeypatch.delenv("DATAFLOW_MILVUS_MILVUS_COPY", raising=False)
    monkeypatch.setattr(
        "services.copy_milvus_milvus.milvus_collection_exists",
        lambda _cfg, coll: True,
    )
    counts = {"src_coll": 20000, "dest_coll": 0}

    def _count(_cfg, coll):
        return counts["dest_coll" if "dest" in coll else "src_coll"]

    monkeypatch.setattr(
        "services.copy_milvus_milvus.milvus_entity_count",
        _count,
    )
    monkeypatch.setattr(
        "services.copy_milvus_milvus.milvus_delete_collection",
        lambda *_a, **_k: None,
    )
    monkeypatch.setattr(
        "services.copy_milvus_milvus.milvus_create_collection_from_source",
        lambda **_k: None,
    )

    def _upsert(**_k):
        counts["dest_coll"] = 20000
        return 20000

    monkeypatch.setattr(
        "services.copy_milvus_milvus.milvus_query_upsert",
        _upsert,
    )
    result = copy_milvus_to_milvus(
        source_cfg={"host": "127.0.0.1", "port": 19530},
        source_table="src_coll",
        dest_cfg={"host": "127.0.0.1", "port": 19530},
        dest_table="dest_coll",
        pairs=[("id", "id")],
        milvus_ddls=["varchar"],
        replace_destination=True,
    )
    assert result.source_rows == 20000
    assert result.target_rows == 20000
    assert result.source_snapshot.get("milvus_read") == "pk_query"
    assert result.source_snapshot.get("cdc_exactly_once_claimed") is False
    assert result.source_snapshot.get("production_sku") is False


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


def test_milvus_int_pk_windows_cover_large_collection_without_offset():
    from connectors.milvus_writer import milvus_int_pk_split_windows

    occupied = list(range(0, 20_000))

    def count_in_range(lo: int, hi: int) -> int:
        return sum(1 for x in occupied if lo <= x <= hi)

    windows = list(milvus_int_pk_split_windows(0, 19_999, count_in_range, 1000))
    assert windows
    assert all(0 < n <= 1000 for _lo, _hi, n in windows)
    assert sum(n for _lo, _hi, n in windows) == 20_000
    covered: set[int] = set()
    for lo, hi, _n in windows:
        covered.update(x for x in occupied if lo <= x <= hi)
    assert covered == set(occupied)


def test_milvus_pk_gt_and_all_filter_int_includes_negatives():
    from connectors.milvus_writer import milvus_all_pk_filter, milvus_pk_gt_filter

    assert milvus_all_pk_filter("pk", "Int64") == "pk >= -9223372036854775808"
    assert milvus_pk_gt_filter("pk", "Int64", 16383) == "pk > 16383"
    assert milvus_pk_gt_filter("doc_id", "VarChar", 'a"b') == 'doc_id > "a\\"b"'


def test_milvus_pks_strictly_increasing():
    from connectors.milvus_writer import milvus_pks_strictly_increasing

    assert milvus_pks_strictly_increasing([1, 2, 3], integer=True) is True
    assert milvus_pks_strictly_increasing([1, 1], integer=True) is False
    assert milvus_pks_strictly_increasing(["a", "b"], integer=False) is True
    assert milvus_pks_strictly_increasing(["b", "a"], integer=False) is False


def test_milvus_query_upsert_keyset_pages(monkeypatch):
    import json
    from connectors.milvus_writer import iter_milvus_query_pages

    pages = {
        None: [{"pk": i, "vector": [0.1]} for i in range(0, 3)],
        2: [{"pk": i, "vector": [0.1]} for i in range(3, 5)],
    }

    class _Resp:
        def __init__(self, payload, status=200):
            self.status_code = status
            self.content = b"x"
            self.text = json.dumps(payload)
            self._payload = payload

        def json(self):
            return self._payload

    class _Session:
        def post(self, url, data=None, headers=None, timeout=None):
            body = json.loads(data)
            filt = str(body.get("filter") or "")
            last = None
            if "pk > " in filt:
                last = int(filt.split("pk > ")[-1].split(")")[0].strip())
            rows = pages.get(last, [])
            if body.get("outputFields") == ["count(*)"]:
                return _Resp({"code": 0, "data": [{"count(*)": 5}]})
            return _Resp({"code": 0, "data": rows})

    got = list(
        iter_milvus_query_pages(
            session=_Session(),
            base_url="http://127.0.0.1:19530",
            headers={},
            collection="c",
            db_name="",
            pk_name="pk",
            pk_type="Int64",
            output_fields=["pk", "vector"],
            page_size=3,
        )
    )
    assert [row["pk"] for page in got for row in page] == [0, 1, 2, 3, 4]
