"""Unit tests for Weaviate identity-COPY batch ack validation."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_fast_path import FastPathUnavailable  # noqa: E402
from services.copy_weaviate_common import (  # noqa: E402
    _batch_object,
    _weaviate_assert_batch_ok,
)


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


def test_weaviate_batch_object_copies_named_vectors():
    obj = {
        "id": "abc",
        "properties": {"title": "x"},
        "vectors": {"default": [0.1, 0.2], "title_vec": [0.3, 0.4]},
    }
    out = _batch_object(obj, "DestClass")
    assert out["class"] == "DestClass"
    assert out["id"] == "abc"
    assert out["vectors"]["default"] == [0.1, 0.2]
    assert "vector" not in out


def test_weaviate_list_offset_cap():
    from services.copy_weaviate_common import (
        _WEAVIATE_LIST_MAX,
        weaviate_list_offset_cap_exceeded,
    )

    assert weaviate_list_offset_cap_exceeded(_WEAVIATE_LIST_MAX) is False
    assert weaviate_list_offset_cap_exceeded(_WEAVIATE_LIST_MAX + 1) is True


def test_weaviate_copy_declines_large_class(monkeypatch):
    from services.copy_weaviate_weaviate import copy_weaviate_to_weaviate

    monkeypatch.delenv("DATAFLOW_WEAVIATE_WEAVIATE_COPY", raising=False)
    monkeypatch.setattr(
        "services.copy_weaviate_weaviate.weaviate_class_exists",
        lambda _cfg, _cls: True,
    )
    monkeypatch.setattr(
        "services.copy_weaviate_weaviate.weaviate_object_count",
        lambda _cfg, _cls: 15000,
    )
    with pytest.raises(FastPathUnavailable, match="cursor pagination"):
        copy_weaviate_to_weaviate(
            source_cfg={"host": "127.0.0.1", "port": 8080},
            source_table="SrcClass",
            dest_cfg={"host": "127.0.0.1", "port": 8080},
            dest_table="DestClass",
            pairs=[("id", "id")],
            weaviate_ddls=["uuid"],
            replace_destination=True,
        )


def test_weaviate_skip_complete_when_counts_match(monkeypatch):
    from services.copy_weaviate_weaviate import copy_weaviate_to_weaviate

    monkeypatch.delenv("DATAFLOW_WEAVIATE_WEAVIATE_COPY", raising=False)
    monkeypatch.setattr(
        "services.copy_weaviate_weaviate.weaviate_class_exists",
        lambda _cfg, _cls: True,
    )
    monkeypatch.setattr(
        "services.copy_weaviate_weaviate.weaviate_object_count",
        lambda _cfg, _cls: 30,
    )
    result = copy_weaviate_to_weaviate(
        source_cfg={"host": "127.0.0.1", "port": 8080},
        source_table="SrcClass",
        dest_cfg={"host": "127.0.0.1", "port": 8080},
        dest_table="DestClass",
        pairs=[("id", "id")],
        weaviate_ddls=["uuid"],
        replace_destination=False,
    )
    assert result.source_snapshot.get("copy_split") == "skip"


def test_weaviate_occupied_mismatch_declines(monkeypatch):
    from services.copy_weaviate_weaviate import copy_weaviate_to_weaviate

    monkeypatch.delenv("DATAFLOW_WEAVIATE_WEAVIATE_COPY", raising=False)
    monkeypatch.setattr(
        "services.copy_weaviate_weaviate.weaviate_class_exists",
        lambda _cfg, _cls: True,
    )

    def _count(_cfg, cls):
        return 2 if "Dest" in cls else 80

    monkeypatch.setattr(
        "services.copy_weaviate_weaviate.weaviate_object_count",
        _count,
    )
    with pytest.raises(FastPathUnavailable, match="occupied Weaviate dest"):
        copy_weaviate_to_weaviate(
            source_cfg={"host": "127.0.0.1", "port": 8080},
            source_table="SrcClass",
            dest_cfg={"host": "127.0.0.1", "port": 8080},
            dest_table="DestClass",
            pairs=[("id", "id")],
            weaviate_ddls=["uuid"],
            replace_destination=False,
        )
