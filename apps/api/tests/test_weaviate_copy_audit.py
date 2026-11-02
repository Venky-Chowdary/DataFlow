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


def test_weaviate_copy_large_class_uses_cursor(monkeypatch):
    from services.copy_weaviate_weaviate import copy_weaviate_to_weaviate

    monkeypatch.delenv("DATAFLOW_WEAVIATE_WEAVIATE_COPY", raising=False)
    monkeypatch.setattr(
        "services.copy_weaviate_weaviate.weaviate_class_exists",
        lambda _cfg, _cls: True,
    )
    counts = {"SrcClass": 15000, "DestClass": 0}

    def _count(_cfg, cls):
        return counts.get(cls, 0)

    monkeypatch.setattr(
        "services.copy_weaviate_weaviate.weaviate_object_count",
        _count,
    )
    monkeypatch.setattr(
        "services.copy_weaviate_weaviate.weaviate_delete_class",
        lambda *_a, **_k: None,
    )
    monkeypatch.setattr(
        "services.copy_weaviate_weaviate.weaviate_create_class_from_source",
        lambda **_k: None,
    )

    def _upsert(**_k):
        counts["DestClass"] = 15000
        return 15000

    monkeypatch.setattr(
        "services.copy_weaviate_weaviate.weaviate_list_batch_upsert",
        _upsert,
    )
    result = copy_weaviate_to_weaviate(
        source_cfg={"host": "127.0.0.1", "port": 8080},
        source_table="SrcClass",
        dest_cfg={"host": "127.0.0.1", "port": 8080},
        dest_table="DestClass",
        pairs=[("id", "id")],
        weaviate_ddls=["uuid"],
        replace_destination=True,
    )
    assert result.source_rows == 15000
    assert result.target_rows == 15000
    assert result.source_snapshot.get("weaviate_read") == "cursor_list"
    assert result.source_snapshot.get("cdc_exactly_once_claimed") is False
    assert result.source_snapshot.get("production_sku") is False


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


def test_weaviate_cursor_params_never_send_offset():
    from connectors.weaviate_writer import weaviate_cursor_list_params

    first = weaviate_cursor_list_params(
        class_name="Article", limit=100, include="vector"
    )
    assert first == {"class": "Article", "limit": 100, "include": "vector"}
    assert "offset" not in first
    nxt = weaviate_cursor_list_params(
        class_name="Article",
        limit=100,
        after="002d5cb3-298b-380d-addb-2e026b76c8ed",
        include="vector",
    )
    assert nxt["after"] == "002d5cb3-298b-380d-addb-2e026b76c8ed"
    assert "offset" not in nxt


def test_weaviate_after_cursor_walks_past_offset_cap():
    import json
    from connectors.weaviate_writer import iter_weaviate_objects_after

    ids = [f"00000000-0000-0000-0000-{i:012d}" for i in range(5)]

    class _Resp:
        def __init__(self, payload, status=200):
            self.status_code = status
            self.content = b"x"
            self.text = json.dumps(payload)
            self._payload = payload

        def json(self):
            return self._payload

    class _Session:
        def __init__(self):
            self.params_seen: list[dict] = []

        def get(self, url, headers=None, params=None, timeout=None):
            p = dict(params or {})
            self.params_seen.append(p)
            assert "offset" not in p
            after = p.get("after")
            if after is None:
                chunk = ids[:3]
            elif after == ids[2]:
                chunk = ids[3:]
            else:
                chunk = []
            return _Resp({"objects": [{"id": oid, "properties": {}} for oid in chunk]})

    session = _Session()
    pages = list(
        iter_weaviate_objects_after(
            session=session,
            base_url="http://127.0.0.1:8080",
            headers={},
            class_name="Article",
            page_size=3,
            include="vector",
        )
    )
    got = [obj["id"] for page in pages for obj in page]
    assert got == ids
    assert session.params_seen[0].get("after") is None
    assert session.params_seen[1]["after"] == ids[2]
