"""CI-stable proofs for Milvus destination writer (no live cluster required)."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.milvus_writer import (
    build_milvus_entities,
    scan_source_ids,
    test_milvus as probe_milvus,
    write_mapped_rows,
)


def test_build_milvus_entities_maps_rows():
    rows = [
        {
            "id": "abc123",
            "content": "hello milvus",
            "source_id": "1",
            "chunk_index": 0,
            "embedding": [0.1, 0.2, 0.3],
            "metadata": {"page": "2", "heading": "Intro", "filename": "doc.pdf"},
        }
    ]
    entities, rejected = build_milvus_entities(rows, dimension=3)
    assert rejected == []
    assert len(entities) == 1
    assert entities[0]["id"] == "abc123"
    assert entities[0]["vector"] == [0.1, 0.2, 0.3]
    assert entities[0]["content"] == "hello milvus"
    assert entities[0]["page"] == "2"
    assert entities[0]["heading"] == "Intro"


def test_build_milvus_entities_refuses_zero_vector():
    entities, rejected = build_milvus_entities(
        [{"id": "x", "content": "c", "embedding": None}],
        dimension=3,
    )
    assert entities == []
    assert rejected
    reason = (rejected[0].get("reason") or "").lower()
    assert "embedding" in reason or "refuse" in reason


def test_milvus_probe_unreachable_fail_closed():
    ok, msg = probe_milvus(host="127.0.0.1", port=1, api_key="", ssl=False)
    assert not ok
    assert msg


def test_milvus_write_unreachable_fail_closed():
    result = write_mapped_rows(
        host="127.0.0.1",
        port=1,
        database="",
        username="root",
        password="Milvus",
        schema="",
        connection_string="",
        ssl=False,
        table_name="chunks",
        headers=["id", "content", "vec"],
        data_rows=[["1", "hello", "[0.1,0.2,0.3]"]],
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "content", "target": "content"},
            {"source": "vec", "target": "vec"},
        ],
        column_types={"id": "STRING", "content": "STRING", "vec": "STRING"},
        content_column="content",
        embedding_column="vec",
    )
    assert not result.ok
    assert result.error


class _FakeResp:
    def __init__(self, status: int, payload: dict):
        self.status_code = status
        self.content = b"{}"
        self.text = ""
        self._payload = payload

    def json(self):
        return self._payload


class _MilvusSession:
    def __init__(self, *, has: bool, fields: list[dict], physical: int, entities: list[dict]):
        self.has = has
        self.fields = fields
        self.physical = physical
        self.entities = entities

    def post(self, url, data=None, headers=None, timeout=None):
        if url.endswith("/collections/has"):
            return _FakeResp(200, {"code": 0, "data": {"has": self.has}})
        if url.endswith("/collections/describe"):
            return _FakeResp(200, {"code": 0, "data": {"fields": self.fields}})
        if url.endswith("/entities/query"):
            import json as _json

            body = _json.loads(data or "{}")
            fields = body.get("outputFields") or []
            if "count(*)" in fields:
                return _FakeResp(200, {"code": 0, "data": [{"count(*)": self.physical}]})
            return _FakeResp(200, {"code": 0, "data": self.entities})
        return _FakeResp(500, {"code": 1, "message": url})


def test_milvus_scan_source_ids_missing_collection_is_zero(monkeypatch):
    monkeypatch.setattr(
        "connectors.milvus_writer._requests_session",
        lambda: _MilvusSession(has=False, fields=[], physical=0, entities=[]),
    )
    state, values = scan_source_ids(
        {"host": "127.0.0.1", "port": 19530}, table_name="chunks"
    )
    assert state == "missing"
    assert values == []


def test_milvus_scan_source_ids_distinct_not_rowcount(monkeypatch):
    fields = [
        {"fieldName": "id", "dataType": "VarChar"},
        {"fieldName": "source_id", "dataType": "VarChar"},
        {"fieldName": "vector", "dataType": "FloatVector"},
    ]
    entities = [
        {"source_id": "doc-1"},
        {"source_id": "doc-1"},
        {"source_id": "doc-1"},
        {"source_id": "doc-2"},
        {"source_id": "doc-2"},
    ]
    monkeypatch.setattr(
        "connectors.milvus_writer._requests_session",
        lambda: _MilvusSession(has=True, fields=fields, physical=5, entities=entities),
    )
    state, values = scan_source_ids(
        {"host": "127.0.0.1", "port": 19530}, table_name="chunks"
    )
    assert state == "complete"
    assert values == ["doc-1", "doc-1", "doc-1", "doc-2", "doc-2"]
    from services.dest_precount import identity_count_from_source_id_scan

    assert identity_count_from_source_id_scan(state, values) == 2


def test_milvus_scan_source_ids_no_source_id_field(monkeypatch):
    fields = [
        {"fieldName": "id", "dataType": "VarChar"},
        {"fieldName": "vector", "dataType": "FloatVector"},
    ]
    monkeypatch.setattr(
        "connectors.milvus_writer._requests_session",
        lambda: _MilvusSession(has=True, fields=fields, physical=5, entities=[]),
    )
    state, values = scan_source_ids(
        {"host": "127.0.0.1", "port": 19530}, table_name="chunks"
    )
    assert state == "no_field"
    assert values == []


def test_milvus_scan_source_ids_truncated_past_rest_window(monkeypatch):
    fields = [
        {"fieldName": "id", "dataType": "VarChar"},
        {"fieldName": "source_id", "dataType": "VarChar"},
    ]
    monkeypatch.setattr(
        "connectors.milvus_writer._requests_session",
        lambda: _MilvusSession(has=True, fields=fields, physical=20_000, entities=[]),
    )
    state, values = scan_source_ids(
        {"host": "127.0.0.1", "port": 19530}, table_name="chunks", max_entities=20_000
    )
    assert state == "truncated"
    assert values == []
