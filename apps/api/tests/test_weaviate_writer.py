"""CI-stable proofs for Weaviate destination writer (no live cluster required)."""

from __future__ import annotations

import json
import sys
import uuid
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.weaviate_writer import (
    _object_uuid,
    build_weaviate_objects,
    scan_source_ids,
    test_weaviate as probe_weaviate,
    write_mapped_rows,
)


def test_object_uuid_from_hash_is_valid_uuid():
    hid = "a" * 32
    uid = _object_uuid(hid)
    parsed = uuid.UUID(uid)
    assert str(parsed) == uid
    assert _object_uuid(hid) == uid  # deterministic


def test_build_weaviate_objects_maps_rows():
    rows = [
        {
            "id": "b" * 32,
            "content": "hello",
            "source_id": "1",
            "chunk_index": 0,
            "embedding": [0.1, 0.2],
            "metadata": {"page": "1", "heading": "Intro"},
        }
    ]
    objects, _rejected = build_weaviate_objects(rows, class_name="DataflowChunk", dimension=2)
    assert len(objects) == 1
    assert objects[0]["class"] == "DataflowChunk"
    assert objects[0]["properties"]["content"] == "hello"
    assert objects[0]["properties"]["page"] == "1"
    assert objects[0]["vector"] == [0.1, 0.2]
    uuid.UUID(objects[0]["id"])  # must be valid UUID


def test_build_weaviate_objects_rejects_missing_embedding():
    rows = [
        {"id": "b" * 32, "content": "ok", "embedding": [0.1, 0.2]},
        {"id": "c" * 32, "content": "bad", "embedding": None},
    ]
    objects, rejected = build_weaviate_objects(rows, class_name="DataflowChunk", dimension=2)
    assert len(objects) == 1
    assert len(rejected) == 1


def test_weaviate_probe_unreachable_fail_closed():
    ok, msg = probe_weaviate(host="127.0.0.1", port=1, api_key="", ssl=False)
    assert not ok
    assert msg


def test_weaviate_write_unreachable_fail_closed():
    result = write_mapped_rows(
        host="127.0.0.1",
        port=1,
        database="",
        username="",
        password="",
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
    def __init__(self, status: int, payload: dict | None = None):
        self.status_code = status
        self._payload = payload or {}
        self.text = json.dumps(self._payload) if payload is not None else ""
        self.content = self.text.encode("utf-8") if self.text else b""

    def json(self):
        return json.loads(self.text) if self.text else {}


class _WeaviateSession:
    def __init__(
        self,
        *,
        schema_status: int = 200,
        properties: list[dict] | None = None,
        physical: int = 0,
        objects: list[dict] | None = None,
        aggregate_ok: bool = True,
    ):
        self.schema_status = schema_status
        self.properties = list(properties or [])
        self.physical = physical
        self.objects = list(objects or [])
        self.aggregate_ok = aggregate_ok

    def get(self, url, headers=None, params=None, timeout=None):
        path = str(url)
        if "/v1/schema/" in path:
            if self.schema_status != 200:
                return _FakeResp(self.schema_status, {})
            return _FakeResp(200, {"class": "Docs", "properties": self.properties})
        if path.endswith("/v1/objects") or "/v1/objects?" in path:
            return _FakeResp(200, {"objects": self.objects})
        return _FakeResp(500, {})

    def post(self, url, data=None, headers=None, timeout=None):
        if str(url).endswith("/v1/graphql"):
            if not self.aggregate_ok:
                return _FakeResp(200, {"errors": [{"message": "aggregate failed"}]})
            return _FakeResp(
                200,
                {"data": {"Aggregate": {"Docs": [{"meta": {"count": self.physical}}]}}},
            )
        return _FakeResp(500, {})


def test_weaviate_scan_source_ids_missing_class_is_zero(monkeypatch):
    monkeypatch.setattr(
        "connectors.weaviate_writer._requests_session",
        lambda: _WeaviateSession(schema_status=404),
    )
    state, values = scan_source_ids(
        {"host": "127.0.0.1", "port": 8080}, table_name="docs"
    )
    assert state == "missing"
    assert values == []
    from services.dest_precount import identity_count_from_source_id_scan

    assert identity_count_from_source_id_scan(state, values) == 0


def test_weaviate_scan_source_ids_distinct_not_aggregate_count(monkeypatch):
    props = [{"name": "source_id", "dataType": ["text"]}, {"name": "content", "dataType": ["text"]}]
    objects = [
        {"id": "1", "properties": {"source_id": "doc-1"}},
        {"id": "2", "properties": {"source_id": "doc-1"}},
        {"id": "3", "properties": {"source_id": "doc-1"}},
        {"id": "4", "properties": {"source_id": "doc-2"}},
        {"id": "5", "properties": {"source_id": "doc-2"}},
    ]
    monkeypatch.setattr(
        "connectors.weaviate_writer._requests_session",
        lambda: _WeaviateSession(properties=props, physical=5, objects=objects),
    )
    state, values = scan_source_ids(
        {"host": "127.0.0.1", "port": 8080}, table_name="docs"
    )
    assert state == "complete"
    assert values == ["doc-1", "doc-1", "doc-1", "doc-2", "doc-2"]
    from services.dest_precount import identity_count_from_source_id_scan

    assert identity_count_from_source_id_scan(state, values) == 2


def test_weaviate_scan_source_ids_no_source_id_field(monkeypatch):
    monkeypatch.setattr(
        "connectors.weaviate_writer._requests_session",
        lambda: _WeaviateSession(
            properties=[{"name": "content", "dataType": ["text"]}],
            physical=5,
            objects=[],
        ),
    )
    state, values = scan_source_ids(
        {"host": "127.0.0.1", "port": 8080}, table_name="docs"
    )
    assert state == "no_field"
    assert values == []


def test_weaviate_scan_source_ids_truncated_past_bound(monkeypatch):
    monkeypatch.setattr(
        "connectors.weaviate_writer._requests_session",
        lambda: _WeaviateSession(
            properties=[{"name": "source_id", "dataType": ["text"]}],
            physical=20_001,
            objects=[],
        ),
    )
    state, values = scan_source_ids(
        {"host": "127.0.0.1", "port": 8080}, table_name="docs", max_entities=20_000
    )
    assert state == "truncated"
    assert values == []


def test_weaviate_aggregate_failure_is_unmeasured_not_object_count(monkeypatch):
    monkeypatch.setattr(
        "connectors.weaviate_writer._requests_session",
        lambda: _WeaviateSession(
            properties=[{"name": "source_id", "dataType": ["text"]}],
            physical=5,
            objects=[{"id": "1", "properties": {"source_id": "doc-1"}}],
            aggregate_ok=False,
        ),
    )
    state, values = scan_source_ids(
        {"host": "127.0.0.1", "port": 8080}, table_name="docs"
    )
    assert state == "unmeasured"
    assert values == []