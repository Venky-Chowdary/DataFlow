"""CI-stable proofs for Weaviate destination writer (no live cluster required)."""

from __future__ import annotations

import sys
import uuid
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.weaviate_writer import (
    _object_uuid,
    build_weaviate_objects,
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
