"""Vector dest metadata omits reader-null via vector_prepare_metadata.

sanitize_json_value leaves extract SQL_NULL_SENTINEL as a string, so
Qdrant / Milvus / Weaviate / pgvector / Pinecone stored the wire spelling.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.milvus_writer import build_milvus_entities  # noqa: E402
from connectors.pinecone_writer import build_pinecone_vectors  # noqa: E402
from connectors.qdrant_writer import build_qdrant_points  # noqa: E402
from connectors.weaviate_writer import build_weaviate_objects  # noqa: E402
from services.value_serializer import SQL_NULL_SENTINEL  # noqa: E402

_ROW = {
    "id": "vec-1",
    "content": "hello",
    "source_id": "1",
    "chunk_index": 0,
    "embedding": [0.1, 0.2, 0.3],
    "metadata": {
        "page": SQL_NULL_SENTINEL,
        "kept": "1",
        "zero": 0,
        "tags": ["a", "b"],
    },
}


def test_qdrant_payload_omits_reader_null():
    points, rejected = build_qdrant_points([_ROW], dimension=3)
    assert rejected == []
    payload = points[0]["payload"]
    assert "page" not in payload
    assert payload["kept"] == "1"
    assert payload["zero"] == 0
    assert SQL_NULL_SENTINEL not in payload.values()


def test_weaviate_props_omit_reader_null():
    objects, rejected = build_weaviate_objects(
        [_ROW], class_name="Chunk", dimension=3
    )
    assert rejected == []
    props = objects[0]["properties"]
    assert "page" not in props
    assert props["kept"] == "1"
    assert props["zero"] == 0
    assert SQL_NULL_SENTINEL not in props.values()


def test_milvus_entity_omits_reader_null_page():
    entities, rejected = build_milvus_entities([_ROW], dimension=3)
    assert rejected == []
    entity = entities[0]
    assert entity["page"] == ""
    assert SQL_NULL_SENTINEL not in entity.values()


def test_pinecone_metadata_still_omits_reader_null():
    vectors, rejected = build_pinecone_vectors([_ROW], dimension=3)
    assert rejected == []
    meta = vectors[0]["metadata"]
    assert "page" not in meta
    assert meta["kept"] == "1"
    assert meta["zero"] == 0
    assert meta["tags"] == ["a", "b"]
    assert SQL_NULL_SENTINEL not in meta.values()
