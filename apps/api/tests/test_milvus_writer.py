"""CI-stable proofs for Milvus destination writer (no live cluster required)."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.milvus_writer import (
    build_milvus_entities,
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
    entities = build_milvus_entities(rows, dimension=3)
    assert len(entities) == 1
    assert entities[0]["id"] == "abc123"
    assert entities[0]["vector"] == [0.1, 0.2, 0.3]
    assert entities[0]["content"] == "hello milvus"
    assert entities[0]["page"] == "2"
    assert entities[0]["heading"] == "Intro"


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
