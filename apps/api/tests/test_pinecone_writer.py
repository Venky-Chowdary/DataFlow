"""CI-stable proofs for Pinecone destination writer (no live index required)."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.pinecone_writer import (
    build_pinecone_vectors,
    test_pinecone as probe_pinecone,
    write_mapped_rows,
)


def test_build_pinecone_vectors_maps_rows():
    rows = [
        {
            "id": "vec-1",
            "content": "hello",
            "source_id": "1",
            "chunk_index": 0,
            "embedding": [0.1, 0.2, 0.3],
            "metadata": {"page": "1", "tags": ["a", "b"]},
        }
    ]
    vectors, _rejected = build_pinecone_vectors(rows, dimension=3)
    assert len(vectors) == 1
    assert vectors[0]["id"] == "vec-1"
    assert vectors[0]["values"] == [0.1, 0.2, 0.3]
    assert vectors[0]["metadata"]["content"] == "hello"
    assert vectors[0]["metadata"]["page"] == "1"
    assert vectors[0]["metadata"]["tags"] == ["a", "b"]


def test_build_pinecone_vectors_rejects_missing_embedding():
    rows = [
        {"id": "a", "content": "x", "embedding": [0.1, 0.2, 0.3]},
        {"id": "b", "content": "y", "embedding": None},
        {"id": "c", "content": "z", "embedding": [0.1, 0.2]},  # dim mismatch
    ]
    vectors, rejected = build_pinecone_vectors(rows, dimension=3)
    assert len(vectors) == 1
    assert len(rejected) == 2
    assert any("missing" in (r.get("reason") or "").lower() or "refuse" in (r.get("reason") or "").lower() for r in rejected)


def test_pinecone_probe_requires_host_and_key():
    ok, msg = probe_pinecone(host="", connection_string="", api_key="")
    assert not ok
    assert "host" in msg.lower()

    ok2, msg2 = probe_pinecone(host="https://example.invalid", api_key="")
    assert not ok2
    assert "key" in msg2.lower()


def test_pinecone_probe_unreachable_fail_closed():
    ok, msg = probe_pinecone(
        host="https://127.0.0.1:1",
        api_key="test-key",
    )
    assert not ok
    assert msg


def test_pinecone_write_missing_host_fail_closed():
    result = write_mapped_rows(
        host="",
        port=443,
        database="",
        username="",
        password="key",
        schema="",
        connection_string="",
        ssl=True,
        table_name="ns",
        headers=["id", "content", "vec"],
        data_rows=[["1", "hello", "[0.1,0.2]"]],
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
    assert "host" in (result.error or "").lower()


def test_pinecone_write_unreachable_fail_closed():
    result = write_mapped_rows(
        host="https://127.0.0.1:1",
        port=443,
        database="",
        username="",
        password="test-key",
        schema="",
        connection_string="",
        ssl=True,
        table_name="ns",
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
