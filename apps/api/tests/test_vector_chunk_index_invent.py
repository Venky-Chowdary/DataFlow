"""Vector chunk_index fail-closed — refuse truncation / boolean invent."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_coerce_chunk_index_defaults_and_refuses_invent():
    from services.vector_embedding import coerce_chunk_index

    assert coerce_chunk_index(None) == 0
    assert coerce_chunk_index("") == 0
    assert coerce_chunk_index("  ") == 0
    assert coerce_chunk_index(3) == 3
    assert coerce_chunk_index("4") == 4
    assert coerce_chunk_index(5.0) == 5
    with pytest.raises(ValueError, match="fractional|truncation"):
        coerce_chunk_index(3.7)
    with pytest.raises(ValueError, match="boolean"):
        coerce_chunk_index(True)
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_chunk_index("maybe")
    with pytest.raises(ValueError, match="negative"):
        coerce_chunk_index(-1)


def test_pinecone_weaviate_qdrant_milvus_quarantine_bad_chunk_index():
    from connectors.milvus_writer import build_milvus_entities
    from connectors.pinecone_writer import build_pinecone_vectors
    from connectors.qdrant_writer import build_qdrant_points
    from connectors.weaviate_writer import build_weaviate_objects

    bad = [
        {
            "id": "v1",
            "content": "hi",
            "embedding": [0.1, 0.2, 0.3],
            "source_id": "s1",
            "chunk_index": 3.7,
        }
    ]
    vectors, rejected = build_pinecone_vectors(bad, dimension=3)
    assert vectors == []
    assert rejected and "chunk_index" in rejected[0]["column"]

    objects, rejected_w = build_weaviate_objects(
        bad, class_name="Doc", dimension=3
    )
    assert objects == []
    assert rejected_w and "chunk_index" in rejected_w[0]["column"]

    points, rejected_q = build_qdrant_points(bad, dimension=3)
    assert points == []
    assert rejected_q and "chunk_index" in rejected_q[0]["column"]

    entities, rejected_m = build_milvus_entities(bad, dimension=3)
    assert entities == []
    assert rejected_m and "chunk_index" in rejected_m[0]["column"]
