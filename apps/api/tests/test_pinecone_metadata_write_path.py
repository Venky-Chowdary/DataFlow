"""Pinecone metadata omits reader-null via vector_prepare_cell.

``if v is None`` left extract SQL_NULL_SENTINEL as a string field.
0 / false stay present. List[str] tags stay a list.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.pinecone_writer import (  # noqa: E402
    _pinecone_metadata_value,
    build_pinecone_vectors,
)
from services.value_serializer import (  # noqa: E402
    DF_MISSING_SENTINEL,
    SQL_NULL_SENTINEL,
    Missing,
)


def test_pinecone_metadata_value_omits_reader_null():
    for wire in (None, SQL_NULL_SENTINEL, "__df_ddb_null__", Missing, DF_MISSING_SENTINEL):
        assert _pinecone_metadata_value(wire) is None, wire
    assert _pinecone_metadata_value(0) == 0
    assert _pinecone_metadata_value(False) is False
    assert _pinecone_metadata_value("kept") == "kept"
    assert _pinecone_metadata_value(["a", "b"]) == ["a", "b"]


def test_build_pinecone_vectors_omits_sentinel_metadata():
    rows = [
        {
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
    ]
    vectors, rejected = build_pinecone_vectors(rows, dimension=3)
    assert rejected == []
    meta = vectors[0]["metadata"]
    assert "page" not in meta
    assert meta["kept"] == "1"
    assert meta["zero"] == 0
    assert meta["tags"] == ["a", "b"]
    assert SQL_NULL_SENTINEL not in meta.values()
