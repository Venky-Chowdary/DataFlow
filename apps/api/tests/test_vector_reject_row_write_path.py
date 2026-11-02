"""Vector quarantine row labels use present_cell_text, not ``or ""``.

``cell_to_string(row.get("id") or "")`` dropped integer ``0`` so the
operator saw an empty row. ``True`` missed dest ``true``. Reader-null
stays an empty label.
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
from services.vector_embedding import vector_reject_row_label  # noqa: E402


def _reject_row(id_value):
    return {
        "id": id_value,
        "content": "hi",
        "source_id": "src",
        "chunk_index": 3.7,
        "embedding": [0.1, 0.2, 0.3],
    }


def _rejected_labels(row):
    pine, pr = build_pinecone_vectors([row], dimension=3)
    mil, mr = build_milvus_entities([row], dimension=3)
    qdr, qr = build_qdrant_points([row], dimension=3)
    wea, wr = build_weaviate_objects([row], class_name="DataflowChunk", dimension=3)
    assert pine == mil == qdr == wea == []
    return {
        "pinecone": pr[0]["row"],
        "milvus": mr[0]["row"],
        "qdrant": qr[0]["row"],
        "weaviate": wr[0]["row"],
    }


def test_vector_reject_label_zero_and_true():
    assert vector_reject_row_label({"id": 0}) == "0"
    assert vector_reject_row_label({"id": False}) == "false"
    assert vector_reject_row_label({"id": True}) == "true"
    assert vector_reject_row_label({"id": SQL_NULL_SENTINEL}) == ""
    assert vector_reject_row_label({"id": None, "source_id": 0}, "id", "source_id") == "0"


def test_builders_label_zero_not_empty():
    labels = _rejected_labels(_reject_row(0))
    assert set(labels.values()) == {"0"}


def test_builders_label_true_as_dest_true():
    labels = _rejected_labels(_reject_row(True))
    assert set(labels.values()) == {"true"}


def test_builders_label_reader_null_empty():
    labels = _rejected_labels(_reject_row(SQL_NULL_SENTINEL))
    assert set(labels.values()) == {""}
