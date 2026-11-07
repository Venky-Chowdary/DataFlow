"""GCS create-new is a PUT, and a vector fallback is visible on the job.

DEF-GCS-DST-CREATE: object write with a missing key is not a CREATE TABLE denial.
DEF-EMBED-SILENT-FALLBACK: a cached TF-IDF embedder still warns on the stream
path that Qdrant uses, and a numeric source cell is a JSON number in the payload.
"""

from __future__ import annotations

import re
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_numeric_source_stays_a_json_number_in_the_qdrant_payload():
    from connectors.qdrant_writer import build_qdrant_points
    from connectors.writer_common import prepare_records_for_vector_write
    from services.value_serializer import json_dumps_exact_numbers

    records, rejected, abort = prepare_records_for_vector_write(
        headers=["id", "price", "sku"],
        data_rows=[["7", "2.36", "2.36"]],
        mappings=[
            {"source": "id", "target": "id", "target_type": "VARCHAR"},
            {"source": "price", "target": "price", "target_type": "VARCHAR"},
            {"source": "sku", "target": "sku", "target_type": "VARCHAR"},
        ],
        column_types={
            "id": "bigint",
            "price": "numeric(10,2)",
            "sku": "text",
        },
    )
    assert abort is None
    assert rejected == []
    assert records[0]["price"] == Decimal("2.36")
    assert records[0]["sku"] == "2.36"
    assert records[0]["id"] == 7

    points, point_rejected = build_qdrant_points(
        [
            {
                "id": records[0]["id"],
                "content": "order",
                "embedding": [0.1, 0.2, 0.3],
                "metadata": {"price": records[0]["price"], "sku": records[0]["sku"]},
                "source_id": "7",
                "chunk_index": 0,
            }
        ],
        dimension=3,
    )
    assert point_rejected == []
    body = json_dumps_exact_numbers(points)
    assert re.search(r'"price":\s*2\.36\b', body)
    assert not re.search(r'"price":\s*"2\.36"', body)
    assert re.search(r'"sku":\s*"2\.36"', body)


def test_stream_job_warning_survives_a_cached_tfidf_embedder(monkeypatch):
    import src.ai.rag.embedding_service as embedding_service
    from services.vectorization import (
        _get_embedder,
        take_embedding_fallback_notice,
        vectorize_records,
    )
    from src.transfer.stream import _writer_diagnostics

    _get_embedder.cache_clear()
    monkeypatch.setitem(sys.modules, "sentence_transformers", None)
    monkeypatch.setattr(embedding_service, "_embedding_service", None)

    first = vectorize_records(
        [{"id": "1", "content": "postgres row about orders", "price": Decimal("2.36")}],
        content_column="content",
        model="sentence-transformers/all-MiniLM-L6-v2",
        skip_chunking=True,
    )
    assert first[0]["metadata"].get("_df_embedding_backend") == "tfidf_fallback"
    # A previous job already consumed the notice. The embedder stays cached.
    assert take_embedding_fallback_notice()

    second = vectorize_records(
        [{"id": "2", "content": "another postgres row about orders"}],
        content_column="content",
        model="sentence-transformers/all-MiniLM-L6-v2",
        skip_chunking=True,
    )
    assert second[0]["metadata"].get("_df_embedding_backend") == "tfidf_fallback"

    class _Result:
        warnings: list[str] = []
        rejected_rows = 0
        coerced_null_rows = 0
        rows_skipped = 0
        rejected_details: list[dict] = []

    summary = _writer_diagnostics(_Result())
    text = " ".join(summary["warnings"])
    assert "tfidf_fallback" in text or "fallback" in text
    assert "MiniLM" in text
    _get_embedder.cache_clear()
    take_embedding_fallback_notice()
