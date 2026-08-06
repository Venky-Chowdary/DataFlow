"""Vector destination honesty — no invent-384, no silent re-embed, durable counts."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_vectorize_refuses_silent_reembed_on_bad_embedding_column():
    from services.vectorization import vectorize_records

    rows = vectorize_records(
        [{"id": "1", "content": "hello world document", "embedding": "not-json"}],
        content_column="content",
        embedding_column="embedding",
        model="hash/32",
    )
    assert len(rows) == 1
    assert rows[0]["embedding"] is None
    assert "refuse silent re-embed" in str(rows[0].get("_df_embed_error") or "")


def test_vectorize_refuses_silent_reembed_on_empty_embedding_list():
    from services.vectorization import vectorize_records

    rows = vectorize_records(
        [{"id": "1", "content": "hello world document", "embedding": []}],
        content_column="content",
        embedding_column="embedding",
        model="hash/32",
    )
    assert len(rows) == 1
    assert rows[0]["embedding"] is None
    assert "empty" in str(rows[0].get("_df_embed_error") or "").lower()
    assert "refuse silent re-embed" in str(rows[0].get("_df_embed_error") or "")


def test_qdrant_refuses_invented_dimension_384():
    from connectors.qdrant_writer import write_mapped_rows

    with patch(
        "connectors.qdrant_writer.vectorize_records",
        return_value=[
            {"id": "a", "content": "hi", "embedding": None, "source_id": "1", "chunk_index": 0}
        ],
    ), patch("connectors.qdrant_writer._requests_session") as sess:
        result = write_mapped_rows(
            host="localhost",
            port=6333,
            database="",
            username="",
            password="key",
            schema="",
            connection_string="",
            ssl=False,
            table_name="chunks",
            headers=["id", "content"],
            data_rows=[["1", "hi"]],
            mappings=[],
            column_types={},
            create_table=True,
            embedding_model="hash/32",
        )
    assert result.ok is False
    assert "dimension" in (result.error or "").lower() or "embedding" in (result.error or "").lower()
    sess.assert_not_called()


def test_milvus_refuses_invented_dimension_384():
    from connectors.milvus_writer import write_mapped_rows

    with patch(
        "connectors.milvus_writer.vectorize_records",
        return_value=[
            {"id": "a", "content": "hi", "embedding": None, "source_id": "1", "chunk_index": 0}
        ],
    ), patch("connectors.milvus_writer._requests_session") as sess:
        result = write_mapped_rows(
            host="localhost",
            port=19530,
            database="default",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            table_name="chunks",
            headers=["id", "content"],
            data_rows=[["1", "hi"]],
            mappings=[],
            column_types={},
            create_table=True,
            embedding_model="hash/32",
        )
    assert result.ok is False
    assert "dimension" in (result.error or "").lower() or "embedding" in (result.error or "").lower()
    sess.assert_not_called()


def test_qdrant_surfaces_df_embed_error_reason():
    from connectors.qdrant_writer import build_qdrant_points

    points, rejected = build_qdrant_points(
        [
            {
                "id": "x",
                "embedding": None,
                "content": "c",
                "source_id": "1",
                "chunk_index": 0,
                "_df_embed_error": "embedding_column 'emb' is not a numeric JSON array — refuse silent re-embed",
            }
        ],
        dimension=3,
    )
    assert points == []
    assert rejected
    assert "refuse silent re-embed" in rejected[0]["reason"]


def test_pgvector_all_embed_rejected_is_not_ok():
    from connectors.pgvector_writer import write_mapped_rows

    fake_conn = MagicMock()
    fake_cur = MagicMock()
    fake_conn.cursor.return_value.__enter__.return_value = fake_cur
    fake_cur.fetchone.return_value = ["public.t"]  # table exists
    with patch(
        "connectors.pgvector_writer.vectorize_records",
        return_value=[
            {
                "id": "a",
                "content": "hi",
                "embedding": None,
                "source_id": "1",
                "chunk_index": 0,
                "_df_embed_error": "bad embed",
            },
            {
                "id": "b",
                "content": "hi2",
                "embedding": [0.1, 0.2],
                "source_id": "2",
                "chunk_index": 0,
            },
        ],
    ), patch(
        "connectors.pgvector_writer.get_connection",
        return_value=fake_conn,
    ), patch(
        "services.vector_embedding.resolve_embedding_dimension",
        return_value=(2, None),
    ), patch(
        "services.vector_embedding.coerce_embedding",
        side_effect=[
            (None, "missing embedding — refuse zero-vector fabrication"),
            (None, "missing embedding — refuse zero-vector fabrication"),
        ],
    ):
        result = write_mapped_rows(
            host="localhost",
            port=5432,
            database="db",
            username="u",
            password="p",
            schema="public",
            connection_string="",
            ssl=False,
            table_name="t",
            headers=["id", "content"],
            data_rows=[["1", "hi"], ["2", "hi2"]],
            mappings=[],
            column_types={},
            create_table=False,
            embedding_model="hash/32",
        )
    assert result.ok is False
    assert result.rows_written == 0
    assert result.rejected_details
