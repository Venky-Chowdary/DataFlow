"""Tests for the pgvector destination writer and vectorization service."""

from __future__ import annotations

import pytest


def _pgvector_available() -> bool:
    try:
        import psycopg2

        conn = psycopg2.connect(
            host="localhost", port=5432, database="dataflow", user="dataflow", password="dataflow"
        )
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM pg_extension WHERE extname='vector';")
        found = cur.fetchone() is not None
        cur.close()
        conn.close()
        return found
    except Exception:
        return False


PGVECTOR_AVAILABLE = _pgvector_available()


def test_chunk_text_basic():
    from services.vectorization import chunk_text

    text = "First paragraph. Second sentence.\n\nThird paragraph."
    chunks = chunk_text(text, chunk_size=80, chunk_overlap=10)
    assert len(chunks) > 0
    assert all(isinstance(c, str) and c for c in chunks)


def test_vectorize_records_with_content_column():
    from services.vectorization import vectorize_records

    records = [
        {"id": "r1", "title": "hello world", "body": "This is a test document."},
    ]
    rows = vectorize_records(records, content_column="body", model="sentence-transformers/all-MiniLM-L6-v2")
    assert len(rows) == 1
    assert rows[0]["content"] == "This is a test document."
    assert rows[0]["source_id"] == "r1"
    assert isinstance(rows[0]["embedding"], list)
    assert len(rows[0]["embedding"]) > 0


def test_vectorize_records_with_precomputed_embedding():
    from services.vectorization import vectorize_records

    records = [
        {"id": "r1", "content": "hello", "embedding": "[0.1,0.2,0.3]"},
    ]
    rows = vectorize_records(records, embedding_column="embedding")
    assert len(rows) == 1
    assert rows[0]["embedding"] == [0.1, 0.2, 0.3]


@pytest.mark.skipif(not PGVECTOR_AVAILABLE, reason="pgvector extension not available")
def test_pgvector_writer_inserts_rows():
    from connectors.pgvector_writer import write_mapped_rows

    table_name = "test_pgvector_writer"
    result = write_mapped_rows(
        host="localhost",
        port=5432,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        schema="public",
        connection_string="",
        ssl=False,
        table_name=table_name,
        headers=["id", "content"],
        data_rows=[["1", "Data integration moves data safely."]],
        mappings=[],
        column_types={"id": "string", "content": "string"},
    )
    assert result.ok is True
    assert result.rows_written == 1

    import psycopg2

    conn = psycopg2.connect(
        host="localhost", port=5432, database="dataflow", user="dataflow", password="dataflow"
    )
    cur = conn.cursor()
    cur.execute(f"SELECT count(*) FROM public.{table_name};")
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    assert count >= 1
