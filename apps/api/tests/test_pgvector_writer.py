"""Tests for the pgvector destination writer and vectorization service."""

from __future__ import annotations

import uuid

import pytest

from tests.host_facts import drop_pg_table, require_pgvector


@pytest.fixture
def pgvector_test_table():
    table_name = f"test_pgvector_writer_{uuid.uuid4().hex[:10]}"
    yield table_name
    drop_pg_table(table_name)


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
    # CI-stable hash embedder — no sentence-transformers download required.
    rows = vectorize_records(records, content_column="body", model="hash/32")
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


def test_pgvector_writer_inserts_rows(pgvector_test_table):
    require_pgvector()
    from connectors.pgvector_writer import write_mapped_rows

    table_name = pgvector_test_table
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


def test_pgvector_writer_rerun_of_unchanged_document_writes_zero_rows(
    pgvector_test_table,
):
    require_pgvector()
    from connectors.pgvector_writer import write_mapped_rows

    options = {
        "host": "localhost",
        "port": 5432,
        "database": "dataflow",
        "username": "dataflow",
        "password": "dataflow",
        "schema": "public",
        "connection_string": "",
        "ssl": False,
        "table_name": pgvector_test_table,
        "headers": ["id", "content"],
        "data_rows": [["1", "Data integration moves data safely."]],
        "mappings": [],
        "column_types": {"id": "string", "content": "string"},
    }
    first = write_mapped_rows(**options)
    second = write_mapped_rows(**options)

    assert first.ok is True
    assert first.rows_written == 1
    assert second.ok is True
    assert second.rows_written == 0
    assert second.meta["vector_docs_unchanged_skipped"] == 1

    import psycopg2

    conn = psycopg2.connect(
        host="localhost", port=5432, database="dataflow", user="dataflow", password="dataflow"
    )
    cur = conn.cursor()
    cur.execute(f"SELECT count(*) FROM public.{pgvector_test_table};")
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    assert count == 1
