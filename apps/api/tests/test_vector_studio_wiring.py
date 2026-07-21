"""Vector destination Studio wiring — catalog honesty + endpoint.extra round-trip."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_catalog_lists_vector_destinations_live():
    from services.catalog_service import _enriched_connectors, load_catalog, search_catalog

    load_catalog.cache_clear()
    _enriched_connectors.cache_clear()

    live = {c["id"] for c in search_catalog(status="live", limit=2000).get("connectors", [])}
    for cid in ("pgvector", "qdrant", "weaviate", "pinecone", "milvus"):
        assert cid in live

    ready = {
        c["id"]: c
        for c in search_catalog(transfer_only=True, limit=2000).get("connectors", [])
    }
    for cid in ("pgvector", "qdrant", "weaviate", "pinecone", "milvus"):
        assert ready[cid].get("transfer_ready") is True
        assert ready[cid].get("capabilities", {}).get("dest_only") is True


def test_capabilities_destination_databases_include_vectors():
    from services.catalog_service import _enriched_connectors, load_catalog
    from src.transfer.registry import get_capabilities

    load_catalog.cache_clear()
    _enriched_connectors.cache_clear()

    caps = get_capabilities()
    dests = set(caps.get("destination_databases") or [])
    for cid in ("pgvector", "qdrant", "weaviate", "pinecone", "milvus"):
        assert cid in dests


def test_endpoint_extra_vector_fields_round_trip():
    from src.transfer.models import EndpointConfig, endpoint_to_dict

    ep = EndpointConfig.from_dict(
        "database",
        {
            "format": "pgvector",
            "host": "localhost",
            "port": 5432,
            "database": "vectors",
            "table": "chunks",
            "extra": {
                "content_column": "body",
                "embedding_model": "sentence-transformers/all-MiniLM-L6-v2",
                "chunk_size": 256,
            },
            "allow_append_only": False,
        },
    )
    assert ep.extra.get("content_column") == "body"
    assert ep.extra.get("embedding_model") == "sentence-transformers/all-MiniLM-L6-v2"
    assert ep.extra.get("chunk_size") == 256
    dumped = endpoint_to_dict(ep)
    assert dumped["extra"]["content_column"] == "body"
    restored = EndpointConfig.from_dict("database", dumped)
    assert restored.extra.get("content_column") == "body"


def test_vectorize_precomputed_embedding_skips_model():
    """CI-stable path: precomputed embedding column — no sentence-transformers required."""
    from services.vectorization import vectorize_records

    rows = vectorize_records(
        [{"id": "1", "body": "hello", "vec": "[0.1, 0.2, 0.3]"}],
        content_column="body",
        embedding_column="vec",
    )
    assert len(rows) == 1
    assert rows[0]["embedding"] == [0.1, 0.2, 0.3]
    assert rows[0]["content"] == "hello"


def test_adapters_pass_vector_extra_into_pgvector_writer(monkeypatch):
    """Prove write_destination_database forwards Studio extra into the real writer kwargs."""
    from src.transfer.adapters import write_destination_database
    from src.transfer.models import EndpointConfig

    captured: dict = {}

    def fake_write(**kwargs):
        captured.update(kwargs)
        from connectors.writer_common import WriteResult

        return WriteResult(
            ok=True,
            rows_written=1,
            table_name=kwargs.get("table_name") or "chunks",
            target_schema=kwargs.get("schema") or "public",
            checksum="abc",
            chunks_completed=1,
            driver="psycopg2",
            load_method="pgvector_upsert",
        )

    monkeypatch.setattr("connectors.pgvector_writer.write_mapped_rows", fake_write)

    ep = EndpointConfig(
        kind="database",
        format="pgvector",
        host="localhost",
        port=5432,
        database="vectors",
        table="chunks",
        schema="public",
        extra={
            "content_column": "body",
            "embedding_column": "vec",
            "metadata_columns": ["id", "tag"],
            "embedding_model": "openai/text-embedding-3-small",
            "chunk_size": 128,
            "chunk_overlap": 16,
        },
    )
    n, ddl, summary = write_destination_database(
        ep,
        [{"id": "1", "body": "hello", "tag": "a", "vec": "[0.1,0.2]"}],
        ["id", "body", "tag", "vec"],
        {"id": "string", "body": "string", "tag": "string", "vec": "string"},
        [
            {"source": "id", "target": "id", "confidence": 1.0},
            {"source": "body", "target": "body", "confidence": 1.0},
            {"source": "tag", "target": "tag", "confidence": 1.0},
            {"source": "vec", "target": "vec", "confidence": 1.0},
        ],
        validation_mode="balanced",
    )
    assert n == 1
    assert captured.get("content_column") == "body"
    assert captured.get("embedding_column") == "vec"
    assert captured.get("metadata_columns") == ["id", "tag"]
    assert captured.get("embedding_model") == "openai/text-embedding-3-small"
    assert captured.get("chunk_size") == 128
    assert captured.get("chunk_overlap") == 16
    assert summary.get("type") == "pgvector"
    assert any("pgvector" in d.lower() for d in ddl)


def test_adapters_pass_vector_extra_into_weaviate_writer(monkeypatch):
    from src.transfer.adapters import write_destination_database
    from src.transfer.models import EndpointConfig

    captured: dict = {}

    def fake_write(**kwargs):
        captured.update(kwargs)
        from connectors.writer_common import WriteResult

        return WriteResult(
            ok=True,
            rows_written=1,
            table_name="DataflowChunk",
            target_schema="",
            checksum="abc",
            chunks_completed=1,
            driver="requests",
            load_method="weaviate_upsert",
        )

    monkeypatch.setattr("connectors.weaviate_writer.write_mapped_rows", fake_write)

    ep = EndpointConfig(
        kind="database",
        format="weaviate",
        host="localhost",
        port=8080,
        database="",
        table="DataflowChunk",
        extra={
            "content_column": "body",
            "embedding_column": "vec",
            "chunk_size": 128,
            "chunk_overlap": 16,
        },
    )
    n, ddl, summary = write_destination_database(
        ep,
        [{"id": "1", "body": "hello", "vec": "[0.1,0.2]"}],
        ["id", "body", "vec"],
        {"id": "string", "body": "string", "vec": "string"},
        [
            {"source": "id", "target": "id", "confidence": 1.0},
            {"source": "body", "target": "body", "confidence": 1.0},
            {"source": "vec", "target": "vec", "confidence": 1.0},
        ],
        validation_mode="balanced",
    )
    assert n == 1
    assert captured.get("content_column") == "body"
    assert captured.get("embedding_column") == "vec"
    assert captured.get("chunk_size") == 128
    assert summary.get("type") == "weaviate"
    assert any("weaviate" in d.lower() for d in ddl)


def test_adapters_pass_vector_extra_into_pinecone_writer(monkeypatch):
    from src.transfer.adapters import write_destination_database
    from src.transfer.models import EndpointConfig

    captured: dict = {}

    def fake_write(**kwargs):
        captured.update(kwargs)
        from connectors.writer_common import WriteResult

        return WriteResult(
            ok=True,
            rows_written=1,
            table_name="ns",
            target_schema="",
            checksum="abc",
            chunks_completed=1,
            driver="requests",
            load_method="pinecone_upsert",
        )

    monkeypatch.setattr("connectors.pinecone_writer.write_mapped_rows", fake_write)

    ep = EndpointConfig(
        kind="database",
        format="pinecone",
        host="my-index.svc.pinecone.io",
        port=443,
        database="",
        table="ns",
        password="pcsk_test",
        extra={
            "content_column": "body",
            "embedding_model": "openai/text-embedding-3-small",
            "skip_chunking": True,
        },
    )
    n, ddl, summary = write_destination_database(
        ep,
        [{"id": "1", "body": "hello"}],
        ["id", "body"],
        {"id": "string", "body": "string"},
        [
            {"source": "id", "target": "id", "confidence": 1.0},
            {"source": "body", "target": "body", "confidence": 1.0},
        ],
        validation_mode="balanced",
    )
    assert n == 1
    assert captured.get("content_column") == "body"
    assert captured.get("embedding_model") == "openai/text-embedding-3-small"
    assert captured.get("skip_chunking") is True
    assert summary.get("type") == "pinecone"
    assert any("pinecone" in d.lower() for d in ddl)


def test_adapters_pass_vector_extra_into_milvus_writer(monkeypatch):
    from src.transfer.adapters import write_destination_database
    from src.transfer.models import EndpointConfig

    captured: dict = {}

    def fake_write(**kwargs):
        captured.update(kwargs)
        from connectors.writer_common import WriteResult

        return WriteResult(
            ok=True,
            rows_written=1,
            table_name="chunks",
            target_schema="",
            checksum="abc",
            chunks_completed=1,
            driver="requests",
            load_method="milvus_upsert",
        )

    monkeypatch.setattr("connectors.milvus_writer.write_mapped_rows", fake_write)

    ep = EndpointConfig(
        kind="database",
        format="milvus",
        host="localhost",
        port=19530,
        database="",
        table="chunks",
        username="root",
        password="Milvus",
        extra={
            "content_column": "body",
            "embedding_column": "vec",
            "chunk_size": 256,
        },
    )
    n, ddl, summary = write_destination_database(
        ep,
        [{"id": "1", "body": "hello", "vec": "[0.1,0.2]"}],
        ["id", "body", "vec"],
        {"id": "string", "body": "string", "vec": "string"},
        [
            {"source": "id", "target": "id", "confidence": 1.0},
            {"source": "body", "target": "body", "confidence": 1.0},
            {"source": "vec", "target": "vec", "confidence": 1.0},
        ],
        validation_mode="balanced",
    )
    assert n == 1
    assert captured.get("content_column") == "body"
    assert captured.get("embedding_column") == "vec"
    assert captured.get("chunk_size") == 256
    assert summary.get("type") == "milvus"
    assert any("milvus" in d.lower() for d in ddl)
