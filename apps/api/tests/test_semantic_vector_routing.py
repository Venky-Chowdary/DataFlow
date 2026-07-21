"""Semantic vector routing — embed / metadata / exclude_pii / skip (no mocks)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_recommend_excludes_pii_and_picks_long_text():
    from services.semantic_vector_routing import recommend_vector_field_roles

    plan = recommend_vector_field_roles(
        ["email", "description", "status", "id"],
        samples_by_column={
            "email": ["alice@example.com", "bob@example.com"],
            "description": ["This is a long product description about widgets and shipping." * 2],
            "status": ["active", "pending"],
            "id": ["1", "2"],
        },
        schema={"email": "VARCHAR", "description": "TEXT", "status": "VARCHAR", "id": "INTEGER"},
        analysis_columns=[
            {"column_name": "email", "is_pii": True, "semantic_type": "email"},
            {"column_name": "description", "is_pii": False, "semantic_type": "text"},
        ],
    )
    assert plan.content_column == "description"
    assert "email" in plan.exclude_pii_columns
    assert "status" in plan.metadata_columns or "id" in plan.metadata_columns
    actions = {f.column: f.action for f in plan.fields}
    assert actions["email"] == "exclude_pii"
    assert actions["description"] == "embed"


def test_recommend_skips_binary_and_detects_embedding_column():
    from services.semantic_vector_routing import recommend_vector_field_roles

    plan = recommend_vector_field_roles(
        ["body", "embedding", "file_data"],
        samples_by_column={
            "body": ["hello world " * 20],
            "embedding": ["[0.1,0.2,0.3]"],
            "file_data": ["AAAA" * 40],
        },
        schema={"body": "TEXT", "embedding": "VARCHAR", "file_data": "BYTEA"},
    )
    assert plan.content_column == "body"
    assert plan.embedding_column == "embedding"
    assert "file_data" in plan.skip_columns


def test_vectorize_strips_excluded_pii_from_metadata():
    from services.vectorization import vectorize_records

    rows = vectorize_records(
        [{"id": "1", "body": "hello", "email": "a@b.com", "tag": "x", "vec": "[0.1,0.2]"}],
        content_column="body",
        embedding_column="vec",
        metadata_columns=["email", "tag", "id"],
        exclude_pii_columns=["email"],
    )
    assert len(rows) == 1
    meta = rows[0]["metadata"]
    assert "email" not in meta
    assert meta.get("tag") == "x"


def test_vectorize_fail_closed_when_content_is_excluded_pii():
    from services.vectorization import vectorize_records

    with pytest.raises(ValueError, match="excluded as PII"):
        vectorize_records(
            [{"email": "a@b.com", "vec": "[0.1]"}],
            content_column="email",
            embedding_column="vec",
            exclude_pii_columns=["email"],
        )


def test_adapters_forward_exclude_pii_columns(monkeypatch):
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
            target_schema="public",
            checksum="x",
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
            "exclude_pii_columns": ["email"],
            "metadata_columns": ["id", "tag"],
        },
    )
    write_destination_database(
        ep,
        [{"id": "1", "body": "hi", "email": "a@b.com", "tag": "t"}],
        ["id", "body", "email", "tag"],
        {"id": "string", "body": "string", "email": "string", "tag": "string"},
        [{"source": c, "target": c, "confidence": 1.0} for c in ["id", "body", "email", "tag"]],
        validation_mode="balanced",
    )
    assert captured.get("exclude_pii_columns") == ["email"]


def test_plan_to_dict_shape():
    from services.semantic_vector_routing import recommend_vector_field_roles

    plan = recommend_vector_field_roles(["notes", "phone"])
    dumped = plan.to_dict()
    assert "fields" in dumped
    assert "exclude_pii_columns" in dumped
    assert "summary" in dumped
