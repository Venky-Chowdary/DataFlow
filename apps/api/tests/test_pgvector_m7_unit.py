from __future__ import annotations

import pytest


@pytest.mark.parametrize(
    ("value", "expected"),
    [(None, 16), (12, 12), ("24", 24)],
)
def test_hnsw_integer_options_accept_only_valid_integers(value, expected):
    from connectors.pgvector_writer import _pgvector_integer_option

    assert _pgvector_integer_option(
        value,
        name="vector_hnsw_m",
        default=16,
        minimum=2,
        maximum=100,
    ) == expected


@pytest.mark.parametrize("value", [True, 1.5, "2.5", 1, 101])
def test_hnsw_integer_options_reject_invalid_values(value):
    from connectors.pgvector_writer import _pgvector_integer_option

    with pytest.raises(ValueError, match="vector_hnsw_m"):
        _pgvector_integer_option(
            value,
            name="vector_hnsw_m",
            default=16,
            minimum=2,
            maximum=100,
        )


def test_halfvec_version_gate_fails_closed_with_upgrade_guidance(monkeypatch):
    from connectors import pgvector_writer

    monkeypatch.setattr(
        pgvector_writer, "_pgvector_extension_version", lambda _cur: (0, 6, 9)
    )
    error = pgvector_writer._pgvector_storage_version_error(object(), "halfvec")
    assert error is not None
    assert "requires pgvector 0.7.0 or newer" in error
    assert "Upgrade pgvector" in error


def test_default_vector_storage_does_not_require_extversion(monkeypatch):
    from connectors import pgvector_writer

    monkeypatch.setattr(
        pgvector_writer, "_pgvector_extension_version", lambda _cur: None
    )
    assert pgvector_writer._pgvector_storage_version_error(object(), "vector") is None


def test_halfvec_rejects_dimensions_above_4000_without_connecting(monkeypatch):
    from connectors import pgvector_writer
    from services.type_ddl_specialty import PGVECTOR_HALFVEC_MAX_DIMENSIONS

    calls = []

    def record_connection(**kwargs):
        calls.append(kwargs)
        raise AssertionError("pgvector dimension validation opened a connection")

    monkeypatch.setattr(pgvector_writer, "get_connection", record_connection)

    result = pgvector_writer.write_mapped_rows(
        host="localhost",
        port=5434,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        schema="public",
        connection_string="",
        ssl=False,
        table_name="m7_halfvec_dimension_limit",
        headers=["id", "content"],
        data_rows=[["doc-1", "dimension limit"]],
        mappings=[
            {"source": "id", "target": "id", "target_type": "VARCHAR"},
            {"source": "content", "target": "content", "target_type": "TEXT"},
        ],
        column_types={"id": "string", "content": "string"},
        content_column="content",
        embedding_model="hash/4001",
        skip_chunking=True,
        vector_storage="halfvec",
        vector_skip_unchanged=False,
    )

    assert calls == []
    assert not result.ok
    assert (
        f"at most {PGVECTOR_HALFVEC_MAX_DIMENSIONS} dimensions"
        in (result.error or "")
    )


def test_vector_hnsw_rejects_dimensions_above_limit_without_connecting(monkeypatch):
    from connectors import pgvector_writer
    from services.type_ddl_specialty import PGVECTOR_HNSW_MAX_DIMENSIONS

    calls = []
    hnsw_limit = PGVECTOR_HNSW_MAX_DIMENSIONS["vector"]

    def record_connection(**kwargs):
        calls.append(kwargs)
        raise AssertionError("pgvector dimension validation opened a connection")

    monkeypatch.setattr(pgvector_writer, "get_connection", record_connection)
    result = pgvector_writer.write_mapped_rows(
        host="localhost",
        port=5434,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        schema="public",
        connection_string="",
        ssl=False,
        table_name="m7_vector_hnsw_dimension_limit",
        headers=["id", "content"],
        data_rows=[["doc-1", "dimension limit"]],
        mappings=[
            {"source": "id", "target": "id", "target_type": "VARCHAR"},
            {"source": "content", "target": "content", "target_type": "TEXT"},
        ],
        column_types={"id": "string", "content": "string"},
        content_column="content",
        embedding_model="hash/2001",
        skip_chunking=True,
        vector_storage="vector",
        vector_index="hnsw",
        vector_skip_unchanged=False,
    )

    assert calls == []
    assert not result.ok
    assert (
        f"HNSW indexing on vector storage supports at most {hnsw_limit} dimensions"
        in (result.error or "")
    )


def test_pgvector_dimension_limit_error_boundaries():
    from services.type_ddl_specialty import (
        PGVECTOR_HALFVEC_MAX_DIMENSIONS,
        PGVECTOR_HNSW_MAX_DIMENSIONS,
        _VECTOR_DIM_CAPS,
        pgvector_dimension_limit_error,
    )

    vector_cap = _VECTOR_DIM_CAPS["postgresql"]
    assert pgvector_dimension_limit_error(3072, storage="vector", index="none") is None
    assert pgvector_dimension_limit_error(
        vector_cap + 1, storage="vector", index="none"
    ) == (
        f"pgvector supports at most {vector_cap} dimensions; this embedding has "
        f"{vector_cap + 1}. Use a lower-dimensional model."
    )
    assert pgvector_dimension_limit_error(
        PGVECTOR_HALFVEC_MAX_DIMENSIONS + 1, storage="halfvec", index="none"
    ) == (
        f"halfvec supports at most {PGVECTOR_HALFVEC_MAX_DIMENSIONS} dimensions; "
        f"this embedding has {PGVECTOR_HALFVEC_MAX_DIMENSIONS + 1}. Use vector "
        "storage or a lower-dimensional model."
    )
    vector_hnsw_limit = PGVECTOR_HNSW_MAX_DIMENSIONS["vector"]
    assert pgvector_dimension_limit_error(
        vector_hnsw_limit, storage="vector", index="hnsw"
    ) is None
    assert pgvector_dimension_limit_error(
        vector_hnsw_limit + 1, storage="vector", index="hnsw"
    ) == (
        f"HNSW indexing on vector storage supports at most {vector_hnsw_limit} "
        f"dimensions; this embedding has {vector_hnsw_limit + 1}. Use halfvec "
        "storage, set vector_index to none, or a lower-dimensional model."
    )
