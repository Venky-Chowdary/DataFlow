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


def _write_dimension_test(embedding_model, storage, index="none"):
    from connectors.pgvector_writer import write_mapped_rows

    return write_mapped_rows(
        host="localhost",
        port=5434,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        schema="public",
        connection_string="",
        ssl=False,
        table_name="m7_dimension_limit_test",
        headers=["id", "content"],
        data_rows=[["doc-1", "dimension limit"]],
        mappings=[
            {"source": "id", "target": "id", "target_type": "VARCHAR"},
            {"source": "content", "target": "content", "target_type": "TEXT"},
        ],
        column_types={"id": "string", "content": "string"},
        content_column="content",
        embedding_model=embedding_model,
        skip_chunking=True,
        vector_storage=storage,
        vector_index=index,
        vector_skip_unchanged=False,
    )


def _stub_pgvector_connection(monkeypatch):
    from connectors import pgvector_writer

    calls = []

    def record_connection(**kwargs):
        calls.append(kwargs)
        raise AssertionError("pgvector writer reached the database connection")

    monkeypatch.setattr(pgvector_writer, "get_connection", record_connection)
    return calls


def test_halfvec_hnsw_rejects_dimensions_above_index_limit_without_connecting(
    monkeypatch,
):
    calls = _stub_pgvector_connection(monkeypatch)
    result = _write_dimension_test("hash/4001", "halfvec", "hnsw")

    assert calls == []
    assert not result.ok
    assert (
        "HNSW indexing on halfvec storage supports at most 4000 dimensions"
        in (result.error or "")
    )


def test_vector_hnsw_rejects_dimensions_above_limit_without_connecting(monkeypatch):
    calls = _stub_pgvector_connection(monkeypatch)
    result = _write_dimension_test("hash/2001", "vector", "hnsw")

    assert calls == []
    assert not result.ok
    assert (
        "HNSW indexing on vector storage supports at most 2000 dimensions"
        in (result.error or "")
    )


def test_halfvec_above_storage_cap_is_rejected_without_connecting(monkeypatch):
    calls = _stub_pgvector_connection(monkeypatch)
    result = _write_dimension_test("hash/16001", "halfvec")

    assert calls == []
    assert not result.ok
    assert (
        "pgvector supports at most 16000 dimensions" in (result.error or "")
    )


def test_halfvec_above_index_limit_without_index_reaches_connection(monkeypatch):
    from connectors import pgvector_writer

    calls = []

    def record_connection(**kwargs):
        calls.append(kwargs)
        raise AssertionError("expected writer to proceed to its connection check")

    monkeypatch.setattr(pgvector_writer, "get_connection", record_connection)
    with pytest.raises(
        AssertionError, match="proceed to its connection check"
    ):
        _write_dimension_test("hash/4001", "halfvec")

    assert len(calls) == 1


@pytest.mark.parametrize(
    ("dimension", "storage", "index", "expected"),
    [
        (4001, "halfvec", "none", None),
        (4000, "halfvec", "hnsw", None),
        (
            4001,
            "halfvec",
            "hnsw",
            "HNSW indexing on halfvec storage supports at most 4000 dimensions; "
            "this embedding has 4001. Set vector_index to none or use a "
            "lower-dimensional model.",
        ),
        (
            4001,
            "halfvec",
            "ivfflat",
            "IVFFlat indexing on halfvec storage supports at most 4000 dimensions; "
            "this embedding has 4001. Set vector_index to none or use a "
            "lower-dimensional model.",
        ),
        (16000, "halfvec", "none", None),
        (
            16001,
            "halfvec",
            "none",
            "pgvector supports at most 16000 dimensions; this embedding has "
            "16001. Use a lower-dimensional model.",
        ),
        (2000, "vector", "hnsw", None),
        (
            2001,
            "vector",
            "hnsw",
            "HNSW indexing on vector storage supports at most 2000 dimensions; "
            "this embedding has 2001. Use halfvec storage, set vector_index to "
            "none, or use a lower-dimensional model.",
        ),
        (
            4001,
            "vector",
            "hnsw",
            "HNSW indexing on vector storage supports at most 2000 dimensions; "
            "this embedding has 4001. Set vector_index to none or use a "
            "lower-dimensional model.",
        ),
        (2001, "vector", "none", None),
        (
            16001,
            "vector",
            "none",
            "pgvector supports at most 16000 dimensions; this embedding has "
            "16001. Use a lower-dimensional model.",
        ),
    ],
)
def test_pgvector_dimension_limit_error_boundaries(
    dimension, storage, index, expected
):
    from services.type_ddl_specialty import (
        pgvector_dimension_limit_error,
    )

    actual = pgvector_dimension_limit_error(
        dimension, storage=storage, index=index
    )
    assert actual == expected


def test_pgvector_dimension_limit_rejects_unknown_index_method():
    from services.type_ddl_specialty import pgvector_dimension_limit_error

    with pytest.raises(ValueError, match="index"):
        pgvector_dimension_limit_error(32, storage="vector", index="unknown")
