"""Schemaless source honesty — header union + Redshift/FLOAT DDL."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.header_union import SCHEMALESS_SOURCE_TYPES, union_attribute_keys
from connectors.postgresql_writer import pg_type
from services.type_system import ddl_type, normalize_logical_type


def test_schemaless_source_set():
    assert "mongodb" in SCHEMALESS_SOURCE_TYPES
    assert "elasticsearch" in SCHEMALESS_SOURCE_TYPES
    assert "redis" in SCHEMALESS_SOURCE_TYPES
    assert "dynamodb" in SCHEMALESS_SOURCE_TYPES


def test_union_preserves_order_and_sparse_keys():
    assert union_attribute_keys(["_id", "a"], ["b", "_id"], ["c"]) == ["_id", "a", "b", "c"]


def test_mongo_reader_unions_when_columns_frozen(monkeypatch):
    """Frozen columns still grow when a later doc has a new field."""
    from connectors import mongodb_reader as mr

    class _FakeCursor:
        def sort(self, *_a, **_k):
            return self

        def skip(self, *_a, **_k):
            return self

        def limit(self, *_a, **_k):
            return self

        def __iter__(self):
            return iter([
                {"_id": "1", "only_a": "x"},
                {"_id": "2", "only_b": 42},
            ])

    class _FakeColl:
        def count_documents(self, *_a, **_k):
            return 2

        def find(self, *_a, **_k):
            return _FakeCursor()

    class _FakeDB:
        def __getitem__(self, _name):
            return _FakeColl()

    class _FakeClient:
        def __getitem__(self, _name):
            return _FakeDB()

    monkeypatch.setattr(mr, "_mongo_client", lambda *_a, **_k: _FakeClient())
    monkeypatch.setattr(mr, "expand_mongo_documents", lambda docs, cfg=None: docs)
    monkeypatch.setattr(mr, "_connection_string", lambda cfg: "mongodb://x")

    batch = mr.read_collection_batch(
        cfg={},
        database="db",
        collection="c",
        columns=["_id", "only_a"],  # frozen from probe
        offset=0,
        limit=50,
    )
    assert "_id" in batch.headers
    assert "only_a" in batch.headers
    assert "only_b" in batch.headers  # must not be silently dropped
    assert len(batch.rows) == 2


def test_redshift_ddl_not_postgres_jsonb():
    assert pg_type("json", engine="redshift") == "SUPER"
    assert pg_type("json", engine="postgresql") == "JSONB"
    assert pg_type("binary", engine="redshift") == "VARBYTE"
    assert pg_type("uuid", engine="redshift") == "VARCHAR(36)"
    assert pg_type("float", engine="redshift") == "DOUBLE PRECISION"
    assert ddl_type("redshift", "FLOAT") == "DOUBLE PRECISION"


def test_generic_sql_float_is_double():
    from connectors.generic_sql import _sa_type_for_logical
    import sqlalchemy as sa

    t = _sa_type_for_logical("float", "postgresql", "databricks")
    assert isinstance(t, sa.Double)
    assert normalize_logical_type("DOUBLE") == "float"


def test_jsonl_unions_keys_beyond_sample_window(tmp_path):
    from src.transfer.file_stream import peek_file_source

    path = tmp_path / "sparse.jsonl"
    lines = []
    for i in range(120):
        if i < 100:
            lines.append('{"id": %d, "early": 1}\n' % i)
        else:
            lines.append('{"id": %d, "late_only": "x"}\n' % i)
    path.write_text("".join(lines), encoding="utf-8")
    headers, schema, total, sample = peek_file_source(str(path), "sparse.jsonl")
    assert total == 120
    assert "early" in headers
    assert "late_only" in headers  # key after sample window must survive
    assert len(sample) <= 100
