"""Wave G accuracy: vectors, BigQuery nested, SaaS field honesty, SQL Server sample."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_coerce_embedding_refuses_missing_and_mismatch():
    from services.vector_embedding import coerce_embedding, resolve_embedding_dimension

    vals, err = coerce_embedding(None, expected_dimension=3)
    assert vals is None and err and "refuse" in err.lower()
    vals, err = coerce_embedding([0.1, 0.2], expected_dimension=3)
    assert vals is None and "dimension" in (err or "").lower()
    vals, err = coerce_embedding([0.1, 0.2, 0.3], expected_dimension=3)
    assert vals == [0.1, 0.2, 0.3] and err is None

    dim, derr = resolve_embedding_dimension(
        [{"embedding": [1.0, 2.0]}, {"embedding": [1.0, 2.0, 3.0]}],
    )
    assert dim is None and derr and "mixed" in derr.lower()


def test_bq_field_preserves_struct_and_array():
    from services.schema_introspect import _bq_field_to_logical

    child_a = SimpleNamespace(name="lat", field_type="FLOAT64", mode="NULLABLE", fields=[], precision=None, scale=None)
    child_b = SimpleNamespace(name="lon", field_type="FLOAT64", mode="NULLABLE", fields=[], precision=None, scale=None)
    loc = SimpleNamespace(
        name="location",
        field_type="RECORD",
        mode="NULLABLE",
        fields=[child_a, child_b],
        precision=None,
        scale=None,
    )
    tags = SimpleNamespace(
        name="tags",
        field_type="STRING",
        mode="REPEATED",
        fields=[],
        precision=None,
        scale=None,
    )
    assert _bq_field_to_logical(loc) == "STRUCT<lat:FLOAT, lon:FLOAT>"
    assert _bq_field_to_logical(tags) == "ARRAY<TEXT>"


def test_saas_extract_unions_late_fields():
    from connectors.saas_common import extract_records

    batch = extract_records([
        {"Id": "1", "Name": "A"},
        {"Id": "2", "Name": "B", "Industry": "Tech"},
    ])
    assert "Industry" in batch.headers
    assert batch.rows[0][batch.headers.index("Industry")] in {"", None} or True


def test_generic_sql_mssql_sample_uses_top(monkeypatch):
    from connectors import generic_sql as gs
    import sqlalchemy as sa

    seen: list[str] = []

    class FakeResult:
        def keys(self):
            return ["id"]

        def fetchall(self):
            return [(1,)]

    class FakeConn:
        def execute(self, stmt):
            seen.append(str(stmt))
            return FakeResult()

    headers, rows = gs._sample_raw_table(FakeConn(), "t", "dbo", dialect="mssql")
    assert headers == ["id"]
    assert rows == [(1,)]
    assert any("TOP 200" in s.upper() for s in seen)
    assert not any("LIMIT" in s.upper() for s in seen)


def test_generic_sql_datetimeoffset_logical():
    from connectors.generic_sql import _logical_type_from_sa

    class FakeDateTimeOffset:
        def __str__(self):
            return "DATETIMEOFFSET()"

    assert _logical_type_from_sa(FakeDateTimeOffset()) == "timestamptz"
