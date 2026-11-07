"""Object store profiling tests."""

from services.object_store_introspect import (
    profile_object_batch,
    profile_schemaless_source_schema,
    rows_from_matrix,
)
from services.type_system import normalize_logical_type


def test_rows_from_matrix():
    rows = rows_from_matrix(["id", "name"], [["1", "Alice"]])
    assert rows == [{"id": "1", "name": "Alice"}]


def test_profile_object_batch_infers_types():
    headers = ["id", "amount", "active"]
    rows = [["1", "10.5", "true"], ["2", "20.0", "false"]]
    result = profile_object_batch(headers, rows)
    assert result["ok"] is True
    assert result["schema"]["id"] == "INTEGER"
    assert result["row_estimate"] == 2


def test_redis_schemaless_schema_unbinds_sampled_decimal_width():
    """Redis has no numeric DDL — a page's DECIMAL(p,s) must not size CREATE."""
    headers = ["id", "amount", "code"]
    rows = [["1", "1000.00", "USD"], ["2", "2000.50", "EUR"]]
    schema = profile_schemaless_source_schema(
        headers, rows, source_format="redis"
    )
    assert normalize_logical_type(schema["id"]) == "integer"
    assert normalize_logical_type(schema["amount"]) == "decimal"
    assert "(" not in str(schema["amount"])
    assert normalize_logical_type(schema["code"]) in {"string", "text"}
