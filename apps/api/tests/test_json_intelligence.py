"""Tests for nested JSON intelligence."""

from services.json_intelligence import flatten_column_recommendations


def test_dot_notation_column():
    recs = flatten_column_recommendations(["address.city", "id"])
    assert any(r["column"] == "address.city" and r["kind"] == "dot_notation" for r in recs)


def test_json_string_column():
    recs = flatten_column_recommendations(
        ["payload"],
        [{"payload": '{"sku": "A1", "qty": 3}'}],
    )
    assert len(recs) == 1
    assert recs[0]["kind"] == "nested_object"
