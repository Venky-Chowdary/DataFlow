"""Tests for nested JSON intelligence."""

from services.json_intelligence import (
    expand_mongo_documents,
    flatten_column_recommendations,
    flatten_document,
)


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


def test_flatten_document_promotes_nested_leaves_and_keeps_parent():
    doc = {
        "_id": "1",
        "address": {"city": "Austin", "zip": "78701", "geo": {"lat": 30.2}},
        "tags": ["a", "b"],
    }
    flat = flatten_document(doc)
    assert flat["address"] == doc["address"]
    assert flat["address_city"] == "Austin"
    assert flat["address_zip"] == "78701"
    assert flat["address_geo_lat"] == 30.2
    assert flat["tags"] == ["a", "b"]


def test_expand_mongo_respects_flatten_nested_false():
    docs = [{"_id": "1", "meta": {"a": 1}}]
    out = expand_mongo_documents(docs, cfg={"flatten_nested": False})
    assert out == docs
    out2 = expand_mongo_documents(docs, cfg={"flatten_nested": True})
    assert out2[0]["meta_a"] == 1
