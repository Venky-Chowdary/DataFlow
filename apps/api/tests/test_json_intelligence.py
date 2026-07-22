"""Tests for nested JSON intelligence."""

from connectors.writer_common import build_mapped_rows
from services.json_intelligence import (
    STRUCT_POLICY_FLATTEN_TOP_LEVEL,
    STRUCT_POLICY_STORE_AS_JSON,
    apply_struct_policies_to_row,
    expand_mongo_documents,
    flatten_column_recommendations,
    flatten_document,
    flatten_struct_field,
    materialize_struct_policies,
    top_level_keys_from_samples,
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
    assert recs[0]["default_struct_policy"] == STRUCT_POLICY_STORE_AS_JSON


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


def test_struct_policy_top_level_only():
    sample = '{"city":"Austin","zip":"78701","geo":{"lat":30},"tags":["a"]}'
    keys = top_level_keys_from_samples([sample])
    assert keys == ["city", "zip", "tags"]
    flat = flatten_struct_field(sample, parent_key="addr")
    assert flat["addr"] == {"city": "Austin", "zip": "78701", "geo": {"lat": 30}, "tags": ["a"]}
    assert flat["addr_city"] == "Austin"
    assert flat["addr_zip"] == "78701"
    assert flat["addr_tags"] == ["a"]
    assert "addr_geo" not in flat
    assert "addr_geo_lat" not in flat


def test_materialize_struct_policies_expands_headers():
    headers = ["id", "addr"]
    rows = [["1", '{"city":"Austin","zip":"78701"}']]
    mappings = [
        {"source": "id", "target": "id"},
        {
            "source": "addr",
            "target": "addr",
            "struct_policy": STRUCT_POLICY_FLATTEN_TOP_LEVEL,
            "transform": "json",
        },
        {"source": "addr_city", "target": "city"},
        {"source": "addr_zip", "target": "zip"},
    ]
    new_headers, new_rows = materialize_struct_policies(headers, rows, mappings)
    assert "addr_city" in new_headers
    assert "addr_zip" in new_headers
    assert new_rows[0][new_headers.index("addr_city")] == "Austin"
    assert new_rows[0][new_headers.index("addr_zip")] == "78701"

    mapped, errors = build_mapped_rows(
        headers=headers,
        data_rows=rows,
        mappings=mappings,
        target_cols=["id", "addr", "city", "zip"],
        column_types={"id": "INTEGER", "addr": "JSON", "city": "VARCHAR", "zip": "VARCHAR"},
    )
    assert not errors
    assert mapped[0][2] == "Austin"
    assert mapped[0][3] == "78701"


def test_store_as_json_is_noop():
    row = {"addr": '{"city":"Austin"}'}
    out = apply_struct_policies_to_row(row, {"addr": STRUCT_POLICY_STORE_AS_JSON})
    assert out == row


def test_flatten_deep_promotes_nested_keys():
    from services.json_intelligence import STRUCT_POLICY_FLATTEN_DEEP

    sample = '{"city":"Austin","geo":{"lat":30,"lon":-97}}'
    flat = flatten_struct_field(sample, parent_key="addr", max_depth=2)
    assert flat["addr_city"] == "Austin"
    assert flat["addr_geo_lat"] == 30
    assert flat["addr_geo_lon"] == -97
    # Parent blob retained.
    assert "addr" in flat

    headers = ["id", "addr"]
    rows = [["1", sample]]
    mappings = [
        {"source": "id", "target": "id"},
        {"source": "addr", "target": "addr", "struct_policy": STRUCT_POLICY_FLATTEN_DEEP},
    ]
    new_headers, new_rows = materialize_struct_policies(headers, rows, mappings)
    assert "addr_geo_lat" in new_headers
    assert new_rows[0][new_headers.index("addr_geo_lat")] in (30, "30")


def test_explode_rows_duplicates_parent():
    from services.json_intelligence import ARRAY_POLICY_EXPLODE

    headers = ["id", "tags"]
    rows = [["1", '["a","b","c"]']]
    mappings = [
        {"source": "id", "target": "id"},
        {"source": "tags", "target": "tags", "struct_policy": ARRAY_POLICY_EXPLODE},
    ]
    new_headers, new_rows = materialize_struct_policies(headers, rows, mappings)
    assert len(new_rows) == 3
    assert "tags_elem" in new_headers
    elems = [r[new_headers.index("tags_elem")] for r in new_rows]
    assert elems == ["a", "b", "c"]
    # Parent array blob retained on every exploded row.
    assert all(r[new_headers.index("tags")] == '["a","b","c"]' for r in new_rows)
