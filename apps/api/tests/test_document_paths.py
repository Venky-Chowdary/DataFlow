"""Mongo field-path rules shared by the document model and the writers."""

from __future__ import annotations

import pytest

from services.document_paths import (
    ARRAY,
    SCALAR,
    PathError,
    check_paths,
    collision_issues,
    flatten,
    nest,
    path_issues,
    split_path,
)


def _codes(issues):
    return [i.code for i in issues]


# --- one path ---------------------------------------------------------------


@pytest.mark.parametrize(
    "path, segments",
    [
        ("name", ("name",)),
        ("address.city", ("address", "city")),
        ("a.b.c.d", ("a", "b", "c", "d")),
        ("_id", ("_id",)),
        ("_id.customer_id", ("_id", "customer_id")),
        ("price$", ("price$",)),
        ("café.ville", ("café", "ville")),
    ],
)
def test_valid_paths_split_into_segments(path, segments):
    assert split_path(path) == segments
    assert path_issues(path) == []


@pytest.mark.parametrize(
    "path, code",
    [
        ("", "path_empty"),
        ("   ", "path_empty"),
        (".a", "path_empty_segment"),
        ("a.", "path_empty_segment"),
        ("a..b", "path_empty_segment"),
        ("$set", "path_dollar_segment"),
        ("a.$b", "path_dollar_segment"),
        ("a\x00b", "path_nul"),
        ("a._id", "path_id_not_root"),
        ("a._id.b", "path_id_not_root"),
    ],
)
def test_invalid_paths_are_named(path, code):
    issues = path_issues(path)
    assert code in _codes(issues)
    with pytest.raises(PathError) as exc:
        split_path(path)
    assert exc.value.code == issues[0].code
    assert exc.value.path == path
    assert "DataFlow" not in str(exc.value)


def test_non_text_path_is_refused():
    assert _codes(path_issues(12)) == ["path_not_text"]
    assert _codes(path_issues(None)) == ["path_not_text"]


def test_id_is_refused_at_the_top_of_a_nested_container():
    # Inside an array element or sub-document, `_id` is not the document id.
    assert _codes(path_issues("_id", allow_id=False)) == ["path_id_not_root"]
    assert path_issues("sku", allow_id=False) == []


def test_messages_are_plain_english():
    (issue,) = path_issues("a..b")
    assert "a..b" in issue.message
    assert issue.to_dict() == {
        "code": "path_empty_segment",
        "path": "a..b",
        "message": issue.message,
    }


# --- collisions -------------------------------------------------------------


def test_disjoint_paths_and_shared_parents_do_not_collide():
    entries = [
        ("name", SCALAR),
        ("address.city", SCALAR),
        ("address.zip", SCALAR),
        ("orders", ARRAY),
        ("_id", SCALAR),
    ]
    assert collision_issues(entries) == []
    assert check_paths(entries) == []


def test_duplicate_path_collides():
    issues = collision_issues([("name", SCALAR), ("name", SCALAR)])
    assert _codes(issues) == ["path_duplicate"]
    assert issues[0].path == "name"


def test_scalar_cannot_also_be_an_object():
    issues = collision_issues([("address", SCALAR), ("address.city", SCALAR)])
    assert _codes(issues) == ["path_prefix_collision"]
    assert issues[0].path == "address.city"
    assert "address" in issues[0].message


def test_collision_names_the_path_it_collides_with():
    (issue,) = collision_issues([("orders", ARRAY), ("orders.total", SCALAR)])
    assert issue.other == "orders"
    assert issue.to_dict()["other"] == "orders"
    (dup,) = collision_issues([("a", SCALAR), ("a", SCALAR)])
    assert dup.other == "" and "other" not in dup.to_dict()


def test_prefix_collision_is_found_whatever_the_order():
    issues = collision_issues([("a.b.c", SCALAR), ("a", SCALAR)])
    assert _codes(issues) == ["path_prefix_collision"]


def test_nothing_may_be_written_inside_an_array_path():
    issues = collision_issues([("orders", ARRAY), ("orders.total", SCALAR)])
    assert _codes(issues) == ["path_array_collision"]
    assert issues[0].path == "orders.total"


def test_two_arrays_on_one_path_collide():
    issues = collision_issues([("orders", ARRAY), ("orders", ARRAY)])
    assert _codes(issues) == ["path_duplicate"]


def test_array_and_scalar_on_one_path_collide():
    issues = collision_issues([("orders", SCALAR), ("orders", ARRAY)])
    assert _codes(issues) == ["path_duplicate"]


def test_unknown_kind_is_a_programming_error():
    with pytest.raises(ValueError):
        collision_issues([("a", "object")])


def test_check_paths_reports_bad_segments_before_collisions():
    issues = check_paths([("a..b", SCALAR), ("x", SCALAR), ("x", SCALAR)])
    assert _codes(issues) == ["path_empty_segment", "path_duplicate"]


def test_check_paths_honours_allow_id():
    assert _codes(check_paths([("_id", SCALAR)], allow_id=False)) == ["path_id_not_root"]


# --- flatten / nest ---------------------------------------------------------


def test_flatten_produces_dotted_leaf_paths():
    doc = {
        "_id": 7,
        "name": "Ada",
        "address": {"city": "Paris", "geo": {"lat": 1.5, "lon": 2.5}},
        "orders": [{"order_id": 1}],
        "empty": {},
        "missing": None,
    }
    assert flatten(doc) == {
        "_id": 7,
        "name": "Ada",
        "address.city": "Paris",
        "address.geo.lat": 1.5,
        "address.geo.lon": 2.5,
        # Arrays are leaf values: their elements are verified per relationship.
        "orders": [{"order_id": 1}],
        "empty": {},
        "missing": None,
    }


def test_nest_inverts_flatten():
    doc = {"a": {"b": {"c": 1}, "d": [1, 2]}, "e": "x", "f": {}}
    assert nest(flatten(doc)) == doc


def test_nest_builds_sub_documents_in_order():
    assert nest({"name": "Ada", "address.city": "Paris", "address.zip": "75001"}) == {
        "name": "Ada",
        "address": {"city": "Paris", "zip": "75001"},
    }


def test_nest_refuses_collisions_and_bad_paths():
    with pytest.raises(PathError) as exc:
        nest({"a": 1, "a.b": 2})
    assert exc.value.code == "path_prefix_collision"
    with pytest.raises(PathError) as exc:
        nest({"a.b": 2, "a": 1})
    assert exc.value.code == "path_prefix_collision"
    with pytest.raises(PathError) as exc:
        nest({"$bad": 1})
    assert exc.value.code == "path_dollar_segment"


def test_flatten_refuses_keys_that_cannot_round_trip():
    with pytest.raises(PathError) as exc:
        flatten({"a.b": 1})
    assert exc.value.code == "path_dotted_key"
    with pytest.raises(PathError):
        flatten({"": 1})
    with pytest.raises(PathError):
        flatten({1: "x"})


def test_flatten_with_prefix():
    assert flatten({"city": "Paris"}, prefix="address") == {"address.city": "Paris"}
