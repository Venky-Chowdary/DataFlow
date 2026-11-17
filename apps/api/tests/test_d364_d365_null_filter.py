"""D364 / D365: source filters use SQL three-valued logic.

NULL does not satisfy lt/gt/eq/ne/contains. ``''`` is a value on PostgreSQL
and is kept by ``is_not_null``. Oracle's ``''`` IS NULL is a separate rule.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API = Path(__file__).resolve().parents[1]
if str(_API) not in sys.path:
    sys.path.insert(0, str(_API))

from services.row_filter import (  # noqa: E402
    apply_row_filter,
    empty_string_is_sql_null,
)
from services.value_serializer import SQL_NULL_SENTINEL  # noqa: E402

_OPS = (
    "eq", "ne", "gt", "gte", "lt", "lte",
    "contains", "startswith", "endswith", "regex", "in", "not_in",
)


def _rows():
    return [
        {"id": 1, "qty": 10, "note": "a"},
        {"id": 2, "qty": 20, "note": ""},
        {"id": 3, "qty": 30, "note": None},
        {"id": 5, "qty": 50, "note": "none"},
        {"id": 6, "qty": 60, "note": "NULL"},
        {"id": 30, "qty": None, "note": SQL_NULL_SENTINEL},
        {"id": 31, "note": "   "},
    ]


def test_comparison_operators_exclude_null_and_absent() -> None:
    rows = _rows() + [{"id": 40, "name": "no-qty"}]
    for op in _OPS:
        value = ["100"] if op in {"in", "not_in"} else "100"
        kept = apply_row_filter(rows, {"column": "qty", "operator": op, "value": value})
        ids = {r["id"] for r in kept}
        assert 30 not in ids, op
        assert 40 not in ids, op
    lt = apply_row_filter(rows, {"column": "qty", "operator": "lt", "value": 100})
    assert {r["id"] for r in lt} == {1, 2, 3, 5, 6}


def test_empty_string_is_not_null_on_postgresql() -> None:
    rows = _rows()
    is_null = apply_row_filter(rows, {"column": "note", "operator": "is_null"})
    is_not = apply_row_filter(rows, {"column": "note", "operator": "is_not_null"})
    assert {r["id"] for r in is_null} == {3, 30}
    assert 2 in {r["id"] for r in is_not}
    assert {r["id"] for r in is_not} == {1, 2, 5, 6, 31}
    assert empty_string_is_sql_null("postgresql") is False
    assert empty_string_is_sql_null("oracle") is True
    assert empty_string_is_sql_null("sqlserver") is False
