"""Row-filter membership and text compare use present_cell_text.

``str(None)`` is ``"None"``, so ``in ["None"]`` matched a SQL NULL.
Reader-wired SQL_NULL_SENTINEL looked like a present token for
contains / eq / in. True and dest ``"true"`` must share one token;
Decimal 1E+2 must match dest-canonical 100. 0 stays present.
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.row_filter import apply_row_filter  # noqa: E402
from services.value_serializer import SQL_NULL_SENTINEL  # noqa: E402


def test_in_does_not_match_str_none_or_sentinel_spelling():
    rows = [
        {"id": "1", "note": None},
        {"id": "2", "note": SQL_NULL_SENTINEL},
        {"id": "3", "note": "None"},
        {"id": "4", "note": "kept"},
    ]
    assert {r["id"] for r in apply_row_filter(
        rows, {"column": "note", "operator": "in", "value": ["None"]}
    )} == {"3"}
    assert {r["id"] for r in apply_row_filter(
        rows, {"column": "note", "operator": "in", "value": [SQL_NULL_SENTINEL]}
    )} == {"1", "2"}
    assert {r["id"] for r in apply_row_filter(
        rows, {"column": "note", "operator": "in", "value": ["kept"]}
    )} == {"4"}


def test_eq_bool_matches_dest_true():
    rows = [{"id": "1", "flag": True}, {"id": "2", "flag": False}, {"id": "3", "flag": "true"}]
    assert {r["id"] for r in apply_row_filter(
        rows, {"column": "flag", "operator": "eq", "value": "true"}
    )} == {"1", "3"}
    assert {r["id"] for r in apply_row_filter(
        rows, {"column": "flag", "operator": "in", "value": [True]}
    )} == {"1", "3"}


def test_eq_scientific_decimal_is_write_path():
    assert str(Decimal("1E+2")) == "1E+2"
    rows = [{"id": "1", "n": Decimal("1E+2")}, {"id": "2", "n": 99}]
    assert {r["id"] for r in apply_row_filter(
        rows, {"column": "n", "operator": "eq", "value": "100"}
    )} == {"1"}
    assert {r["id"] for r in apply_row_filter(
        rows, {"column": "n", "operator": "in", "value": ["100"]}
    )} == {"1"}


def test_contains_does_not_search_sentinel_spelling():
    rows = [
        {"id": "1", "note": SQL_NULL_SENTINEL},
        {"id": "2", "note": "hello DF world"},
        {"id": "3", "note": None},
    ]
    assert {r["id"] for r in apply_row_filter(
        rows, {"column": "note", "operator": "contains", "value": "DF"}
    )} == {"2"}


def test_eq_keeps_integer_zero():
    rows = [{"id": "1", "n": 0}, {"id": "2", "n": SQL_NULL_SENTINEL}, {"id": "3", "n": 2}]
    assert {r["id"] for r in apply_row_filter(
        rows, {"column": "n", "operator": "eq", "value": 0}
    )} == {"1"}
    assert {r["id"] for r in apply_row_filter(
        rows, {"column": "n", "operator": "in", "value": [0]}
    )} == {"1"}
