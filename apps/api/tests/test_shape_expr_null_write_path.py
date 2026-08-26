"""Shape is_null / coalesce use is_null_evidence, not a second stringify.

Reader-wired SQL_NULL_SENTINEL used to look like a present string, so
is_null was false and coalesce returned the sentinel token.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.shape_expr import compile_expression, is_blank  # noqa: E402
from services.value_serializer import SQL_NULL_SENTINEL  # noqa: E402


def value(source: str, row: dict | None = None):
    return compile_expression(source).evaluate(row or {})


def test_is_blank_matches_reader_wire():
    assert is_blank(None) is True
    assert is_blank("") is True
    assert is_blank("   ") is True
    assert is_blank(SQL_NULL_SENTINEL) is True
    assert is_blank(float("nan")) is True
    assert is_blank("kept") is False
    assert is_blank("0") is False
    assert is_blank(0) is False


def test_is_null_sees_reader_sentinel():
    assert value("is_null([x])", {"x": SQL_NULL_SENTINEL}) is True
    assert value("is_not_null([x])", {"x": SQL_NULL_SENTINEL}) is False
    assert value("is_null([x])", {"x": "kept"}) is False


def test_coalesce_skips_reader_null():
    assert value(
        "coalesce([a], [b], 'z')",
        {"a": SQL_NULL_SENTINEL, "b": None},
    ) == "z"
    assert value(
        "coalesce([a], [b])",
        {"a": SQL_NULL_SENTINEL, "b": "kept"},
    ) == "kept"
