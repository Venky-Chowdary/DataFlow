"""Shape is_null / coalesce / text use is_null_evidence, not a second stringify.

Reader-wired SQL_NULL_SENTINEL used to look like a present string, so
is_null was false, coalesce returned the sentinel token, and upper /
contains invented the sentinel spelling. Bytes used str() b'...'.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.shape_expr import _as_text, compile_expression, is_blank  # noqa: E402
from services.value_serializer import SQL_NULL_SENTINEL, cell_to_string  # noqa: E402


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


def test_not_null_is_unknown_and_does_not_match():
    assert value("not [x]", {"x": None}) is None
    assert value("not [x]", {"x": SQL_NULL_SENTINEL}) is None
    assert compile_expression("not [x]").matches({"x": None}) is False
    assert compile_expression("not [x]").matches({"x": SQL_NULL_SENTINEL}) is False
    assert compile_expression("not [x]").matches({"x": False}) is True
    assert compile_expression("not [x]").matches({"x": True}) is False


def test_as_text_skips_reader_null_and_wires_typed_cells():
    assert _as_text(None) is None
    assert _as_text("") is None
    assert _as_text("   ") is None
    assert _as_text(SQL_NULL_SENTINEL) is None
    assert _as_text("kept") == "kept"
    assert _as_text("  kept  ") == "  kept  "
    assert _as_text(True) == "true"
    assert _as_text(True) != str(True)
    blob = bytes([0xFF, 0xFE, 0x00])
    assert _as_text(blob) == cell_to_string(blob, preserve_sql_null=True)
    assert _as_text(blob) != str(blob)


def test_upper_contains_to_text_skip_reader_null():
    assert value("upper([x])", {"x": SQL_NULL_SENTINEL}) is None
    assert value("length([x])", {"x": SQL_NULL_SENTINEL}) is None
    assert value("to_text([x])", {"x": SQL_NULL_SENTINEL}) is None
    assert value("contains([x], 'DF')", {"x": SQL_NULL_SENTINEL}) is None
    assert value("concat([x], 'z')", {"x": SQL_NULL_SENTINEL}) == "z"
    assert value("upper([x])", {"x": "kept"}) == "KEPT"
    assert value("to_text([x])", {"x": True}) == "true"
