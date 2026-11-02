"""Shape apply concat / split / null_if use _as_text, not a second stringify.

Reader-wired SQL_NULL_SENTINEL used to look like present text, so
concat joined the wire token, split cut the sentinel spelling, and
null_if could not match True to dest ``"true"``. Blank cells stay
absent. Empty name after title-case is None, not an invented ``""``.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.shape_engine import _same_value, shape_records  # noqa: E402
from services.shape_models import ShapeRecipe  # noqa: E402
from services.value_serializer import SQL_NULL_SENTINEL, cell_to_string  # noqa: E402


def _recipe(*steps, columns):
    return ShapeRecipe.parse({"steps": list(steps)}, source_columns=columns)


def test_concat_skips_reader_null():
    shaped, _ = shape_records(
        _recipe(
            {
                "op": "concat_columns",
                "options": {
                    "to": "joined",
                    "columns": ["a", "b"],
                    "separator": "-",
                },
            },
            columns=["a", "b"],
        ),
        [{"a": SQL_NULL_SENTINEL, "b": "z"}, {"a": None, "b": "z"}, {"a": "x", "b": "z"}],
    )
    assert [r["joined"] for r in shaped] == ["z", "z", "x-z"]
    assert SQL_NULL_SENTINEL not in shaped[0]["joined"]


def test_split_reader_null_is_absent():
    shaped, _ = shape_records(
        _recipe(
            {
                "op": "split_column",
                "column": "full",
                "options": {"separator": " ", "into": ["first", "last"]},
            },
            columns=["full"],
        ),
        [{"full": SQL_NULL_SENTINEL}, {"full": "Ada Lovelace"}],
    )
    assert shaped[0]["first"] is None
    assert shaped[0]["last"] is None
    assert shaped[1]["first"] == "Ada"
    assert shaped[1]["last"] == "Lovelace"


def test_null_if_matches_dest_true_and_collapses_blank():
    shaped, effect = shape_records(
        _recipe(
            {"op": "null_if", "column": "flag", "options": {"values": ["true"]}},
            columns=["flag"],
        ),
        [
            {"flag": True},
            {"flag": "keep"},
            {"flag": SQL_NULL_SENTINEL},
            {"flag": ""},
        ],
    )
    assert [r["flag"] for r in shaped] == [None, "keep", None, None]
    assert effect.nulls_introduced == 1


def test_same_value_treats_reader_null_as_absent():
    assert _same_value(None, SQL_NULL_SENTINEL) is True
    assert _same_value("", SQL_NULL_SENTINEL) is True
    assert _same_value("kept", SQL_NULL_SENTINEL) is False
    blob = bytes([0xFF, 0xFE])
    assert _same_value(blob, cell_to_string(blob, preserve_sql_null=True)) is True
    assert _same_value(True, "true") is True
    assert _same_value(True, str(True)) is False
