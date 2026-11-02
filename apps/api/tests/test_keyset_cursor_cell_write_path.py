"""Page-max cursor and keyset bookmarks use present_cell_text.

Reader-wired SQL_NULL_SENTINEL used to look like a present bookmark
part, so the next seek started at the sentinel spelling. ``if not
row[idx]`` dropped integer 0. True and dest ``"true"`` must share one
token; Decimal 1E+2 must compare as dest-canonical 100.
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.keyset_pagination import (  # noqa: E402
    KEYSET_SEP,
    encode_keyset_bookmark,
    max_keyset_bookmark,
)
from services.sync_cursor import max_cursor_value  # noqa: E402
from services.value_serializer import (  # noqa: E402
    DF_MISSING_SENTINEL,
    SQL_NULL_SENTINEL,
    Missing,
    present_cell_text,
)


def test_encode_bookmark_matches_reader_wire():
    assert encode_keyset_bookmark([None]) == ""
    assert encode_keyset_bookmark([SQL_NULL_SENTINEL]) == ""
    assert encode_keyset_bookmark([""]) == ""
    assert encode_keyset_bookmark(["   "]) == ""
    assert encode_keyset_bookmark([DF_MISSING_SENTINEL]) == ""
    assert encode_keyset_bookmark([Missing]) == ""
    assert encode_keyset_bookmark([0]) == "0"
    assert encode_keyset_bookmark([True]) == "true"
    assert encode_keyset_bookmark([True]) != str(True)
    assert encode_keyset_bookmark([Decimal("1E+2")]) == present_cell_text(
        Decimal("1E+2")
    )
    assert encode_keyset_bookmark(["ord-1", True]) == encode_keyset_bookmark(
        ["ord-1", "true"]
    )


def test_max_keyset_skips_reader_null_parts():
    rows = [
        [SQL_NULL_SENTINEL, "z"],
        [None, "y"],
        ["", "x"],
        ["   ", "w"],
        [200, "b"],
        [99, "a"],
    ]
    assert max_keyset_bookmark(rows, ["id", "p"], ["id"]) == "200"


def test_max_keyset_keeps_integer_zero():
    rows = [[0, "a"], [SQL_NULL_SENTINEL, "b"]]
    assert max_keyset_bookmark(rows, ["id", "p"], ["id"]) == "0"


def test_max_keyset_bool_matches_dest_true():
    rows = [[False, "a"], [True, "b"]]
    assert max_keyset_bookmark(rows, ["flag", "p"], ["flag"]) == "true"


def test_max_cursor_skips_reader_null():
    rows = [
        [SQL_NULL_SENTINEL],
        [None],
        [""],
        ["   "],
        ["2024-06-01"],
        ["2024-01-01"],
    ]
    assert max_cursor_value(rows, ["updated_at"], "updated_at") == "2024-06-01"


def test_max_cursor_keeps_integer_zero():
    """``if not row[idx]`` used to drop 0, so a page of zeros wrote no watermark."""
    assert max_cursor_value([[0], [SQL_NULL_SENTINEL]], ["n"], "n") == "0"
    assert max_cursor_value([[0], [2], [1]], ["n"], "n") == "2"


def test_max_cursor_scientific_decimal_is_write_path():
    assert str(Decimal("1E+2")) == "1E+2"
    assert (
        max_cursor_value([[Decimal("1E+2")], [99]], ["n"], "n")
        == present_cell_text(Decimal("1E+2"))
    )


def test_max_cursor_bool_matches_dest_true():
    assert max_cursor_value([[True], [False]], ["flag"], "flag") == "true"


def test_max_cursor_composite_uses_present_tiebreak():
    rows = [
        ["2024-01-01", SQL_NULL_SENTINEL],
        ["2024-01-01", True],
        ["2024-01-01", False],
    ]
    wm = max_cursor_value(rows, ["updated_at", "id"], "updated_at", "id")
    assert wm == encode_keyset_bookmark(["2024-01-01", True])
    assert wm == f"2024-01-01{KEYSET_SEP}true"
    assert SQL_NULL_SENTINEL not in (wm or "")
    assert str(True) not in (wm or "")


def test_max_cursor_composite_integer_tiebreak_still_picks_2():
    rows = [["2024-01-01", 1], ["2024-01-01", 2], ["2024-01-01", SQL_NULL_SENTINEL]]
    wm = max_cursor_value(rows, ["updated_at", "id"], "updated_at", "id")
    assert wm == encode_keyset_bookmark(["2024-01-01", 2])
