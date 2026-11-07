"""Incremental cursor cells use present_cell_text, not str(value).

Reader-wired SQL_NULL_SENTINEL used to look like a present cursor, so a
file incremental could treat the sentinel as new. Decimal 1E+2 used
str() scientific and lost the compare against dest-canonical 100.
True and dest "true" share one cursor token.
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.destination_key_collision_probe import (  # noqa: E402
    rows_a_cursor_read_will_deliver,
)
from services.sync_cursor import records_after_watermark  # noqa: E402
from services.value_serializer import SQL_NULL_SENTINEL  # noqa: E402


def test_file_delta_reader_null_is_unbounded_not_new():
    delta, unbounded = records_after_watermark(
        [
            {"id": "1", "updated_at": SQL_NULL_SENTINEL},
            {"id": "2", "updated_at": None},
            {"id": "3", "updated_at": ""},
            {"id": "4", "updated_at": "2024-01-03T00:00:00"},
        ],
        "updated_at",
        "2024-01-02T00:00:00",
    )
    assert [r["id"] for r in delta] == ["4"]
    assert unbounded == 3


def test_file_delta_scientific_decimal_compares_as_write_path():
    assert str(Decimal("1E+2")) == "1E+2"
    delta, unbounded = records_after_watermark(
        [{"id": "1", "n": Decimal("1E+2")}],
        "n",
        "99",
    )
    assert unbounded == 0
    assert [r["id"] for r in delta] == ["1"]


def test_file_delta_bool_matches_dest_true():
    delta, unbounded = records_after_watermark(
        [{"id": "1", "flag": True}],
        "flag",
        "true",
    )
    assert unbounded == 0
    assert delta == []


def test_prewrite_delta_compares_write_path_text():
    delta = rows_a_cursor_read_will_deliver(
        [
            {"id": 1, "n": Decimal("1E+2")},
            {"id": 2, "n": SQL_NULL_SENTINEL},
            {"id": 3, "n": 50},
        ],
        cursor_column="n",
        watermark="99",
    )
    assert [r["id"] for r in delta] == [1, 2]
