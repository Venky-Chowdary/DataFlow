"""Verification ladder L2 cells use cell_to_string, not str(value).

str(True) invented True. str(Decimal('1E+2')) invented scientific.
SQL_NULL_SENTINEL was counted as a non-null string, so L2 under-counted
NULLs after PostgreSQL / Iceberg / procedure extract. Empty string stays
a value.
"""

from __future__ import annotations

import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.value_serializer import SQL_NULL_SENTINEL, cell_to_string  # noqa: E402
from services.verification_ladder import _cell_text, compute_column_aggregates  # noqa: E402

LONG = "1.234567890123456789"
BLOB = bytes([0xFF, 0xFE, 0x00])
TS = datetime(2024, 1, 2, 3, 4, 5)


def test_cell_text_matches_sql_reader_wire():
    assert _cell_text(None) is None
    assert _cell_text(SQL_NULL_SENTINEL) is None
    assert _cell_text("\x00NULL\x00") is None
    assert _cell_text("") == ""
    assert _cell_text(True) == "true"
    assert _cell_text(True) != str(True)
    assert _cell_text(Decimal(LONG)) == LONG
    assert _cell_text(Decimal("1E+2")) == "100"
    assert str(Decimal("1E+2")) == "1E+2"
    assert _cell_text(TS) == "2024-01-02T03:04:05"
    assert _cell_text(BLOB) == cell_to_string(BLOB, preserve_sql_null=True)


def test_l2_counts_sql_null_as_null_not_a_string():
    aggs = compute_column_aggregates(
        [
            {"note": SQL_NULL_SENTINEL, "amt": Decimal(LONG)},
            {"note": "", "amt": Decimal("1E+2")},
            {"note": None, "amt": Decimal("1.50")},
        ],
        ["note", "amt"],
    )
    assert aggs["note"].null_count == 2
    assert aggs["note"].non_null_count == 1
    assert aggs["note"].min_value == ""
    assert aggs["note"].max_value == ""
    assert aggs["amt"].null_count == 0
    assert aggs["amt"].sum_value == format(Decimal(LONG) + Decimal("100") + Decimal("1.50"), "f")
    assert aggs["amt"].min_value == LONG
    assert aggs["amt"].max_value == "100"


def test_l2_does_not_sum_auto_ambiguous_group():
    aggs = compute_column_aggregates(
        [{"amt": "1,234"}, {"amt": "1.2345"}],
        ["amt"],
    )
    assert aggs["amt"].non_null_count == 2
    assert aggs["amt"].sum_value is None
