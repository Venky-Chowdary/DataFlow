"""Verification ladder L2 cells use cell_to_string, not str(value).

str(True) invented True. str(Decimal('1E+2')) invented scientific.
SQL_NULL_SENTINEL was counted as a non-null string, so L2 under-counted
NULLs after PostgreSQL / Iceberg / procedure extract. Empty string stays
a value. Min/max of dest-canonical numbers compare as Decimal — wire
lexicographic ``10`` < ``9`` invented the wrong extrema.
"""

from __future__ import annotations

import sqlite3
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.value_serializer import SQL_NULL_SENTINEL, cell_to_string  # noqa: E402
from services.verification_ladder import (  # noqa: E402
    _cell_text,
    _index_by_pk,
    _pk_key,
    compute_column_aggregates,
    read_sqlite_rows,
)

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


def test_l2_minmax_uses_numeric_order_not_lexicographic_wire():
    aggs = compute_column_aggregates(
        [{"amt": "9"}, {"amt": "10"}, {"amt": Decimal("1E+2")}],
        ["amt"],
    )
    assert aggs["amt"].min_value == "9"
    assert aggs["amt"].max_value == "100"
    assert aggs["amt"].min_value != "10"


def test_l2_does_not_sum_auto_ambiguous_group():
    aggs = compute_column_aggregates(
        [{"amt": "1,234"}, {"amt": "1.2345"}],
        ["amt"],
    )
    assert aggs["amt"].non_null_count == 2
    assert aggs["amt"].sum_value is None


def test_pk_key_matches_extract_wire():
    assert _pk_key(None) is None
    assert _pk_key("") is None
    assert _pk_key(SQL_NULL_SENTINEL) is None
    assert _pk_key(True) == "true"
    assert _pk_key(True) != str(True)
    assert _pk_key(Decimal("1E+2")) == "100"
    assert _pk_key("true") == "true"
    assert _pk_key(1) == "1"


def test_index_joins_native_bool_to_dest_text():
    src = _index_by_pk([{"id": True, "amt": Decimal("1E+2")}], "id")
    dst = _index_by_pk([{"id": "true", "amt": "100"}], "id")
    assert set(src) == {"true"}
    assert set(dst) == {"true"}
    assert src["true"]["amt"] == Decimal("1E+2")
    assert "True" not in src
    assert SQL_NULL_SENTINEL not in _index_by_pk(
        [{"id": SQL_NULL_SENTINEL, "amt": "1"}], "id"
    )


def test_sqlite_ladder_read_is_transfer_wire(tmp_path: Path):
    db = tmp_path / "ladder.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE t (id INTEGER, note TEXT, amt REAL)")
    conn.execute("INSERT INTO t VALUES (?, ?, ?)", (1, None, 1.5))
    conn.execute("INSERT INTO t VALUES (?, ?, ?)", (2, "", 1.25))
    conn.commit()
    conn.close()
    rows = read_sqlite_rows(database=str(db), table="t")
    assert rows[0] == {"id": "1", "note": SQL_NULL_SENTINEL, "amt": "1.5"}
    assert rows[1]["note"] == ""
    assert rows[1]["note"] != rows[0]["note"]
    assert type(rows[0]["id"]) is str


def test_l5_displays_transfer_wire_not_native_invent():
    from services.verification_ladder import binary_search_row_diff

    src = _index_by_pk([{"id": True, "amt": Decimal("1E+2")}], "id")
    dst = _index_by_pk([{"id": "true", "amt": Decimal("99")}], "id")
    report = binary_search_row_diff(
        source_by_pk=src,
        target_by_pk=dst,
        columns=["id", "amt"],
        pk_column="id",
        dest_db_type="sqlite",
        dest_types={"id": "BOOLEAN", "amt": "DECIMAL"},
        focus_columns=["amt"],
    )
    assert report.details["mismatches"]
    hit = report.details["mismatches"][0]
    assert hit["pk"] == "true"
    assert hit["source_value"] == "100"
    assert hit["target_value"] == "99"
    assert hit["source_value"] != "1E+2"
