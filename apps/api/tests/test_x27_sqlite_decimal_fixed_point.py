"""X-27: SQL Server->SQLite norecipe writes 0.0000000150 as '1.50E-8'.

The driver yields a typed ``Decimal``; ``apply_transform`` spelled non-str
values with ``str(raw)`` — and ``str(Decimal('0.0000000150'))`` is the
scientific '1.50E-8'. An identity/carry transform then bound that text
verbatim into the SQLite TEXT carrier, where the verification ladder
correctly failed it closed.

The wire spelling for a typed Decimal is now the same canonical form the
readers emit via ``cell_to_string`` (``safe_decimal_text`` — scale-exact
fixed-point), covering every source that funnels through the shared
transform helper (MySQL->SQLite, mongo, etc.) not just the reported pair.

Tests use a real sqlite3 destination: the value written is read back and
compared byte-for-byte with the canonical text.
"""

from __future__ import annotations

import sqlite3
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import connectors.sqlite_common  # noqa: E402,F401 — registers the Decimal bind adapter
from services.transform_engine import apply_transform  # noqa: E402

TINY = Decimal("0.0000000150")
WIDE_MAX = Decimal("9999999999999999999999999999.9999999999")


def test_identity_transform_emits_fixed_point_for_decimal():
    for transform in ("none", "identity", "text", "varchar"):
        value, err = apply_transform(TINY, transform)
        assert err is None
        assert value == "0.0000000150", transform
        assert "E" not in value.upper()


def test_identity_transform_keeps_38_digit_fixed_point():
    value, err = apply_transform(WIDE_MAX, "none")
    assert err is None
    assert value == "9999999999999999999999999999.9999999999"


def test_identity_transform_nondecimal_untouched():
    assert apply_transform("0.0000000150", "none") == ("0.0000000150", None)
    assert apply_transform("Zoë", "text") == ("Zoë", None)


def test_typed_decimal_transform_still_parses():
    value, err = apply_transform(TINY, "decimal")
    assert err is None
    assert value == TINY


def _write_roundtrip(value: object) -> object:
    """Bind the transform output into a real SQLite TEXT dest and read it back."""
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("CREATE TABLE t (amount TEXT)")
        conn.execute("INSERT INTO t VALUES (?)", (value,))
        conn.commit()
        return conn.execute("SELECT amount FROM t").fetchone()[0]
    finally:
        conn.close()


def test_sqlite_dest_stores_canonical_text_not_scientific():
    value, err = apply_transform(TINY, "none")
    assert err is None
    assert _write_roundtrip(value) == "0.0000000150"


def test_sqlite_dest_full_row_matrix():
    """Simulated MS->SQLite norecipe row set (reader output shape faithful to
    pyodbc/pymssql Decimal yields) — every cell lands exact."""
    rows = [
        (1, "Zoë", TINY),
        (2, "plain", WIDE_MAX),
        (3, None, Decimal("0.5")),
    ]
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("CREATE TABLE t (id INTEGER, name TEXT, amount TEXT)")
        for rid, name, amount in rows:
            bound = []
            for v in (rid, name, amount):
                mapped, err = apply_transform(v, "none")
                assert err is None
                bound.append(mapped)
            conn.execute("INSERT INTO t VALUES (?,?,?)", bound)
        conn.commit()
        stored = conn.execute("SELECT id, name, amount FROM t ORDER BY id").fetchall()
        assert stored == [
            (1, "Zoë", "0.0000000150"),
            (2, "plain", "9999999999999999999999999999.9999999999"),
            (3, None, "0.5"),
        ]
    finally:
        conn.close()
