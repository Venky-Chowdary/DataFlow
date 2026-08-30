"""PostgreSQL COPY TEXT treats reader-null as ``\\N``, not the wire token.

``_copy_text_value`` used to only map Python None / Missing. After extract
emits SQL_NULL_SENTINEL, COPY wrote that spelling as TEXT and checksums
diverged from parameter binds. Empty string stays a present field.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.postgresql_writer import _copy_text_value  # noqa: E402
from services.value_serializer import (  # noqa: E402
    DF_MISSING_SENTINEL,
    SQL_NULL_SENTINEL,
    Missing,
)


def test_copy_reader_null_is_copy_null_marker():
    for wire in (None, SQL_NULL_SENTINEL, "__df_ddb_null__", Missing, DF_MISSING_SENTINEL):
        assert _copy_text_value(wire) == "\\N", wire


def test_copy_empty_string_stays_present():
    assert _copy_text_value("") == ""
    assert _copy_text_value("   ") == "   "
    assert _copy_text_value("kept") == "kept"


def test_copy_zero_and_false_stay_present():
    assert _copy_text_value(0) == "0"
    assert _copy_text_value(False) == "f"
    assert _copy_text_value(True) == "t"


def test_copy_decimal_uses_dest_canonical_text():
    from decimal import Decimal

    from services.value_serializer import safe_decimal_text

    assert _copy_text_value(Decimal("1E+2")) == "100"
    assert _copy_text_value(Decimal("100")) == "100"
    assert _copy_text_value(Decimal("1E+2")) == safe_decimal_text(Decimal("1E+2"))
    assert _copy_text_value(Decimal("1E+2")) != str(Decimal("1E+2"))
    assert _copy_text_value(Decimal("0")) == "0"
