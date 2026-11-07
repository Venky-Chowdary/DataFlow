"""Sample orphan FK keys use is_null_evidence + cell_to_string.

Reader-wired SQL_NULL_SENTINEL used to look like a present parent key,
so a NULL FK was probed as a string and True vs dest "true" missed.
NULL FKs stay skipped (SQL allows NULL). Empty / whitespace stay blank.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.sample_orphan_probe import (  # noqa: E402
    _fk_key,
    distinct_fk_values,
    orphan_values,
)
from services.value_serializer import (  # noqa: E402
    DF_MISSING_SENTINEL,
    SQL_NULL_SENTINEL,
    Missing,
)


def test_fk_key_matches_reader_wire():
    assert _fk_key(None) is None
    assert _fk_key("") is None
    assert _fk_key("   ") is None
    assert _fk_key(SQL_NULL_SENTINEL) is None
    assert _fk_key(DF_MISSING_SENTINEL) is None
    assert _fk_key(Missing) is None
    assert _fk_key(0) == "0"
    assert _fk_key("0") == "0"
    assert _fk_key(True) == "true"
    assert _fk_key(True) != str(True)
    assert _fk_key("kept") == "kept"


def test_distinct_skips_reader_null_not_as_key():
    rows = [
        {"customer_id": SQL_NULL_SENTINEL},
        {"customer_id": None},
        {"customer_id": ""},
        {"customer_id": "   "},
        {"customer_id": DF_MISSING_SENTINEL},
        {"customer_id": 1},
        {"customer_id": "1"},
        {"customer_id": True},
        {"customer_id": "true"},
    ]
    assert distinct_fk_values(rows, "customer_id") == [1, True]


def test_orphan_skips_reader_null():
    assert orphan_values(
        [SQL_NULL_SENTINEL, None, "", 2],
        [1, 3],
    ) == [2]


def test_orphan_bool_matches_dest_true():
    assert orphan_values([True, "true"], ["true"]) == []
    assert orphan_values(["true"], [True]) == []


def test_orphan_still_flags_missing_parent():
    assert orphan_values([1, 2, 3], [1, 3]) == [2]
    assert orphan_values(["true"], [False]) == ["true"]
