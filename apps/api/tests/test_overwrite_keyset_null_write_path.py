"""Leftover MERGE / delete identity skips reader nulls.

Reader-wired SQL_NULL_SENTINEL used to look like a present PK, so
OverwriteSourceKeySet stored the sentinel spelling in S and
format_delete_keys emitted DELETE WHERE id = '__DF_SQL_NULL__'.
A missing PK cell leaves leftover MERGE unmeasured. Empty /
whitespace stay incomplete identity.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.dest_precount import OverwriteSourceKeySet  # noqa: E402
from services.row_conservation import (  # noqa: E402
    _unique_source_keys,
    format_delete_keys,
)
from services.value_serializer import SQL_NULL_SENTINEL  # noqa: E402


def test_overwrite_keyset_reader_null_is_unusable():
    acc = OverwriteSourceKeySet(1)
    acc.observe_tuples([(1,), (SQL_NULL_SENTINEL,)])
    assert acc.export() is None
    blank = OverwriteSourceKeySet(1)
    blank.observe_tuples([("",)])
    assert blank.export() is None
    none = OverwriteSourceKeySet(1)
    none.observe_tuples([(None,)])
    assert none.export() is None


def test_overwrite_keyset_real_keys_still_export():
    acc = OverwriteSourceKeySet(1)
    acc.observe_tuples([(1,), (2,)])
    acc.observe_tuples([(3,)])
    assert acc.export() == [(1,), (2,), (3,)]


def test_unique_source_keys_reader_null_is_unmeasured():
    assert _unique_source_keys([(1,), (SQL_NULL_SENTINEL,)], 1) is None
    assert _unique_source_keys([(1,), (2,)], 1) == [(1,), (2,)]


def test_format_delete_keys_skips_reader_null():
    assert format_delete_keys([(SQL_NULL_SENTINEL,), (None,), ("",), (1,), (True,)]) == [
        "1",
        "True",
    ]
