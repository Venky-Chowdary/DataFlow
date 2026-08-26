"""In-batch conflict-key dedupe uses _conflict_key_identity.

Last-wins and LSN-wins keyed on the raw cell, so extract SQL_NULL_SENTINEL
and dest None were two keys. At-least-once redelivery wrote both.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.lsn_guards import (  # noqa: E402
    DF_LSN_COL,
    dedupe_rows_by_pk_and_lsn,
)
from connectors.writer_common import dedupe_rows  # noqa: E402
from services.value_serializer import SQL_NULL_SENTINEL  # noqa: E402


def test_dedupe_collapses_reader_null_and_blank_to_one_key():
    rows = [
        (SQL_NULL_SENTINEL, "first"),
        (None, "second"),
        ("  ", "third"),
        ("1", "kept"),
    ]
    kept = dedupe_rows(rows, ["id"], ["id", "v"])
    by_v = {r[1] for r in kept}
    assert by_v == {"third", "kept"}
    assert len(kept) == 2


def test_dedupe_zero_stays_distinct_from_null():
    rows = [(0, "zero"), (None, "n"), (SQL_NULL_SENTINEL, "s")]
    kept = dedupe_rows(rows, ["id"], ["id", "v"])
    by_v = {r[1] for r in kept}
    assert by_v == {"zero", "s"}
    assert len(kept) == 2


def test_dedupe_present_text_still_last_wins():
    rows = [("a", "1"), ("b", "2"), ("a", "3")]
    kept = dedupe_rows(rows, ["id"], ["id", "v"])
    assert kept == [("a", "3"), ("b", "2")]


def test_lsn_dedupe_collapses_reader_null_and_keeps_newest():
    cols = ["id", "v", DF_LSN_COL]
    rows = [
        (SQL_NULL_SENTINEL, "old", "0/100"),
        (None, "new", "0/300"),
        ("  ", "mid", "0/200"),
        ("1", "other", "0/100"),
    ]
    kept = dedupe_rows_by_pk_and_lsn(rows, ["id"], cols)
    by_v = {r[1] for r in kept}
    assert by_v == {"new", "other"}
    assert len(kept) == 2
