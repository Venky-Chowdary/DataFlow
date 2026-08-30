"""Mongo conflict keys use _is_nullish_conflict_key.

Prefetch / refuse / in-batch dedupe only saw Python None / \"\". After
extract emits SQL_NULL_SENTINEL, that token passed the incomplete-key
gate and ReplaceOne filtered on the wire spelling.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.mongodb_writer import (  # noqa: E402
    _mongo_conflict_key,
    _mongo_incomplete_pk_cols,
)
from services.value_serializer import SQL_NULL_SENTINEL  # noqa: E402


def test_mongo_incomplete_pk_treats_reader_null_and_blank():
    pk = ["id"]
    for wire in (None, SQL_NULL_SENTINEL, "__df_ddb_null__", "", "  "):
        assert _mongo_incomplete_pk_cols({"id": wire}, pk) == ["id"]
        assert _mongo_conflict_key({"id": wire}, pk) == (None,)


def test_mongo_present_zero_and_false_are_complete_keys():
    assert _mongo_incomplete_pk_cols({"id": 0}, ["id"]) == []
    assert _mongo_incomplete_pk_cols({"id": False}, ["id"]) == []
    assert _mongo_conflict_key({"id": 0}, ["id"]) == (0,)
    assert _mongo_conflict_key({"id": False}, ["id"]) == (False,)
    assert _mongo_conflict_key({"id": "1"}, ["id"]) == ("1",)


def test_mongo_dedupe_identity_collapses_sentinel_to_none():
    a = _mongo_conflict_key({"id": SQL_NULL_SENTINEL, "v": "a"}, ["id"])
    b = _mongo_conflict_key({"id": None, "v": "b"}, ["id"])
    c = _mongo_conflict_key({"id": "1", "v": "c"}, ["id"])
    assert a == b == (None,)
    assert c == ("1",)
    assert a != c
