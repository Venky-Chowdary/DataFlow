"""Portable MERGE key-hit refuses reader-null conflict keys.

_existing_conflict_keys skipped only Python None / \"\". After extract
emits SQL_NULL_SENTINEL, the lookup bound equality to the token and
update_insert could INSERT a second NULL-PK row.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.merge_dialects import update_insert_upsert  # noqa: E402
from services.value_serializer import SQL_NULL_SENTINEL  # noqa: E402


def test_update_insert_refuses_reader_null_conflict_key():
    with pytest.raises(ValueError, match="null/empty conflict key"):
        update_insert_upsert(
            None,
            None,
            [{"id": SQL_NULL_SENTINEL, "v": "x"}],
            ["id"],
            ["id", "v"],
        )


def test_update_insert_refuses_blank_conflict_key():
    with pytest.raises(ValueError, match="null/empty conflict key"):
        update_insert_upsert(
            None, None, [{"id": "", "v": "x"}], ["id"], ["id", "v"]
        )


def test_update_insert_empty_batch_is_noop():
    assert update_insert_upsert(None, None, [], ["id"], ["id"]) == 0
