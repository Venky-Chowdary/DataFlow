"""Gate-8 key census over repeated keys (append loads every copy)."""

from __future__ import annotations

import sqlite3

from src.transfer.reconcile_step import _census_join


def _db(rows: list[tuple[int, str, str]]) -> sqlite3.Connection:
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE k (side INTEGER, pk TEXT, h TEXT)")
    db.executemany("INSERT INTO k VALUES (?, ?, ?)", rows)
    return db


def test_same_repeated_rows_on_both_sides_are_not_changed() -> None:
    rows = [(s, "SKU-1", h) for s in (0, 1) for h in ("a", "b")] + [(0, "SKU-2", "c"), (1, "SKU-2", "c")]
    assert _census_join(_db(rows)) == ([], [], [])


def test_repeated_key_with_a_differing_copy_is_changed() -> None:
    rows = [(0, "SKU-1", "a"), (0, "SKU-1", "b"), (1, "SKU-1", "a"), (1, "SKU-1", "x")]
    assert _census_join(_db(rows))[0] == ["SKU-1"]


def test_repeated_key_with_a_missing_copy_is_changed() -> None:
    rows = [(0, "SKU-1", "a"), (0, "SKU-1", "b"), (1, "SKU-1", "a")]
    assert _census_join(_db(rows))[0] == ["SKU-1"]


def test_single_key_value_change_still_fails() -> None:
    rows = [(0, "SKU-1", "a"), (1, "SKU-1", "z"), (0, "SKU-2", "c")]
    assert _census_join(_db(rows)) == (["SKU-1"], ["SKU-2"], [])
