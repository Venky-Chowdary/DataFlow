"""Gate-8 upsert: batch-key checksum vs full-table cardinality."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from connectors.writer_common import (
    gate8_writer_meta,
    written_ids_from_mapped_rows,
)
from services.reconciliation import verify_sqlite_table


def test_written_ids_from_mapped_rows_single_pk() -> None:
    rows = [("1", "a"), ("2", "b"), ("1", "c")]
    ids = written_ids_from_mapped_rows(rows, ["id", "v"], ["id"])
    assert ids == ["1", "2"]


def test_written_ids_refuse_composite_pk() -> None:
    rows = [("1", "x", "a")]
    assert (
        written_ids_from_mapped_rows(rows, ["a", "b", "v"], ["a", "b"]) is None
    )


def test_gate8_meta_stamps_written_ids_from_conflict() -> None:
    meta = gate8_writer_meta(
        [("1", "a"), ("3", "c")],
        ["id", "amount"],
        conflict_columns=["id"],
    )
    assert meta["written_ids"] == ["1", "3"]
    assert len(meta["reconcile_sample"]) == 2


def test_verify_sqlite_keyed_checksum_preserves_full_count(tmp_path: Path) -> None:
    db = tmp_path / "gate8.db"
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE payments (id INTEGER PRIMARY KEY, amount TEXT)")
    conn.executemany(
        "INSERT INTO payments VALUES (?, ?)",
        [(1, "1000.00"), (2, "2000.50"), (3, "3000.00")],
    )
    conn.commit()
    conn.close()

    full_count, full_chk = verify_sqlite_table(
        connection_string=str(db),
        database=str(db),
        table_name="payments",
    )
    assert full_count == 3
    assert full_chk

    keyed_count, keyed_chk = verify_sqlite_table(
        connection_string=str(db),
        database=str(db),
        table_name="payments",
        written_ids=["1", "3"],
        pk_column="id",
    )
    # Cardinality stays whole-table; checksum is batch-scoped.
    assert keyed_count == 3
    assert keyed_chk
    assert keyed_chk != full_chk
