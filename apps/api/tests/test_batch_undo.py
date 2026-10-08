"""A failed checksum must not leave a partial batch in a table this run found empty."""

from __future__ import annotations

import sqlite3

from services.batch_undo import undo_failed_batch_if_dest_was_empty
from services.dest_precount import PRECOUNT_KEY
from src.transfer.models import EndpointConfig


def _endpoint(path: str, table: str = "orders") -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="sqlite",
        database=path,
        table=table,
    )


def _seed(path: str, rows: int) -> None:
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, name TEXT)")
    conn.executemany(
        "INSERT INTO orders (id, name) VALUES (?, ?)",
        [(i, f"n{i}") for i in range(1, rows + 1)],
    )
    conn.commit()
    conn.close()


def _count(path: str) -> int:
    conn = sqlite3.connect(path)
    n = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    conn.close()
    return int(n)


def test_empty_destination_loses_the_partial_batch(tmp_path):
    path = str(tmp_path / "empty.db")
    _seed(path, 40)
    summary = {PRECOUNT_KEY: 0, "table": "orders", "sync_mode": "full_refresh_append"}
    note = undo_failed_batch_if_dest_was_empty(
        destination=_endpoint(path),
        dest_summary=summary,
        sync_mode="full_refresh_append",
    )
    assert _count(path) == 0
    assert summary["partial_batch_removed"] is True
    assert "removed" in note
    again = undo_failed_batch_if_dest_was_empty(
        destination=_endpoint(path),
        dest_summary=summary,
        sync_mode="full_refresh_append",
    )
    assert again == note


def test_occupied_destination_is_not_cleared(tmp_path):
    path = str(tmp_path / "occupied.db")
    _seed(path, 40)
    summary = {PRECOUNT_KEY: 10, "table": "orders"}
    note = undo_failed_batch_if_dest_was_empty(
        destination=_endpoint(path),
        dest_summary=summary,
        sync_mode="incremental_append",
    )
    assert _count(path) == 40
    assert summary["partial_batch_undo"] == "retained"
    assert "already held 10" in note


def test_cdc_is_not_rolled_back_as_a_batch(tmp_path):
    path = str(tmp_path / "cdc.db")
    _seed(path, 40)
    summary = {PRECOUNT_KEY: 0, "table": "orders", "sync_mode": "cdc"}
    undo_failed_batch_if_dest_was_empty(
        destination=_endpoint(path),
        dest_summary=summary,
        sync_mode="cdc",
    )
    assert _count(path) == 40
    assert summary["partial_batch_undo"] == "retained"


def test_unmeasured_precount_does_not_delete(tmp_path):
    path = str(tmp_path / "unknown.db")
    _seed(path, 40)
    summary = {"table": "orders"}
    undo_failed_batch_if_dest_was_empty(
        destination=_endpoint(path),
        dest_summary=summary,
        sync_mode="full_refresh_append",
    )
    assert _count(path) == 40
    assert "not measured" in summary["partial_batch_undo_note"]
