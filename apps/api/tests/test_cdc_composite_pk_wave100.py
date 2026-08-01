"""Composite PK + per-table column cache + explicit buffer PK (wave 100).

These three defects shared one root cause: the streaming CDC path treated a
primary key as a single opaque string and a column cache as a single shared
list. Combined they turned every composite-PK table into append-only with
zero deletes, remapped sibling tables into the wrong columns, and collapsed
distinct rows in the net-effect buffer. The incremental-snapshot path already
did the right thing; this wave makes the streaming path share that helper.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from connectors.mysql_change_stream import MySqlChangeStreamCdc
from connectors.sqlserver_cdc_native import classify_mssql_cdc_rows
from connectors.table_manager import delete_by_primary_keys
from services.cdc_net_effect import coalesce_cdc_txn_events, infer_row_pk, CdcTxnEvent
from services.cdc_snapshot_window import _pk_columns, _pk_value, _pk_row_dict
from services.sync_cursor import SyncContract
from src.transfer.cdc_transfer import _apply_change_batch


class TestCompositePkHelpers:
    def test_comma_joined_string_expands(self) -> None:
        assert _pk_columns("order_id,line_id") == ["order_id", "line_id"]
        assert _pk_columns("a; b") == ["a", "b"]

    def test_list_is_preserved(self) -> None:
        assert _pk_columns(["order_id", "line_id"]) == ["order_id", "line_id"]

    def test_pk_value_joins_with_unit_separator(self) -> None:
        key = _pk_value({"order_id": "7", "line_id": "3", "qty": 1}, "order_id,line_id")
        assert key == "7\x1f3"
        assert _pk_row_dict("order_id,line_id", key) == {
            "order_id": "7",
            "line_id": "3",
        }

    def test_missing_part_returns_none_rather_than_partial_key(self) -> None:
        assert _pk_value({"order_id": "7"}, "order_id,line_id") is None

    def test_sync_contract_columns_agree_with_helper(self) -> None:
        c = SyncContract.from_dict(
            {"name": "lines", "primary_key": "order_id,line_id"}
        )
        assert c.primary_key_columns() == _pk_columns(c.primary_key)


class TestInferRowPkNoLongerGuesses:
    def test_explicit_wins(self) -> None:
        assert infer_row_pk({"name": "a"}, explicit="42") == "42"

    def test_well_known_id_field_still_works(self) -> None:
        assert infer_row_pk({"id": 9, "name": "a"}) == "9"

    def test_first_nonempty_is_not_guessed(self) -> None:
        """The collapse that made two rows sharing a status value become one."""
        assert infer_row_pk({"status": "active", "sku": "X"}) == ""

    def test_coalesce_keeps_unkeyed_rows_under_synthetic_keys(self) -> None:
        events = [
            CdcTxnEvent(op="i", pk="", row={"status": "active", "sku": "X"}),
            CdcTxnEvent(op="i", pk="", row={"status": "active", "sku": "Y"}),
        ]
        inserts, updates, deletes = coalesce_cdc_txn_events(events)
        assert len(inserts) == 2
        assert not deletes and not updates


class TestMysqlPerTableColumnCache:
    def test_cache_is_keyed_by_table(self) -> None:
        reader = MySqlChangeStreamCdc.__new__(MySqlChangeStreamCdc)
        reader.database = "db"
        reader.table = "orders"
        reader.tables = ["orders", "customers"]
        reader.primary_key = "id"
        reader.primary_keys = {"orders": "id", "customers": "cust_id"}
        reader._column_names_cache = {
            "orders": ["id", "amount"],
            "customers": ["cust_id", "email"],
        }

        def fake_ordered(table=None):
            return reader._column_names_cache[table or reader.table]

        reader._ordered_columns = fake_ordered  # type: ignore[method-assign]

        orders_row = {"UNKNOWN_COL0": "1", "UNKNOWN_COL1": "9.99"}
        customers_row = {"UNKNOWN_COL0": "42", "UNKNOWN_COL1": "a@b.c"}
        assert reader._remap_positional(orders_row, table="orders") == {
            "id": "1",
            "amount": "9.99",
        }
        assert reader._remap_positional(customers_row, table="customers") == {
            "cust_id": "42",
            "email": "a@b.c",
        }

    def test_composite_pk_value_from_remapped_row(self) -> None:
        reader = MySqlChangeStreamCdc.__new__(MySqlChangeStreamCdc)
        reader.database = "db"
        reader.table = "lines"
        reader.tables = ["lines"]
        reader.primary_key = "order_id,line_id"
        reader.primary_keys = {"lines": "order_id,line_id"}
        reader._column_names_cache = {
            "lines": ["order_id", "line_id", "qty"],
        }
        reader._ordered_columns = lambda table=None: reader._column_names_cache[
            table or reader.table
        ]  # type: ignore[method-assign]

        row = {"UNKNOWN_COL0": "7", "UNKNOWN_COL1": "3", "UNKNOWN_COL2": "1"}
        assert reader._pk_value(row, table="lines") == "7\x1f3"


class TestMssqlClassifyComposite:
    def test_delete_key_is_composite(self) -> None:
        rows = [
            {
                "__$operation": 1,
                "order_id": "7",
                "line_id": "3",
                "qty": "1",
            }
        ]
        inserts, updates, deletes = classify_mssql_cdc_rows(
            rows, primary_key="order_id,line_id"
        )
        assert not inserts and not updates
        assert deletes == ["7\x1f3"]


class TestCompositeDeleteSqlite:
    def test_composite_delete_removes_the_right_row(self, tmp_path: Path) -> None:
        db = tmp_path / "t.db"
        conn = sqlite3.connect(db)
        conn.execute(
            "CREATE TABLE lines (order_id TEXT, line_id TEXT, qty INT, "
            "PRIMARY KEY (order_id, line_id))"
        )
        conn.executemany(
            "INSERT INTO lines VALUES (?, ?, ?)",
            [("7", "1", 1), ("7", "3", 2), ("8", "1", 3)],
        )
        conn.commit()
        conn.close()

        deleted = delete_by_primary_keys(
            "sqlite",
            {"database": str(db)},
            "lines",
            ["order_id", "line_id"],
            ["7\x1f3"],
        )
        assert deleted == 1
        conn = sqlite3.connect(db)
        try:
            remaining = conn.execute(
                "SELECT order_id, line_id FROM lines ORDER BY order_id, line_id"
            ).fetchall()
        finally:
            conn.close()
        assert remaining == [("7", "1"), ("8", "1")]

    def test_comma_joined_string_is_accepted(self, tmp_path: Path) -> None:
        db = tmp_path / "t.db"
        conn = sqlite3.connect(db)
        conn.execute(
            "CREATE TABLE lines (order_id TEXT, line_id TEXT, PRIMARY KEY (order_id, line_id))"
        )
        conn.execute("INSERT INTO lines VALUES ('1', '2')")
        conn.commit()
        conn.close()
        deleted = delete_by_primary_keys(
            "sqlite",
            {"database": str(db)},
            "lines",
            "order_id,line_id",
            ["1\x1f2"],
        )
        assert deleted == 1


class TestApplyChangeBatchUsesColumnList:
    def test_conflict_columns_are_a_list_not_a_joined_string(self, monkeypatch) -> None:
        """Regression: a joined string made every writer filter conflicts to []."""
        captured: dict[str, object] = {}

        def fake_write_batch(*_a, **kwargs):
            captured["conflict_columns"] = kwargs.get("conflict_columns")
            return 1, "checksum", {}

        monkeypatch.setattr(
            "src.transfer.cdc_transfer._write_batch", fake_write_batch
        )
        monkeypatch.setattr(
            "src.transfer.cdc_transfer.with_retry",
            lambda fn, **_k: fn(),
        )
        monkeypatch.setattr(
            "src.transfer.cdc_transfer.classify_replay_safety",
            lambda **_k: type("S", (), {"allows_retry": lambda *_a, **_k: True})(),
        )

        from services.cdc_engine import ChangeBatch

        change = ChangeBatch(
            inserts=[{"order_id": "7", "line_id": "3", "qty": "1"}],
            updates=[],
            deletes=[],
            resume_token={"file": "binlog.000001", "pos": 4},
        )
        _apply_change_batch(
            "sqlite",
            type("E", (), {"format": "sqlite", "table": "lines", "extra": {}})(),
            {"database": ":memory:"},
            "lines",
            change,
            [
                {"source": "order_id", "target": "order_id"},
                {"source": "line_id", "target": "line_id"},
                {"source": "qty", "target": "qty"},
            ],
            {"order_id": "string", "line_id": "string", "qty": "integer"},
            ["order_id", "line_id", "qty"],
            "order_id,line_id",
            0,
            1,
        )
        assert captured["conflict_columns"] == ["order_id", "line_id"]
