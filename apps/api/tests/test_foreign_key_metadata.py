"""FOREIGN KEY catalog probe — one measured shape per dialect."""

from __future__ import annotations

import os
from unittest.mock import MagicMock

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from services.foreign_key_metadata import (
    foreign_keys_from_payload,
    normalize_action,
    probe_foreign_keys,
)


class FakeCursor:
    """A DB-API cursor. Not a MagicMock: that auto-creates ``exec_driver_sql``
    and would be mistaken for a SQLAlchemy connection."""

    def __init__(self, rows=(), error: Exception | None = None):
        self.rows = list(rows)
        self.error = error
        self.calls: list[tuple[str, object]] = []

    def execute(self, sql, params=()):
        self.calls.append((sql, params))
        if self.error is not None:
            raise self.error

    def fetchall(self):
        return list(self.rows)


def _cursor(rows):
    return FakeCursor(rows)


def test_postgres_composite_key_keeps_column_pairs_in_order():
    cur = _cursor(
        [
            ("fk_line", "order_id", "public", "orders", "id", "a", "a", 1),
            ("fk_line", "line_no", "public", "orders", "line_no", "a", "a", 2),
        ]
    )
    measured = probe_foreign_keys("postgresql", cur, "public", "order_lines")
    assert measured.status == "measured"
    assert measured.items[0].columns == ["order_id", "line_no"]
    assert measured.items[0].referenced_columns == ["id", "line_no"]


def test_postgres_action_chars_are_spelled_out():
    cur = _cursor([("fk", "customer_id", "public", "customers", "id", "n", "c", 1)])
    fk = probe_foreign_keys("postgresql", cur, "public", "orders").items[0]
    assert fk.on_delete == "SET NULL"
    assert fk.on_update == "CASCADE"


def test_sqlserver_probe_survives_either_driver_paramstyle():
    cur = MagicMock()
    calls: list[str] = []

    def execute(sql, _params):
        calls.append(sql)
        if "%s" in sql:
            raise RuntimeError("pyodbc: invalid parameter marker")

    cur.execute.side_effect = execute
    cur.fetchall.return_value = [
        ("FK_orders", "customer_id", "dbo", "customers", "id", "CASCADE", "NO_ACTION")
    ]
    measured = probe_foreign_keys("sqlserver", cur, "dbo", "orders")
    assert measured.status == "measured"
    assert measured.items[0].on_delete == "CASCADE"
    assert measured.items[0].on_update == "NO ACTION"
    assert len(calls) == 2


def test_oracle_probe_folds_identifiers_to_the_catalog_spelling():
    cur = _cursor([("FK_ORDERS", "CUSTOMER_ID", "APP", "CUSTOMERS", "ID", "CASCADE", "NO ACTION")])
    measured = probe_foreign_keys("oracle", cur, "app", "orders")
    assert measured.status == "measured"
    assert cur.calls[0][1] == {"owner": "APP", "tab": "ORDERS"}


def test_unreadable_catalog_is_unavailable_not_an_empty_key_list():
    cur = MagicMock()
    cur.execute.side_effect = RuntimeError("permission denied for schema")
    measured = probe_foreign_keys("postgresql", cur, "public", "orders")
    assert measured.status == "unavailable"
    assert measured.items == []
    assert "permission denied" in measured.detail


def test_dialect_without_a_probe_says_so():
    measured = probe_foreign_keys("mongodb", MagicMock(), "", "orders")
    assert measured.status == "unavailable"
    assert "not implemented" in measured.detail


def test_payload_roundtrip_tolerates_catalogs_without_actions():
    keys = foreign_keys_from_payload(
        {
            "foreign_keys": [
                {
                    "name": "fk",
                    "columns": ["a"],
                    "referenced_table": "t",
                    "referenced_columns": ["b"],
                }
            ]
        }
    )
    assert keys[0].on_delete == ""
    assert keys[0].referenced_table == "t"


def test_action_normalization_is_case_and_underscore_insensitive():
    assert normalize_action("no_action") == "NO ACTION"
    assert normalize_action(None) == ""
