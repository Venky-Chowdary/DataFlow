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


class ScriptedCursor(FakeCursor):
    """Answers the namespace query, then the catalog query."""

    def __init__(self, namespace, rows):
        super().__init__(rows)
        self.namespace = namespace
        self._pending = None

    def execute(self, sql, params=()):
        self.calls.append((sql, params))
        upper = sql.upper()
        self._pending = (
            [(self.namespace,)]
            if "DATABASE()" in upper
            or "CURRENT_SCHEMA" in upper
            or "SCHEMA_NAME()" in upper
            else list(self.rows)
        )

    def fetchall(self):
        return list(self._pending or [])


def test_blank_namespace_resolves_the_session_default_before_measuring():
    """An empty schema must never be certified as "no foreign keys".

    MySQL keeps the namespace in ``database``, so callers hand the probe an
    empty schema; querying TABLE_SCHEMA = '' answered "measured, none" and a
    carried key read back as not enforced on the destination.
    """
    cursor = ScriptedCursor(
        "shop",
        [("fk_o", "cust_id", "shop", "customers", "id", "NO ACTION", "NO ACTION")],
    )
    keys = probe_foreign_keys("mysql", cursor, "", "orders")
    assert keys.measured is True
    assert keys.schema == "shop"
    assert [k.referenced_table for k in keys.items] == ["customers"]
    assert any("DATABASE()" in sql.upper() for sql, _ in cursor.calls)


def test_unresolvable_namespace_is_unknown_not_absent():
    cursor = ScriptedCursor(None, [])
    keys = probe_foreign_keys("mysql", cursor, "", "orders")
    assert keys.measured is False
    assert "unknown, not empty" in keys.detail


def test_explicit_namespace_is_never_second_guessed():
    cursor = ScriptedCursor("other", [])
    probe_foreign_keys("postgresql", cursor, "public", "orders")
    assert not any("CURRENT_SCHEMA" in sql.upper() for sql, _ in cursor.calls)
