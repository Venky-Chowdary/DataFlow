"""FOREIGN KEY catalog probe — one measured shape per dialect."""

from __future__ import annotations

import os
from unittest.mock import MagicMock

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from services.foreign_key_metadata import (
    ForeignKey,
    ForeignKeys,
    covers_existing_rows,
    enforced_relationship_identities,
    row_proof_gap,
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
            ("fk_line", "order_id", "public", "orders", "id", "a", "a", True, 1),
            ("fk_line", "line_no", "public", "orders", "line_no", "a", "a", True, 2),
        ]
    )
    measured = probe_foreign_keys("postgresql", cur, "public", "order_lines")
    assert measured.status == "measured"
    assert measured.items[0].columns == ["order_id", "line_no"]
    assert measured.items[0].referenced_columns == ["id", "line_no"]


def test_postgres_action_chars_are_spelled_out():
    cur = _cursor([("fk", "customer_id", "public", "customers", "id", "n", "c", True, 1)])
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
        ("FK_orders", "customer_id", "dbo", "customers", "id", "CASCADE", "NO_ACTION", 0, 0)
    ]
    measured = probe_foreign_keys("sqlserver", cur, "dbo", "orders")
    assert measured.status == "measured"
    assert measured.items[0].on_delete == "CASCADE"
    assert measured.items[0].on_update == "NO ACTION"
    assert len(calls) == 2


def test_oracle_probe_folds_identifiers_to_the_catalog_spelling():
    cur = _cursor(
        [("FK_ORDERS", "CUSTOMER_ID", "APP", "CUSTOMERS", "ID", "CASCADE", "NO ACTION", "VALIDATED")]
    )
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


def test_postgres_not_valid_is_recorded_on_the_foreign_key():
    cur = _cursor(
        [("fk", "customer_id", "public", "customers", "id", "a", "a", False, 1)]
    )
    fk = probe_foreign_keys("postgresql", cur, "public", "orders").items[0]
    assert "convalidated" in cur.calls[0][0]
    assert fk.validated is False
    assert covers_existing_rows("postgresql", fk.validated) is False


def test_sqlserver_untrusted_key_does_not_cover_existing_rows():
    cur = _cursor(
        [("FK_orders", "customer_id", "dbo", "customers", "id", "NO ACTION", "NO ACTION", 0, 1)]
    )
    fk = probe_foreign_keys("sqlserver", cur, "dbo", "orders").items[0]
    assert fk.validated is False
    trusted = _cursor(
        [("FK_orders", "customer_id", "dbo", "customers", "id", "NO ACTION", "NO ACTION", 0, 0)]
    )
    assert probe_foreign_keys("sqlserver", trusted, "dbo", "orders").items[0].validated is True


def test_oracle_not_validated_does_not_cover_existing_rows():
    cur = _cursor(
        [("FK_ORDERS", "CUSTOMER_ID", "APP", "CUSTOMERS", "ID", "CASCADE", "NO ACTION", "NOT VALIDATED")]
    )
    fk = probe_foreign_keys("oracle", cur, "app", "orders").items[0]
    assert fk.validated is False
    assert covers_existing_rows("oracle", False) is False
    assert covers_existing_rows("sqlite", None) is True


def test_not_valid_inspector_fk_is_not_an_enforced_identity():
    """SQLAlchemy omits NOT VALID. The catalog bit is what the scan trusts."""
    inspector = [
        {
            "constrained_columns": ["customer_id"],
            "referred_schema": "public",
            "referred_table": "customers",
            "referred_columns": ["id"],
        }
    ]
    unvalidated = ForeignKeys(
        dialect="postgresql",
        status="measured",
        items=[
            ForeignKey(
                name="orders_customer_fk",
                columns=["customer_id"],
                referenced_schema="public",
                referenced_table="customers",
                referenced_columns=["id"],
                validated=False,
            )
        ],
    )
    assert enforced_relationship_identities("postgresql", inspector, unvalidated) == []
    checked = ForeignKeys(
        dialect="postgresql",
        status="measured",
        items=[
            ForeignKey(
                name="orders_customer_fk",
                columns=["customer_id"],
                referenced_schema="public",
                referenced_table="customers",
                referenced_columns=["id"],
                validated=True,
            )
        ],
    )
    assert len(enforced_relationship_identities("postgresql", inspector, checked)) == 1
    assert enforced_relationship_identities("postgresql", inspector, None) == []
    assert len(enforced_relationship_identities("sqlite", inspector, None)) == 1


def test_redshift_foreign_key_does_not_prove_existing_rows():
    """The engine stores the constraint and does not check rows against it."""
    inspector = [
        {
            "constrained_columns": ["customer_id"],
            "referred_schema": "public",
            "referred_table": "customers",
            "referred_columns": ["id"],
        }
    ]
    assert row_proof_gap("redshift", None) == "unenforced"
    assert row_proof_gap("amazon_redshift", True) == "unenforced"
    assert row_proof_gap("redshift_serverless", False) == "unenforced"
    assert covers_existing_rows("redshift", True) is False
    assert enforced_relationship_identities("redshift", inspector, None) == []
    assert enforced_relationship_identities("amazon_redshift", inspector, None) == []
    assert covers_existing_rows("sqlite", None) is True
    assert len(enforced_relationship_identities("sqlite", inspector, None)) == 1


def test_payload_keeps_an_explicit_validation_bit():
    keys = foreign_keys_from_payload(
        [{"name": "fk", "columns": ["a"], "referenced_table": "t", "referenced_columns": ["b"], "validated": False}]
    )
    assert keys[0].validated is False


def test_explicit_namespace_is_never_second_guessed():
    cursor = ScriptedCursor("other", [])
    probe_foreign_keys("postgresql", cursor, "public", "orders")
    assert not any("CURRENT_SCHEMA" in sql.upper() for sql, _ in cursor.calls)
