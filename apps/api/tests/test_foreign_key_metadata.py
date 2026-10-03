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
    informational_key_engine,
    inspector_row_proof_gaps,
    normalize_snowflake_table_kind,
    relationship_actions,
    relationship_match_type,
    row_proof_gap,
    row_proof_reason,
    foreign_keys_from_payload,
    normalize_action,
    probe_foreign_keys,
    uniqueness_proof_gap,
    uniqueness_proof_reason,
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


def test_postgres_match_full_is_recorded_on_the_foreign_key():
    """confmatchtype sits after the ordinal, so an older fixture stays unreported."""
    full = _cursor(
        [("fk", "a", "public", "parent", "id", "a", "a", True, 1, "f"),
         ("fk", "b", "public", "parent", "code", "a", "a", True, 2, "f")]
    )
    fk = probe_foreign_keys("postgresql", full, "public", "child").items[0]
    assert "confmatchtype" in full.calls[0][0]
    assert fk.match == "full"
    assert fk.validated is True
    simple = _cursor(
        [("fk", "a", "public", "parent", "id", "a", "a", True, 1, "s")]
    )
    assert probe_foreign_keys("postgresql", simple, "public", "child").items[0].match == "simple"
    partial = _cursor(
        [("fk", "a", "public", "parent", "id", "a", "a", False, 1, "p")]
    )
    assert probe_foreign_keys("postgresql", partial, "public", "child").items[0].match == "partial"
    legacy = _cursor(
        [("fk", "customer_id", "public", "customers", "id", "a", "a", False, 1)]
    )
    assert probe_foreign_keys("postgresql", legacy, "public", "orders").items[0].match == ""


def test_probe_match_wins_over_the_inspector_clause():
    """One reader: the metadata probe names the rule, then inspector options."""
    from services.foreign_key_identity import fk_identity

    inspector = [
        {
            "constrained_columns": ["parent_id"],
            "referred_schema": "public",
            "referred_table": "parent",
            "referred_columns": ["id"],
            "options": {"match": "SIMPLE"},
        }
    ]
    identity = fk_identity(inspector[0])
    probed = ForeignKeys(
        dialect="postgresql",
        status="measured",
        items=[
            ForeignKey(
                name="fk",
                columns=["parent_id"],
                referenced_schema="public",
                referenced_table="parent",
                referenced_columns=["id"],
                validated=True,
                match="full",
            )
        ],
    )
    assert relationship_match_type(identity, probed, inspector) == "full"
    assert relationship_match_type(identity, None, inspector) == "simple"
    conflicted = ForeignKeys(
        dialect="postgresql",
        status="measured",
        items=[
            ForeignKey(
                name="fk",
                columns=["parent_id"],
                referenced_schema="public",
                referenced_table="parent",
                referenced_columns=["id"],
                match="full",
            ),
            ForeignKey(
                name="fk",
                columns=["parent_id"],
                referenced_schema="public",
                referenced_table="parent",
                referenced_columns=["id"],
                match="simple",
            ),
        ],
    )
    assert relationship_match_type(identity, conflicted, inspector) == "unknown"
    assert relationship_match_type(None, probed, inspector) == ""


def test_probe_referential_action_wins_over_the_inspector_clause():
    """One reader: the metadata probe names the action, then inspector keys."""
    from services.foreign_key_identity import fk_identity

    inspector = [
        {
            "constrained_columns": ["parent_id"],
            "referred_schema": "public",
            "referred_table": "parent",
            "referred_columns": ["id"],
            "ondelete": "NO ACTION",
            "onupdate": "NO ACTION",
        }
    ]
    identity = fk_identity(inspector[0])
    probed = ForeignKeys(
        dialect="postgresql",
        status="measured",
        items=[
            ForeignKey(
                name="fk",
                columns=["parent_id"],
                referenced_schema="public",
                referenced_table="parent",
                referenced_columns=["id"],
                on_delete="CASCADE",
                on_update="RESTRICT",
            )
        ],
    )
    assert relationship_actions(identity, probed, inspector) == ("CASCADE", "RESTRICT")
    assert relationship_actions(identity, None, inspector) == ("NO ACTION", "NO ACTION")
    conflicted = ForeignKeys(
        dialect="postgresql",
        status="measured",
        items=[
            ForeignKey(
                name="fk",
                columns=["parent_id"],
                referenced_schema="public",
                referenced_table="parent",
                referenced_columns=["id"],
                on_delete="CASCADE",
                on_update="CASCADE",
            ),
            ForeignKey(
                name="fk",
                columns=["parent_id"],
                referenced_schema="public",
                referenced_table="parent",
                referenced_columns=["id"],
                on_delete="SET NULL",
                on_update="CASCADE",
            ),
        ],
    )
    assert relationship_actions(identity, conflicted, inspector) == ("unknown", "unknown")
    assert relationship_actions(None, probed, inspector) == ("", "")


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
    assert inspector_row_proof_gaps("postgresql", inspector, unvalidated) == ["not_checked"]
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
    assert inspector_row_proof_gaps("postgresql", inspector, checked) == [""]
    assert enforced_relationship_identities("postgresql", inspector, None) == []
    assert inspector_row_proof_gaps("postgresql", inspector, None) == ["unreported"]
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
    assert inspector_row_proof_gaps("redshift", inspector, None) == ["unenforced"]
    assert inspector_row_proof_gaps("sqlite", inspector, None) == [""]


def test_informational_warehouse_foreign_key_does_not_prove_existing_rows():
    """The planner can see the key. The engine does not check rows against it.

    A stray validated bit cannot override that. Hosted connector names are
    the same engine. These are catalog-shaped facts, not a live warehouse.
    """
    inspector = [
        {
            "constrained_columns": ["customer_id"],
            "referred_schema": "public",
            "referred_table": "customers",
            "referred_columns": ["id"],
        }
    ]
    dialects = (
        "snowflake",
        "snowflake_aws",
        "snowflake_azure",
        "snowflake_gcp",
        "bigquery",
        "google_bigquery",
        "bq",
        "bigquery_us",
        "databricks",
        "databricks_sql",
        "databricks_azure",
    )
    for dialect in dialects:
        assert row_proof_gap(dialect, True) == "unenforced", dialect
        assert row_proof_gap(dialect, None) == "unenforced", dialect
        assert covers_existing_rows(dialect, True) is False
        assert enforced_relationship_identities(dialect, inspector, None) == []
        assert inspector_row_proof_gaps(dialect, inspector, None) == ["unenforced"]
    assert covers_existing_rows("postgresql", True) is True
    assert covers_existing_rows("sqlite", None) is True


def test_informational_warehouse_unique_key_is_not_row_proof():
    """The same engines that store an unenforced foreign key store an unenforced key.

    A primary key and a unique constraint on these engines are planner
    metadata. Hive, Spark, and Flink are not in that set. These are dialect
    names, not a live warehouse.
    """
    unenforced = (
        "snowflake",
        "snowflake_aws",
        "snowflake_azure",
        "snowflake_gcp",
        "snowflake_standard",
        "snowflake_enterprise",
        "bigquery",
        "google_bigquery",
        "bq",
        "bigquery_us",
        "bigquery_eu",
        "databricks",
        "databricks_sql",
        "databricks_azure",
        "databricks_aws",
        "databricks_gcp",
        "unity_catalog",
        "redshift",
        "amazon_redshift",
        "redshift_serverless",
    )
    for dialect in unenforced:
        assert informational_key_engine(dialect) is True, dialect
        assert uniqueness_proof_gap(dialect) == "unenforced", dialect
        reason = uniqueness_proof_reason(dialect)
        assert "does not enforce" in reason, dialect
    assert "NOT ENFORCED" in uniqueness_proof_reason("bigquery")
    assert "BigQuery" in uniqueness_proof_reason("google_bigquery")
    snowflake = uniqueness_proof_reason("snowflake_aws")
    assert "Snowflake" in snowflake
    assert "standard table" in snowflake
    assert "hybrid" in snowflake
    assert "planner" in uniqueness_proof_reason("amazon_redshift")
    assert "informational" in uniqueness_proof_reason("databricks_sql")
    for dialect in (
        "postgresql",
        "sqlite",
        "mysql",
        "sqlserver",
        "oracle",
        "hive",
        "spark",
        "flink",
        "",
    ):
        assert informational_key_engine(dialect) is False, dialect
        assert uniqueness_proof_gap(dialect) == "", dialect
        assert uniqueness_proof_reason(dialect) == "", dialect


def test_measured_snowflake_hybrid_table_is_row_proof():
    """IS_HYBRID YES is the catalog fact. A dialect name and a stamped flag are not.

    These are catalog-shaped values from Snowflake's documented columns.
    A live Snowflake account was not used.
    """
    assert normalize_snowflake_table_kind("YES") == "hybrid"
    assert normalize_snowflake_table_kind("NO") == "standard"
    assert normalize_snowflake_table_kind(True) == "hybrid"
    assert normalize_snowflake_table_kind(False) == "standard"
    assert normalize_snowflake_table_kind("BASE TABLE") == ""
    assert normalize_snowflake_table_kind("snowflake") == ""
    assert normalize_snowflake_table_kind(None) == ""

    for dialect in ("snowflake", "snowflake_aws", "snowflake_azure", "snowflake_gcp"):
        assert uniqueness_proof_gap(dialect, table_kind="YES") == "", dialect
        assert uniqueness_proof_reason(dialect, table_kind="hybrid") == "", dialect
        assert uniqueness_proof_gap(dialect, table_kind="NO") == "unenforced", dialect
        assert "IS_HYBRID" in uniqueness_proof_reason(dialect, table_kind="NO")
        assert uniqueness_proof_gap(dialect) == "unenforced", dialect
        assert row_proof_gap(dialect, True, table_kind="YES") == "", dialect
        assert covers_existing_rows(dialect, True, table_kind="YES") is True
        assert row_proof_gap(dialect, None, table_kind="hybrid") == "unreported"
        assert row_proof_gap(dialect, False, table_kind="hybrid") == "not_checked"
        assert "ENFORCED" in row_proof_reason(
            "not_checked", dialect, table_kind="YES"
        )
        assert row_proof_gap(dialect, True) == "unenforced", dialect

    assert uniqueness_proof_gap("bigquery", table_kind="hybrid") == "unenforced"
    assert uniqueness_proof_gap("redshift", table_kind="YES") == "unenforced"
    assert uniqueness_proof_gap("databricks", table_kind="hybrid") == "unenforced"
    assert row_proof_gap("google_bigquery", True, table_kind="YES") == "unenforced"

    inspector = [
        {
            "constrained_columns": ["customer_id"],
            "referred_schema": "public",
            "referred_table": "customers",
            "referred_columns": ["id"],
        }
    ]
    enforced = ForeignKeys(
        dialect="snowflake",
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
    assert inspector_row_proof_gaps(
        "snowflake", inspector, enforced, table_kind="YES"
    ) == [""]
    assert len(
        enforced_relationship_identities(
            "snowflake_aws", inspector, enforced, table_kind="hybrid"
        )
    ) == 1
    assert inspector_row_proof_gaps(
        "snowflake", inspector, None, table_kind="YES"
    ) == ["unreported"]
    assert inspector_row_proof_gaps("snowflake", inspector, enforced) == ["unenforced"]
    denied = ForeignKeys(
        dialect="snowflake",
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
    assert inspector_row_proof_gaps(
        "snowflake", inspector, denied, table_kind="YES"
    ) == ["not_checked"]


def test_payload_keeps_an_explicit_validation_bit():
    keys = foreign_keys_from_payload(
        [{"name": "fk", "columns": ["a"], "referenced_table": "t", "referenced_columns": ["b"], "validated": False}]
    )
    assert keys[0].validated is False


def test_explicit_namespace_is_never_second_guessed():
    cursor = ScriptedCursor("other", [])
    probe_foreign_keys("postgresql", cursor, "public", "orders")
    assert not any("CURRENT_SCHEMA" in sql.upper() for sql, _ in cursor.calls)


def test_postgres_deferral_pair_is_recorded_after_the_match_type():
    """condeferrable sits after match, so a match-only fixture stays unreported."""
    from services.foreign_key_metadata import normalize_deferral

    deferred = _cursor(
        [("fk", "a", "public", "parent", "id", "a", "a", True, 1, "s", True, True)]
    )
    fk = probe_foreign_keys("postgresql", deferred, "public", "child").items[0]
    assert "condeferrable" in deferred.calls[0][0]
    assert "condeferred" in deferred.calls[0][0]
    assert fk.deferral == "deferred"
    assert fk.match == "simple"
    immediate = _cursor(
        [("fk", "a", "public", "parent", "id", "a", "a", True, 1, "f", True, False)]
    )
    assert (
        probe_foreign_keys("postgresql", immediate, "public", "child").items[0].deferral
        == "immediate"
    )
    plain = _cursor(
        [("fk", "a", "public", "parent", "id", "a", "a", True, 1, "s", False, False)]
    )
    assert (
        probe_foreign_keys("postgresql", plain, "public", "child").items[0].deferral
        == "not_deferrable"
    )
    legacy = _cursor(
        [("fk", "a", "public", "parent", "id", "a", "a", True, 1, "s")]
    )
    assert probe_foreign_keys("postgresql", legacy, "public", "child").items[0].deferral == ""
    assert normalize_deferral(False, True) == "unknown"


def test_oracle_deferral_columns_are_recorded_after_validation():
    deferred = _cursor(
        [
            (
                "FK_ORDERS",
                "CUSTOMER_ID",
                "APP",
                "CUSTOMERS",
                "ID",
                "NO ACTION",
                "NO ACTION",
                "VALIDATED",
                "DEFERRABLE",
                "DEFERRED",
            )
        ]
    )
    fk = probe_foreign_keys("oracle", deferred, "app", "orders").items[0]
    assert fk.validated is True
    assert fk.deferral == "deferred"
    assert "c.deferrable" in deferred.calls[0][0].lower()
    immediate = _cursor(
        [
            (
                "FK_ORDERS",
                "CUSTOMER_ID",
                "APP",
                "CUSTOMERS",
                "ID",
                "CASCADE",
                "NO ACTION",
                "VALIDATED",
                "DEFERRABLE",
                "IMMEDIATE",
            )
        ]
    )
    assert (
        probe_foreign_keys("oracle", immediate, "app", "orders").items[0].deferral
        == "immediate"
    )
    legacy = _cursor(
        [("FK_ORDERS", "CUSTOMER_ID", "APP", "CUSTOMERS", "ID", "CASCADE", "NO ACTION", "VALIDATED")]
    )
    assert probe_foreign_keys("oracle", legacy, "app", "orders").items[0].deferral == ""


def test_mysql_and_sqlserver_cannot_postpone_a_foreign_key():
    """Those engines have no DEFERRABLE clause. The mode is measured."""
    mysql = ScriptedCursor(
        "shop",
        [("fk_o", "cust_id", "shop", "customers", "id", "NO ACTION", "NO ACTION")],
    )
    assert probe_foreign_keys("mysql", mysql, "", "orders").items[0].deferral == "not_deferrable"
    sqlserver = _cursor(
        [("FK_orders", "customer_id", "dbo", "customers", "id", "NO ACTION", "NO ACTION", 0, 0)]
    )
    assert (
        probe_foreign_keys("sqlserver", sqlserver, "dbo", "orders").items[0].deferral
        == "not_deferrable"
    )


def test_sqlite_reads_deferrable_from_the_create_table():
    """PRAGMA foreign_key_list omits DEFERRABLE. The CREATE text names it."""
    import sqlite3

    from services.foreign_key_metadata import sqlite_clause_deferral

    connection = sqlite3.connect(":memory:")
    connection.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, code TEXT)")
    connection.execute(
        """
        CREATE TABLE child (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED,
            other_id INTEGER REFERENCES parent(id),
            note TEXT DEFAULT 'DEFERRABLE INITIALLY DEFERRED',
            pair_id INTEGER,
            FOREIGN KEY (pair_id) REFERENCES parent(id) DEFERRABLE INITIALLY IMMEDIATE
        )
        """
    )
    keys = probe_foreign_keys("sqlite", connection.cursor(), "", "child")
    by_col = {tuple(key.columns): key.deferral for key in keys.items}
    assert by_col[("parent_id",)] == "deferred"
    assert by_col[("other_id",)] == "not_deferrable"
    assert by_col[("pair_id",)] == "immediate"
    assert sqlite_clause_deferral("", "parent_id", "parent") == ""
    assert (
        sqlite_clause_deferral(
            "CREATE TABLE child (parent_id INT REFERENCES parent(id) NOT DEFERRABLE)",
            "parent_id",
            "parent",
        )
        == "not_deferrable"
    )
