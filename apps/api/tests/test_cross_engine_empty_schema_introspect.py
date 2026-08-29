"""Cross-engine: wrong schema/DB must still load columns — never empty→create-new.

Covers PostgreSQL, MySQL, Snowflake, SQL Server, Oracle, BigQuery recovery paths.
Shared failure modes that made Map invent identity CREATE when the destination
object already existed (enterprise proof bar — not MySQL→Postgres only).
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from services.mapping_pipeline import run_mapping_pipeline
from services.schema_introspect import (
    _introspect_bigquery,
    _introspect_mysql,
    _introspect_oracle,
    _introspect_postgresql,
    _introspect_snowflake,
    _introspect_sqlserver,
)


def test_pipeline_existing_empty_targets_never_create_new_across_dest_families():
    for dest in ("postgresql", "mysql", "snowflake", "sqlserver", "oracle", "bigquery"):
        result = run_mapping_pipeline(
            ["id", "title"],
            [],
            destination_db_type=dest,
            destination_table_exists=True,
            use_llm=False,
        )
        mappings = result["mappings"]
        assert mappings, dest
        assert all(m.get("create_new") is False for m in mappings), dest
        assert all(m.get("assignment_strategy") == "pending_dest_schema" for m in mappings), dest
        assert all("New destination table" not in (m.get("reasoning") or "") for m in mappings), dest


def test_pg_cross_schema_recovery():
    # _PG_COLUMN_SQL returns 10-tuples: name, dtype, nullable, identity,
    # default, collation, coll_det, generated, type_oid, typ_type.
    pg_cols = [
        ("id", "text", "YES", "", None, "", True, "", 25, "b"),
        ("title", "text", "YES", "", None, "", True, "", 25, "b"),
    ]
    cur = MagicMock()
    cur.fetchall.side_effect = [
        [],  # tables in wrong schema
        [],  # columns in wrong schema
        [("public", "jobs")],  # cross-schema recovery
        pg_cols,  # columns in public
        [],  # unique keys
        [],  # foreign keys
    ]
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = False
    with patch("connectors.postgresql_conn.get_connection", return_value=conn), patch(
        "services.schema_introspect._refine_columns_by_samples",
        side_effect=lambda _c, cols, *_a, **_k: cols,
    ), patch(
        "services.schema_introspect._pg_fetch_unique_keys",
        return_value={"primary_key_columns": [], "unique_keys": []},
    ), patch(
        "services.schema_introspect._fetch_foreign_keys",
        return_value=([], {"status": "measured", "items": []}),
    ):
        result = _introspect_postgresql(
            host="h", port=5432, database="db", username="u", password="p",
            schema="wrong", connection_string="", ssl=True, table="jobs",
        )
    assert result["ok"] is True, result.get("error")
    assert result["schema"] == "public"
    assert [c["name"] for c in result["columns"]] == ["id", "title"]


def test_mysql_cross_database_recovery():
    cur = MagicMock()
    cur.fetchall.side_effect = [
        [],
        [],
        [],
        [("app_prod", "jobs")],
        [("id", "varchar(36)", "YES"), ("title", "text", "YES")],
    ]
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = False
    with patch("connectors.mysql_conn.get_connection", return_value=conn), patch(
        "services.schema_introspect._refine_columns_by_samples",
        side_effect=lambda _c, cols, *_a, **_k: cols,
    ):
        result = _introspect_mysql(
            host="h", port=3306, database="wrong_db", username="u", password="p",
            schema="", connection_string="", ssl=True, table="jobs",
        )
    assert result["ok"] is True
    assert result["schema"] == "app_prod"
    assert [c["name"] for c in result["columns"]] == ["id", "title"]


def test_snowflake_cross_schema_recovery():
    # Answer by SQL shape, not by call order: the column read walks a
    # projection ladder, so a positional script silently tests the wrong query.
    def answer(sql, *_args):
        upper = str(sql).upper()
        params = _args[0] if _args else ()
        if "FROM INFORMATION_SCHEMA.COLUMNS" in upper:
            schema_arg = str(params[0]).upper() if params else ""
            cur._rows = (
                [("ID", "NUMBER", "YES"), ("TITLE", "TEXT", "YES")]
                if schema_arg == "PUBLIC"
                else []
            )
        elif "FROM INFORMATION_SCHEMA.TABLES" in upper:
            # The requested schema holds nothing; the table lives in PUBLIC.
            cur._rows = [("PUBLIC", "JOBS")] if "TABLE_NAME) = UPPER" in upper else []
        else:
            cur._rows = []

    cur = MagicMock()
    cur._rows = []
    cur.execute.side_effect = answer
    cur.fetchall.side_effect = lambda: list(cur._rows)
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = False
    with patch("connectors.snowflake_conn.get_connection", return_value=conn), patch(
        "connectors.snowflake_conn.normalize_account", return_value="acct",
    ), patch(
        "services.schema_introspect._snowflake_resolve_schema",
        return_value=("WRONG", [], None),
    ), patch(
        "connectors.snowflake_conn.resolve_or_fold_snowflake_table",
        side_effect=lambda _c, _s, t: t,
    ):
        result = _introspect_snowflake(
            host="acct", database="DB", username="u", password="p",
            schema="WRONG", warehouse="WH", connection_string="", table="JOBS",
        )
    assert result["ok"] is True
    assert str(result["schema"]).upper() == "PUBLIC"
    assert [c["name"] for c in result["columns"]] == ["ID", "TITLE"]


def test_sqlserver_cross_schema_recovery():
    pytest.importorskip("sqlalchemy")

    class FakeResult:
        def __init__(self, rows):
            self._rows = rows

        def fetchall(self):
            return self._rows

    def execute(stmt, params=None):
        sql_u = str(stmt).upper()
        if "INFORMATION_SCHEMA.TABLES" in sql_u and "LOWER(TABLE_NAME)" in sql_u:
            return FakeResult([("dbo", "jobs")])
        if "INFORMATION_SCHEMA.TABLES" in sql_u:
            return FakeResult([])
        if "INFORMATION_SCHEMA.COLUMNS" in sql_u:
            if params and params.get("schema") == "dbo":
                # 8-tuple matches live SELECT (precision/scale/len/dt_prec/collation/null).
                return FakeResult([
                    ("id", "varchar", None, None, 36, None, None, "YES"),
                    ("title", "nvarchar", None, None, 200, None, None, "YES"),
                ])
            return FakeResult([])
        if "SYS.COMPUTED_COLUMNS" in sql_u:
            return FakeResult([])
        return FakeResult([])

    conn = MagicMock()
    conn.execute.side_effect = execute
    conn_cm = MagicMock()
    conn_cm.__enter__.return_value = conn
    conn_cm.__exit__.return_value = False
    engine = MagicMock()
    engine.connect.return_value = conn_cm

    with patch("connectors.generic_sql._engine", return_value=engine):
        result = _introspect_sqlserver(
            host="h", port=1433, database="db", username="u", password="p",
            schema="wrong", connection_string="", table="jobs",
        )
    assert result["ok"] is True
    assert result["schema"] == "dbo"
    assert [c["name"] for c in result["columns"]] == ["id", "title"]


def test_oracle_cross_owner_recovery():
    pytest.importorskip("sqlalchemy")

    class FakeResult:
        def __init__(self, rows):
            self._rows = rows

        def fetchall(self):
            return self._rows

    def execute(stmt, params=None):
        sql = str(stmt).upper().replace("\n", " ")
        if "FROM ALL_TABLES" in sql and "UPPER(TABLE_NAME)" in sql:
            return FakeResult([("APP", "JOBS")])
        if "FROM ALL_TABLES" in sql:
            return FakeResult([])
        if "FROM ALL_TAB_COL" in sql:  # ALL_TAB_COLS / ALL_TAB_COLUMNS
            if params and params.get("owner") == "APP":
                return FakeResult([
                    ("ID", "VARCHAR2", None, None, "Y"),
                    ("TITLE", "VARCHAR2", None, None, "Y"),
                ])
            return FakeResult([])
        return FakeResult([])

    conn = MagicMock()
    conn.execute.side_effect = execute
    conn_cm = MagicMock()
    conn_cm.__enter__.return_value = conn
    conn_cm.__exit__.return_value = False
    engine = MagicMock()
    engine.connect.return_value = conn_cm

    with patch("connectors.generic_sql._engine", return_value=engine):
        result = _introspect_oracle(
            host="h", port=1521, database="ORCL", username="wrong_user", password="p",
            schema="WRONG", connection_string="", table="jobs",
        )
    assert result["ok"] is True
    assert result["schema"] == "APP"
    assert [c["name"] for c in result["columns"]] == ["ID", "TITLE"]


def test_bigquery_cross_dataset_recovery():
    field = SimpleNamespace(name="id", field_type="STRING", mode="NULLABLE")
    field2 = SimpleNamespace(name="title", field_type="STRING", mode="NULLABLE")
    good_table = SimpleNamespace(schema=[field, field2])

    client = MagicMock()
    client.list_tables.return_value = []

    def get_table(ref: str):
        if ref.endswith(".analytics.jobs"):
            return good_table
        raise Exception("not found")

    client.get_table.side_effect = get_table
    client.list_datasets.return_value = [SimpleNamespace(dataset_id="analytics")]

    with patch("connectors.bigquery_conn.get_client", return_value=client):
        result = _introspect_bigquery(
            database="myproj", schema="wrong_ds", connection_string="", table="jobs",
        )
    assert result["ok"] is True
    assert result["schema"] == "analytics"
    assert [c["name"] for c in result["columns"]] == ["id", "title"]


def test_oracle_single_table_probe_skips_owner_catalog_list():
    """Dest-exists must not SELECT every owner table — that hung Oracle XE."""
    executed: list[str] = []

    class FakeResult:
        def __init__(self, rows):
            self._rows = rows

        def fetchall(self):
            return self._rows

    def execute(stmt, params=None):
        sql = str(stmt).upper().replace("\n", " ")
        executed.append(sql)
        if "FROM ALL_TAB_COL" in sql:
            return FakeResult([
                ("ID", "NUMBER", 10, 0, "N", None, None, "NO", "NO", None, None),
            ])
        if "FROM ALL_TABLES" in sql:
            raise AssertionError(f"owner catalog listed on single-table probe: {sql}")
        return FakeResult([])

    conn = MagicMock()
    conn.execute.side_effect = execute
    conn_cm = MagicMock()
    conn_cm.__enter__.return_value = conn
    conn_cm.__exit__.return_value = False
    engine = MagicMock()
    engine.connect.return_value = conn_cm

    with patch("connectors.generic_sql._engine", return_value=engine), patch(
        "services.sql_object_identity.resolve_object_identity",
        return_value=SimpleNamespace(exists=True, table="JOBS", schema="APP"),
    ):
        result = _introspect_oracle(
            host="h", port=1521, database="ORCL", username="app", password="p",
            schema="APP", connection_string="", table="JOBS",
            strict_namespace=True,
        )
    assert result["ok"] is True
    assert [c["name"] for c in result["columns"]] == ["ID"]
    assert not any(
        "SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER" in sql
        or "SELECT TABLE_NAME FROM USER_TABLES" in sql
        for sql in executed
    )


def test_sqlserver_single_table_probe_skips_schema_catalog_list():
    executed: list[str] = []

    class FakeResult:
        def __init__(self, rows):
            self._rows = rows

        def fetchall(self):
            return self._rows

    def execute(stmt, params=None):
        sql = str(stmt).upper().replace("\n", " ")
        executed.append(sql)
        if "INFORMATION_SCHEMA.COLUMNS" in sql:
            return FakeResult([
                ("id", "int", 10, 0, None, None, None, "NO", None),
            ])
        if "INFORMATION_SCHEMA.TABLES" in sql:
            raise AssertionError("schema catalog listed on single-table probe")
        return FakeResult([])

    conn = MagicMock()
    conn.execute.side_effect = execute
    conn_cm = MagicMock()
    conn_cm.__enter__.return_value = conn
    conn_cm.__exit__.return_value = False
    engine = MagicMock()
    engine.connect.return_value = conn_cm

    with patch("connectors.generic_sql._engine", return_value=engine):
        result = _introspect_sqlserver(
            host="h", port=1433, database="dataflow", username="sa", password="p",
            schema="dbo", connection_string="", table="jobs",
            strict_namespace=True,
        )
    assert result["ok"] is True
    assert [c["name"] for c in result["columns"]] == ["id"]
    assert not any("INFORMATION_SCHEMA.TABLES" in sql for sql in executed)
