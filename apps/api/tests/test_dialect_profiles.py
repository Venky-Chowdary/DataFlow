"""Dialect profiles — nowhere→anywhere physical naming must not leak Postgres defaults."""

from __future__ import annotations

from services.dialect_profiles import (
    default_schema_for,
    fold_identifier,
    is_oracle_like,
    is_sqlserver_like,
    normalize_schema,
    quote_char_for,
    schema_from_cfg,
    warehouse_sql_quote_dialect,
)
from connectors.sql_identifiers import quote_table_ref


def test_default_schemas_by_dialect():
    assert default_schema_for("postgresql") == "public"
    assert default_schema_for("snowflake") == "PUBLIC"
    assert default_schema_for("sqlserver") == "dbo"
    assert default_schema_for("mssql+pyodbc") == "dbo"
    assert default_schema_for("bigquery") == "dataflow"
    assert default_schema_for("mysql") is None
    assert default_schema_for("oracle") is None


def test_normalize_schema_never_leaks_postgres_public_to_snowflake():
    assert normalize_schema("snowflake", None) == "PUBLIC"
    assert normalize_schema("snowflake", "public") == "PUBLIC"
    # Mixed-case intentional identifier preserved (quoted-identifier semantics)
    assert normalize_schema("snowflake", "MySchema") == "MySchema"


def test_normalize_schema_sqlserver_and_mysql():
    assert normalize_schema("sqlserver", None) == "dbo"
    assert normalize_schema("mysql", "anything") is None
    assert normalize_schema("mysql", None) is None


def test_catalog_namespace_does_not_look_up_mysql_under_public():
    """information_schema on MySQL is keyed by database, never Postgres public."""
    from services.dialect_profiles import catalog_namespace

    cfg = {"database": "appdb", "schema": "public"}
    assert catalog_namespace("mysql", cfg) == "appdb"
    assert catalog_namespace("mariadb", cfg, schema="public") == "appdb"
    assert catalog_namespace("mysql", {"database": "appdb"}, schema="") == "appdb"
    assert catalog_namespace("postgresql", {"schema": "sales"}) == "sales"
    assert catalog_namespace("postgresql", {}) == "public"


def test_oracle_falls_back_to_username():
    assert normalize_schema("oracle", None, username="APP_USER") == "APP_USER"
    assert normalize_schema("oracle", "hr") == "HR"


def test_fold_identifier():
    assert fold_identifier("postgresql", "PUBLIC") == "public"
    assert fold_identifier("snowflake", "public") == "PUBLIC"
    assert fold_identifier("sqlserver", "dbo") == "dbo"


def test_quote_table_ref_per_dialect():
    assert quote_table_ref("jobs", "public", dialect="postgresql") == '"public"."jobs"'
    assert quote_table_ref("jobs", "public", dialect="snowflake") == '"PUBLIC"."JOBS"'
    assert quote_table_ref("jobs", "dbo", dialect="sqlserver") == "[dbo].[jobs]"
    assert quote_table_ref("jobs", "dbo", dialect="azure_sql_database") == "[dbo].[jobs]"
    assert quote_table_ref("jobs", "hr", dialect="oracle") == '"HR"."JOBS"'
    assert quote_table_ref("jobs", "hr", dialect="amazon_rds_oracle") == '"HR"."JOBS"'
    assert quote_table_ref("jobs", None, dialect="mysql") == "`jobs`"
    assert quote_table_ref("jobs", "analytics", dialect="bigquery", project="p1") == "`p1.analytics.jobs`"


def test_warehouse_sql_quote_dialect_aliases_onto_exact_engines():
    """Catalog SKUs must quote as sqlserver/oracle so leftover MERGE can list keys."""
    for sku in (
        "sqlserver",
        "mssql",
        "azure_sql_database",
        "amazon_rds_sql_server",
        "synapse_analytics",
        "azure_sql",
    ):
        assert warehouse_sql_quote_dialect(sku) == "sqlserver", sku
        assert is_sqlserver_like(sku)
        assert quote_char_for(sku) == "["
        assert default_schema_for(sku) == "dbo"
    for sku in (
        "oracle",
        "oracle_db",
        "amazon_rds_oracle",
        "oracle_autonomous_warehouse",
        "autonomous_database",
    ):
        assert warehouse_sql_quote_dialect(sku) == "oracle", sku
        assert is_oracle_like(sku)
        assert quote_char_for(sku) == '"'
    assert warehouse_sql_quote_dialect("snowflake") == "snowflake"
    assert warehouse_sql_quote_dialect("motherduck") == "duckdb"
    assert warehouse_sql_quote_dialect("postgresql") is None
    assert warehouse_sql_quote_dialect("bigquery") == "bigquery"
    assert warehouse_sql_quote_dialect("duckdb") == "duckdb"
    assert warehouse_sql_quote_dialect("databricks") == "databricks"
    assert warehouse_sql_quote_dialect("databricks_sql") == "databricks"
    assert warehouse_sql_quote_dialect("redshift") == "redshift"
    assert warehouse_sql_quote_dialect("amazon_redshift") == "redshift"
    assert warehouse_sql_quote_dialect("redshift_serverless") == "redshift"
    assert warehouse_sql_quote_dialect("clickhouse") is None
    assert normalize_schema("azure_sql_database", "public") == "dbo"
    assert normalize_schema("amazon_rds_oracle", None, username="app") == "APP"


def test_empty_schema_matrix_all_major_dialects():
    """Regression: empty schema must never become Postgres public on non-PG engines."""
    cases = [
        ("postgresql", "public"),
        ("redshift", "public"),
        ("snowflake", "PUBLIC"),
        ("sqlserver", "dbo"),
        ("mssql+pyodbc", "dbo"),
        ("bigquery", "dataflow"),
        ("databricks", "default"),
        ("duckdb", "main"),
        ("mysql", ""),
        ("mariadb", ""),
        ("sqlite", ""),
    ]
    for driver, expected in cases:
        assert schema_from_cfg(driver, {"schema": ""}) == expected, driver
        assert schema_from_cfg(driver, {}) == expected, driver


def test_postgres_public_literal_folded_per_dialect():
    """Operator typed 'public' — remap off PG-family engines to dialect defaults."""
    assert schema_from_cfg("snowflake", {"schema": "public"}) == "PUBLIC"
    assert schema_from_cfg("sqlserver", {"schema": "public"}) == "dbo"
    assert schema_from_cfg("bigquery", {"schema": "public"}) == "dataflow"
    assert schema_from_cfg("oracle", {"schema": "public", "username": "APP"}) == "APP"
    assert schema_from_cfg("duckdb", {"schema": "public"}) == "main"
    assert schema_from_cfg("postgresql", {"schema": "PUBLIC"}) == "public"
    assert schema_from_cfg("mysql", {"schema": "public"}) == ""  # schema N/A


def test_sql_bool_predicates_numeric_vs_ansi():
    from services.dialect_profiles import (
        sql_bool_false_literal,
        sql_bool_is_not_true,
        sql_bool_is_true,
        sql_bool_true_literal,
        stores_boolean_as_numeric,
    )

    for dialect in ("mssql", "sqlserver", "azure_sql_database", "oracle", "sqlite"):
        assert stores_boolean_as_numeric(dialect), dialect
        assert sql_bool_is_true(dialect, "c") == "c = 1"
        assert "IS TRUE" not in sql_bool_is_not_true(dialect, "c").upper()
        assert sql_bool_true_literal(dialect) == "1"
        assert sql_bool_false_literal(dialect) == "0"
    for dialect in ("postgresql", "mysql", "snowflake"):
        assert not stores_boolean_as_numeric(dialect), dialect
        assert sql_bool_is_true(dialect, "c") == "c IS TRUE"
        assert sql_bool_is_not_true(dialect, "c") == "c IS NOT TRUE"
        assert sql_bool_true_literal(dialect) == "TRUE"
        assert sql_bool_false_literal(dialect) == "FALSE"


def test_quote_matrix_no_postgres_leak():
    assert '"PUBLIC"."T"' == quote_table_ref("t", schema_from_cfg("snowflake", {"schema": "public"}), dialect="snowflake")
    assert "[dbo].[t]" == quote_table_ref("t", schema_from_cfg("sqlserver", {"schema": "public"}), dialect="sqlserver")
    assert "`t`" == quote_table_ref("t", None, dialect="mysql")
    assert '"public"."t"' == quote_table_ref("t", "public", dialect="postgresql")
    assert "`default`.`jobs`" == quote_table_ref("jobs", "default", dialect="databricks")
    assert "`p1.analytics.jobs`" == quote_table_ref(
        "jobs", "analytics", dialect="bigquery", project="p1"
    )
