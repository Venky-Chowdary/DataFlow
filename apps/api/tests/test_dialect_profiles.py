"""Dialect profiles — nowhere→anywhere physical naming must not leak Postgres defaults."""

from __future__ import annotations

from services.dialect_profiles import (
    default_schema_for,
    fold_identifier,
    normalize_schema,
    quote_char_for,
    schema_from_cfg,
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
    assert quote_table_ref("jobs", None, dialect="mysql") == "`jobs`"
    assert quote_table_ref("jobs", "analytics", dialect="bigquery", project="p1") == "`p1.analytics.jobs`"


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


def test_quote_matrix_no_postgres_leak():
    assert '"PUBLIC"."T"' == quote_table_ref("t", schema_from_cfg("snowflake", {"schema": "public"}), dialect="snowflake")
    assert "[dbo].[t]" == quote_table_ref("t", schema_from_cfg("sqlserver", {"schema": "public"}), dialect="sqlserver")
    assert "`t`" == quote_table_ref("t", None, dialect="mysql")
    assert '"public"."t"' == quote_table_ref("t", "public", dialect="postgresql")
