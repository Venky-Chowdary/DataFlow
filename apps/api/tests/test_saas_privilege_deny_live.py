"""Env-gated live SaaS privilege deny matrices.

These tests intentionally skip unless operators set credentials for a
*read-only / intentionally under-privileged* principal. They never invent
denies against production admin roles.

Enable one engine at a time:

  DATAFLOW_SNOWFLAKE_PRIVILEGE_DENY=1
  DATAFLOW_SNOWFLAKE_ACCOUNT=...
  DATAFLOW_SNOWFLAKE_USER=...          # role with SELECT-only
  DATAFLOW_SNOWFLAKE_PASSWORD=...
  DATAFLOW_SNOWFLAKE_DATABASE=...
  DATAFLOW_SNOWFLAKE_SCHEMA=PUBLIC
  DATAFLOW_SNOWFLAKE_TABLE=...
  DATAFLOW_SNOWFLAKE_WAREHOUSE=...
  DATAFLOW_SNOWFLAKE_ROLE=...

  DATAFLOW_BIGQUERY_PRIVILEGE_DENY=1
  DATAFLOW_BQ_PROJECT=...
  DATAFLOW_BQ_DATASET=...
  DATAFLOW_BQ_TABLE=...
  DATAFLOW_BQ_SERVICE_ACCOUNT_JSON=...  # viewer-only SA JSON

  DATAFLOW_SQLSERVER_PRIVILEGE_DENY=1
  DATAFLOW_MSSQL_HOST=...
  DATAFLOW_MSSQL_DATABASE=...
  DATAFLOW_MSSQL_SCHEMA=dbo
  DATAFLOW_MSSQL_TABLE=...
  DATAFLOW_MSSQL_USER=...               # SELECT-only login
  DATAFLOW_MSSQL_PASSWORD=...

  DATAFLOW_ORACLE_PRIVILEGE_DENY=1
  DATAFLOW_ORACLE_HOST=...
  DATAFLOW_ORACLE_DATABASE=...
  DATAFLOW_ORACLE_SCHEMA=...
  DATAFLOW_ORACLE_TABLE=...
  DATAFLOW_ORACLE_USER=...
  DATAFLOW_ORACLE_PASSWORD=...
"""

from __future__ import annotations

import os

import pytest

from services.destination_privilege_probe import probe_destination_privileges


def _flag(name: str) -> bool:
    return (os.environ.get(name) or "").strip() in {"1", "true", "TRUE", "yes"}


@pytest.mark.skipif(not _flag("DATAFLOW_SNOWFLAKE_PRIVILEGE_DENY"), reason="Snowflake deny matrix not enabled")
def test_live_snowflake_select_only_is_denied():
    result = probe_destination_privileges(
        "snowflake",
        account=os.environ["DATAFLOW_SNOWFLAKE_ACCOUNT"],
        host=os.environ.get("DATAFLOW_SNOWFLAKE_ACCOUNT", ""),
        warehouse=os.environ.get("DATAFLOW_SNOWFLAKE_WAREHOUSE", ""),
        database=os.environ["DATAFLOW_SNOWFLAKE_DATABASE"],
        schema=os.environ.get("DATAFLOW_SNOWFLAKE_SCHEMA", "PUBLIC"),
        table=os.environ["DATAFLOW_SNOWFLAKE_TABLE"],
        username=os.environ["DATAFLOW_SNOWFLAKE_USER"],
        password=os.environ["DATAFLOW_SNOWFLAKE_PASSWORD"],
        role=os.environ.get("DATAFLOW_SNOWFLAKE_ROLE", ""),
        table_exists=True,
    )
    assert result.status == "denied"
    assert result.can_write is False


@pytest.mark.skipif(not _flag("DATAFLOW_BIGQUERY_PRIVILEGE_DENY"), reason="BigQuery deny matrix not enabled")
def test_live_bigquery_viewer_is_denied():
    result = probe_destination_privileges(
        "bigquery",
        project_id=os.environ["DATAFLOW_BQ_PROJECT"],
        database=os.environ["DATAFLOW_BQ_PROJECT"],
        dataset=os.environ["DATAFLOW_BQ_DATASET"],
        schema=os.environ["DATAFLOW_BQ_DATASET"],
        table=os.environ["DATAFLOW_BQ_TABLE"],
        service_account=os.environ["DATAFLOW_BQ_SERVICE_ACCOUNT_JSON"],
        table_exists=True,
    )
    assert result.status == "denied"
    assert result.can_write is False


@pytest.mark.skipif(not _flag("DATAFLOW_SQLSERVER_PRIVILEGE_DENY"), reason="SQL Server deny matrix not enabled")
def test_live_sqlserver_select_only_is_denied():
    result = probe_destination_privileges(
        "sqlserver",
        host=os.environ["DATAFLOW_MSSQL_HOST"],
        database=os.environ["DATAFLOW_MSSQL_DATABASE"],
        schema=os.environ.get("DATAFLOW_MSSQL_SCHEMA", "dbo"),
        table=os.environ["DATAFLOW_MSSQL_TABLE"],
        username=os.environ["DATAFLOW_MSSQL_USER"],
        password=os.environ["DATAFLOW_MSSQL_PASSWORD"],
        table_exists=True,
    )
    assert result.status == "denied"
    assert result.can_write is False


@pytest.mark.skipif(not _flag("DATAFLOW_ORACLE_PRIVILEGE_DENY"), reason="Oracle deny matrix not enabled")
def test_live_oracle_select_only_is_denied():
    result = probe_destination_privileges(
        "oracle",
        host=os.environ["DATAFLOW_ORACLE_HOST"],
        database=os.environ.get("DATAFLOW_ORACLE_DATABASE", ""),
        schema=os.environ["DATAFLOW_ORACLE_SCHEMA"],
        table=os.environ["DATAFLOW_ORACLE_TABLE"],
        username=os.environ["DATAFLOW_ORACLE_USER"],
        password=os.environ["DATAFLOW_ORACLE_PASSWORD"],
        table_exists=True,
    )
    assert result.status == "denied"
    assert result.can_write is False
