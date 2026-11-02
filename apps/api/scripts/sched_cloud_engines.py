"""Emulated cloud warehouses for ``live_schedule_matrix`` — native driver access.

These are *emulator-measured* cells, never hosted-cloud proof:

* ``bigquery``  → goccy/bigquery-emulator (``docker compose up bigquery-emulator``,
  REST on :9050). The product connector already speaks to it anonymously when the
  host is local.
* ``snowflake`` → fakesnow (Snowflake SQL on DuckDB, in-process). The product
  connector patches ``snowflake.connector.connect`` when the account is ``local``;
  the harness verifies through the same emulator with its own connection.
* ``redshift``  → PostgreSQL 16 on :5439 speaking the Redshift wire protocol the
  product's ``redshift`` type uses (psycopg2). Redshift-only DDL (DISTKEY,
  SORTKEY, COPY FROM S3) is not exercised by this emulator.

Each adapter exposes a DB-API-shaped ``connect(cfg)`` returning an object with
``cursor().execute/executemany/fetchall`` and ``commit/close``, so the harness's
``Engine`` treats them like any other SQL engine.
"""

from __future__ import annotations

import os
from typing import Any

CLOUD_ENGINES: dict[str, dict[str, Any]] = {
    "bigquery": dict(type="bigquery", host="127.0.0.1", port=9050, database="dataflow-test",
                     schema="dataflow", username="", password="", ssl=False),
    "snowflake": dict(type="snowflake", host="local", port=443, database="SCHED_PROOF",
                      schema="PUBLIC", warehouse="COMPUTE_WH", username="proof",
                      password="proof", ssl=False),
    "redshift": dict(type="redshift", host="localhost", port=5439, database="dataflow",
                     username="dataflow", password="dataflow", ssl=False),
}

CLOUD_TYPES: dict[str, dict[str, str]] = {
    "bigquery": {"id": "INT64", "name": "STRING", "amount": "NUMERIC", "updated_seq": "INT64"},
    "snowflake": {"id": "NUMBER(19,0)", "name": "VARCHAR(64)", "amount": "NUMBER(12,3)",
                  "updated_seq": "NUMBER(19,0)"},
    "redshift": {"id": "BIGINT", "name": "VARCHAR(64)", "amount": "DECIMAL(12,3)",
                 "updated_seq": "BIGINT"},
}

#: Paramstyle placeholder the emulator's driver accepts.
PLACEHOLDER = {"bigquery": "?", "snowflake": "%s", "redshift": "%s"}


def quote(engine: str, ident: str) -> str:
    if engine == "bigquery":
        return f"`{ident}`"
    if engine == "snowflake":
        # Snowflake folds unquoted identifiers to upper case; the product writer
        # creates tables unquoted, so the harness must resolve the same way.
        return ident.upper()
    return f'"{ident}"'


def reachable(engine: str, cfg: dict[str, Any]) -> bool:
    if engine == "snowflake":
        try:
            import fakesnow  # noqa: F401
        except ImportError:
            return False
        return True
    import socket

    try:
        with socket.create_connection((cfg["host"], cfg["port"]), timeout=1):
            return True
    except OSError:
        return False


# --------------------------------------------------------------------------- bigquery


class _BQCursor:
    def __init__(self, client: Any, dataset: str):
        self._client = client
        self._dataset = dataset
        self._rows: list[tuple] = []

    def _qualify(self, sql: str) -> str:
        return sql

    def execute(self, sql: str, params: tuple | None = None) -> None:
        from google.cloud import bigquery

        job_config = None
        if params:
            job_config = bigquery.QueryJobConfig(
                query_parameters=[_bq_param(v) for v in params]
            )
        job = self._client.query(sql, job_config=job_config)
        result = job.result()
        try:
            self._rows = [tuple(r.values()) for r in result]
        except Exception:  # noqa: BLE001 - DML has no rows
            self._rows = []

    def executemany(self, sql: str, seq: list[tuple]) -> None:
        if not sql.lstrip().upper().startswith("INSERT"):
            for row in seq:
                self.execute(sql, row)
            return
        # One multi-row INSERT per chunk: the emulator accepts standard SQL DML.
        head, _, _ = sql.partition("VALUES")
        width = len(seq[0]) if seq else 0
        for i in range(0, len(seq), 500):
            chunk = seq[i:i + 500]
            values = ", ".join("(" + ", ".join(_bq_literal(v) for v in row) + ")" for row in chunk)
            assert width == len(chunk[0])
            self.execute(f"{head} VALUES {values}")

    def fetchall(self) -> list[tuple]:
        return list(self._rows)

    def fetchone(self) -> tuple | None:
        return self._rows[0] if self._rows else None


def _bq_literal(v: Any) -> str:
    from decimal import Decimal

    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, Decimal):
        return f"NUMERIC '{v}'"
    if isinstance(v, float):
        return repr(v)
    s = str(v).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{s}'"


def _bq_param(v: Any) -> Any:
    from decimal import Decimal

    from google.cloud import bigquery

    if isinstance(v, bool):
        return bigquery.ScalarQueryParameter(None, "BOOL", v)
    if isinstance(v, int):
        return bigquery.ScalarQueryParameter(None, "INT64", v)
    if isinstance(v, Decimal):
        return bigquery.ScalarQueryParameter(None, "NUMERIC", v)
    if isinstance(v, float):
        return bigquery.ScalarQueryParameter(None, "FLOAT64", v)
    return bigquery.ScalarQueryParameter(None, "STRING", str(v))


class _BQConnection:
    def __init__(self, cfg: dict[str, Any]):
        from connectors.bigquery_conn import get_client

        self._client = get_client(project_id=cfg["database"], host=cfg["host"], port=cfg["port"])
        self._dataset = cfg.get("schema") or "dataflow"

    def cursor(self) -> _BQCursor:
        return _BQCursor(self._client, self._dataset)

    def commit(self) -> None:
        return None

    def close(self) -> None:
        self._client.close()


def bq_table(cfg: dict[str, Any], table: str) -> str:
    return f"`{cfg['database']}.{cfg.get('schema') or 'dataflow'}.{table}`"


# --------------------------------------------------------------------------- snowflake (fakesnow)


def _snowflake_connect(cfg: dict[str, Any]) -> Any:
    from connectors.snowflake_conn import get_connection

    conn = get_connection(
        account=cfg["host"],
        username=cfg["username"],
        password=cfg["password"],
        database=cfg["database"],
        schema=cfg.get("schema") or "PUBLIC",
        warehouse=cfg.get("warehouse") or "",
        connection_string="",
    )
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {cfg['database']}")
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {cfg['database']}.{cfg.get('schema') or 'PUBLIC'}")
    cur.execute(f"USE DATABASE {cfg['database']}")
    cur.execute(f"USE SCHEMA {cfg.get('schema') or 'PUBLIC'}")
    return conn


# --------------------------------------------------------------------------- redshift (pg wire)


def _redshift_connect(cfg: dict[str, Any]) -> Any:
    import psycopg2

    return psycopg2.connect(host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
                            user=cfg["username"], password=cfg["password"])


def connect(engine: str, cfg: dict[str, Any]) -> Any:
    if engine == "bigquery":
        return _BQConnection(cfg)
    if engine == "snowflake":
        return _snowflake_connect(cfg)
    if engine == "redshift":
        return _redshift_connect(cfg)
    raise AssertionError(engine)


def table_ref(engine: str, cfg: dict[str, Any], table: str) -> str:
    """How the harness names a table in its own SQL against the emulator."""
    if engine == "bigquery":
        return bq_table(cfg, table)
    return quote(engine, table)


def bq_columns(conn: Any, cfg: dict[str, Any], table: str) -> list[str] | None:
    """Column names via the tables API (the emulator hangs on INFORMATION_SCHEMA.COLUMNS)."""
    from google.api_core.exceptions import NotFound

    try:
        t = conn._client.get_table(f"{cfg['database']}.{cfg.get('schema') or 'dataflow'}.{table}")
    except NotFound:
        return None
    return [f.name for f in t.schema]


def columns_sql(engine: str, cfg: dict[str, Any], table: str) -> str:
    if engine == "bigquery":
        raise AssertionError("use bq_columns")
    if engine == "snowflake":
        return (
            "SELECT column_name FROM information_schema.columns WHERE UPPER(table_name) = "
            f"'{table.upper()}' AND UPPER(table_schema) = '{(cfg.get('schema') or 'PUBLIC').upper()}' "
            "ORDER BY ordinal_position"
        )
    return (
        "SELECT column_name FROM information_schema.columns WHERE table_name = "
        f"'{table}' AND table_schema = current_schema() ORDER BY ordinal_position"
    )


def amount_sum_sql(engine: str, amount_col: str) -> str:
    if engine == "bigquery":
        return f"SUM(CAST({amount_col} AS NUMERIC))"
    return f"SUM(CAST({amount_col} AS DECIMAL(20,3)))"


def concat_sql(engine: str, name_col: str) -> str:
    if engine == "bigquery":
        return f"CONCAT({name_col}, '-v2')"
    return f"{name_col} || '-v2'"


def env_wanted() -> list[str]:
    return [e for e in (os.environ.get("SCHED_CLOUD_ENGINES") or "").split(",") if e]
