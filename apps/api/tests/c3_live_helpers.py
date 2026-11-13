"""Shared harness for the C3 live lossy-pair proofs (MySQL, SQL Server, Oracle).

Each proof runs the real engine (``UniversalTransferEngine.execute_tracked``,
Validate on) from a live source into a live destination and reads the result
back. Kept out of the test modules so the SQL Server and Oracle files, which
only CI can run, use the harness the local MySQL file exercises.
"""

from __future__ import annotations

import os
import uuid
from typing import Any

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")


def pg_endpoint(table: str) -> Any:
    from src.transfer.models import EndpointConfig
    from tests.helpers.live_env import pg_creds

    c = pg_creds()
    return EndpointConfig(
        kind="database", format="postgresql", host=c["host"], port=int(c["port"]),
        database=c["database"], username=c["username"], password=c["password"],
        schema="public", table=table,
    )


def mysql_endpoint(table: str) -> Any:
    from src.transfer.models import EndpointConfig
    from tests.helpers.live_env import mysql_creds

    c = mysql_creds()
    return EndpointConfig(
        kind="database", format="mysql", host=c["host"], port=int(c["port"]),
        database=c["database"], username=c["username"], password=c["password"], table=table,
    )


def pg_sql(statement: str, params: tuple = (), fetch: bool = False) -> Any:
    import psycopg2

    from tests.helpers.live_env import pg_creds

    c = pg_creds()
    conn = psycopg2.connect(
        host=c["host"], port=c["port"], dbname=c["database"], user=c["username"],
        password=c["password"],
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(statement, params)
            return cur.fetchall() if fetch else None
    finally:
        conn.close()


def mysql_sql(statement: str, params: tuple = (), fetch: bool = False) -> Any:
    import pymysql

    from tests.helpers.live_env import mysql_creds

    c = mysql_creds()
    conn = pymysql.connect(
        host=c["host"], port=int(c["port"]), user=c["username"], password=c["password"],
        database=c["database"], autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(statement, params)
            return cur.fetchall() if fetch else None
    finally:
        conn.close()


def sqlalchemy_sql(cfg: dict[str, Any], statement: str, fetch: bool = False) -> Any:
    """Run one statement on a SQL Server / Oracle source through the product's engine."""
    import sqlalchemy as sa

    from connectors.generic_sql import _engine

    with _engine(dict(cfg)).begin() as conn:
        result = conn.execute(sa.text(statement))
        return [tuple(r) for r in result.fetchall()] if fetch else None


def run_transfer(store: Any, source: Any, destination: Any) -> Any:
    """One full-refresh append with Validate (preflight) on, through the real engine."""
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import TransferRequest

    request = TransferRequest(
        source=source, destination=destination, sync_mode="full_refresh_append",
        validation_mode="strict",
    )
    job_id = "c3l" + uuid.uuid4().hex[:12]
    store.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def refused_at_validate(result: Any, column: str) -> bool:
    """The run failed and its error names the column (callers check nothing was written)."""
    text = str(getattr(result, "error", "") or "")
    return (not result.success) and column in text


def token() -> str:
    return uuid.uuid4().hex[:8]
