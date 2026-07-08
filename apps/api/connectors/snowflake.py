"""Snowflake connector — live probe when snowflake-connector-python is available."""

from __future__ import annotations

from connectors.base import ConnectResult
from connectors.snowflake_conn import normalize_account


def test_snowflake(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    schema: str,
    connection_string: str,
    ssl: bool,
    warehouse: str = "",
) -> ConnectResult:
    del port, ssl
    try:
        import snowflake.connector
    except ImportError:
        return _stub_fallback(host, database, username, connection_string, warehouse)

    account = normalize_account(host)
    if not connection_string.strip() and (not account or not username):
        return ConnectResult(
            ok=False,
            tables=[],
            error="Provide account (host) + username or a Snowflake connection string",
        )

    try:
        from connectors.snowflake_conn import get_connection

        conn = get_connection(
            account=account,
            username=username,
            password=password,
            database=database,
            schema=schema or "PUBLIC",
            warehouse=warehouse,
            connection_string=connection_string,
        )
        with conn.cursor() as cur:
            if warehouse:
                cur.execute(f"USE WAREHOUSE {warehouse}")
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s AND table_type = 'BASE TABLE'
                ORDER BY table_name
                LIMIT 50
                """,
                (schema or "PUBLIC",),
            )
            tables = [row[0] for row in cur.fetchall()]
        conn.close()
        return ConnectResult(
            ok=True,
            tables=tables or ["(no tables in schema)"],
            message=f"Snowflake connected — {len(tables)} tables in schema '{schema or 'PUBLIC'}'",
            driver="snowflake-connector-python",
        )
    except Exception as exc:
        return ConnectResult(ok=False, tables=[], error=str(exc), driver="snowflake-connector-python")


def _stub_fallback(
    host: str,
    database: str,
    username: str,
    connection_string: str,
    warehouse: str,
) -> ConnectResult:
    del host, database, username, connection_string, warehouse
    return ConnectResult(
        ok=False,
        tables=[],
        error="Snowflake driver not installed — pip install snowflake-connector-python",
        driver="none",
    )
