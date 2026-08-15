"""Snowflake connector — live probe when snowflake-connector-python is available."""

from __future__ import annotations

import logging

from connectors.base import ConnectResult
from connectors.snowflake_conn import get_connection, normalize_account
from services.connector_auth import engine_login_role

logger = logging.getLogger(__name__)


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
    role: str = "",
    private_key: str = "",
    auth_role: str = "",
    auth_mode: str = "",
) -> ConnectResult:
    del port, ssl, auth_mode
    account = normalize_account(host)
    pem = (private_key or "").strip()
    if not connection_string.strip() and (not account or not username):
        return ConnectResult(
            ok=False,
            tables=[],
            error="Provide account (host) + username or a Snowflake connection string",
        )
    if not connection_string.strip() and not pem and not (password or "").strip():
        return ConnectResult(
            ok=False,
            tables=[],
            error="Provide a password or a PKCS#8 private key for Snowflake",
        )

    wh = ""
    if warehouse:
        # Identifier-quote only — never interpolate raw operator input into SQL.
        wh = warehouse.strip().strip('"').replace('"', "")
        if not wh or not all(c.isalnum() or c in ("_", "$") for c in wh):
            return ConnectResult(
                ok=False,
                tables=[],
                error="Invalid Snowflake warehouse identifier",
            )

    conn = None
    try:
        conn = get_connection(
            account=account,
            username=username,
            password=password,
            database=database,
            schema=schema or "PUBLIC",
            warehouse=wh,
            connection_string=connection_string,
            role=engine_login_role(auth_role, role),
            private_key=pem,
            private_key_passphrase=password if pem else "",
        )
        with conn.cursor() as cur:
            if wh:
                cur.execute(f'USE WAREHOUSE "{wh}"')
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
        return ConnectResult(
            ok=True,
            tables=tables or ["(no tables in schema)"],
            message=f"Snowflake connected — {len(tables)} tables in schema '{schema or 'PUBLIC'}'",
            driver="snowflake-connector-python",
        )
    except Exception as exc:
        return ConnectResult(ok=False, tables=[], error=str(exc), driver="snowflake-connector-python")
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as exc:
                logger.warning("Exception suppressed: %s", exc, exc_info=exc)
