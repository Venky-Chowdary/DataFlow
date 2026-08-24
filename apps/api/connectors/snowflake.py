"""Snowflake connector — live probe when snowflake-connector-python is available."""

from __future__ import annotations

import logging

from connectors.base import ConnectResult
from connectors.snowflake_conn import (
    classify_snowflake_connect_error,
    get_connection,
    normalize_account,
    parse_snowflake_url,
)
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
    list_tables: bool = True,
) -> ConnectResult:
    del port, ssl, auth_mode
    parsed = parse_snowflake_url(connection_string) if (connection_string or "").strip() else {}
    account = parsed.get("account") or normalize_account(host)
    user = parsed.get("user") or username
    secret = parsed.get("password") or password
    database = parsed.get("database") or database
    schema = parsed.get("schema") or schema
    pem = (private_key or "").strip()
    if not account or not user:
        return ConnectResult(
            ok=False,
            tables=[],
            error=(
                "Provide account (host) + username, or a Snowflake login URL "
                "(snowflake://user:password@account/DATABASE/SCHEMA?warehouse=…)."
            ),
        )
    if not pem and not (secret or "").strip():
        return ConnectResult(
            ok=False,
            tables=[],
            error="Provide a password or a PKCS#8 private key for Snowflake",
        )

    wh = ""
    warehouse = parsed.get("warehouse") or warehouse
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
            username=user,
            password=secret,
            database=database,
            schema=schema or "PUBLIC",
            warehouse=wh,
            connection_string=connection_string,
            role=engine_login_role(parsed.get("role"), auth_role, role),
            private_key=pem,
            private_key_passphrase=secret if pem else "",
        )
        with conn.cursor() as cur:
            if wh:
                cur.execute(f'USE WAREHOUSE "{wh}"')
            if list_tables:
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
            else:
                cur.execute("SELECT 1")
                tables = []
        return ConnectResult(
            ok=True,
            tables=tables or (["(no tables in schema)"] if list_tables else []),
            message=(
                f"Snowflake connected — {len(tables)} tables in schema '{schema or 'PUBLIC'}'"
                if list_tables
                else "Snowflake connected"
            ),
            driver="snowflake-connector-python",
        )
    except Exception as exc:
        classified = classify_snowflake_connect_error(str(exc))
        return ConnectResult(
            ok=False,
            tables=[],
            error=classified or str(exc),
            driver="snowflake-connector-python",
        )
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as exc:
                logger.warning("Exception suppressed: %s", exc, exc_info=exc)
