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
    del port, ssl
    parsed = parse_snowflake_url(connection_string) if (connection_string or "").strip() else {}
    account = parsed.get("account") or normalize_account(host)
    user = parsed.get("user") or username
    secret = parsed.get("password") or password
    database = parsed.get("database") or database
    schema = parsed.get("schema") or schema
    # PAT/password records must not silently fall back to a stray stored
    # private_key — honor the declared auth mode (X-PAT).
    mode = (auth_mode or "").strip().lower()
    raw_pem = (private_key or "").strip()
    if mode in ("user_pass", "connection_string", "pat"):
        pem = ""
    elif raw_pem and "BEGIN" not in raw_pem and (secret or "").strip():
        # Masked/truncated stored key is not usable key material — a present
        # password/PAT must route to password auth, never silent key-pair.
        pem = ""
    else:
        pem = raw_pem
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
            auth_mode=mode,
        )
        with conn.cursor() as cur:
            if wh:
                cur.execute(f'USE WAREHOUSE "{wh}"')
            if list_tables:
                tables = _list_tables_metadata_only(cur, schema or "PUBLIC")
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
        classified = classify_snowflake_connect_error(str(exc), auth_mode=mode)
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


def _list_tables_metadata_only(cur, schema: str) -> list[str]:
    """Up to 50 table names in ``schema``, answered by Snowflake's metadata
    service (``SHOW``): no warehouse needed.

    ``information_schema.tables`` runs on the warehouse, so on a suspended
    one the connection probe paid the 5-10s resume inside the 45s plan
    deadline (D187). The catalog query stays as the fallback.
    """
    from connectors.sql_identifiers import snowflake_fold_identifier
    from connectors.writer_common import quote_sql_identifier

    try:
        cur.execute(
            f"SHOW TERSE TABLES IN SCHEMA {quote_sql_identifier(snowflake_fold_identifier(schema))} LIMIT 50"
        )
        names = [desc[0] for desc in (cur.description or [])]
        index = names.index("name") if "name" in names else 1
        return sorted(str(row[index]) for row in cur.fetchall())[:50]
    except Exception as exc:  # noqa: BLE001 - fall back to the catalog query
        logger.info("SHOW TERSE TABLES unavailable (%s); using information_schema", exc)
    cur.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s AND table_type = 'BASE TABLE'
        ORDER BY table_name
        LIMIT 50
        """,
        (schema,),
    )
    return [row[0] for row in cur.fetchall()]
