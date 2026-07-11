"""Snowflake connection helper."""

from __future__ import annotations

from typing import Any


def normalize_account(host: str) -> str:
    host = host.strip()
    if not host:
        return ""
    if ".snowflakecomputing.com" in host:
        return host.split(".snowflakecomputing.com")[0]
    return host


def get_connection(
    *,
    account: str,
    username: str,
    password: str,
    database: str,
    schema: str,
    warehouse: str,
    connection_string: str,
) -> Any:
    try:
        import snowflake.connector
    except ImportError as exc:
        from connectors.driver_guard import require_driver
        raise RuntimeError(require_driver("snowflake.connector", "snowflake-connector-python")) from exc

    if connection_string.strip():
        return snowflake.connector.connect(connection_string, login_timeout=10)

    kwargs: dict[str, Any] = {
        "account": normalize_account(account),
        "user": username,
        "password": password,
        "login_timeout": 10,
    }
    if database:
        kwargs["database"] = database
    if schema:
        kwargs["schema"] = schema
    if warehouse:
        kwargs["warehouse"] = warehouse
    return snowflake.connector.connect(**kwargs)
