"""Shared MySQL connection helper."""

from __future__ import annotations

from typing import Any

from connectors.sql_dsn import private_cloud_host_hint, resolve_sql_endpoint


def _parse_mysql_url(url: str) -> dict[str, Any]:
    from connectors.sql_dsn import parse_sql_url

    return parse_sql_url(url, family="mysql")


def get_connection(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    connection_string: str,
    ssl: bool,
) -> Any:
    import pymysql

    ep = resolve_sql_endpoint(
        family="mysql",
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        connection_string=connection_string,
        default_port=3306,
    )

    kwargs: dict[str, Any] = {
        "host": ep["host"],
        "port": ep["port"],
        "user": ep["username"],
        "password": ep["password"],
        "database": ep["database"] or None,
        "connect_timeout": 15,
        "charset": "utf8mb4",
        "read_timeout": 30,
        "write_timeout": 30,
    }
    if ssl:
        kwargs["ssl"] = {"ssl": {}}
    try:
        return pymysql.connect(**kwargs)
    except Exception as exc:
        hint = private_cloud_host_hint(ep["host"], connection_string)
        if hint:
            raise RuntimeError(f"{exc}{hint}") from exc
        raise
