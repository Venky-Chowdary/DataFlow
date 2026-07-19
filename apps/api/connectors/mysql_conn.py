"""Shared MySQL connection helper."""

from __future__ import annotations

from typing import Any

from connectors.sql_dsn import private_cloud_host_hint, resolve_sql_endpoint
from connectors.write_resilience import is_public_proxy_host


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

    public_proxy = is_public_proxy_host(ep["host"])
    # Bulk transfers routinely exceed the old 30s socket deadline on public proxies.
    io_timeout = 300 if public_proxy else 120
    kwargs: dict[str, Any] = {
        "host": ep["host"],
        "port": ep["port"],
        "user": ep["username"],
        "password": ep["password"],
        "database": ep["database"] or None,
        "connect_timeout": 15,
        "charset": "utf8mb4",
        "read_timeout": io_timeout,
        "write_timeout": io_timeout,
    }
    if ssl or public_proxy:
        kwargs["ssl"] = {"ssl": {}}
    try:
        conn = pymysql.connect(**kwargs)
    except Exception as exc:
        hint = private_cloud_host_hint(ep["host"], connection_string)
        if hint:
            raise RuntimeError(f"{exc}{hint}") from exc
        raise

    # TCP keepalive so public proxies do not drop idle bulk-write sockets.
    try:
        import socket

        sock = getattr(conn, "_sock", None) or getattr(conn, "socket", None)
        if sock is not None:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            if hasattr(socket, "TCP_KEEPIDLE"):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 30)
            if hasattr(socket, "TCP_KEEPINTVL"):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)
            if hasattr(socket, "TCP_KEEPCNT"):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 5)
    except Exception:
        pass

    from connectors.write_resilience import apply_mysql_session_guards

    apply_mysql_session_guards(conn)
    return conn
