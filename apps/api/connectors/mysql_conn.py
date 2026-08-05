"""Shared MySQL connection helper."""

from __future__ import annotations

import logging
from typing import Any

from connectors.sql_dsn import private_cloud_host_hint, resolve_sql_endpoint
from connectors.write_resilience import is_public_proxy_host


def _parse_mysql_url(url: str) -> dict[str, Any]:
    from connectors.sql_dsn import parse_sql_url

    return parse_sql_url(url, family="mysql")

logger = logging.getLogger(__name__)


def get_connection(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    connection_string: str,
    ssl: bool,
    purpose: str = "write",
) -> Any:
    """Open a MySQL connection.

    ``purpose``:
      - ``write`` / ``bulk`` — long I/O timeouts for large transfers on public proxies
      - ``ddl`` — short connect/I/O + tight lock waits so DROP/CREATE cannot hang a
        5-row demo for minutes behind a metadata lock (fail with a clear timeout)
    """
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
    purpose_l = (purpose or "write").strip().lower()
    ddl_mode = purpose_l in {"ddl", "drop", "setup", "probe"}
    # Bulk transfers routinely exceed short socket deadlines on public proxies.
    # DDL/demo paths must fail fast — lock waits, not silent multi-minute hangs.
    if ddl_mode:
        connect_timeout = 10
        # Short lock wait always (fail-fast on metadata locks). Setup on public
        # proxies may need longer socket I/O for CREATE/ALTER; DROP/probe stay tight.
        lock_wait_s = 30
        if purpose_l == "setup" and public_proxy:
            io_timeout = 180
        else:
            io_timeout = 45
    else:
        connect_timeout = 15
        io_timeout = 300 if public_proxy else 120
        lock_wait_s = 120
    kwargs: dict[str, Any] = {
        "host": ep["host"],
        "port": ep["port"],
        "user": ep["username"],
        "password": ep["password"],
        "database": ep["database"] or None,
        "connect_timeout": connect_timeout,
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
    except Exception as exc:
        logger.warning("Exception suppressed: %s", exc, exc_info=exc)

    from connectors.write_resilience import apply_mysql_session_guards

    apply_mysql_session_guards(conn, lock_wait_seconds=lock_wait_s)
    return conn
