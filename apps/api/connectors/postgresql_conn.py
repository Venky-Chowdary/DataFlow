"""Shared PostgreSQL connection helper."""

from __future__ import annotations

import logging
from typing import Any

from connectors.sql_dsn import private_cloud_host_hint, resolve_sql_endpoint
from connectors.write_resilience import is_public_proxy_host

logger = logging.getLogger(__name__)


def _parse_postgres_url(url: str) -> dict[str, Any]:
    from connectors.sql_dsn import parse_sql_url

    return parse_sql_url(url, family="postgresql")


def _ssl_not_supported(msg: str) -> bool:
    """Detect the common psycopg2 error when the server does not support TLS."""
    lower = msg.lower()
    return (
        "server does not support ssl, but ssl was required" in lower
        or "server does not support ssl" in lower
        or "ssl not supported" in lower
        or "ssl is not enabled" in lower
    )


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
    try:
        import psycopg2
    except ImportError as exc:
        from connectors.driver_guard import require_driver

        raise RuntimeError(require_driver("psycopg2", "psycopg2-binary")) from exc

    ep = resolve_sql_endpoint(
        family="postgresql",
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        connection_string=connection_string,
        default_port=5432,
    )

    # Prefer discrete fields (merged from URL + form). For public cloud proxies
    # (Railway *.proxy.rlwy.net, etc.) try SSL require if prefer fails — many
    # managed proxies expect TLS on the public port.
    public_proxy = is_public_proxy_host(ep["host"])
    sslmode = "require" if (ssl or public_proxy) else "prefer"
    kwargs: dict[str, Any] = {
        "host": ep["host"],
        "port": ep["port"],
        "dbname": ep["database"] or "postgres",
        "user": ep["username"] or "postgres",
        "password": ep["password"],
        "connect_timeout": 15,
        "sslmode": sslmode,
        # Keep the socket alive during long COPY/INSERT batches and mapping pauses
        # so public TCP proxies do not silently drop the session mid-transfer.
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5,
    }

    from services.egress_guard import assert_connector_dial_allowed

    assert_connector_dial_allowed(ep["host"], connection_string)
    from connectors.pg_infinity import register_pg_temporal_infinity

    register_pg_temporal_infinity()
    from services.endpoint_session import borrow

    from src.ai.copilot.tool_deadline import remaining_seconds

    left = remaining_seconds()
    if left is not None:
        # Inside a tool call the connect may not outlive the call (D169/D181).
        kwargs["connect_timeout"] = max(2, min(int(kwargs["connect_timeout"]), int(left) or 2))
    key = tuple(str(kwargs.get(k) or "") for k in ("host", "port", "dbname", "user", "password", "sslmode"))
    try:
        conn, release = borrow(
            "postgresql",
            key,
            lambda: _open_guarded(psycopg2, kwargs),
            close=_really_close,
            alive=lambda c: not c.closed,
            reset=_reset_for_reuse,
        )
    except Exception as first_exc:
        if ssl and _ssl_not_supported(str(first_exc)):
            # ssl=True is the connector's explicit TLS requirement (see
            # SavedConnector.ssl_mode): it is never retried without TLS.
            raise RuntimeError(
                f"{first_exc} - TLS is required by this connection and the server "
                "does not offer it; refusing to fall back to plaintext. Enable TLS on "
                "the server, or clear SSL/TLS on the connector to allow an unencrypted "
                "connection."
            ) from first_exc
        hint = private_cloud_host_hint(ep["host"], connection_string)
        if hint:
            raise RuntimeError(f"{first_exc}{hint}") from first_exc
        raise
    if release is not None:
        conn._df_release = release
    return conn


def _scoped_connection_class(psycopg2: Any) -> type:
    """A psycopg2 connection whose ``close`` can hand it back to an endpoint scope.

    It stays a real ``psycopg2.extensions.connection`` (``sql.Composed`` and
    the type casters require one), so no consumer can tell the difference.
    """
    global _SCOPED_CLASS
    if _SCOPED_CLASS is None:

        class ScopedConnection(psycopg2.extensions.connection):
            _df_release = None

            def close(self) -> None:
                release, self._df_release = self._df_release, None
                if release is not None:
                    release()
                    return
                super().close()

        _SCOPED_CLASS = ScopedConnection
    return _SCOPED_CLASS


_SCOPED_CLASS: type | None = None


def _open_guarded(psycopg2: Any, kwargs: dict[str, Any]) -> Any:
    from connectors.write_resilience import apply_postgres_session_guards

    conn = psycopg2.connect(
        **kwargs, connection_factory=_scoped_connection_class(psycopg2)
    )
    apply_postgres_session_guards(conn)
    return conn


def _really_close(conn: Any) -> None:
    conn._df_release = None
    conn.close()


def _reset_for_reuse(conn: Any) -> None:
    """Hand a session to its next consumer as a fresh connect would."""
    from connectors.write_resilience import apply_postgres_session_guards

    conn.rollback()
    conn.cursor_factory = None
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("RESET ALL")
    apply_postgres_session_guards(conn)
