"""Shared PostgreSQL connection helper."""

from __future__ import annotations

import logging
from typing import Any

from connectors.sql_dsn import private_cloud_host_hint, resolve_sql_endpoint
from connectors.write_resilience import is_public_proxy_host

logger = logging.getLogger(__name__)
_ssl_fallback_hosts: set[str] = set()


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
    allow_ssl_downgrade: bool = True,
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
    try:
        conn = psycopg2.connect(**kwargs)
    except Exception as first_exc:
        fallback_sslmode: str | None = None
        if not ssl and public_proxy and sslmode != "require":
            fallback_sslmode = "require"
        elif (
            ssl
            and allow_ssl_downgrade
            and sslmode == "require"
            and _ssl_not_supported(str(first_exc))
        ):
            # ssl=True with a server that does not advertise TLS is often a stale
            # connector setting. Fall back to negotiate/try TLS first, then plain.
            fallback_sslmode = "prefer"

        if fallback_sslmode:
            try:
                conn = psycopg2.connect(**{**kwargs, "sslmode": fallback_sslmode})
                if ep["host"] not in _ssl_fallback_hosts:
                    _ssl_fallback_hosts.add(ep["host"])
                    logger.warning(
                        "PostgreSQL sslmode='%s' failed for %s; connected with sslmode='%s'. "
                        "Review the connector SSL setting if encryption is required.",
                        sslmode,
                        ep["host"],
                        fallback_sslmode,
                    )
            except psycopg2.Error as second_exc:
                # The retry's error is the truth — the first failure was for a
                # *different* sslmode. Raising first_exc reports the stale
                # "server does not support SSL" and hides the real reason the
                # working-mode connection failed (CI-1 PG-SSL masking).
                hint = private_cloud_host_hint(ep["host"], connection_string)
                if hint:
                    raise RuntimeError(f"{second_exc}{hint}") from second_exc
                raise second_exc from first_exc
        else:
            hint = private_cloud_host_hint(ep["host"], connection_string)
            if hint:
                raise RuntimeError(f"{first_exc}{hint}") from first_exc
            raise

    from connectors.write_resilience import apply_postgres_session_guards

    apply_postgres_session_guards(conn)
    return conn
