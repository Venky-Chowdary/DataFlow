"""Shared PostgreSQL connection helper."""

from __future__ import annotations

from typing import Any

from connectors.sql_dsn import private_cloud_host_hint, resolve_sql_endpoint
from connectors.write_resilience import is_public_proxy_host


def _parse_postgres_url(url: str) -> dict[str, Any]:
    from connectors.sql_dsn import parse_sql_url

    return parse_sql_url(url, family="postgresql")


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

    try:
        conn = psycopg2.connect(**kwargs)
    except Exception as first_exc:
        if not ssl and public_proxy and sslmode != "require":
            try:
                conn = psycopg2.connect(**{**kwargs, "sslmode": "require"})
            except Exception:
                hint = private_cloud_host_hint(ep["host"], connection_string)
                if hint:
                    raise RuntimeError(f"{first_exc}{hint}") from first_exc
                raise first_exc
        else:
            hint = private_cloud_host_hint(ep["host"], connection_string)
            if hint:
                raise RuntimeError(f"{first_exc}{hint}") from first_exc
            raise

    from connectors.write_resilience import apply_postgres_session_guards

    apply_postgres_session_guards(conn)
    return conn
