"""Redis connector — PING probe when redis-py is available."""

from __future__ import annotations

import logging

from connectors.base import ConnectResult

_logger = logging.getLogger(__name__)


def test_redis(
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
) -> ConnectResult:
    del schema, warehouse

    try:
        import redis
    except ImportError:
        from connectors.driver_guard import require_driver
        if not (host or connection_string):
            return ConnectResult(ok=False, tables=[], error="Redis host is required.")
        return ConnectResult(
            ok=False,
            tables=[],
            error=require_driver("redis", "redis"),
            driver="none",
        )

    from connectors import redis_reader

    tls = bool(ssl) or connection_string.strip().lower().startswith("rediss://")
    try:
        client = redis_reader._redis_client(
            {
                "host": host,
                "port": port or 6379,
                "database": database,
                "username": username,
                "password": password,
                "connection_string": connection_string,
                "ssl": ssl,
            },
            socket_timeout=8,
        )
        client.ping()
        from connectors.redis_reader import redis_prefix_inventory

        # INFO keyspace returns "db0". That is the database index, not a key
        # prefix, so sampling it scanned db0:* and read none of the hashes.
        prefixes = redis_prefix_inventory(client)
        client.close()
        return ConnectResult(
            ok=True,
            tables=prefixes,
            message=(
                f"Redis connected — {len(prefixes)} key prefix(es)."
                if prefixes
                else "Redis connected — no keys in this database."
            ),
            driver="redis-py",
        )
    except redis.exceptions.TimeoutError as exc:
        where = f"{host}:{port or 6379}" if host else "the Redis server"
        if tls:
            error = (
                f"Redis TLS handshake with {where} timed out ({exc}). The server "
                "may not accept TLS: if it is a plaintext Redis, set ssl=false "
                "on the connector."
            )
        else:
            error = f"Redis at {where} did not answer in time ({exc})."
        _logger.warning("Redis probe timed out: endpoint=%s tls=%s", where, tls)
        return ConnectResult(ok=False, tables=[], error=error, driver="redis-py")
    except Exception as exc:
        return ConnectResult(ok=False, tables=[], error=str(exc), driver="redis-py")
