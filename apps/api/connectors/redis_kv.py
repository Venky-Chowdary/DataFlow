"""Redis connector — PING probe when redis-py is available."""

from __future__ import annotations

from connectors.base import ConnectResult


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

    try:
        if connection_string.strip():
            client = redis.from_url(connection_string.strip(), socket_timeout=8)
        else:
            client = redis.Redis(
                host=host or "localhost",
                port=port or 6379,
                db=int(database) if database.isdigit() else 0,
                username=username or None,
                password=password or None,
                ssl=ssl,
                socket_timeout=8,
            )
        client.ping()
        info = client.info("keyspace")
        keyspaces = list(info.keys()) or ["db0 (empty)"]
        client.close()
        return ConnectResult(
            ok=True,
            tables=keyspaces,
            message=f"Redis connected — {len(keyspaces)} keyspace(s)",
            driver="redis-py",
        )
    except Exception as exc:
        return ConnectResult(ok=False, tables=[], error=str(exc), driver="redis-py")
