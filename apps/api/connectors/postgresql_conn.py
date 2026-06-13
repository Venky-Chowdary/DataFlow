"""Shared PostgreSQL connection helper."""

from __future__ import annotations

from typing import Any


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
    import psycopg2

    if connection_string.strip():
        return psycopg2.connect(connection_string, connect_timeout=10)
    return psycopg2.connect(
        host=host,
        port=port,
        dbname=database,
        user=username,
        password=password,
        connect_timeout=10,
        sslmode="require" if ssl else "prefer",
    )
