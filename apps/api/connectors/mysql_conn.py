"""Shared MySQL connection helper."""

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
    import pymysql

    if connection_string.strip():
        # mysql://user:pass@host:3306/db
        url = connection_string.strip()
        if url.startswith("mysql://"):
            return pymysql.connect(
                host=host or "localhost",
                port=port or 3306,
                user=username,
                password=password,
                database=database,
                connect_timeout=10,
                charset="utf8mb4",
            )
    return pymysql.connect(
        host=host or "localhost",
        port=port or 3306,
        user=username,
        password=password,
        database=database,
        connect_timeout=10,
        charset="utf8mb4",
        ssl={"ssl": {}} if ssl else None,
    )
