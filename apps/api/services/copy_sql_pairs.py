"""Shared pg/mysql pair helpers — the connect and identifier wrappers the
pairwise copy modules used to each carry their own copy of.

One owner per concern (AGENTS/enterprise rule): ``mysql_connect`` and
``pg_connect`` both open write-mode connections with autocommit off so the
pair fast path can commit or roll back a batch range atomically; the ident
quoters delegate to the canonical ``quote_sql_identifier``.
"""

from __future__ import annotations

from typing import Any


def mysql_ident(name: str) -> str:
    from connectors.sql_identifiers import quote_sql_identifier

    return quote_sql_identifier(name, "`")


def pg_ident(name: str) -> str:
    from connectors.sql_identifiers import (
        quote_sql_identifier,
        require_safe_identifier,
    )

    return quote_sql_identifier(require_safe_identifier(name, preserve_case=True))


def mysql_write_conn(cfg: dict[str, Any]) -> Any:
    from connectors.mysql_conn import get_connection as mysql_connect

    conn = mysql_connect(
        host=cfg.get("host", ""),
        port=int(cfg.get("port") or 3306),
        database=cfg.get("database", ""),
        username=cfg.get("username") or cfg.get("user") or "",
        password=cfg.get("password", ""),
        connection_string=cfg.get("connection_string", ""),
        ssl=bool(cfg.get("ssl", False)),
        purpose="write",
    )
    conn.autocommit = False
    return conn


def pg_write_conn(cfg: dict[str, Any]) -> Any:
    from connectors.postgresql_conn import get_connection as pg_connect

    conn = pg_connect(
        host=cfg.get("host", ""),
        port=int(cfg.get("port") or 5432),
        database=cfg.get("database") or cfg.get("dbname") or "",
        username=cfg.get("username") or cfg.get("user") or "",
        password=cfg.get("password", ""),
        connection_string=cfg.get("connection_string", ""),
        ssl=bool(cfg.get("ssl", False)),
    )
    conn.autocommit = False
    return conn
