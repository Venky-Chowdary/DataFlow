"""Snowflake SQLAlchemy engines that ride an endpoint scope's shared login (D187).

Split out of ``connectors.generic_sql`` so the shared-login plumbing lives
beside nothing else; ``generic_sql`` imports it lazily on the Snowflake path.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import create_engine
from sqlalchemy.exc import NoSuchModuleError


def endpoint_scoped_engine(cfg: dict[str, Any]) -> Any:
    """Inside an endpoint scope, a Snowflake engine that rides the scope's login.

    The process-wide pooled engine owns a login of its own and pings it on
    every checkout: inside one plan the reflection and the GROUP BY of the
    destination uniqueness probe paid a second Snowflake login (2-4s) and a
    ``SELECT 1`` round trip per checkout (D187). The scope's engine draws its
    DBAPI connection from the shared login (no pool, nothing to ping) and is
    disposed with the scope.
    """
    if str(cfg.get("type") or "").lower().strip() != "snowflake":
        return None
    from services.endpoint_session import borrow, in_scope

    if not in_scope("snowflake"):
        return None
    from connectors.generic_sql import _dialect_unavailable, _warehouse_creator

    built = _warehouse_creator(cfg, "snowflake")
    if built is None:
        return None
    url, _owned_creator = built
    kwargs = snowflake_login_kwargs(cfg)

    def _shared_login() -> Any:
        from connectors.snowflake_conn import get_connection

        return get_connection(**kwargs)

    from sqlalchemy.pool import NullPool

    key = tuple(sorted((k, str(v)) for k, v in kwargs.items()))
    try:
        engine, _release = borrow(
            "snowflake-sqlalchemy",
            key,
            lambda: create_engine(url, creator=_shared_login, poolclass=NullPool),
            close=lambda e: e.dispose(),
            shared=True,
        )
    except (NoSuchModuleError, ImportError) as exc:
        raise _dialect_unavailable(url, cfg.get("type")) from exc
    return engine


def snowflake_login_kwargs(cfg: dict[str, Any]) -> dict[str, Any]:
    from connectors.snowflake_conn import normalize_account
    from services.copy_snowflake_common import snowflake_database_of, snowflake_schema_of

    return {
        "account": normalize_account(str(cfg.get("host") or "")),
        "username": str(cfg.get("username") or cfg.get("user") or ""),
        "password": str(cfg.get("password") or ""),
        "database": snowflake_database_of(cfg),
        "schema": snowflake_schema_of(cfg),
        "warehouse": str(cfg.get("warehouse") or ""),
        "connection_string": str(cfg.get("connection_string") or ""),
        "role": str(cfg.get("role") or ""),
        "private_key": str(cfg.get("private_key") or ""),
        "private_key_passphrase": str(cfg.get("private_key_passphrase") or ""),
    }
