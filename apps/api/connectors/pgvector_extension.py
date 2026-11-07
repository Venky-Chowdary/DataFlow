"""The ``vector`` extension a pgvector typed table needs, ensured or refused.

A typed pgvector table (owner decision 1, QA D209) is created by the
PostgreSQL writer with ``embedding vector(n)`` next to the source columns. Only
the vector-document writer ever ran ``CREATE EXTENSION``, so a typed table in a
database where nobody had created the extension failed its CREATE with "type
vector does not exist" — and, through the writer's fallback, "current
transaction is aborted" (QA round 11, PG -> pgvector upsert, 2 of 2 attempts).

* :func:`ensure_vector_extension` — the writer creates the extension (inside a
  savepoint) before a CREATE that declares a vector column, or raises the
  named operator error.
* :func:`vector_extension_refusal` — Validate's question: when the type is
  absent and this role cannot create it, the run is refused before Confirm
  with the same named reason.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Iterable, Mapping

logger = logging.getLogger(__name__)

_VECTOR_TYPE = re.compile(r"^\s*(?:public\.)?(?:vector|halfvec|sparsevec)\s*(?:\(|$)", re.IGNORECASE)


def declares_vector_column(types: Iterable[Any]) -> bool:
    return any(_VECTOR_TYPE.match(str(t or "")) for t in types or [])


def _named(exc: BaseException) -> str:
    from connectors.pgvector_writer import pgvector_extension_unavailable_reason

    return pgvector_extension_unavailable_reason(exc) or (
        f"PostgreSQL could not create the pgvector extension: {exc}"
    )


def ensure_vector_extension(cursor: Any, types: Iterable[Any], *, engine: str = "") -> None:
    """Create ``vector`` before a CREATE TABLE that declares a vector column."""
    if not declares_vector_column(types):
        return
    cursor.execute("SELECT to_regtype('vector') IS NOT NULL")
    row = cursor.fetchone()
    if row and row[0]:
        return
    from connectors.pg_tx_guard import savepoint

    try:
        with savepoint(cursor, engine=engine, label="CREATE EXTENSION vector"):
            cursor.execute("CREATE EXTENSION IF NOT EXISTS vector")
    except Exception as exc:
        raise RuntimeError(_named(exc)) from exc


_CAPABILITY_SQL = (
    "SELECT to_regtype('vector') IS NOT NULL, "
    "EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'vector'), "
    "COALESCE((SELECT rolsuper FROM pg_roles WHERE rolname = current_user), false), "
    "COALESCE((SELECT bool_or(trusted) FROM pg_available_extension_versions "
    "WHERE name = 'vector'), false), "
    "has_database_privilege(current_database(), 'CREATE')"
)


def vector_extension_refusal(cfg: Mapping[str, Any]) -> str | None:
    """The named refusal when the type is absent and this role cannot create it.

    ``None`` when the type exists, when the role can create it (the writer will),
    or when the catalog cannot be read (the writer still refuses by name).
    """
    try:
        from connectors.postgresql_conn import get_connection

        conn = get_connection(
            host=str(cfg.get("host") or ""), port=int(cfg.get("port") or 5432),
            database=str(cfg.get("database") or ""),
            username=str(cfg.get("username") or cfg.get("user") or ""),
            password=str(cfg.get("password") or ""),
            connection_string=str(cfg.get("connection_string") or ""),
            ssl=bool(cfg.get("ssl") or False),
        )
    except Exception as exc:  # noqa: BLE001 - connectivity is G2's verdict
        logger.debug("pgvector capability probe could not connect: %s", exc)
        return None
    try:
        with conn.cursor() as cur:
            cur.execute(_CAPABILITY_SQL)
            present, available, superuser, trusted, can_create = cur.fetchone()
    except Exception as exc:  # noqa: BLE001 - unreadable catalog is not a refusal
        logger.debug("pgvector capability probe failed: %s", exc)
        return None
    finally:
        try:
            conn.close()
        except Exception as exc:  # noqa: BLE001 - closing a probe connection
            logger.debug("pgvector capability probe close failed: %s", exc)
    if present:
        return None
    if available and (superuser or (trusted and can_create)):
        return None
    why = (
        "the extension is not installed on this server (vector.control missing)"
        if not available
        else "the role cannot CREATE EXTENSION vector (it needs superuser, or a trusted "
        "pgvector build and CREATE on the database)"
    )
    return (
        f"pgvector destination: the database {cfg.get('database') or ''!s} has no vector type and "
        f"{why}. Ask an administrator to run CREATE EXTENSION vector in that database, "
        "or pick a database that has it."
    )
