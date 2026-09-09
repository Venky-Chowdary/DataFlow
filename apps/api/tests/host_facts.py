"""Host capability probes — skip when the engine cannot prove the claim.

A listening port is not a collation inventory and not a pgvector install.
Property-8 unicode form and live pgvector writes must skip on capability,
not fail a product assertion that is actually a host fact.
"""

from __future__ import annotations

import os
from collections.abc import Iterable
from typing import Any

import pytest

PGVECTOR_SKIP_REASON = (
    "pgvector extension unavailable on this PostgreSQL "
    "(vector.control missing or not listed in pg_available_extensions)"
)


def _pg_connect_kwargs() -> dict[str, Any]:
    return {
        "host": os.environ.get("P8_PG_HOST", os.environ.get("P2_PG_HOST", "127.0.0.1")),
        "port": int(os.environ.get("P8_PG_PORT", os.environ.get("P2_PG_PORT", "5432"))),
        "dbname": os.environ.get("P8_PG_DB", os.environ.get("P2_PG_DB", "dataflow")),
        "user": os.environ.get("P8_PG_USER", os.environ.get("P2_PG_USER", "dataflow")),
        "password": os.environ.get(
            "P8_PG_PASSWORD", os.environ.get("P2_PG_PASSWORD", "dataflow")
        ),
        "connect_timeout": 2,
    }


def pgvector_extension_available() -> bool:
    """True when this PostgreSQL can create or already has ``vector``."""
    try:
        import psycopg2

        conn = psycopg2.connect(**_pg_connect_kwargs())
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM pg_extension WHERE extname = 'vector'
                    ) OR EXISTS (
                        SELECT 1 FROM pg_available_extensions WHERE name = 'vector'
                    )
                    """
                )
                row = cur.fetchone()
                return bool(row and row[0])
        finally:
            conn.close()
    except Exception:
        return False


def require_pgvector() -> None:
    """Skip the calling test when this host cannot install or use pgvector."""
    if not pgvector_extension_available():
        pytest.skip(PGVECTOR_SKIP_REASON)


def listed_mysql_collations(cur: Any, names: Iterable[str]) -> set[str]:
    """Return the subset of ``names`` this MySQL/MariaDB actually ships.

    ``utf8mb4_0900_ai_ci`` is MySQL 8; MariaDB 10.x often has neither 0900
    nor uca1400. Asserting absence or presence is a host fact — callers must
    skip or continue when a name is missing, not fail the product.
    """
    wanted = tuple(str(n) for n in names if str(n).strip())
    if not wanted:
        return set()
    placeholders = ",".join(["%s"] * len(wanted))
    cur.execute(
        "SELECT COLLATION_NAME FROM information_schema.COLLATIONS "
        f"WHERE COLLATION_NAME IN ({placeholders})",
        wanted,
    )
    return {str(r[0]) for r in cur.fetchall()}
