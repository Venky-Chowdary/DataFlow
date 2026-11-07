"""One dispatcher per store operation, shared by every suite in the matrix.

The CDC, batch-mode and scheduler suites all need the same five things from a
store — endpoint, seed, count, projection, drop — and each needed them for
PostgreSQL, MySQL and MongoDB. Keeping one dispatcher here is what stops the
three suites from drifting into three slightly different definitions of "the
destination row count".
"""

from __future__ import annotations

from typing import Any, Sequence

from tests.scale import live_engines as L

DIALECTS = ("postgresql", "mysql", "mongodb")

PORTS = {"postgresql": 5432, "mysql": 3306, "mongodb": 27017}


def endpoint(dialect: str, obj: str) -> Any:
    from tests.typed_fidelity_helpers import (
        mongo_endpoint,
        mysql_endpoint,
        pg_endpoint,
    )

    return {
        "postgresql": pg_endpoint,
        "mysql": mysql_endpoint,
        "mongodb": mongo_endpoint,
    }[dialect](obj)


def reachable(dialect: str) -> bool:
    return L.reachable("localhost", PORTS[dialect])


def seed(dialect: str, obj: str, rows: int, *, start: int = 1) -> None:
    if dialect == "postgresql":
        L.seed_pg_scale(obj, rows, start=start)
    elif dialect == "mysql":
        L.seed_mysql_scale(obj, rows, start=start)
    else:
        L.seed_mongo_scale(obj, rows, start=start)


def append(dialect: str, obj: str, rows: int, *, start: int) -> None:
    if dialect == "postgresql":
        L.pg_append_scale(obj, rows, start=start)
    elif dialect == "mysql":
        L.mysql_append_scale(obj, rows, start=start)
    else:
        client = L.mongo_client()
        try:
            client[L.MONGO_DB][obj].insert_many(
                [L.mongo_doc(i) for i in range(start, start + rows)], ordered=False
            )
        finally:
            client.close()


def count(dialect: str, obj: str, where: str = "") -> int:
    if dialect == "postgresql":
        return L.pg_count(obj, where)
    if dialect == "mysql":
        return L.mysql_count(obj, where)
    return L.mongo_count(obj)


def projection(dialect: str, obj: str, cols: Sequence[str], *, order: str = "id",
               where: str = "") -> list[tuple]:
    if dialect == "postgresql":
        return L.pg_projection(obj, cols, order=order, where=where)
    if dialect == "mysql":
        return L.mysql_projection(obj, cols, order=order, where=where)
    return L.mongo_projection(obj, cols, order=order)


def drop(dialect: str, obj: str) -> None:
    if dialect == "postgresql":
        L.pg_drop(obj)
    elif dialect == "mysql":
        L.mysql_drop(obj)
    else:
        L.mongo_drop(obj)


def columns(dialect: str, obj: str) -> set[str]:
    if dialect == "postgresql":
        return L.pg_columns(obj)
    if dialect == "mysql":
        return L.mysql_columns(obj)
    client = L.mongo_client()
    try:
        doc = client[L.MONGO_DB][obj].find_one() or {}
        return set(doc.keys())
    finally:
        client.close()


def omitted_columns(dialect: str) -> list[str]:
    """Source columns the store mints itself and the route declares omitted."""
    return ["_id"] if dialect == "mongodb" else []
