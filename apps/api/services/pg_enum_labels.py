"""PostgreSQL ``pg_enum`` label lookup for enum-typed columns.

Split out of ``services.schema_introspect`` (at its LOC budget); that module
re-exports the name so existing imports keep working.
"""

from __future__ import annotations

from typing import Any


def _pg_fetch_enum_labels(cur: Any, type_oids: list[int]) -> dict[int, list[str]]:
    """Load ``pg_enum`` labels ordered by ``enumsortorder`` (Postgres catalog)."""
    if not type_oids:
        return {}
    # Deduplicate while preserving order for stable ANY/IN binds.
    uniq: list[int] = []
    seen: set[int] = set()
    for oid in type_oids:
        if oid in seen:
            continue
        seen.add(oid)
        uniq.append(int(oid))
    placeholders = ",".join(["%s"] * len(uniq))
    cur.execute(
        f"""
        SELECT enumtypid, enumlabel
        FROM pg_catalog.pg_enum
        WHERE enumtypid IN ({placeholders})
        ORDER BY enumtypid, enumsortorder
        """,
        tuple(uniq),
    )
    out: dict[int, list[str]] = {}
    for typoid, label in cur.fetchall():
        out.setdefault(int(typoid), []).append(str(label))
    return out
