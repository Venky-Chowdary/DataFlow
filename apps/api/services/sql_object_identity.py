"""Canonical destination-object identity for case-folding SQL engines.

Oracle, Snowflake and DB2 fold *unquoted* identifiers to a single case but keep
quoted ones verbatim, so ``users`` and ``"users"`` can both exist and are
different physical tables. Every step that touches a destination object —
existence probe, CREATE decision, write, read-back, reconcile — must agree on
which one it means.

They did not. A dialect fold in one place and a verbatim spelling in another let
an Oracle append create a *second* table under the folded name while the
operator's table sat beside it: rows landed in the wrong object and Gate-8 still
passed, because both sides were self-consistent within a single run.

This module is the single resolver: ask the catalog which objects exist, prefer
the exact spelling the caller asked for, fall back to a unique case-insensitive
match, and report ``exists=False`` only when the catalog really has no such
object. Nothing here invents a name — an unreadable catalog returns the caller's
own spelling with ``resolved=False`` so callers can fail closed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ObjectIdentity:
    """Stored spelling of a destination object."""

    schema: str | None
    table: str
    exists: bool
    #: False when the catalog could not be read — never treat as "absent".
    resolved: bool = True
    #: Requested column name -> stored column name (only for existing objects).
    columns: dict[str, str] = field(default_factory=dict)


def _pick(candidates: list[str], wanted: str) -> str | None:
    """Exact spelling first, then a unique case-insensitive match."""
    if wanted in candidates:
        return wanted
    matches = [c for c in candidates if c.lower() == wanted.lower()]
    if len(matches) == 1:
        return matches[0]
    return None


def _dialect_name(engine: Any) -> str:
    return str(getattr(getattr(engine, "dialect", None), "name", "") or "").lower()


def _oracle_identity(
    engine: Any,
    want_table: str,
    want_schema: str | None,
    columns: list[str] | None,
) -> ObjectIdentity:
    """Read Oracle's catalog directly.

    SQLAlchemy's Oracle inspector *denormalizes* names: an all-upper stored name
    comes back lower-cased, so ``USERS`` and a quoted ``users`` are both
    reported as ``users`` and the two objects become indistinguishable. The
    catalog keeps the real spelling.
    """
    import sqlalchemy as sa

    sql = (
        "SELECT owner, table_name FROM all_tables WHERE UPPER(table_name) = :t"
        " AND (:s IS NULL OR UPPER(owner) = :s)"
        " UNION ALL "
        "SELECT owner, view_name FROM all_views WHERE UPPER(view_name) = :t"
        " AND (:s IS NULL OR UPPER(owner) = :s)"
    )
    params = {
        "t": want_table.upper(),
        "s": want_schema.upper() if want_schema else None,
    }
    try:
        with _connection(engine) as conn:
            rows = conn.execute(sa.text(sql), params).fetchall()
            found = [(str(r[0]), str(r[1])) for r in rows]
            if not found:
                return ObjectIdentity(want_schema, want_table, False)
            exact = [f for f in found if f[1] == want_table]
            owner, stored_table = exact[0] if exact else found[0]
            col_map: dict[str, str] = {}
            if columns:
                stored_cols = [
                    str(r[0])
                    for r in conn.execute(
                        sa.text(
                            "SELECT column_name FROM all_tab_columns "
                            "WHERE table_name = :t AND owner = :o"
                        ),
                        {"t": stored_table, "o": owner},
                    ).fetchall()
                ]
                for want in columns:
                    hit = _pick(stored_cols, str(want or ""))
                    if hit:
                        col_map[str(want)] = hit
            return ObjectIdentity(owner, stored_table, True, columns=col_map)
    except Exception:
        return ObjectIdentity(want_schema, want_table, False, resolved=False)


def _connection(engine: Any) -> Any:
    """Context manager yielding a connection for an Engine or Connection."""
    from contextlib import nullcontext

    # An Engine opens a session; a live Connection is reused as-is.
    if hasattr(engine, "connect"):
        return engine.connect()
    return nullcontext(engine)


def resolve_object_identity(
    engine: Any,
    table: str,
    schema: str | None = None,
    *,
    columns: list[str] | None = None,
) -> ObjectIdentity:
    """Resolve ``schema.table`` (and optionally columns) against the catalog."""
    import sqlalchemy as sa

    want_table = str(table or "")
    want_schema = str(schema).strip() if schema else None
    if not want_table:
        return ObjectIdentity(want_schema, want_table, False)

    if _dialect_name(engine) == "oracle":
        return _oracle_identity(engine, want_table, want_schema, columns)

    try:
        insp = sa.inspect(engine)
    except Exception:
        return ObjectIdentity(want_schema, want_table, False, resolved=False)

    stored_schema = want_schema
    if want_schema:
        try:
            stored_schema = _pick(list(insp.get_schema_names()), want_schema) or want_schema
        except Exception:
            stored_schema = want_schema

    try:
        names = list(insp.get_table_names(schema=stored_schema))
        try:
            names += list(insp.get_view_names(schema=stored_schema))
        except Exception:
            pass
    except Exception:
        return ObjectIdentity(stored_schema, want_table, False, resolved=False)

    stored_table = _pick(names, want_table)
    if not stored_table:
        return ObjectIdentity(stored_schema, want_table, False)

    col_map: dict[str, str] = {}
    if columns:
        try:
            stored_cols = [
                str(c.get("name"))
                for c in insp.get_columns(stored_table, schema=stored_schema)
            ]
        except Exception:
            stored_cols = []
        for want in columns:
            hit = _pick(stored_cols, str(want or ""))
            if hit:
                col_map[str(want)] = hit

    return ObjectIdentity(stored_schema, stored_table, True, columns=col_map)
