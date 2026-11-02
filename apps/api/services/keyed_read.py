"""Reading one batch back by key, when the destination typed the key differently.

Gate-8 re-reads a written batch by binding the *source* key values, whose Python
type follows the source column. The destination often stores that key as text —
routine for create-new on vector and document targets — and PostgreSQL rejects
``text = integer`` outright rather than coercing. The read then fails and a
correct write is reported as an unreadable sample.

Shared by every PostgreSQL-family read-back so the typed attempt and its text
fallback cannot drift apart between them.
"""

from __future__ import annotations

from typing import Any


def is_operand_type_mismatch(exc: Exception) -> bool:
    """True when PostgreSQL refused a comparison for want of a cast."""
    text = str(exc).lower()
    return (
        "operator does not exist" in text
        or "could not identify an equality operator" in text
    )


def execute_keyed_read(
    conn: Any, cur: Any, sql: str, key_col: str, keys: list[Any], tail: tuple
) -> None:
    """Run a keyed read, retrying as text when PostgreSQL wants an explicit cast.

    ``sql`` carries a ``{key}`` placeholder for the key expression. The typed
    form is tried first so the key index still serves the common case; the cast
    only pays a scan on the bounded sample that would otherwise be lost.
    """
    try:
        cur.execute(sql.format(key=key_col), (*keys, *tail))
    except Exception as exc:
        if not is_operand_type_mismatch(exc):
            raise
        conn.rollback()
        cur.execute(
            sql.format(key=f"{key_col}::text"), (*[str(k) for k in keys], *tail)
        )
