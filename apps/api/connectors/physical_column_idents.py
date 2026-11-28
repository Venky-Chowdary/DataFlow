"""Stored spelling of mapped destination columns on case-folding SQL engines.

Extracted from :mod:`connectors.generic_sql` (module size budget), which
re-exports :func:`_resolve_physical_column_idents`. The catalog read stays in
``generic_sql._stored_column_spellings`` and is looked up there at call time.
"""

from __future__ import annotations

from typing import Any


def _resolve_physical_column_idents(
    engine: Any, table: str, schema: str | None, columns: list[str]
) -> dict[str, str]:
    """Stored spelling of each destination column on case-folding engines.

    Every statement here quotes its identifiers, so on Oracle/Snowflake/DB2 a
    mapped ``label`` is asked for as ``"label"`` — a *different* column from the
    ``LABEL`` an ordinary ``CREATE TABLE`` produced. Appending into a table the
    client created therefore failed with ORA-00904 on a column that is plainly
    there, and drift widening emitted ``MODIFY ("name" CLOB)`` against ``NAME``.

    Only columns whose stored spelling actually differs are returned; a column
    the catalog does not have is left alone so ADD COLUMN still creates it.
    """
    dialect = str(getattr(getattr(engine, "dialect", None), "name", "") or "").lower()
    from connectors.generic_sql import _FOLDING_DIALECTS

    if dialect not in _FOLDING_DIALECTS or not columns:
        return {}
    from connectors import generic_sql

    stored = generic_sql._stored_column_spellings(engine, engine, table, schema)
    if not stored:
        return {}
    from services.dialect_profiles import fold_identifier

    # A column drift is about to add must follow the convention of the table it
    # joins: adding a quoted lower-case column beside folded ones leaves a
    # column the client's own SELECT cannot see.
    folded_table = all(name == name.upper() for name in stored.values())
    renames: dict[str, str] = {}
    spellings = set(stored.values())
    landed: dict[str, list[str]] = {}
    for col in dict.fromkeys(columns):  # a key passed twice is one column
        # The exact stored spelling first (a deliberately quoted column), then
        # the case-folded match the stored-name rule gives this engine.
        hit = col if col in spellings else stored.get(str(col).casefold())
        if hit is None and folded_table:
            hit = fold_identifier(dialect, col)
        landed.setdefault(hit or col, []).append(col)
        if hit and hit != col:
            renames[col] = hit
    collisions = [(name, cols) for name, cols in landed.items() if len(cols) > 1]
    if collisions:
        # Two mapped columns bound to one stored column: refused by name before
        # any write, never the engine's own duplicate-column error.
        from services.target_name_collision import target_collision_message

        raise ValueError(target_collision_message(collisions))
    return renames
