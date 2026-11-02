"""SQL Server IDENTITY_INSERT session for loads that carry source keys.

Extracted from ``writer_common`` (Phase F8 size freeze). The opt-in is per
session and per table, so it must be released again — a pooled connection left
with IDENTITY_INSERT ON makes the next table's load fail naming the wrong table.
"""

from __future__ import annotations

import logging
from typing import Any

from connectors.sql_identifiers import quote_table_ref

logger = logging.getLogger(__name__)


class IdentityInsertSession:
    """SQL Server session that is allowed to write its own identity values.

    Migrating a table means carrying the *keys*, not letting the destination
    mint new ones: children reference the source's values. SQL Server refuses
    an explicit value for an ``IDENTITY`` column unless the session opts in,
    and the opt-in is per session and per table, so it must be released again —
    a pooled connection left with ``IDENTITY_INSERT`` ON makes the *next*
    table's load fail with an error that names the wrong table.
    """

    def __init__(self, conn: Any, qualified: str) -> None:
        self._conn = conn
        self._qualified = qualified
        self._open = False

    def open(self) -> None:
        import sqlalchemy as sa

        self._conn.execute(sa.text(f"SET IDENTITY_INSERT {self._qualified} ON"))
        self._open = True

    def close(self) -> None:
        if not self._open:
            return
        import sqlalchemy as sa

        self._open = False
        try:
            self._conn.execute(sa.text(f"SET IDENTITY_INSERT {self._qualified} OFF"))
        except Exception as exc:  # pragma: no cover - connection already dead
            logger.warning(
                "could not release IDENTITY_INSERT on %s: %s", self._qualified, exc
            )


def sqlserver_identity_columns(conn: Any, schema: str, table: str) -> set[str]:
    """Identity columns of one SQL Server table, read from ``sys.identity_columns``."""
    import sqlalchemy as sa

    rows = conn.execute(
        sa.text(
            "SELECT c.name FROM sys.identity_columns c "
            "JOIN sys.tables t ON t.object_id = c.object_id "
            "JOIN sys.schemas s ON s.schema_id = t.schema_id "
            "WHERE t.name = :t AND s.name = COALESCE(:s, SCHEMA_NAME())"
        ),
        {"t": str(table), "s": str(schema) if schema else None},
    ).fetchall()
    return {str(r[0]) for r in rows}


def begin_identity_insert(
    conn: Any,
    *,
    dialect_name: str,
    schema: str,
    table: str,
    target_cols: list[str],
) -> IdentityInsertSession | None:
    """Open an identity-insert session when the load supplies the key itself.

    Returns ``None`` when the destination is not SQL Server, when the table has
    no identity column, or when no identity column is among the mapped columns
    (the generator fills it and there is nothing to override). Probe failures
    return ``None`` too: the write then fails loudly on the first row rather
    than silently letting the destination renumber the keys.
    """
    if (dialect_name or "").lower() not in {"mssql", "sqlserver"}:
        return None
    try:
        identity_cols = sqlserver_identity_columns(conn, schema, table)
    except Exception as exc:
        logger.debug("identity column probe failed for %s: %s", table, exc)
        return None
    folded = {c.casefold() for c in identity_cols}
    if not any(str(c).casefold() in folded for c in target_cols):
        return None
    session = IdentityInsertSession(
        conn, quote_table_ref(table, schema=schema, dialect="sqlserver")
    )
    session.open()
    return session
