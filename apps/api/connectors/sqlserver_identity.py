"""SQL Server IDENTITY_INSERT opt-in for loads that supply their own keys.

Lives outside ``writer_common`` so that module stays inside its line budget.
``writer_common`` re-exports these names; callers and tests keep that path.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger("connectors.writer_common")


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
) -> Any:
    """Open an identity-insert session when the load supplies the key itself.

    Returns ``None`` when the destination is not SQL Server, when the table has
    no identity column, or when no identity column is among the mapped columns
    (the generator fills it and there is nothing to override). Probe failures
    return ``None`` too: the write then fails loudly on the first row rather
    than silently letting the destination renumber the keys.

    The column probe is read from ``writer_common`` at call time so a test can
    replace that name without this module binding the original function.
    """
    from connectors import writer_common

    if (dialect_name or "").lower() not in {"mssql", "sqlserver"}:
        return None
    try:
        identity_cols = writer_common.sqlserver_identity_columns(conn, schema, table)
    except Exception as exc:
        logger.debug("identity column probe failed for %s: %s", table, exc)
        return None
    folded = {c.casefold() for c in identity_cols}
    if not any(str(c).casefold() in folded for c in target_cols):
        return None
    session = writer_common.IdentityInsertSession(
        conn, writer_common.quote_table_ref(table, schema=schema, dialect="sqlserver")
    )
    session.open()
    return session
