"""Schema-drift handling — add missing columns to existing destination tables.

Used by SQLAlchemy-based writers and by native SQL writers to safely evolve
a destination schema when the source introduces new columns (backfill mode).
"""

from __future__ import annotations

from typing import Any


def add_missing_columns(
    engine: Any,
    table_name: str,
    schema: str | None,
    target_cols: list[str],
    sa_col_types: dict[str, Any],
    *,
    backfill: bool = False,
    connection: Any | None = None,
) -> list[str]:
    """Return DDL statements for any columns that need to be added.

    If ``backfill`` is False no changes are made.  When True, existing tables
    are inspected and ``ALTER TABLE ADD COLUMN`` statements are issued for each
    missing column.  Statements are idempotent: ``IF NOT EXISTS`` is used when the
    dialect supports it, and "already exists" errors are swallowed so concurrent
    or resume runs do not fail.  Returns the list of DDL statements executed.
    """
    if not backfill:
        return []

    import sqlalchemy as sa

    inspector = sa.inspect(engine)
    if not inspector.has_table(table_name, schema=schema):
        return []

    existing = {c["name"] for c in inspector.get_columns(table_name, schema=schema)}
    missing = [c for c in target_cols if c not in existing]
    if not missing:
        return []

    dialect = engine.dialect
    dialect_name = getattr(dialect, "name", "")
    keyword = "ADD COLUMN" if dialect_name not in ("mssql", "oracle", "sybase") else "ADD"
    supports_if_not_exists = dialect_name in {"postgresql", "duckdb"}
    # SQLite rejects "ADD COLUMN IF NOT EXISTS" (syntax error near EXISTS).
    if_not_exists = " IF NOT EXISTS" if supports_if_not_exists else ""
    log: list[str] = []
    quoted_schema = f'"{schema}"' if schema else None

    def _column_exists_error(exc: Exception) -> bool:
        text = str(exc).lower()
        return any(
            phrase in text
            for phrase in (
                "already exists",
                "duplicate column",
                "column already exists",
                "duplicate key",
            )
        )

    def _run(conn: Any) -> None:
        for col in missing:
            sa_type = sa_col_types.get(col)
            if sa_type is None:
                continue
            col_ddl = str(
                sa.schema.CreateColumn(sa.Column(col, sa_type, quote=True)).compile(dialect=dialect)
            )
            if quoted_schema:
                qualified = f"{quoted_schema}.\"{table_name}\""
            else:
                qualified = f'"{table_name}"'
            alter = f"ALTER TABLE {qualified} {keyword}{if_not_exists} {col_ddl}"
            try:
                conn.execute(sa.text(alter))
                conn.commit()
                log.append(alter)
            except Exception as exc:
                if _column_exists_error(exc):
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    continue
                raise

    if connection is None:
        with engine.connect() as conn:
            _run(conn)
    else:
        _run(connection)
    return log
