"""Gate-8 sample reader for a generic SQL destination (backlog GSQL-G8-1).

A ``generic_sql`` destination is a SQLAlchemy URL to an engine with no
dedicated reader in ``services.target_sample``. With no reader, Gate-8 failed
every run after a correct write ("no sample reader is wired") and the failure
path's undo ran against the table. This reader selects the requested columns
through the same SQLAlchemy engine the writer used, keyed and ordered the way
the dedicated readers are.
"""

from __future__ import annotations

from typing import Any


def read_generic_sql_sample(
    dest: dict[str, Any],
    *,
    schema: str,
    table_name: str,
    cols: list[str],
    keys: list[Any],
    keys_has_null: bool,
    sort_key: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    import sqlalchemy as sa

    from connectors.generic_sql import get_sqlalchemy_engine
    from services.engine_pool import release_engine

    engine = get_sqlalchemy_engine(dest)
    try:
        quote = engine.dialect.identifier_preparer.quote
        sch = (schema or dest.get("schema") or "").strip()
        table_ref = (f"{quote(sch)}." if sch else "") + quote(table_name)
        col_sql = "*" if cols == ["*"] else ", ".join(quote(c) for c in cols)
        params: dict[str, Any] = {}
        where = ""
        live_keys = [k for k in keys if k is not None]
        if sort_key and (live_keys or keys_has_null):
            parts = []
            if live_keys:
                names = []
                for i, key in enumerate(live_keys):
                    params[f"k{i}"] = str(key)
                    names.append(f":k{i}")
                # Keys arrive as wire text: compare the stored key as text too.
                parts.append(f"CAST({quote(sort_key)} AS VARCHAR(255)) IN ({', '.join(names)})")
            if keys_has_null:
                parts.append(f"{quote(sort_key)} IS NULL")
            where = " WHERE " + " OR ".join(parts)
        order = f" ORDER BY {quote(sort_key)}" if sort_key else ""
        stmt = sa.text(f"SELECT {col_sql} FROM {table_ref}{where}{order}")  # nosec B608
        with engine.connect() as conn:
            result = conn.execute(stmt, params)
            names = list(cols) if cols != ["*"] else list(result.keys())
            rows = result.fetchmany(max(1, int(limit or 50)))
        return [dict(zip(names, row)) for row in rows]
    finally:
        release_engine(engine)
