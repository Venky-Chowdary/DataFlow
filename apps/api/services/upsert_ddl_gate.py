"""D101 — upsert into an existing table without a unique key.

An upsert writer needs a unique constraint on the upsert columns to merge on.
When the destination table already exists but has no such key, the writers
used to silently ``CREATE UNIQUE INDEX uidx_…`` at run time — undisclosed DDL
on a user table that fails raw when duplicates exist. Validate must inspect,
count the duplicates, disclose the planned DDL, and require explicit approval.
"""
from __future__ import annotations

from typing import Any


def dest_unique_covering_columns(
    destination_pk_columns: list[str] | None,
    destination_unique_keys: list[dict[str, Any]] | None,
    key_columns: list[str],
) -> bool:
    """True when the destination already enforces uniqueness on all key cols."""
    from services.destination_key_collision_probe import (
        destination_enforces_columns,
    )

    return destination_enforces_columns(
        list(key_columns),
        destination_pk_columns=destination_pk_columns,
        destination_unique_keys=destination_unique_keys,
    )


def count_dest_duplicate_keys(
    cfg: dict[str, Any],
    table: str,
    columns: list[str],
    limit: int = 5,
) -> dict[str, Any]:
    """Count duplicate key tuples on an existing destination table.

    Returns ``{"ran": bool, "duplicate_keys": int, "duplicate_rows": int,
    "samples": [...], "message": str}``. A probe that cannot run reports
    ``ran=False`` — never invents a clean zero.
    """
    import sqlalchemy as sa

    from connectors.generic_sql import _engine
    from connectors.sql_identifiers import split_qualified_table
    from services.sql_object_identity import resolve_object_identity

    engine = _engine(cfg)
    schema, table = split_qualified_table(
        table, (cfg.get("schema") or "").strip() or None
    )
    ident = resolve_object_identity(engine, table, schema, columns=list(columns))
    if not ident.exists:
        return {
            "ran": False,
            "duplicate_keys": 0,
            "duplicate_rows": 0,
            "samples": [],
            "message": f"destination object '{table}' not found",
        }
    resolved = [ident.columns.get(c) for c in columns]
    if any(r is None for r in resolved):
        return {
            "ran": False,
            "duplicate_keys": 0,
            "duplicate_rows": 0,
            "samples": [],
            "message": (
                f"upsert key column(s) {sorted(set(columns) - set(ident.columns))} "
                f"not found on destination '{table}'"
            ),
        }
    table_q = sa.sql.quoted_name(ident.table, True)
    schema_q = sa.sql.quoted_name(ident.schema, True) if ident.schema else None
    tbl = sa.table(table_q, schema=schema_q)
    cols = [sa.column(sa.sql.quoted_name(str(r), True)) for r in resolved]
    cnt = sa.func.count().label("_cnt")
    stmt = (
        sa.select(*cols, cnt)
        .select_from(tbl)
        .group_by(*cols)
        .having(cnt > 1)
    )
    with engine.connect() as conn:
        rows = conn.execute(stmt).fetchall()
    dup_keys = len(rows)
    dup_rows = sum(int(r[len(cols)]) for r in rows)
    samples = [
        {
            "values": {
                columns[i]: (None if r[i] is None else str(r[i]))
                for i in range(len(columns))
            },
            "count": int(r[len(cols)]),
        }
        for r in rows[:limit]
    ]
    return {
        "ran": True,
        "duplicate_keys": dup_keys,
        "duplicate_rows": dup_rows,
        "samples": samples,
        "message": (
            f"{dup_keys} duplicate key value(s) across {dup_rows} row(s)"
            if dup_keys
            else "no duplicate key values"
        ),
    }


def destination_unique_covering(
    cfg: dict[str, Any],
    table: str,
    columns: list[str],
    dialect: str,
) -> bool | None:
    """Live check: does the destination already enforce uniqueness on columns?

    Returns None when the inspection cannot run — caller treats that as
    "unproven", not as "enforced".
    """
    import sqlalchemy as sa

    from connectors.generic_sql import _engine
    from connectors.sql_identifiers import split_qualified_table
    from services.sql_object_identity import resolve_object_identity

    try:
        engine = _engine(cfg)
        schema, table = split_qualified_table(
            table, (cfg.get("schema") or "").strip() or None
        )
        ident = resolve_object_identity(engine, table, schema)
        if not ident.exists:
            return False
        insp = sa.inspect(engine)
        want = {c.lower() for c in columns}
        for uc in insp.get_unique_constraints(
            ident.table, schema=ident.schema
        ):
            cols = {str(c).lower() for c in (uc.get("column_names") or [])}
            if cols and cols <= want or want <= cols:
                if want <= cols:
                    return True
        for idx in insp.get_indexes(ident.table, schema=ident.schema):
            if not idx.get("unique"):
                continue
            cols = {str(c).lower() for c in (idx.get("column_names") or [])}
            if want <= cols:
                return True
        pk = insp.get_pk_constraint(ident.table, schema=ident.schema)
        pk_cols = {
            str(c).lower() for c in (pk.get("constrained_columns") or [])
        }
        return bool(pk_cols) and want <= pk_cols
    except Exception:  # noqa: BLE001 — unproven, not "enforced"
        return None
