"""D101 — upsert into an existing table without a unique key.

An upsert writer needs a unique constraint on the upsert columns to merge on.
When the destination table already exists but has no such key, the writers
used to silently ``CREATE UNIQUE INDEX uidx_…`` at run time — undisclosed DDL
on a user table that fails raw when duplicates exist. Validate must inspect,
count the duplicates, disclose the planned DDL, and require explicit approval.
"""
from __future__ import annotations

from typing import Any

#: Longest a plan or a replay spends proving an existing destination's upsert
#: keys unique (D187: a Snowflake reflection + GROUP BY ran 25 s and killed a
#: second-run upsert plan). Unfinished is ``ran=False``, never a clean 0.
DEST_DUPLICATE_KEY_STEP_SECONDS = 8.0


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


def _rows_a_keyed_write_compares(cols: list[Any]) -> Any:
    """Rows whose key can collide in a keyed write: every key column is set.

    The engines DataFlow builds a merge index on (PostgreSQL ON CONFLICT,
    MySQL/MariaDB ON DUPLICATE KEY — ``dest_upsert_needs_unique_index``) never
    compare a key that holds a NULL, so many NULL keys are legal there. MERGE
    engines (Snowflake, SQL Server, Oracle, BigQuery, Databricks) build no
    index and key the write by equality; a NULL target key matches no source
    row (a NULL source key is refused before the write, D46). So for every
    engine a NULL-key row is not a duplicate of anything the write can hit.
    """
    import sqlalchemy as sa

    return sa.and_(*[c.is_not(None) for c in cols])


def probe_dest_duplicate_keys(
    cfg: dict[str, Any],
    table: str,
    columns: list[str],
    *,
    seconds: float = DEST_DUPLICATE_KEY_STEP_SECONDS,
) -> dict[str, Any]:
    """:func:`count_dest_duplicate_keys`, bounded — the one probe plan and replay run.

    A probe that times out or fails answers ``ran=False`` with the reason; the
    keyed write still enforces the key.
    """
    from src.ai.copilot.tool_deadline import bounded_step

    try:
        run = bounded_step(
            f"counting duplicate upsert keys on destination {table}",
            lambda: count_dest_duplicate_keys(dict(cfg or {}), table, list(columns)),
            seconds=seconds,
            conn=cfg,
            role="destination",
        )
        if run.error is not None:
            raise run.error
    except Exception as exc:  # noqa: BLE001 — an optional proof never decides alone
        return {
            "ran": False, "duplicate_keys": 0, "duplicate_rows": 0, "samples": [],
            "message": f"duplicate-key probe failed: {exc}"[:300],
        }
    if run.timed_out:
        return {
            "ran": False, "duplicate_keys": 0, "duplicate_rows": 0, "samples": [],
            "message": (
                "duplicate-key probe did not finish within the plan's step budget "
                f"({seconds:g}s at most); Execute's keyed write still enforces the key"
            ),
        }
    return run.value


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
            "message": (
                f"destination catalog could not be read for '{table}'"
                if not ident.resolved
                else f"destination object '{table}' not found"
            ),
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
    stmt = stmt.where(_rows_a_keyed_write_compares(cols))
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

    The same rule as Validate (:func:`dest_unique_covering_columns`): a primary
    key or unique constraint / index on exactly this column set — the arbiter an
    ``ON CONFLICT (cols)`` merge needs. Returns None when the inspection cannot
    run — caller treats that as "unproven", not as "enforced". A table that
    does not exist enforces nothing (``False``).
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
        if not ident.resolved:
            return None  # catalog unreadable: unproven, not "no constraint" (D266)
        if not ident.exists:
            return False
        insp = sa.inspect(engine)
        unique_keys = [
            {"columns": list(uc.get("column_names") or [])}
            for uc in insp.get_unique_constraints(ident.table, schema=ident.schema)
        ] + [
            {"columns": list(idx.get("column_names") or [])}
            for idx in insp.get_indexes(ident.table, schema=ident.schema)
            if idx.get("unique")
        ]
        pk = insp.get_pk_constraint(ident.table, schema=ident.schema)
        return dest_unique_covering_columns(
            list(pk.get("constrained_columns") or []), unique_keys, list(columns)
        )
    except Exception:  # noqa: BLE001 — unproven, not "enforced"
        return None


def existing_merge_target_lacks_unique_key(
    cfg: dict[str, Any],
    table: str,
    columns: list[str],
    dialect: str,
) -> bool:
    """True only when the table exists and provably has no unique key on ``columns``.

    A server-side staged merge (``ON CONFLICT`` / ``ON DUPLICATE KEY``) into such
    a table fails raw on PostgreSQL / SQLite and silently duplicates on MySQL.
    The row writer is the one path that discloses — and, once approved, runs —
    the ``CREATE UNIQUE INDEX`` the merge needs (D101). A new table, or a
    catalog that cannot be read, answers False.
    """
    if not [c for c in columns if str(c or "").strip()]:
        return False
    return destination_unique_covering(cfg, table, columns, dialect) is False and (
        _table_exists(cfg, table)
    )


def _table_exists(cfg: dict[str, Any], table: str) -> bool:
    from connectors.generic_sql import _engine
    from connectors.sql_identifiers import split_qualified_table
    from services.sql_object_identity import resolve_object_identity

    try:
        schema, name = split_qualified_table(table, (cfg.get("schema") or "").strip() or None)
        ident = resolve_object_identity(_engine(cfg), name, schema)
        return bool(ident.resolved and ident.exists)
    except Exception:  # noqa: BLE001 — unknown is not "exists"
        return False
