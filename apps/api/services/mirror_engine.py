"""Inferred-delete (mirror) support for full-refresh SQL transfers.

A mirror sync keeps the destination table in sync with the source:
- source rows are upserted and marked active
- destination rows no longer in the source are soft-deleted
- rows that reappear after being deleted are reactivated.

This is the Fivetran-style "inferred deletes via full re-sync" behavior:
when the source cannot emit delete events, we infer them by comparing the
complete source snapshot to the destination.
"""

from __future__ import annotations

from typing import Any

SOFT_DELETE_COLUMN = "_deleted"
_KEY_SEP = "\x1f"


def _qualified_name(table: str, schema: str | None) -> str:
    from connectors.writer_common import quote_sql_identifier

    table_quoted = quote_sql_identifier(table)
    schema_quoted = quote_sql_identifier(schema) if schema else None
    return f"{schema_quoted}.{table_quoted}" if schema_quoted else table_quoted


def _key_value(record: dict[str, Any], column: str) -> str:
    from services.value_serializer import cell_to_string

    return cell_to_string(record.get(column))


def _compose_key(record: dict[str, Any], columns: list[str]) -> str:
    return _KEY_SEP.join(_key_value(record, c) for c in columns)


def _pk_or_clause(columns: list[str], keys: list[str], *, prefix: str) -> tuple[str, dict[str, Any]]:
    from connectors.writer_common import quote_sql_identifier

    if not keys or not columns:
        return "1=0", {}
    quoted = [quote_sql_identifier(c) for c in columns]
    clauses: list[str] = []
    params: dict[str, Any] = {}
    for i, key in enumerate(keys):
        parts = key.split(_KEY_SEP)
        if len(parts) != len(columns):
            continue
        ands = []
        for j, col_q in enumerate(quoted):
            pname = f"{prefix}{i}_{j}"
            ands.append(f"{col_q} = :{pname}")
            params[pname] = parts[j]
        if ands:
            clauses.append("(" + " AND ".join(ands) + ")")
    if not clauses:
        return "1=0", {}
    return "(" + " OR ".join(clauses) + ")", params


def _target_columns(
    records_columns: list[str],
    mappings: list[dict[str, Any]] | None,
    source_schema: dict[str, str] | None,
) -> list[str]:
    """Return the ordered target column names for checksum comparison."""
    from connectors.writer_common import resolve_target_columns

    if mappings:
        target_cols, _ = resolve_target_columns(mappings, source_schema or {}, preserve_case=True)
        return target_cols
    return records_columns


def _ensure_soft_delete_column(
    conn: Any,
    qualified: str,
    soft_delete_column: str,
) -> None:
    import sqlalchemy as sa

    from connectors.writer_common import quote_sql_identifier

    col_quoted = quote_sql_identifier(soft_delete_column)
    # Best-effort add.  Most dialects accept BOOLEAN DEFAULT FALSE; if they do
    # not, the exception is ignored because the column likely already exists.
    for ddl in (
        f"ALTER TABLE {qualified} ADD COLUMN {col_quoted} BOOLEAN DEFAULT FALSE",
        f"ALTER TABLE {qualified} ADD COLUMN {col_quoted} BOOLEAN",
    ):
        try:
            conn.execute(sa.text(ddl))
            conn.commit()
            return
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass


def _update_deleted_batch(
    conn: Any,
    qualified: str,
    pk_columns: list[str],
    activate_keys: list[str],
    delete_keys: list[str],
    soft_delete_column: str,
) -> tuple[int, int]:
    import sqlalchemy as sa

    from connectors.writer_common import quote_sql_identifier

    col_quoted = quote_sql_identifier(soft_delete_column)
    activated = 0
    deactivated = 0

    if activate_keys:
        where_keys, params = _pk_or_clause(pk_columns, activate_keys, prefix="a")
        stmt = f"UPDATE {qualified} SET {col_quoted} = FALSE WHERE {where_keys}"
        try:
            result = conn.execute(sa.text(stmt), params)
            conn.commit()
            activated = result.rowcount or 0
        except Exception:
            conn.rollback()

    if delete_keys:
        where_keys, params = _pk_or_clause(pk_columns, delete_keys, prefix="d")
        stmt = (
            f"UPDATE {qualified} SET {col_quoted} = TRUE "
            f"WHERE {where_keys} "
            f"AND ({col_quoted} IS NULL OR {col_quoted} = FALSE)"
        )
        try:
            result = conn.execute(sa.text(stmt), params)
            conn.commit()
            deactivated = result.rowcount or 0
        except Exception:
            conn.rollback()

    return activated, deactivated


def _compute_active_checksum(
    conn: Any,
    qualified: str,
    target_cols: list[str],
    soft_delete_column: str,
    batch_size: int,
) -> tuple[int, str]:
    """Read active rows and return an order-independent checksum."""
    import sqlalchemy as sa

    from connectors.writer_common import quote_sql_identifier
    from services.reconciliation import canonical_checksum

    col_quoted = quote_sql_identifier(soft_delete_column)
    cols_quoted = ",".join(quote_sql_identifier(c) for c in target_cols)
    active_rows: list[dict[str, Any]] = []
    active_count = 0
    offset = 0
    while True:
        sql = (
            f"SELECT {cols_quoted} FROM {qualified} "
            f"WHERE {col_quoted} IS NOT TRUE "
            f"LIMIT {batch_size} OFFSET {offset}"
        )
        result = conn.execute(sa.text(sql))
        rows = result.fetchall()
        if not rows:
            break
        for row in rows:
            active_count += 1
            active_rows.append({c: row._mapping.get(c) for c in target_cols})
        if len(rows) < batch_size:
            break
        offset += batch_size

    checksum = canonical_checksum(active_rows, target_cols) if active_rows else ""
    return active_count, checksum


def apply_inferred_soft_deletes(
    endpoint: Any,
    records: list[dict[str, Any]],
    columns: list[str],
    schema: dict[str, str] | None,
    mappings: list[dict[str, Any]] | None,
    conflict_columns: list[str],
    *,
    soft_delete_column: str = SOFT_DELETE_COLUMN,
    batch_size: int = 10_000,
) -> dict[str, Any]:
    """Soft-delete destination rows that are no longer in ``records``.

    ``endpoint`` must be a database ``EndpointConfig`` for an SQLAlchemy-backed
    destination.  ``conflict_columns`` is the destination primary key.  This
    function expects the source rows to have already been upserted into the
    destination table.  It returns both the delete/reactivate counts and an
    active-row checksum that the reconciliation step can use instead of a
    full-table scan.
    """
    if not conflict_columns:
        raise ValueError("mirror sync requires a primary key / conflict column")

    pk_columns = [c for c in conflict_columns if c]
    if not pk_columns:
        raise ValueError("mirror sync requires a primary key / conflict column")

    # Map each destination PK column back to the source column used in mappings.
    pk_sources: list[str] = []
    for pk_target in pk_columns:
        pk_source = pk_target
        if mappings:
            for m in mappings:
                if (m.get("target") or m.get("source")) == pk_target:
                    src = m.get("source")
                    if src:
                        pk_source = src
                        break
        pk_sources.append(pk_source)

    source_keys = {
        _compose_key(r, pk_sources)
        for r in records
        if _compose_key(r, pk_sources) and not all(p == "" for p in _compose_key(r, pk_sources).split(_KEY_SEP))
    }
    if not source_keys:
        raise ValueError("mirror sync could not build a non-empty source key set from the primary key")

    from connectors.generic_sql import get_sql_schema, get_sqlalchemy_engine
    from src.transfer.adapters import resolve_connector_config

    cfg = resolve_connector_config(endpoint)
    table = endpoint.table or endpoint.collection or "dt_import"
    schema_name = get_sql_schema(cfg)
    qualified = _qualified_name(table, schema_name)

    import sqlalchemy as sa

    from connectors.writer_common import quote_sql_identifier

    target_cols = _target_columns(columns, mappings, schema)
    engine = get_sqlalchemy_engine(cfg)
    activated_total = 0
    deactivated_total = 0
    scanned = 0
    try:
        with engine.connect() as conn:
            _ensure_soft_delete_column(conn, qualified, soft_delete_column)

            pk_quoted = ", ".join(quote_sql_identifier(c) for c in pk_columns)
            order_by = ", ".join(quote_sql_identifier(c) for c in pk_columns)
            col_quoted = quote_sql_identifier(soft_delete_column)
            offset = 0
            while True:
                sql = (
                    f"SELECT {pk_quoted}, {col_quoted} FROM {qualified} "
                    f"ORDER BY {order_by} LIMIT {batch_size} OFFSET {offset}"
                )
                result = conn.execute(sa.text(sql))
                rows = result.fetchall()
                if not rows:
                    break
                scanned += len(rows)

                target_keys: set[str] = set()
                for row in rows:
                    pk_val = _compose_key(dict(row._mapping), pk_columns)
                    if pk_val and not all(p == "" for p in pk_val.split(_KEY_SEP)):
                        target_keys.add(pk_val)

                activate = [k for k in target_keys if k in source_keys]
                delete = [k for k in target_keys if k not in source_keys]
                a, d = _update_deleted_batch(
                    conn, qualified, pk_columns, activate, delete, soft_delete_column
                )
                activated_total += a
                deactivated_total += d

                if len(rows) < batch_size:
                    break
                offset += batch_size

            active_rows, active_checksum = _compute_active_checksum(
                conn, qualified, target_cols, soft_delete_column, batch_size
            )

    finally:
        engine.dispose()

    return {
        "soft_deleted": deactivated_total,
        "reactivated": activated_total,
        "rows_scanned": scanned,
        "active_rows": active_rows,
        "active_checksum": active_checksum,
        "target_columns": target_cols,
        "primary_key_columns": pk_columns,
        "soft_delete_column": soft_delete_column,
        "mode": "mirror",
    }


def quote_sql_identifier(name: str, quote_char: str = '"') -> str:
    """Re-export the writer-common identifier quoting helper."""
    from connectors.writer_common import quote_sql_identifier as _quote
    return _quote(name, quote_char)
