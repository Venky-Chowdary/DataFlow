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


def _qualified_name(table: str, schema: str | None) -> str:
    from connectors.writer_common import quote_sql_identifier

    table_quoted = quote_sql_identifier(table)
    schema_quoted = quote_sql_identifier(schema) if schema else None
    return f"{schema_quoted}.{table_quoted}" if schema_quoted else table_quoted


def _key_value(record: dict[str, Any], column: str) -> str:
    from services.value_serializer import cell_to_string

    return cell_to_string(record.get(column))


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
    from connectors.writer_common import quote_sql_identifier
    import sqlalchemy as sa

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
    pk_column: str,
    activate_keys: list[str],
    delete_keys: list[str],
    soft_delete_column: str,
) -> tuple[int, int]:
    from connectors.writer_common import quote_sql_identifier
    import sqlalchemy as sa

    pk_quoted = quote_sql_identifier(pk_column)
    col_quoted = quote_sql_identifier(soft_delete_column)
    activated = 0
    deactivated = 0

    if activate_keys:
        placeholders = ",".join([f":a{i}" for i in range(len(activate_keys))])
        params = {f"a{i}": k for i, k in enumerate(activate_keys)}
        stmt = (
            f"UPDATE {qualified} SET {col_quoted} = FALSE "
            f"WHERE {pk_quoted} IN ({placeholders})"
        )
        try:
            result = conn.execute(sa.text(stmt), params)
            conn.commit()
            activated = result.rowcount or 0
        except Exception:
            conn.rollback()

    if delete_keys:
        placeholders = ",".join([f":d{i}" for i in range(len(delete_keys))])
        params = {f"d{i}": k for i, k in enumerate(delete_keys)}
        stmt = (
            f"UPDATE {qualified} SET {col_quoted} = TRUE "
            f"WHERE {pk_quoted} IN ({placeholders}) "
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
    from connectors.writer_common import quote_sql_identifier
    from services.reconciliation import canonical_checksum
    import sqlalchemy as sa

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
    if len(conflict_columns) > 1:
        raise NotImplementedError("composite primary key mirror is not yet supported")

    pk_target = conflict_columns[0]

    # Map the destination PK column back to the source column used in mappings.
    pk_source = pk_target
    if mappings:
        for m in mappings:
            if (m.get("target") or m.get("source")) == pk_target:
                src = m.get("source")
                if src:
                    pk_source = src
                    break

    source_keys = {_key_value(r, pk_source) for r in records if _key_value(r, pk_source) != ""}
    if not source_keys:
        raise ValueError("mirror sync could not build a non-empty source key set from the primary key")

    from src.transfer.adapters import resolve_connector_config
    from connectors.generic_sql import get_sqlalchemy_engine, get_sql_schema

    cfg = resolve_connector_config(endpoint)
    table = endpoint.table or endpoint.collection or "dt_import"
    schema_name = get_sql_schema(cfg)
    qualified = _qualified_name(table, schema_name)

    import sqlalchemy as sa

    target_cols = _target_columns(columns, mappings, schema)
    engine = get_sqlalchemy_engine(cfg)
    activated_total = 0
    deactivated_total = 0
    scanned = 0
    try:
        with engine.connect() as conn:
            _ensure_soft_delete_column(conn, qualified, soft_delete_column)

            pk_quoted = quote_sql_identifier(pk_target)
            col_quoted = quote_sql_identifier(soft_delete_column)
            offset = 0
            while True:
                sql = (
                    f"SELECT {pk_quoted}, {col_quoted} FROM {qualified} "
                    f"ORDER BY {pk_quoted} LIMIT {batch_size} OFFSET {offset}"
                )
                result = conn.execute(sa.text(sql))
                rows = result.fetchall()
                if not rows:
                    break
                scanned += len(rows)

                target_keys: set[str] = set()
                for row in rows:
                    pk_val = _key_value(row._mapping, pk_target)
                    if pk_val:
                        target_keys.add(pk_val)

                activate = [k for k in target_keys if k in source_keys]
                delete = [k for k in target_keys if k not in source_keys]
                a, d = _update_deleted_batch(conn, qualified, pk_target, activate, delete, soft_delete_column)
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
        "soft_delete_column": soft_delete_column,
        "mode": "mirror",
    }


def quote_sql_identifier(name: str, quote_char: str = '"') -> str:
    """Re-export the writer-common identifier quoting helper."""
    from connectors.writer_common import quote_sql_identifier as _quote
    return _quote(name, quote_char)
