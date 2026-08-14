"""Inferred-delete (mirror) support for full-refresh SQL transfers.

A mirror sync keeps the destination table in sync with the source:
- source rows are upserted (business columns only)
- destination rows no longer in the source are soft-deleted
- rows that reappear after being deleted are reactivated.

``_deleted`` is dest-owned lattice, not a business column. Fivetran SET
``_fivetran_deleted=false`` on every synced row, so this-run reactivate is
invisible and physical ``COUNT(*)`` never drops (the Fivetran hole). Our
writer never SET / INSERT the lattice: new keys take dest DEFAULT (active);
existing tombstones stay tombstones until this module's inferred-delete
pass transitions them. Dest-after ``currently deleted ∩ snapshot`` then
equals dest-before tombstone ∩ snapshot — the writer cannot un-delete
first. Delete+insert fallback is forbidden while the lattice column exists
(INSERT DEFAULT would materialize active). Native ON CONFLICT / MERGE SET
exclude the lattice; the portable UPDATE+INSERT fallback in
``connectors.merge_dialects`` is the same identity when no unique index
exists.
"""

from __future__ import annotations

import logging
from typing import Any

from services.engine_pool import release_engine

SOFT_DELETE_COLUMN = "_deleted"
_KEY_SEP = "\x1f"


def lattice_column_names(columns: Any) -> tuple[str, ...]:
    """Dest-owned mirror lattice columns present on this table object."""
    folded = SOFT_DELETE_COLUMN.casefold()
    found: list[str] = []
    for raw in columns:
        name = str(getattr(raw, "name", raw) or "")
        if name and name.casefold() == folded:
            found.append(name)
    return tuple(found)


def upsert_set_columns(
    target_cols: list[str],
    conflict_cols: list[str] | None = None,
    lattice: tuple[str, ...] | None = None,
) -> list[str]:
    """Columns eligible for ON CONFLICT / ON DUPLICATE / MERGE SET.

    Dest-owned lattice is never SET — that is the Fivetran
    ``_fivetran_deleted=false`` hole. Conflict/PK columns are never SET.
    Pass a physical probe as ``lattice`` when the mapped column list omits
    ``_deleted``; otherwise names present on ``target_cols`` are excluded.
    """
    names = lattice if lattice is not None else lattice_column_names(target_cols)
    owned = {str(n).casefold() for n in names}
    conflict = {str(c) for c in (conflict_cols or [])}
    return [
        c
        for c in target_cols
        if c not in conflict and str(c).casefold() not in owned
    ]


def upsert_insert_columns(
    target_cols: list[str],
    lattice: tuple[str, ...] | None = None,
) -> list[str]:
    """INSERT column list with dest-owned lattice removed.

    New keys take dest DEFAULT (active). Existing keys must not be rewritten
    by INSERT of the lattice column.
    """
    names = lattice if lattice is not None else lattice_column_names(target_cols)
    owned = {str(n).casefold() for n in names}
    return [c for c in target_cols if str(c).casefold() not in owned]


def lattice_columns_on_table(conn: Any, table_obj: Any) -> tuple[str, ...]:
    """Lattice columns on the *physical* dest table, not the mapped write Table.

    ``_build_table_for_write`` builds a Table from Map target columns, so
    ``table_obj.c`` does not include ``_deleted`` added by the inferred-delete
    pass. Trusting that shape would keep delete+insert as the fallback and
    INSERT DEFAULT would un-delete. Probe once per Table (cached on
    ``table_obj.info``).
    """
    info = getattr(table_obj, "info", None)
    if isinstance(info, dict) and "df_mirror_lattice" in info:
        return tuple(info["df_mirror_lattice"])
    found = lattice_column_names(getattr(table_obj, "c", []) or [])
    if not found:
        found = _probe_physical_lattice(conn, table_obj)
    if isinstance(info, dict):
        info["df_mirror_lattice"] = found
    return found


def _probe_physical_lattice(conn: Any, table_obj: Any) -> tuple[str, ...]:
    """SELECT 1=0 of the lattice column. Failure must not abort the write txn."""
    import sqlalchemy as sa
    from connectors.writer_common import quote_sql_identifier

    col_q = quote_sql_identifier(SOFT_DELETE_COLUMN)
    parts: list[str] = []
    schema = getattr(table_obj, "schema", None)
    if schema:
        parts.append(quote_sql_identifier(str(schema)))
    name = getattr(table_obj, "name", None)
    if not name:
        return ()
    parts.append(quote_sql_identifier(str(name)))
    qualified = ".".join(parts)
    nested = None
    try:
        nested = conn.begin_nested()
    except Exception:
        nested = None
    try:
        conn.execute(sa.text(f"SELECT {col_q} FROM {qualified} WHERE 1=0"))  # nosec B608
        if nested is not None:
            nested.commit()
        return (SOFT_DELETE_COLUMN,)
    except Exception:
        if nested is not None:
            try:
                nested.rollback()
            except Exception as exc:
                logging.getLogger(__name__).debug(
                    "Exception suppressed: %s", exc, exc_info=exc
                )
        return ()


def strip_lattice_from_upsert(
    rows: list[dict[str, Any]],
    update_cols: list[str],
    target_cols: list[str],
    lattice: tuple[str, ...],
) -> tuple[list[dict[str, Any]], list[str], list[str]]:
    """Remove dest-owned lattice columns from upsert SET / INSERT lists.

    Native ON CONFLICT / MERGE SET of ``_deleted`` from the payload (or a
    mapped default 0) is an un-delete the inferred-delete pass never saw.
    INSERT of the lattice column is the same lie: new keys use dest DEFAULT
    (active); existing keys must not be rewritten.
    """
    if not lattice:
        return rows, update_cols, target_cols
    owned = {n.casefold() for n in lattice}

    def keep(name: str) -> bool:
        return str(name).casefold() not in owned

    return (
        [{k: v for k, v in row.items() if keep(k)} for row in rows],
        [c for c in update_cols if keep(c)],
        [c for c in target_cols if keep(c)],
    )


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
    from services.dialect_profiles import (
        sql_bool_false_literal,
        stores_boolean_as_numeric,
        warehouse_sql_quote_dialect,
    )

    col_quoted = quote_sql_identifier(soft_delete_column)
    dialect_name = str(getattr(getattr(conn, "dialect", None), "name", "") or "")
    false_lit = sql_bool_false_literal(dialect_name)
    family = warehouse_sql_quote_dialect(dialect_name)
    if family == "sqlserver" or dialect_name.lower().startswith("mssql"):
        type_sql = "BIT"
    elif family == "oracle":
        type_sql = "NUMBER(1)"
    elif stores_boolean_as_numeric(dialect_name):
        type_sql = "INTEGER"
    else:
        type_sql = "BOOLEAN"
    ddl = (
        f"ALTER TABLE {qualified} ADD COLUMN {col_quoted} {type_sql} DEFAULT {false_lit}"
    )
    try:
        conn.execute(sa.text(ddl))
        conn.commit()
        return
    except Exception:
        try:
            conn.rollback()
        except Exception as exc:
            logging.getLogger(__name__).debug("Exception suppressed: %s", exc, exc_info=exc)


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
    from services.dialect_profiles import sql_bool_false_literal, sql_bool_true_literal

    dialect_name = str(getattr(getattr(conn, "dialect", None), "name", "") or "")
    col_quoted = quote_sql_identifier(soft_delete_column)
    true_lit = sql_bool_true_literal(dialect_name)
    false_lit = sql_bool_false_literal(dialect_name)
    activated = 0
    deactivated = 0

    if activate_keys:
        where_keys, params = _pk_or_clause(pk_columns, activate_keys, prefix="a")
        stmt = f"UPDATE {qualified} SET {col_quoted} = {false_lit} WHERE {where_keys}"  # nosec B608
        try:
            result = conn.execute(sa.text(stmt), params)
            conn.commit()
            activated = result.rowcount or 0
        except Exception:
            conn.rollback()

    if delete_keys:
        where_keys, params = _pk_or_clause(pk_columns, delete_keys, prefix="d")
        stmt = (
            f"UPDATE {qualified} SET {col_quoted} = {true_lit} "  # nosec B608
            f"WHERE {where_keys} "
            f"AND ({col_quoted} IS NULL OR {col_quoted} = {false_lit})"
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
    """Active-row digest: one streamed SELECT. Never OFFSET."""
    import sqlalchemy as sa
    from connectors.writer_common import quote_sql_identifier
    from services.dialect_profiles import sql_bool_is_not_true
    from services.reconciliation_api import stream_select_checksum

    dialect_name = str(getattr(getattr(conn, "dialect", None), "name", "") or "")
    col_quoted = quote_sql_identifier(soft_delete_column)
    pred = sql_bool_is_not_true(dialect_name, col_quoted)
    cols_quoted = ",".join(quote_sql_identifier(c) for c in target_cols)
    sql = f"SELECT {cols_quoted} FROM {qualified} WHERE {pred}"  # nosec B608
    return stream_select_checksum(
        conn,
        sa.text(sql),
        target_cols,
        itersize=max(1, int(batch_size)),
        dest_db_type=dialect_name,
    )


def _dest_engine_count(conn: Any, sql: str) -> int | None:
    """Dest-engine COUNT(*). Failure is unmeasured — never 0-from-error."""
    import sqlalchemy as sa

    try:
        row = conn.execute(sa.text(sql)).fetchone()
    except Exception as exc:
        logging.getLogger(__name__).info("mirror transition COUNT failed: %s", exc)
        return None
    if row is None or row[0] is None:
        return None
    try:
        return int(row[0])
    except (TypeError, ValueError):
        return None


def apply_inferred_deletes_via_staging(
    conn: Any,
    target_qualified: str,
    staging_qualified: str,
    pk_columns: list[str],
    *,
    soft_delete_column: str = SOFT_DELETE_COLUMN,
    dialect: str = "",
) -> dict[str, Any]:
    """Set-based inferred deletes: keys in staging stay active; dest \\ staging soft-delete.

    This-run census is dest-engine COUNT of *transitions* before UPDATE:

    * reactivated = currently deleted AND in staging
    * soft_deleted = currently active AND not in staging

    Because upsert does not own ``_deleted``, currently-deleted ∩ staging
    equals dest-before tombstone ∩ snapshot. Driver ``rowcount`` is not this
    proof (SQLAlchemy may return -1; a reactivate UPDATE that touches
    already-active rows is not a reactivate). ``RETURNING`` / ``OUTPUT`` is
    a future enhancement of this kernel, not a second path. Does not close
    Gate-8. Unmeasured COUNT stays ``None``.
    """
    import sqlalchemy as sa
    from connectors.writer_common import quote_sql_identifier
    from services.dialect_profiles import (
        sql_bool_false_literal,
        sql_bool_is_not_true,
        sql_bool_is_true,
        sql_bool_true_literal,
    )

    _ensure_soft_delete_column(conn, target_qualified, soft_delete_column)
    dialect_name = dialect or str(getattr(getattr(conn, "dialect", None), "name", "") or "")
    col_quoted = quote_sql_identifier(soft_delete_column)
    true_lit = sql_bool_true_literal(dialect_name)
    false_lit = sql_bool_false_literal(dialect_name)
    deleted_pred = sql_bool_is_true(dialect_name, col_quoted)
    active_pred = sql_bool_is_not_true(dialect_name, col_quoted)
    join_pred = " AND ".join(
        f"{staging_qualified}.{quote_sql_identifier(c)} = "
        f"{target_qualified}.{quote_sql_identifier(c)}"
        for c in pk_columns
    )
    reactivated = _dest_engine_count(
        conn,
        f"SELECT COUNT(*) FROM {target_qualified} "  # nosec B608
        f"WHERE {deleted_pred} "
        f"AND EXISTS (SELECT 1 FROM {staging_qualified} WHERE {join_pred})",
    )
    soft_deleted = _dest_engine_count(
        conn,
        f"SELECT COUNT(*) FROM {target_qualified} "  # nosec B608
        f"WHERE {active_pred} "
        f"AND NOT EXISTS (SELECT 1 FROM {staging_qualified} WHERE {join_pred})",
    )
    conn.execute(
        sa.text(
            f"UPDATE {target_qualified} "  # nosec B608
            f"SET {col_quoted} = {false_lit} "
            f"WHERE EXISTS (SELECT 1 FROM {staging_qualified} WHERE {join_pred}) "
            f"AND {deleted_pred}"
        )
    )
    conn.execute(
        sa.text(
            f"UPDATE {target_qualified} "  # nosec B608
            f"SET {col_quoted} = {true_lit} "
            f"WHERE NOT EXISTS (SELECT 1 FROM {staging_qualified} WHERE {join_pred}) "
            f"AND {active_pred}"
        )
    )
    return {
        "soft_deleted": soft_deleted,
        "reactivated": reactivated,
        "soft_delete_column": soft_delete_column,
    }


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
            col_quoted = quote_sql_identifier(soft_delete_column)
            from services.reconciliation_api import iter_select_row_dicts

            sql = f"SELECT {pk_quoted}, {col_quoted} FROM {qualified}"  # nosec B608
            from services.tombstone import is_tombstone_set

            for rows in iter_select_row_dicts(
                conn,
                sa.text(sql),
                list(pk_columns) + [soft_delete_column],
                itersize=batch_size,
            ):
                scanned += len(rows)
                reactivate_keys: list[str] = []
                delete_keys: list[str] = []
                for row in rows:
                    pk_val = _compose_key(row, pk_columns)
                    if not pk_val or all(p == "" for p in pk_val.split(_KEY_SEP)):
                        continue
                    deleted = is_tombstone_set(row, soft_delete_column)
                    if pk_val in source_keys:
                        if deleted:
                            reactivate_keys.append(pk_val)
                    elif not deleted:
                        delete_keys.append(pk_val)

                _update_deleted_batch(
                    conn, qualified, pk_columns, reactivate_keys, delete_keys, soft_delete_column
                )
                activated_total += len(reactivate_keys)
                deactivated_total += len(delete_keys)

            active_rows, active_checksum = _compute_active_checksum(
                conn, qualified, target_cols, soft_delete_column, batch_size
            )

    finally:
        release_engine(engine)

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
