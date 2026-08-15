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
import uuid
from collections.abc import Iterable, Iterator
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
    a future enhancement of this kernel, not a second path. Physical
    ``COUNT(*)`` (including ``_deleted`` rows) is dest-engine, never a
    Python dest scan and never stuffed active COUNT. Does not close
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
    # Physical dest population including tombstones. Soft-delete does not
    # drop COUNT(*) (Fivetran _fivetran_deleted hole). Dest-engine COUNT(*)
    # is this figure — never a Python dest scan, never stuffed active COUNT.
    physical = _dest_engine_count(
        conn,
        f"SELECT COUNT(*) FROM {target_qualified}",  # nosec B608
    )
    return {
        "soft_deleted": soft_deleted,
        "reactivated": reactivated,
        "soft_delete_column": soft_delete_column,
        "physical_rows": physical,
        "rows_scanned": physical,
    }


def mirror_pk_sources(
    conflict_columns: list[str], mappings: list[dict[str, Any]] | None
) -> list[str]:
    """Map destination PK columns back to source names.

    Same identity the buffered writer and the engine spill use. One owner —
    adapters and ``engine_record_spill`` must not copy this loop.
    """
    pk_sources: list[str] = []
    for pk_target in conflict_columns:
        if not pk_target:
            continue
        pk_source = pk_target
        for m in mappings or []:
            if (m.get("target") or m.get("source")) == pk_target:
                src = m.get("source")
                if src:
                    pk_source = str(src)
                    break
        pk_sources.append(str(pk_source))
    return pk_sources


def complete_mirror_pk_tuple(values: Iterable[Any]) -> tuple[Any, ...] | None:
    """Return the PK tuple, or None when identity is incomplete.

    Incomplete is skip, not invent: None, empty string, or ``DF_MISSING``.
    A missing CDC field is not a key. Do not coerce it to NULL or ``""``.
    """
    from services.value_serializer import is_missing_sentinel

    out: list[Any] = []
    for value in values:
        if value is None or value == "" or is_missing_sentinel(value):
            return None
        out.append(value)
    if not out:
        return None
    return tuple(out)


def iter_mirror_pk_tuples_from_records(
    records: Iterable[dict[str, Any]], pk_sources: list[str]
) -> Iterator[tuple[Any, ...]]:
    """Complete PK tuples from unexpanded source records. Duplicates are kept.

    EXISTS/NOT EXISTS on dest staging is set-based — a second copy of the
    same key does not change the inferred-delete census. Callers that need
    a unique list (tests, small fixtures) use ``unique_mirror_pk_tuples``.
    """
    for rec in records:
        if not isinstance(rec, dict):
            continue
        tup = complete_mirror_pk_tuple(rec.get(c) for c in pk_sources)
        if tup is not None:
            yield tup


def iter_mirror_pk_tuples_from_spool(
    source_spool: Any, pk_sources: list[str]
) -> Iterator[tuple[Any, ...]]:
    """Complete PK tuples from a JSONL spool (payload or keys-only).

    Header lookup is exact source-name match. A PK column absent from the
    spool headers is incomplete identity — skip, not invent.
    """
    headers = list(getattr(source_spool, "headers", None) or [])
    index = {name: i for i, name in enumerate(headers)}
    for row in source_spool.iter_rows():
        values: list[Any] = []
        for col in pk_sources:
            pos = index.get(col)
            if pos is None or pos >= len(row):
                values.append(None)
            else:
                values.append(row[pos])
        tup = complete_mirror_pk_tuple(values)
        if tup is not None:
            yield tup


def unique_mirror_pk_tuples(
    tuples: Iterable[tuple[Any, ...]],
) -> list[tuple[Any, ...]]:
    """Unique complete keys for tests and small fixtures. Not the write path.

    Unhashable key parts are kept (same as the historical seen-set). This
    materializes the unique set — ``apply_inferred_soft_deletes`` must not
    call it. Peak RAM is keys-only, not payloads.
    """
    seen: set[tuple[Any, ...]] = set()
    out: list[tuple[Any, ...]] = []
    for tup in tuples:
        try:
            if tup in seen:
                continue
            seen.add(tup)
        except TypeError:
            pass
        out.append(tup)
    return out


def _source_pk_tuples(
    records: list[dict[str, Any]], pk_sources: list[str]
) -> list[tuple[Any, ...]]:
    """Unique complete PK tuples from source records. Incomplete identity is skip."""
    return unique_mirror_pk_tuples(
        iter_mirror_pk_tuples_from_records(records, pk_sources)
    )


def _iter_mirror_census_keys(
    *,
    records: list[dict[str, Any]] | None,
    pk_sources: list[str],
    source_pk_tuples: Iterable[tuple[Any, ...]] | None,
    source_spool: Any,
    source_key_spool: Any,
) -> Iterator[tuple[Any, ...]]:
    """Prefer keys-only spool, then payload spool, then an iterator, then records.

    The keys-only spool is written from *unexpanded* records during the same
    ingest pass as the payload spool — raw PK types, one key per source row,
    STRUCT explode does not invent a second parent key. The payload spool is
    the write image (exploded); use it only when no key spool was retained.
    """
    if source_key_spool is not None:
        yield from iter_mirror_pk_tuples_from_spool(source_key_spool, pk_sources)
        return
    if source_spool is not None:
        yield from iter_mirror_pk_tuples_from_spool(source_spool, pk_sources)
        return
    if source_pk_tuples is not None:
        yield from source_pk_tuples
        return
    yield from iter_mirror_pk_tuples_from_records(records or [], pk_sources)


def _stream_insert_pk_staging(
    conn: Any,
    insert_stmt: Any,
    tuples: Iterable[tuple[Any, ...]],
    pk_width: int,
    batch_size: int,
) -> int:
    """Chunk-INSERT keys into dest staging. Peak RAM is one insert chunk."""
    chunk_n = max(int(batch_size), 1)
    batch: list[dict[str, Any]] = []
    inserted = 0

    def _flush() -> None:
        nonlocal inserted
        if not batch:
            return
        conn.execute(insert_stmt, batch)
        inserted += len(batch)
        batch.clear()

    for tup in tuples:
        batch.append({f"p{i}": val for i, val in enumerate(tup[:pk_width])})
        if len(batch) >= chunk_n:
            _flush()
    _flush()
    return inserted


def _create_pk_staging(
    conn: Any,
    stg_qualified: str,
    dest_qualified: str,
    pk_quoted: str,
    dialect_name: str,
) -> None:
    """Empty staging table with dest PK column types. Never invent TEXT vs INTEGER."""
    import sqlalchemy as sa
    from services.dialect_profiles import warehouse_sql_quote_dialect

    family = warehouse_sql_quote_dialect(dialect_name)
    if family == "sqlserver":
        sql = (
            f"SELECT {pk_quoted} INTO {stg_qualified} "  # nosec B608
            f"FROM {dest_qualified} WHERE 1=0"
        )
    else:
        sql = (
            f"CREATE TABLE {stg_qualified} AS "  # nosec B608
            f"SELECT {pk_quoted} FROM {dest_qualified} WHERE 1=0"
        )
    conn.execute(sa.text(sql))


def _drop_pk_staging(conn: Any, stg_qualified: str) -> None:
    import sqlalchemy as sa

    try:
        conn.execute(sa.text(f"DROP TABLE IF EXISTS {stg_qualified}"))  # nosec B608
        conn.commit()
        return
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
    try:
        conn.execute(sa.text(f"DROP TABLE {stg_qualified}"))  # nosec B608
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass


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
    source_pk_tuples: Iterable[tuple[Any, ...]] | None = None,
    source_spool: Any = None,
    source_key_spool: Any = None,
    pk_sources: list[str] | None = None,
) -> dict[str, Any]:
    """Soft-delete destination rows that are no longer in the source snapshot.

    ``endpoint`` must be a database ``EndpointConfig`` for an SQLAlchemy-backed
    destination. ``conflict_columns`` is the destination primary key. Source
    rows must already be upserted. This-run ``soft_deleted`` / ``reactivated``
    are dest-engine COUNT of transitions (same staging anti-join as the
    stream path). Driver ``rowcount`` and a Python dest scan are not this
    census. Physical dest COUNT is dest-engine ``COUNT(*)`` of the table
    (tombstones included). Active checksum is still a streamed dest read
    for Gate-8.

    Key census prefers ``source_key_spool`` (keys-only, unexpanded, written
    during payload ingest) then ``source_spool`` (write image) then an
    iterator of tuples then ``records``. Keys stream into dest staging in
    ``batch_size`` chunks — the full unique PK set is not a Python list.
    Duplicate keys in staging do not change EXISTS. Empty key set after a
    non-empty source is fail-closed. File-stream is disabled for mirror so
    this census always sees the full snapshot, not the last chunk.
    """
    if not conflict_columns:
        raise ValueError("mirror sync requires a primary key / conflict column")

    pk_columns = [c for c in conflict_columns if c]
    if not pk_columns:
        raise ValueError("mirror sync requires a primary key / conflict column")

    resolved_pk_sources = list(pk_sources) if pk_sources else mirror_pk_sources(
        pk_columns, mappings
    )
    if not resolved_pk_sources:
        raise ValueError("mirror sync requires a primary key / conflict column")

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
    stg_name = f"_df_mirrorkeys_{uuid.uuid4().hex[:12]}"
    stg_qualified = _qualified_name(stg_name, schema_name)
    pk_quoted = ", ".join(quote_sql_identifier(c) for c in pk_columns)
    placeholders = ", ".join(f":p{i}" for i in range(len(pk_columns)))
    insert_sql = (
        f"INSERT INTO {stg_qualified} ({pk_quoted}) VALUES ({placeholders})"  # nosec B608
    )
    census: dict[str, Any] = {}
    active_rows = 0
    active_checksum = ""
    source_key_rows = 0
    try:
        with engine.connect() as conn:
            _ensure_soft_delete_column(conn, qualified, soft_delete_column)
            dialect_name = str(getattr(getattr(conn, "dialect", None), "name", "") or "")
            _create_pk_staging(conn, stg_qualified, qualified, pk_quoted, dialect_name)
            conn.commit()
            source_key_rows = _stream_insert_pk_staging(
                conn,
                sa.text(insert_sql),
                _iter_mirror_census_keys(
                    records=records,
                    pk_sources=resolved_pk_sources,
                    source_pk_tuples=source_pk_tuples,
                    source_spool=source_spool,
                    source_key_spool=source_key_spool,
                ),
                len(pk_columns),
                batch_size,
            )
            if source_key_rows <= 0:
                raise ValueError(
                    "mirror sync could not build a non-empty source key set "
                    "from the primary key"
                )
            conn.commit()
            census = apply_inferred_deletes_via_staging(
                conn,
                qualified,
                stg_qualified,
                pk_columns,
                soft_delete_column=soft_delete_column,
                dialect=dialect_name,
            )
            conn.commit()
            active_rows, active_checksum = _compute_active_checksum(
                conn, qualified, target_cols, soft_delete_column, batch_size
            )
            conn.commit()
            _drop_pk_staging(conn, stg_qualified)
    except Exception:
        try:
            with engine.connect() as conn:
                _drop_pk_staging(conn, stg_qualified)
        except Exception as exc:
            logging.getLogger(__name__).debug(
                "Exception suppressed: %s", exc, exc_info=exc
            )
        raise
    finally:
        release_engine(engine)

    physical = census.get("physical_rows")
    if physical is None:
        physical = census.get("rows_scanned")
    return {
        "soft_deleted": census.get("soft_deleted"),
        "reactivated": census.get("reactivated"),
        "physical_rows": physical,
        "rows_scanned": physical,
        "active_rows": active_rows,
        "active_checksum": active_checksum,
        "target_columns": target_cols,
        "primary_key_columns": pk_columns,
        "soft_delete_column": census.get("soft_delete_column") or soft_delete_column,
        "source_key_rows": source_key_rows,
        "mode": "mirror",
    }


def quote_sql_identifier(name: str, quote_char: str = '"') -> str:
    """Re-export the writer-common identifier quoting helper."""
    from connectors.writer_common import quote_sql_identifier as _quote
    return _quote(name, quote_char)
