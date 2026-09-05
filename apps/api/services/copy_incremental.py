"""Identity incremental COPY — cursor-filtered staging load, then append or upsert.

Full-table identity COPY already moves PostgreSQL→MySQL and PostgreSQL→PostgreSQL
without Python seeing a row. After the first load, operators run incremental.
The COPY router used to decline every incremental route, so the second run paid
the row-path tax and never advanced a watermark when COPY had succeeded.

This module is the missing second run for the handover SQL core
(PostgreSQL and MySQL, either direction):

1. Build the same lexicographic ``(cursor, pk) > (watermark, pk)`` predicate
   the engine reader uses (Airbyte timestamp-cursor trap).
2. COPY that filtered population into staging (one shard — do not PK-range a
   subset).
3. ``incremental_append``: ``INSERT INTO dest SELECT FROM staging`` (duplicate
   PK fails closed, not IGNORE).
4. ``incremental_deduped``: existing ``ON DUPLICATE`` / ``ON CONFLICT``.
5. Proof: staging COUNT = filtered source COUNT. Append: dest_after =
   dest_before + staging. Deduped: dest PK ⋈ staging = staging. Dest COUNT
   independently reread.
6. High-water is MAX(cursor[, pk]) on staging *before* it is dropped — the
   same population the COPY moved, not dest MAX of older rows.

No watermark (first incremental) is a full-table COPY of that sync mode, then
the same high-water write. CDC / SCD2 / mirror stay on the row path.
"""

from __future__ import annotations

import logging
from typing import Any

from services.copy_fast_path import FastPathResult, FastPathUnavailable, _quote
from services.copy_pg_mysql import _pg_quoted_literal
from services.keyset_pagination import (
    encode_keyset_bookmark,
    present_cursor_bookmark,
    split_cursor_bookmark,
)

logger = logging.getLogger(__name__)

COPY_INCREMENTAL_MODES = frozenset({"incremental_append", "incremental_deduped"})
_SQL_CORE = frozenset({"postgresql", "postgres", "mysql", "mariadb"})
APPEND_PROOF_SCOPE = (
    "staging_count_equals_filtered_source_and_dest_count_equals_before_plus_staging"
)


def identity_incremental_route(src_type: str, dest_type: str) -> bool:
    """True when identity incremental COPY is proven for this pair."""
    return (src_type or "").strip().lower() in _SQL_CORE and (
        dest_type or ""
    ).strip().lower() in _SQL_CORE


def mapped_pair(
    pairs: list[tuple[str, str]], source_col: str
) -> tuple[str, str] | None:
    want = (source_col or "").strip().lower()
    if not want:
        return None
    for src, dest in pairs:
        if str(src).lower() == want:
            return str(src), str(dest)
    return None


def pg_cursor_predicate_sql(
    cur: Any,
    *,
    cursor_column: str,
    watermark: str | None,
    pk_column: str = "",
) -> str:
    """SQL fragment matching ``postgresql_reader`` lexicographic cursor seek.

    COPY cannot bind ``%s`` inside ``COPY (SELECT … WHERE …) TO STDOUT``, so
    values are rendered with the same ``mogrify`` the engine uses for PK-range
    literals. Empty watermark → empty predicate (first incremental = full table).
    """
    bookmark = present_cursor_bookmark(watermark)
    if bookmark is None:
        return ""
    cursor_ident = _quote(cursor_column)
    pk = (pk_column or "").strip()
    if pk and pk != cursor_column:
        cur_val, pk_val = split_cursor_bookmark(bookmark, has_tiebreak=True)
        pk_ident = _quote(pk)
        return (
            f"({cursor_ident}, {pk_ident}) > "
            f"({_pg_quoted_literal(cur, cur_val)}, {_pg_quoted_literal(cur, pk_val)})"
        )
    cur_val, _ = split_cursor_bookmark(bookmark, has_tiebreak=False)
    return f"{cursor_ident} > {_pg_quoted_literal(cur, cur_val)}"


def mysql_cursor_predicate_sql(
    cur: Any,
    *,
    cursor_column: str,
    watermark: str | None,
    pk_column: str = "",
) -> str:
    """SQL fragment matching ``mysql_reader`` lexicographic cursor seek.

    INSERT SELECT / LOAD DATA cannot bind ``%s`` in the COPY-shaped SELECT, so
    values are ``mogrify``'d the same way MySQL PK-range predicates are.
    """
    from services.copy_mysql_pg import _mysql_ident

    bookmark = present_cursor_bookmark(watermark)
    if bookmark is None:
        return ""
    cursor_ident = _mysql_ident(cursor_column)
    pk = (pk_column or "").strip()
    if pk and pk != cursor_column:
        cur_val, pk_val = split_cursor_bookmark(bookmark, has_tiebreak=True)
        pk_ident = _mysql_ident(pk)
        clause = f"({cursor_ident}, {pk_ident}) > (%s, %s)"
        params: tuple[Any, ...] = (cur_val, pk_val)
    else:
        cur_val, _ = split_cursor_bookmark(bookmark, has_tiebreak=False)
        clause = f"{cursor_ident} > %s"
        params = (cur_val,)
    pred = cur.mogrify(clause, params)
    if isinstance(pred, bytes):
        pred = pred.decode()
    return str(pred)


def mysql_insert_from_staging_sql(
    dest_q: str, staging_q: str, columns: list[str], quote
) -> str:
    """Fail-closed append. ``INSERT IGNORE`` would hide duplicate PKs."""
    cols = ", ".join(quote(c) for c in columns)
    return f"INSERT INTO {dest_q} ({cols}) SELECT {cols} FROM {staging_q}"


def pg_insert_from_staging_sql(
    dest_ref: str, staging_ref: str, columns: list[str], quote
) -> str:
    cols = ", ".join(quote(c) for c in columns)
    return f"INSERT INTO {dest_ref} ({cols}) SELECT {cols} FROM {staging_ref}"


def read_high_water_row(
    cur: Any,
    table_sql: str,
    columns: list[str],
    quote,
    *,
    mysql: bool = False,
) -> tuple[Any, ...] | None:
    """MAX(cursor[, pk]) of the copied population. NULLs do not advance the mark."""
    if not columns:
        return None
    idents = [quote(c) for c in columns]
    order = ", ".join(
        f"{ident} DESC" if mysql else f"{ident} DESC NULLS LAST" for ident in idents
    )
    cur.execute(
        f"SELECT {', '.join(idents)} FROM {table_sql} "  # nosec B608
        f"WHERE {idents[0]} IS NOT NULL ORDER BY {order} LIMIT 1"
    )
    row = cur.fetchone()
    return tuple(row) if row else None


def encode_high_water(row: tuple[Any, ...] | None) -> str:
    if not row:
        return ""
    from services.value_serializer import present_cell_text

    parts = [present_cell_text(v) for v in row]
    if any(p is None for p in parts):
        return ""
    return encode_keyset_bookmark(list(parts))


def _require_mapped_cursor(
    pairs: list[tuple[str, str]], cursor_column: str, pk_column: str
) -> tuple[str, str, str, str]:
    mapped = mapped_pair(pairs, cursor_column)
    if mapped is None:
        raise FastPathUnavailable(
            "incremental COPY requires the cursor column to be mapped"
        )
    src_cursor, dest_cursor = mapped
    dest_pk = ""
    src_pk = ""
    if (pk_column or "").strip() and pk_column.strip() != cursor_column:
        pk_mapped = mapped_pair(pairs, pk_column)
        if pk_mapped is None:
            raise FastPathUnavailable(
                "incremental COPY tie-break PK must be mapped when the watermark is composite"
            )
        src_pk, dest_pk = pk_mapped
    return src_cursor, dest_cursor, src_pk, dest_pk


def _source_where(
    source_cfg: dict[str, Any],
    source_schema: str,
    source_table: str,
    cursor_column: str,
    watermark: str | None,
    pk_column: str,
) -> str:
    from services.copy_pg_mysql import _pg_connect

    conn = _pg_connect(source_cfg)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            # Probe only — identifiers are quoted; the table must exist.
            cur.execute(
                "SELECT 1 FROM pg_catalog.pg_class c "
                "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = %s AND c.relname = %s LIMIT 1",
                (source_schema or "public", source_table),
            )
            if cur.fetchone() is None:
                raise FastPathUnavailable("incremental COPY source table is absent")
            return pg_cursor_predicate_sql(
                cur,
                cursor_column=cursor_column,
                watermark=watermark,
                pk_column=pk_column,
            )
    finally:
        try:
            conn.close()
        except Exception:
            logger.debug("incremental WHERE probe close skipped", exc_info=True)


def _mysql_source_where(
    source_cfg: dict[str, Any],
    source_table: str,
    cursor_column: str,
    watermark: str | None,
    pk_column: str,
) -> str:
    from services.copy_mysql_pg import _mysql_connect

    conn = _mysql_connect(source_cfg)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_name = %s LIMIT 1",
                (source_table,),
            )
            if cur.fetchone() is None:
                raise FastPathUnavailable("incremental COPY source table is absent")
            return mysql_cursor_predicate_sql(
                cur,
                cursor_column=cursor_column,
                watermark=watermark,
                pk_column=pk_column,
            )
    finally:
        try:
            conn.close()
        except Exception:
            logger.debug("mysql incremental WHERE probe close skipped", exc_info=True)


def _stamp_incremental(
    result: FastPathResult,
    *,
    watermark: str,
    dest_count: int,
    dest_count_before: int,
    staging_count: int,
    sync_mode: str,
    proof_scope: str,
) -> FastPathResult:
    snapshot = dict(result.source_snapshot or {})
    snapshot["incremental"] = True
    snapshot["incremental_sync_mode"] = sync_mode
    snapshot["incremental_watermark"] = watermark
    snapshot["dest_count"] = dest_count
    snapshot["dest_count_before"] = dest_count_before
    snapshot["staging_count"] = staging_count
    return FastPathResult(
        rows_copied=result.source_rows,
        source_rows=result.source_rows,
        source_checksum=result.source_checksum,
        target_rows=result.target_rows,
        target_checksum=result.target_checksum,
        source_snapshot=snapshot,
        indexes_carried=result.indexes_carried,
        proof_scope=proof_scope,
    )


def _apply_staging_to_mysql(
    dst_cur: Any,
    *,
    dest_q: str,
    staging_q: str,
    staging_name: str,
    dest_table: str,
    target_cols: list[str],
    dest_pk: str,
    dest_cursor: str,
    dest_pk_col: str,
    dest_count_before: int,
    mode: str,
    result: FastPathResult,
    quote,
) -> FastPathResult:
    from services.copy_upsert import (
        UPSERT_PROOF_SCOPE,
        mysql_upsert_from_staging_sql,
        pk_join_count_sql,
        _result_with_upsert_proof,
    )

    wm_cols = [dest_cursor] + ([dest_pk_col] if dest_pk_col else [])
    high = encode_high_water(
        read_high_water_row(dst_cur, staging_q, wm_cols, quote, mysql=True)
    )
    staging_count = int(result.source_rows)
    if staging_count == 0:
        dst_cur.execute(f"DROP TABLE IF EXISTS {staging_q}")  # nosec B608
        return _stamp_incremental(
            result,
            watermark="",
            dest_count=dest_count_before,
            dest_count_before=dest_count_before,
            staging_count=0,
            sync_mode=mode,
            proof_scope=APPEND_PROOF_SCOPE
            if mode == "incremental_append"
            else UPSERT_PROOF_SCOPE,
        )
    if mode == "incremental_deduped":
        dst_cur.execute(
            mysql_upsert_from_staging_sql(
                dest_q, staging_q, target_cols, dest_pk, quote
            )
        )
        pk_ident = quote(dest_pk)
        dst_cur.execute(pk_join_count_sql(dest_q, staging_q, pk_ident))
        join_count = int(dst_cur.fetchone()[0])
        dst_cur.execute(f"SELECT COUNT(*) FROM {dest_q}")  # nosec B608
        dest_count = int(dst_cur.fetchone()[0])
        dst_cur.execute(f"DROP TABLE IF EXISTS {staging_q}")  # nosec B608
        proven = _result_with_upsert_proof(
            result,
            join_count=join_count,
            dest_count=dest_count,
            staging_table=staging_name,
            dest_table=dest_table,
        )
        return _stamp_incremental(
            proven,
            watermark=high,
            dest_count=dest_count,
            dest_count_before=dest_count_before,
            staging_count=staging_count,
            sync_mode=mode,
            proof_scope=UPSERT_PROOF_SCOPE,
        )
    dst_cur.execute(
        mysql_insert_from_staging_sql(dest_q, staging_q, target_cols, quote)
    )
    dst_cur.execute(f"SELECT COUNT(*) FROM {dest_q}")  # nosec B608
    dest_count = int(dst_cur.fetchone()[0])
    expected = dest_count_before + staging_count
    if dest_count != expected:
        raise ValueError(
            "incremental append refused: dest COUNT(*) "
            f"{dest_count} != dest_before {dest_count_before} "
            f"+ staging {staging_count}"
        )
    dst_cur.execute(f"DROP TABLE IF EXISTS {staging_q}")  # nosec B608
    return _stamp_incremental(
        result,
        watermark=high,
        dest_count=dest_count,
        dest_count_before=dest_count_before,
        staging_count=staging_count,
        sync_mode=mode,
        proof_scope=APPEND_PROOF_SCOPE,
    )


def _apply_staging_to_pg(
    dst_cur: Any,
    *,
    dest_ref: str,
    staging_ref: str,
    staging_name: str,
    dest_table: str,
    target_cols: list[str],
    dest_pk: str,
    dest_cursor: str,
    dest_pk_col: str,
    dest_count_before: int,
    mode: str,
    result: FastPathResult,
) -> FastPathResult:
    from services.copy_upsert import (
        UPSERT_PROOF_SCOPE,
        pg_upsert_from_staging_sql,
        pk_join_count_sql,
        _result_with_upsert_proof,
    )

    wm_cols = [dest_cursor] + ([dest_pk_col] if dest_pk_col else [])
    high = encode_high_water(
        read_high_water_row(dst_cur, staging_ref, wm_cols, _quote, mysql=False)
    )
    staging_count = int(result.source_rows)
    if staging_count == 0:
        dst_cur.execute(f"DROP TABLE IF EXISTS {staging_ref}")  # nosec B608
        return _stamp_incremental(
            result,
            watermark="",
            dest_count=dest_count_before,
            dest_count_before=dest_count_before,
            staging_count=0,
            sync_mode=mode,
            proof_scope=APPEND_PROOF_SCOPE
            if mode == "incremental_append"
            else UPSERT_PROOF_SCOPE,
        )
    if mode == "incremental_deduped":
        dst_cur.execute(
            pg_upsert_from_staging_sql(
                dest_ref, staging_ref, target_cols, dest_pk, _quote
            )
        )
        pk_ident = _quote(dest_pk)
        dst_cur.execute(pk_join_count_sql(dest_ref, staging_ref, pk_ident))
        join_count = int(dst_cur.fetchone()[0])
        dst_cur.execute(f"SELECT COUNT(*) FROM {dest_ref}")  # nosec B608
        dest_count = int(dst_cur.fetchone()[0])
        dst_cur.execute(f"DROP TABLE IF EXISTS {staging_ref}")  # nosec B608
        proven = _result_with_upsert_proof(
            result,
            join_count=join_count,
            dest_count=dest_count,
            staging_table=staging_name,
            dest_table=dest_table,
        )
        return _stamp_incremental(
            proven,
            watermark=high,
            dest_count=dest_count,
            dest_count_before=dest_count_before,
            staging_count=staging_count,
            sync_mode=mode,
            proof_scope=UPSERT_PROOF_SCOPE,
        )
    dst_cur.execute(
        pg_insert_from_staging_sql(dest_ref, staging_ref, target_cols, _quote)
    )
    dst_cur.execute(f"SELECT COUNT(*) FROM {dest_ref}")  # nosec B608
    dest_count = int(dst_cur.fetchone()[0])
    expected = dest_count_before + staging_count
    if dest_count != expected:
        raise ValueError(
            "incremental append refused: dest COUNT(*) "
            f"{dest_count} != dest_before {dest_count_before} "
            f"+ staging {staging_count}"
        )
    dst_cur.execute(f"DROP TABLE IF EXISTS {staging_ref}")  # nosec B608
    return _stamp_incremental(
        result,
        watermark=high,
        dest_count=dest_count,
        dest_count_before=dest_count_before,
        staging_count=staging_count,
        sync_mode=mode,
        proof_scope=APPEND_PROOF_SCOPE,
    )


def copy_postgres_to_mysql_incremental(
    *,
    source_cfg: dict[str, Any],
    source_schema: str,
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    mysql_ddls: list[str],
    sync_mode: str,
    cursor_column: str,
    watermark: str | None,
    pk_column: str = "",
) -> FastPathResult:
    """Filtered PG→MySQL COPY into staging, then append INSERT or upsert MERGE."""
    from services.copy_pg_mysql import (
        _mysql_connect,
        _mysql_create_sql,
        _mysql_ident,
        _pg_connect,
        copy_postgres_to_mysql,
        mapped_single_pk,
    )
    from services.copy_fast_path import source_table_shape
    from services.copy_upsert import staging_table_name

    mode = (sync_mode or "").strip().lower()
    if mode not in COPY_INCREMENTAL_MODES:
        raise FastPathUnavailable(f"incremental COPY does not cover {sync_mode!r}")
    src_cursor, dest_cursor, _src_pk, dest_pk_col = _require_mapped_cursor(
        pairs, cursor_column, pk_column
    )
    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    source_where = _source_where(
        source_cfg, source_schema, source_table, src_cursor, watermark, pk_column
    )

    source_conn = _pg_connect(source_cfg)
    try:
        source_conn.autocommit = True
        with source_conn.cursor() as src_cur:
            shape = source_table_shape(
                src_cur, source_schema, source_table, source_cols
            )
        pk_map = mapped_single_pk(list(shape.primary_key or []), pairs)
        if pk_map is None:
            raise FastPathUnavailable(
                "incremental COPY requires exactly one mapped primary key"
            )
        _src_pk_table, dest_pk = pk_map
    finally:
        source_conn.close()

    dest_q = _mysql_ident(dest_table)
    staging = staging_table_name(dest_table)
    staging_q = _mysql_ident(staging)
    dest_conn = _mysql_connect(dest_cfg)
    created_dest = False
    dest_count_before = 0
    try:
        with dest_conn.cursor() as dst_cur:
            dst_cur.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_name = %s LIMIT 1",
                (dest_table,),
            )
            if dst_cur.fetchone() is None:
                create_sql = _mysql_create_sql(
                    dest_table, pairs, mysql_ddls, [dest_pk]
                )
                dst_cur.execute(create_sql)  # nosec B608
                dest_conn.commit()
                created_dest = True
            else:
                dst_cur.execute(f"SELECT COUNT(*) FROM {dest_q}")  # nosec B608
                dest_count_before = int(dst_cur.fetchone()[0])
    finally:
        try:
            dest_conn.close()
        except Exception:
            logger.debug("mysql incremental dest probe close skipped", exc_info=True)

    dest_conn = None
    try:
        result = copy_postgres_to_mysql(
            source_cfg=source_cfg,
            source_schema=source_schema,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=staging,
            pairs=pairs,
            mysql_ddls=mysql_ddls,
            replace_destination=True,
            source_where=source_where,
        )
        dest_conn = _mysql_connect(dest_cfg)
        with dest_conn.cursor() as dst_cur:
            out = _apply_staging_to_mysql(
                dst_cur,
                dest_q=dest_q,
                staging_q=staging_q,
                staging_name=staging,
                dest_table=dest_table,
                target_cols=target_cols,
                dest_pk=dest_pk,
                dest_cursor=dest_cursor,
                dest_pk_col=dest_pk_col,
                dest_count_before=dest_count_before,
                mode=mode,
                result=result,
                quote=_mysql_ident,
            )
            dest_conn.commit()
            return out
    except Exception:
        cleanup = dest_conn or _mysql_connect(dest_cfg)
        try:
            with cleanup.cursor() as dst_cur:
                dst_cur.execute(f"DROP TABLE IF EXISTS {staging_q}")  # nosec B608
                if created_dest:
                    dst_cur.execute(f"DROP TABLE IF EXISTS {dest_q}")  # nosec B608
            cleanup.commit()
        except Exception:
            logger.debug("mysql incremental cleanup skipped", exc_info=True)
        if cleanup is not dest_conn:
            try:
                cleanup.close()
            except Exception:
                logger.debug("mysql incremental cleanup close skipped", exc_info=True)
        raise
    finally:
        if dest_conn is not None:
            try:
                dest_conn.close()
            except Exception:
                logger.debug("mysql incremental dest close skipped", exc_info=True)


def copy_between_postgres_incremental(
    *,
    source_cfg: dict[str, Any],
    source_schema: str,
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_schema: str,
    dest_table: str,
    pairs: list[tuple[str, str]],
    sync_mode: str,
    cursor_column: str,
    watermark: str | None,
    pk_column: str = "",
) -> FastPathResult:
    """Filtered PG→PG binary COPY into staging, then append INSERT or upsert MERGE."""
    from services.copy_fast_path import (
        _connect,
        _table_ref,
        copy_between_postgres,
        create_destination_like_source,
        source_table_shape,
    )
    from services.copy_pg_mysql import mapped_single_pk
    from services.copy_upsert import staging_table_name

    mode = (sync_mode or "").strip().lower()
    if mode not in COPY_INCREMENTAL_MODES:
        raise FastPathUnavailable(f"incremental COPY does not cover {sync_mode!r}")
    src_cursor, dest_cursor, _src_pk, dest_pk_col = _require_mapped_cursor(
        pairs, cursor_column, pk_column
    )
    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    source_where = _source_where(
        source_cfg, source_schema, source_table, src_cursor, watermark, pk_column
    )

    source_conn = _connect(source_cfg)
    dest_conn = _connect(dest_cfg)
    dest_ref = _table_ref(dest_schema, dest_table)
    staging = staging_table_name(dest_table)
    staging_ref = _table_ref(dest_schema, staging)
    created_dest = False
    dest_count_before = 0
    dest_pk = ""
    try:
        source_conn.autocommit = True
        dest_conn.autocommit = True
        with source_conn.cursor() as src_cur, dest_conn.cursor() as dst_cur:
            shape = source_table_shape(
                src_cur, source_schema, source_table, source_cols
            )
            pk_map = mapped_single_pk(list(shape.primary_key or []), pairs)
            if pk_map is None:
                raise FastPathUnavailable(
                    "incremental COPY requires exactly one mapped primary key"
                )
            _src_pk_table, dest_pk = pk_map
            dst_cur.execute(
                "SELECT to_regclass(%s)",
                (f"{dest_schema or 'public'}.{dest_table}",),
            )
            if dst_cur.fetchone()[0] is None:
                create_destination_like_source(
                    dst_cur, dest_schema, dest_table, pairs, shape
                )
                created_dest = True
            else:
                dst_cur.execute(f"SELECT COUNT(*) FROM {dest_ref}")  # nosec B608
                dest_count_before = int(dst_cur.fetchone()[0])
    finally:
        try:
            source_conn.close()
        except Exception:
            logger.debug("pg incremental source probe close skipped", exc_info=True)
        try:
            dest_conn.close()
        except Exception:
            logger.debug("pg incremental dest probe close skipped", exc_info=True)

    result = copy_between_postgres(
        source_cfg=source_cfg,
        source_schema=source_schema,
        source_table=source_table,
        dest_cfg=dest_cfg,
        dest_schema=dest_schema,
        dest_table=staging,
        pairs=pairs,
        replace_destination=True,
        source_where=source_where,
    )

    dest_conn = _connect(dest_cfg)
    try:
        dest_conn.autocommit = False
        with dest_conn.cursor() as dst_cur:
            out = _apply_staging_to_pg(
                dst_cur,
                dest_ref=dest_ref,
                staging_ref=staging_ref,
                staging_name=staging,
                dest_table=dest_table,
                target_cols=target_cols,
                dest_pk=dest_pk,
                dest_cursor=dest_cursor,
                dest_pk_col=dest_pk_col,
                dest_count_before=dest_count_before,
                mode=mode,
                result=result,
            )
            dest_conn.commit()
            return out
    except Exception:
        dest_conn.rollback()
        try:
            dest_conn.autocommit = True
            with dest_conn.cursor() as dst_cur:
                dst_cur.execute(f"DROP TABLE IF EXISTS {staging_ref}")  # nosec B608
                if created_dest:
                    dst_cur.execute(f"DROP TABLE IF EXISTS {dest_ref}")  # nosec B608
        except Exception:
            logger.debug("pg incremental cleanup skipped", exc_info=True)
        raise
    finally:
        try:
            dest_conn.close()
        except Exception:
            logger.debug("pg incremental dest close skipped", exc_info=True)


def copy_mysql_to_postgres_incremental(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_schema: str,
    dest_table: str,
    pairs: list[tuple[str, str]],
    pg_ddls: list[str],
    sync_mode: str,
    cursor_column: str,
    watermark: str | None,
    pk_column: str = "",
) -> FastPathResult:
    """Filtered MySQL→PG COPY into staging, then append INSERT or upsert MERGE."""
    from services.copy_fast_path import _connect, _table_ref
    from services.copy_mysql_pg import (
        _mysql_connect,
        _mysql_table_pk_and_types,
        _pg_create_sql,
        copy_mysql_to_postgres,
    )
    from services.copy_pg_mysql import mapped_single_pk
    from services.copy_upsert import staging_table_name

    mode = (sync_mode or "").strip().lower()
    if mode not in COPY_INCREMENTAL_MODES:
        raise FastPathUnavailable(f"incremental COPY does not cover {sync_mode!r}")
    src_cursor, dest_cursor, _src_pk, dest_pk_col = _require_mapped_cursor(
        pairs, cursor_column, pk_column
    )
    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    source_where = _mysql_source_where(
        source_cfg, source_table, src_cursor, watermark, pk_column
    )

    source_conn = _mysql_connect(source_cfg)
    try:
        source_conn.autocommit = True
        with source_conn.cursor() as src_cur:
            pk_cols, _live = _mysql_table_pk_and_types(
                src_cur, source_table, source_cols
            )
        pk_map = mapped_single_pk(pk_cols, pairs)
        if pk_map is None:
            raise FastPathUnavailable(
                "incremental COPY requires exactly one mapped primary key"
            )
        _src_pk_table, dest_pk = pk_map
    finally:
        source_conn.close()

    dest_ref = _table_ref(dest_schema, dest_table)
    staging = staging_table_name(dest_table)
    staging_ref = _table_ref(dest_schema, staging)
    dest_conn = _connect(dest_cfg)
    created_dest = False
    dest_count_before = 0
    try:
        dest_conn.autocommit = True
        with dest_conn.cursor() as dst_cur:
            dst_cur.execute(
                "SELECT to_regclass(%s)",
                (f"{dest_schema or 'public'}.{dest_table}",),
            )
            if dst_cur.fetchone()[0] is None:
                dst_cur.execute(
                    _pg_create_sql(
                        dest_schema, dest_table, pairs, pg_ddls, [dest_pk]
                    )
                )
                created_dest = True
            else:
                dst_cur.execute(f"SELECT COUNT(*) FROM {dest_ref}")  # nosec B608
                dest_count_before = int(dst_cur.fetchone()[0])
    finally:
        try:
            dest_conn.close()
        except Exception:
            logger.debug("mysql→pg incremental dest probe close skipped", exc_info=True)

    result = copy_mysql_to_postgres(
        source_cfg=source_cfg,
        source_table=source_table,
        dest_cfg=dest_cfg,
        dest_schema=dest_schema,
        dest_table=staging,
        pairs=pairs,
        pg_ddls=pg_ddls,
        replace_destination=True,
        source_where=source_where,
    )

    dest_conn = _connect(dest_cfg)
    try:
        dest_conn.autocommit = False
        with dest_conn.cursor() as dst_cur:
            out = _apply_staging_to_pg(
                dst_cur,
                dest_ref=dest_ref,
                staging_ref=staging_ref,
                staging_name=staging,
                dest_table=dest_table,
                target_cols=target_cols,
                dest_pk=dest_pk,
                dest_cursor=dest_cursor,
                dest_pk_col=dest_pk_col,
                dest_count_before=dest_count_before,
                mode=mode,
                result=result,
            )
            dest_conn.commit()
            return out
    except Exception:
        dest_conn.rollback()
        try:
            dest_conn.autocommit = True
            with dest_conn.cursor() as dst_cur:
                dst_cur.execute(f"DROP TABLE IF EXISTS {staging_ref}")  # nosec B608
                if created_dest:
                    dst_cur.execute(f"DROP TABLE IF EXISTS {dest_ref}")  # nosec B608
        except Exception:
            logger.debug("mysql→pg incremental cleanup skipped", exc_info=True)
        raise
    finally:
        try:
            dest_conn.close()
        except Exception:
            logger.debug("mysql→pg incremental dest close skipped", exc_info=True)


def copy_mysql_to_mysql_incremental(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    mysql_ddls: list[str],
    sync_mode: str,
    cursor_column: str,
    watermark: str | None,
    pk_column: str = "",
) -> FastPathResult:
    """Filtered MySQL→MySQL COPY into staging, then append INSERT or upsert MERGE."""
    from services.copy_mysql_mysql import copy_mysql_to_mysql
    from services.copy_mysql_pg import _mysql_connect, _mysql_table_pk_and_types
    from services.copy_pg_mysql import _mysql_create_sql, _mysql_ident, mapped_single_pk
    from services.copy_upsert import staging_table_name

    mode = (sync_mode or "").strip().lower()
    if mode not in COPY_INCREMENTAL_MODES:
        raise FastPathUnavailable(f"incremental COPY does not cover {sync_mode!r}")
    src_cursor, dest_cursor, _src_pk, dest_pk_col = _require_mapped_cursor(
        pairs, cursor_column, pk_column
    )
    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    source_where = _mysql_source_where(
        source_cfg, source_table, src_cursor, watermark, pk_column
    )

    source_conn = _mysql_connect(source_cfg)
    try:
        source_conn.autocommit = True
        with source_conn.cursor() as src_cur:
            pk_cols, _live = _mysql_table_pk_and_types(
                src_cur, source_table, source_cols
            )
        pk_map = mapped_single_pk(pk_cols, pairs)
        if pk_map is None:
            raise FastPathUnavailable(
                "incremental COPY requires exactly one mapped primary key"
            )
        _src_pk_table, dest_pk = pk_map
    finally:
        source_conn.close()

    dest_q = _mysql_ident(dest_table)
    staging = staging_table_name(dest_table)
    staging_q = _mysql_ident(staging)
    dest_conn = _mysql_connect(dest_cfg)
    created_dest = False
    dest_count_before = 0
    try:
        with dest_conn.cursor() as dst_cur:
            dst_cur.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_name = %s LIMIT 1",
                (dest_table,),
            )
            if dst_cur.fetchone() is None:
                dst_cur.execute(
                    _mysql_create_sql(dest_table, pairs, mysql_ddls, [dest_pk])
                )
                dest_conn.commit()
                created_dest = True
            else:
                dst_cur.execute(f"SELECT COUNT(*) FROM {dest_q}")  # nosec B608
                dest_count_before = int(dst_cur.fetchone()[0])
    finally:
        try:
            dest_conn.close()
        except Exception:
            logger.debug("mysql incremental dest probe close skipped", exc_info=True)

    dest_conn = None
    try:
        result = copy_mysql_to_mysql(
            source_cfg=source_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=staging,
            pairs=pairs,
            mysql_ddls=mysql_ddls,
            replace_destination=True,
            source_where=source_where,
        )
        dest_conn = _mysql_connect(dest_cfg)
        with dest_conn.cursor() as dst_cur:
            out = _apply_staging_to_mysql(
                dst_cur,
                dest_q=dest_q,
                staging_q=staging_q,
                staging_name=staging,
                dest_table=dest_table,
                target_cols=target_cols,
                dest_pk=dest_pk,
                dest_cursor=dest_cursor,
                dest_pk_col=dest_pk_col,
                dest_count_before=dest_count_before,
                mode=mode,
                result=result,
                quote=_mysql_ident,
            )
            dest_conn.commit()
            return out
    except Exception:
        cleanup = dest_conn or _mysql_connect(dest_cfg)
        try:
            with cleanup.cursor() as dst_cur:
                dst_cur.execute(f"DROP TABLE IF EXISTS {staging_q}")  # nosec B608
                if created_dest:
                    dst_cur.execute(f"DROP TABLE IF EXISTS {dest_q}")  # nosec B608
            cleanup.commit()
        except Exception:
            logger.debug("mysql incremental cleanup skipped", exc_info=True)
        if cleanup is not dest_conn:
            try:
                cleanup.close()
            except Exception:
                logger.debug("mysql incremental cleanup close skipped", exc_info=True)
        raise
    finally:
        if dest_conn is not None:
            try:
                dest_conn.close()
            except Exception:
                logger.debug("mysql incremental dest close skipped", exc_info=True)
