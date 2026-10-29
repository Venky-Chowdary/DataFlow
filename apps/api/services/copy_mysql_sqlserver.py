"""MySQL SELECT → SQL Server fast_executemany (cross-engine bulk).

MySQL has no ``COPY TO STDOUT``. One ``START TRANSACTION WITH CONSISTENT
SNAPSHOT`` streams ``SELECT`` (SSCursor); each row is bound with pyodbc
``fast_executemany``. Not BCP / ``BULK INSERT`` CSV (quoted empty string
collapses to NULL on Linux SQL Server). Dest ``COUNT(*)`` must equal the
source snapshot.

Empty dest SELECTs the table once. Occupied dest with a mapped single PK
skips a complete range. Append declines a partial range to the row path; overwrite DELETE+reloads it. No mapped single
PK on an occupied dest: decline.

Declines (row path keeps quarantine): transforms that change values,
blob/json/geometry/bit, public proxy, occupied dest without a mapped
single PK.
"""

from __future__ import annotations

import logging
from typing import Any

from services.cancel_check import cancel_checkpoint
from services.brand_env import getenv_brand
from services.copy_fast_path import (
    FastPathResult,
    FastPathUnavailable,
    occupied_pk_range_action,
    settle_fast_path_create_on,
)
from services.copy_mysql_pg import (
    _FETCH_BATCH,
    _mysql_connect,
    _mysql_ident,
    _mysql_table_pk_and_types,
    _plan_pk_partitions,
    _select_sql,
    mysql_time_column_out_of_range,
    mysql_type_is_copy_safe,
)
from services.copy_pg_mysql import (
    _jsonable_bound,
    mapped_single_pk,
    pg_mysql_copy_partitions,
    pg_mysql_copy_workers,
)
from services.copy_pg_sqlserver import (
    _enable_fast_executemany,
    bind_row_for_sqlserver,
    bulk_executemany,
    driver_failure_decline,
    pg_sqlserver_copy_batch,
    set_wide_decimal_inputsizes,
    sqlserver_insert_placeholders,
)
from services.copy_sqlserver_pg import _close_ss
from services.copy_sqlserver_sqlserver import (
    _count as _ss_count,
    _create_sql as _ss_create_sql,
    _delete_range as _ss_delete_range,
    _drop_sql as _ss_drop_sql,
    _has_identity,
    _ident as _ss_ident,
    _range_count as _ss_range_count,
    _schema_of as _ss_schema_of,
    _ss_connect,
    _table_exists as _ss_table_exists,
    _table_ref as _ss_table_ref,
)

logger = logging.getLogger(__name__)


def mysql_sqlserver_copy_enabled() -> bool:
    raw = (getenv_brand("MYSQL_SQLSERVER_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _select_into_sqlserver(
    source_conn: Any,
    dst_cur: Any,
    *,
    select_sql: str,
    insert_sql: str,
    batch_size: int,
) -> int:
    from pymysql.cursors import SSCursor

    copied = 0
    cur = source_conn.cursor(SSCursor)
    try:
        cur.execute(select_sql)
        batch: list[tuple[Any, ...]] = []
        while True:
            # D102: a cancel stops the copy between fetch batches.
            cancel_checkpoint()
            rows = cur.fetchmany(_FETCH_BATCH)
            # D168: rows read per fetch batch feed the job's progress.
            from services.cancel_check import report_rows

            report_rows(len(rows))
            if not rows:
                break
            for row in rows:
                batch.append(bind_row_for_sqlserver(dst_cur, tuple(row)))
                if len(batch) >= batch_size:
                    bulk_executemany(dst_cur, insert_sql, batch)
                    copied += len(batch)
                    batch.clear()
        if batch:
            bulk_executemany(dst_cur, insert_sql, batch)
            copied += len(batch)
        return copied
    finally:
        try:
            cur.close()
        except Exception:
            logger.debug("MySQL stream cursor close skipped", exc_info=True)


def copy_mysql_to_sqlserver(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    sqlserver_ddls: list[str],
    replace_destination: bool,
    dest_schema: str | None = None,
) -> FastPathResult:
    """Stream MySQL rows into SQL Server. Dest COUNT is the proof."""
    if not pairs or len(pairs) != len(sqlserver_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not mysql_sqlserver_copy_enabled():
        raise FastPathUnavailable("MySQL→SQL Server COPY disabled")

    from connectors.write_resilience import is_public_proxy_host

    if is_public_proxy_host(dest_cfg.get("host") or "") or is_public_proxy_host(
        dest_cfg.get("connection_string") or ""
    ) or is_public_proxy_host(source_cfg.get("host") or ""):
        raise FastPathUnavailable("public proxy: SQL Server bulk copy not assumed")

    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    table_q = _mysql_ident(source_table)
    dst_schema = _ss_schema_of(dest_cfg, dest_schema)
    dest_ref = _ss_table_ref(dst_schema, dest_table)
    col_sql = ", ".join(_ss_ident(c) for c in target_cols)
    placeholders = sqlserver_insert_placeholders(sqlserver_ddls)
    batch_size = pg_sqlserver_copy_batch()

    source_conn = _mysql_connect(source_cfg)
    dest_conn = _ss_connect(dest_cfg)
    created_here = False
    existed_before = False
    reset_empty_dest_on_failure = False
    pk_map: tuple[str, str] | None = None
    preserve_dest_on_failure = False
    dst_cur = dest_conn.cursor()
    try:
        with source_conn.cursor() as src_cur:
            src_cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            src_cur.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT")
            pk_cols, live = _mysql_table_pk_and_types(src_cur, source_table, source_cols)
            live_l = {k.lower(): v for k, v in live.items()}
            for col in source_cols:
                declared = live_l.get(col.lower()) or ""
                if not mysql_type_is_copy_safe(declared):
                    raise FastPathUnavailable(
                        f"source column {col!r} type {declared} is not COPY-safe"
                    )
            pk_map = mapped_single_pk(pk_cols, pairs)

            bad_time_col = mysql_time_column_out_of_range(
                src_cur, table_q, pairs, sqlserver_ddls, live_l
            )
            if bad_time_col:
                raise FastPathUnavailable(
                    f"source column {bad_time_col!r} holds MySQL TIME outside "
                    "the 0-24h domain the SQL Server time carrier stores — "
                    "row path emits a typed finding per row instead of a bulk "
                    "bind error"
                )

            exists = _ss_table_exists(dst_cur, dst_schema, dest_table)
            existed_before = bool(exists)
            dest_occupied = False
            swap_at_end = False
            swap_schema = dst_schema
            swap_dest_ref = dest_ref
            swap_dest_table = dest_table
            if replace_destination and exists:
                # C03/X-OW2 — stage the overwrite, sp_rename swap inside the txn.
                from services.dest_overwrite_swap import (
                    overwrite_trash_name,
                    staged_overwrite_name,
                )

                stage_table = staged_overwrite_name(dest_table)
                swap_trash_table = overwrite_trash_name(dest_table)
                dest_ref = _ss_table_ref(swap_schema, stage_table)
                dest_table = stage_table
                swap_at_end = True
                exists = False
            if exists:
                dest_occupied = _ss_count(dst_cur, dest_ref) > 0
                reset_empty_dest_on_failure = not dest_occupied
                if dest_occupied and pk_map is None:
                    raise FastPathUnavailable(
                        "append into non-empty SQL Server dest stays on the row path"
                    )
            else:
                pk_dest = [
                    rename
                    for src_pk in pk_cols
                    for src_col, rename in pairs
                    if src_col.lower() == src_pk.lower()
                ]
                dst_cur.execute(
                    _ss_create_sql(
                        dest_ref, dest_table, pairs, sqlserver_ddls, pk_dest
                    )
                )
                settle_fast_path_create_on(
                    dst_cur,
                    dest_dialect="sqlserver",
                    dest_table=dest_table,
                    dest_schema=dst_schema,
                )
                dest_conn.commit()
                created_here = True

            src_cur.execute(f"SELECT COUNT(*) FROM {table_q}")  # nosec B608
            source_count = int(src_cur.fetchone()[0])
            workers = pg_mysql_copy_workers(source_count)
            n_parts = pg_mysql_copy_partitions(source_count, workers)
            partitions: list[dict[str, Any]] = []
            shard_mode = "serial"
            copy_split = "serial"
            to_copy: list[str] = [_select_sql(table_q, source_cols, "")]

            if pk_map is not None:
                src_pk, dest_pk = pk_map
                src_ident = _mysql_ident(src_pk)
                shard_mode = "pk"
                preserve_dest_on_failure = True
                pk_declared = live_l.get(src_pk.lower()) or ""
                partitions = _plan_pk_partitions(
                    src_cur, table_q, src_ident, pk_declared, n_parts, source_count
                )
                if dest_occupied:
                    copy_split = "pk"
                    dest_ident = _ss_ident(dest_pk)
                    dest_conn.commit()
                    to_copy = []
                    for part in partitions:
                        already = _ss_range_count(
                            dst_cur, dest_ref, dest_ident, part
                        )
                        expected = int(part["source_count"])
                        if already == expected:
                            part["action"] = "skip"
                            part["dest_count"] = already
                        elif already == 0:
                            part["action"] = "load"
                            to_copy.append(
                                _select_sql(
                                    table_q,
                                    source_cols,
                                    str(part.get("predicate") or ""),
                                )
                            )
                        else:
                            occupied_pk_range_action(already, expected, replace_destination=replace_destination)
                            _ss_delete_range(
                                dst_cur, dest_ref, dest_ident, part
                            )
                            part["action"] = "reload"
                            to_copy.append(
                                _select_sql(
                                    table_q,
                                    source_cols,
                                    str(part.get("predicate") or ""),
                                )
                            )
                    dest_conn.commit()
                else:
                    to_copy = [_select_sql(table_q, source_cols, "")]

        # Built after the overwrite decision: a staged overwrite loads the
        # staging table, never the live destination it later replaces.
        insert_sql = (
            f"INSERT INTO {dest_ref} WITH (TABLOCK) ({col_sql}) "  # nosec B608
            f"VALUES ({placeholders})"
        )
        identity = _has_identity(dst_cur, dst_schema, dest_table)
        if identity:
            dst_cur.execute(f"SET IDENTITY_INSERT {dest_ref} ON")  # nosec B608
        # Wide-decimal descriptors are declared ONLY on this write cursor.
        # pyodbc applies ``setinputsizes`` positionally to every execute on
        # the cursor, so declaring them on ``dst_cur`` poisons the
        # OBJECT_ID probe / range counts with DECIMAL descriptors
        # (X-40: HY104 'Invalid precision value' on the existence probe).
        write_cur = dest_conn.cursor()
        _enable_fast_executemany(write_cur)
        set_wide_decimal_inputsizes(write_cur, sqlserver_ddls)
        try:
            for sql in to_copy:
                _select_into_sqlserver(
                    source_conn,
                    write_cur,
                    select_sql=sql,
                    insert_sql=insert_sql,
                    batch_size=batch_size,
                )
                dest_conn.commit()
        finally:
            try:
                write_cur.close()
            except Exception:
                logger.debug("SQL Server write cursor close skipped", exc_info=True)
            if identity:
                try:
                    dst_cur.execute(f"SET IDENTITY_INSERT {dest_ref} OFF")  # nosec B608
                except Exception:
                    logger.debug("IDENTITY_INSERT OFF skipped", exc_info=True)

        dest_count = _ss_count(dst_cur, dest_ref)
        if dest_count != source_count:
            raise ValueError(
                "MySQL→SQL Server COPY refused: dest COUNT(*) "
                f"{dest_count} != source snapshot {source_count}"
            )
        if shard_mode == "pk" and pk_map is not None:
            dest_ident = _ss_ident(pk_map[1])
            dest_conn.commit()
            for part in partitions:
                dest_part = _ss_range_count(dst_cur, dest_ref, dest_ident, part)
                part["dest_count"] = dest_part
                if dest_part != int(part["source_count"]):
                    raise ValueError(
                        "PK range dest COUNT "
                        f"{dest_part} != source {part['source_count']} "
                        f"(lo={part['lo']!r} hi={part['hi']!r})"
                    )
        if swap_at_end:
            from services.dest_overwrite_swap import sqlserver_swap_staged_dest

            sqlserver_swap_staged_dest(
                dst_cur,
                schema=swap_schema,
                dest_table=swap_dest_table,
                staging_table=dest_table,
                trash_table=swap_trash_table,
                quote=_ss_ident,
            )
            dest_ref = swap_dest_ref
            dest_table = swap_dest_table
        dest_conn.commit()
        try:
            source_conn.commit()
        except Exception:
            logger.debug("MySQL source commit skipped", exc_info=True)
        proof = f"dest_count:{dest_count}"
        partition_proof = [
            {
                "lo": _jsonable_bound(p.get("lo")),
                "hi": _jsonable_bound(p.get("hi")),
                "null_shard": bool(p.get("null_shard")),
                "source_count": int(p["source_count"]),
                "dest_count": int(p.get("dest_count") or 0),
                "action": str(p.get("action") or "load"),
            }
            for p in partitions
        ]
        skipped = sum(1 for p in partitions if p.get("action") == "skip")
        return FastPathResult(
            rows_copied=dest_count,
            source_rows=source_count,
            source_checksum=proof,
            target_rows=dest_count,
            target_checksum=proof,
            source_snapshot={
                "mysql_consistent_snapshot": True,
                "copy_workers": 1,
                "copy_split": copy_split,
                "copy_partitions": max(len(partitions), 1),
                "partitions_skipped": skipped,
                "partitions_loaded": len(to_copy),
                "shard_mode": shard_mode,
                "partition_proof": partition_proof,
            },
            dest_table_existed_before=existed_before,
            proof_scope=(
                "partition_dest_count_equals_source_snapshot"
                if partition_proof
                else "dest_count_equals_source_snapshot_count"
            ),
        )
    except Exception as exc:
        # A table this run created (or an existing dest that was empty before)
        # must end byte-identical to before the failure — never a partial
        # commit. PK-shard resume preserves committed rows only for a
        # pre-existing occupied dest. Roll back first: after a bind/insert
        # failure the cleanup statements would otherwise run inside the
        # aborted transaction and be silently skipped (X-35/X-36).
        try:
            dest_conn.rollback()
        except Exception:
            logger.debug("SQL Server dest rollback skipped", exc_info=True)
        if created_here:
            try:
                dst_cur.execute(_ss_drop_sql(dest_ref))
                dest_conn.commit()
            except Exception:
                logger.debug("dest drop after copy failure skipped", exc_info=True)
        elif existed_before and reset_empty_dest_on_failure:
            try:
                dst_cur.execute(f"TRUNCATE TABLE {dest_ref}")  # nosec B608
                dest_conn.commit()
            except Exception:
                logger.debug(
                    "dest truncate after copy failure skipped", exc_info=True
                )
        elif preserve_dest_on_failure:
            raise
        # A residual driver failure declines to the row path — typed
        # finding per row, never raw ODBC text in the job error (X-40).
        _decline = driver_failure_decline(exc)
        if _decline is not None:
            raise _decline from exc
        raise
    finally:
        try:
            dst_cur.close()
        except Exception:
            logger.debug("SQL Server dest cursor close skipped", exc_info=True)
        try:
            _close_ss(dest_conn)
        except Exception:
            logger.debug("SQL Server dest close skipped", exc_info=True)
        try:
            source_conn.close()
        except Exception:
            logger.debug("MySQL source close skipped", exc_info=True)
