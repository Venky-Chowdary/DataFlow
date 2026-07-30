"""MySQL bulk writer — batched INSERT with checkpoint callbacks."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from services.type_system import ddl_type

from connectors.mysql_conn import get_connection
from connectors.schema_drift import is_wider_type, widen_existing_columns_native
from connectors.sql_temporal import (
    extract_column_from_sql_error,
    is_sql_data_error,
)
from connectors.write_resilience import (
    build_write_batch_key,
    close_quietly,
    ensure_mysql_write_ledger,
    is_connection_lost,
    is_public_proxy_host,
    mark_mysql_chunk_committed,
    mysql_chunk_committed,
    reconnect_backoff_seconds,
    should_retry_connection_lost,
    write_chunk_size,
)
from connectors.writer_common import (
    DF_LSN_COL,
    _coerced_null_row_count,
    _rejected_row_count,
    build_mapped_rows_with_details,
    dedupe_rows,
    dedupe_rows_by_pk_and_lsn,
    filter_stale_lsn_rows,
    quarantine_unfit_decimals,
    quarantine_unfit_specialty_types,
    quarantine_unfit_strings,
    quote_sql_identifier,
    resolve_target_columns,
    row_checksum,
    sanitize_identifier,
    transform_error_policy,
)
from connectors.writer_common import (
    WriteResult as _WriteResult,
)

logger = logging.getLogger(__name__)


@dataclass
class WriteResult(_WriteResult):
    driver: str = "pymysql"


def mysql_type(inferred: str) -> str:
    return ddl_type("mysql", inferred)


def _fetch_mysql_column_types(cursor: Any, table_name: str) -> dict[str, str]:
    """Return physical ``COLUMN_TYPE`` for an existing table (empty if missing)."""
    try:
        cursor.execute(
            "SELECT COLUMN_NAME, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s",
            (table_name,),
        )
        return {str(name): str(ctype or "") for name, ctype in cursor.fetchall()}
    except Exception:
        return {}


def _apply_physical_temporal_types(
    target_cols: list[str],
    target_types: list[str],
    physical: dict[str, str],
) -> list[str]:
    """Prefer live MySQL column types for temporal coercion.

    Mapping/schema may label a TIMESTAMP source as TEXT after cell serialize, while
    the destination column is DATETIME — without this, ISO ``…T…Z`` literals hit
    MySQL 1292 Incorrect datetime value.
    """
    from connectors.sql_temporal import is_temporal_ddl, sql_base_type

    if not physical:
        return target_types
    out = list(target_types)
    for i, col in enumerate(target_cols):
        phys = physical.get(col) or physical.get(col.lower()) or physical.get(col.upper())
        if not phys:
            continue
        base = sql_base_type(phys)
        if is_temporal_ddl(base) or base in {
            "DATETIME", "TIMESTAMP", "DATE", "TIME", "YEAR",
        }:
            out[i] = phys
    return out


def _to_mysql_value(value: Any, source_type: str) -> Any:
    """Normalize transform-engine values to forms pymysql/MySQL can bind."""
    from connectors.sql_bind import normalize_sql_bind_value
    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        return value
    return normalize_sql_bind_value(value, source_type, engine="mysql")


def _mysql_apply_sparse_upsert(
    cursor: Any,
    *,
    table_q: str,
    target_cols: list[str],
    conflict_columns: list[str],
    sparse_rows: list[tuple],
) -> int:
    """Per-row upsert that omits DF_MISSING columns (never SET col=NULL for absent)."""
    from connectors.writer_common import (
        DF_LSN_COL,
        assert_sparse_upsert_has_pk,
        sparse_present_bindings,
    )
    from services.cdc_effectively_once import should_apply_pk_row

    conflict = [c for c in conflict_columns if c in target_cols]
    if not conflict:
        raise ValueError("sparse MySQL upsert requires conflict_columns")
    written = 0
    for row in sparse_rows:
        present = sparse_present_bindings(row, target_cols)
        assert_sparse_upsert_has_pk(present, conflict)
        non_pk = {k: v for k, v in present.items() if k not in conflict}
        pk_vals = [present[c] for c in conflict]
        where_sql = " AND ".join(
            f"{quote_sql_identifier(c, '`')}=%s" for c in conflict
        )
        if DF_LSN_COL in present and DF_LSN_COL in target_cols:
            lsn_q = quote_sql_identifier(DF_LSN_COL, "`")
            cursor.execute(
                f"SELECT {lsn_q} FROM {table_q} WHERE {where_sql}",  # nosec B608
                pk_vals,
            )
            existing = cursor.fetchone()
            if existing is not None:
                decision = should_apply_pk_row(
                    existing_lsn=existing[0],
                    incoming_lsn=present[DF_LSN_COL],
                )
                if not decision.applied:
                    continue
        if non_pk:
            set_cols = list(non_pk.keys())
            set_sql = ", ".join(
                f"{quote_sql_identifier(c, '`')}=%s" for c in set_cols
            )
            sql_u = f"UPDATE {table_q} SET {set_sql} WHERE {where_sql}"  # nosec B608
            cursor.execute(sql_u, [non_pk[c] for c in set_cols] + pk_vals)
            if cursor.rowcount and cursor.rowcount > 0:
                written += 1
                continue
        cols = list(present.keys())
        col_sql = ", ".join(quote_sql_identifier(c, "`") for c in cols)
        ph = ", ".join(["%s"] * len(cols))
        sql_i = f"INSERT INTO {table_q} ({col_sql}) VALUES ({ph})"  # nosec B608
        try:
            cursor.execute(sql_i, [present[c] for c in cols])
            written += 1
        except Exception:
            if not non_pk:
                raise
            set_cols = list(non_pk.keys())
            set_sql = ", ".join(
                f"{quote_sql_identifier(c, '`')}=%s" for c in set_cols
            )
            sql_u = f"UPDATE {table_q} SET {set_sql} WHERE {where_sql}"  # nosec B608
            cursor.execute(sql_u, [non_pk[c] for c in set_cols] + pk_vals)
            written += 1
    return written

def _open_mysql(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    connection_string: str,
    ssl: bool,
):
    conn = get_connection(
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        connection_string=connection_string,
        ssl=ssl,
    )
    conn.autocommit = False
    return conn


def write_mapped_rows(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    schema: str,
    connection_string: str,
    ssl: bool,
    table_name: str,
    headers: list[str],
    data_rows: list[list[str]],
    mappings: list[dict],
    column_types: dict[str, str],
    on_checkpoint: Callable[[int, int, int], None] | None = None,
    create_table: bool = True,
    error_policy: str | None = None,
    write_mode: str = "insert",
    conflict_columns: list[str] | None = None,
    backfill_new_fields: bool = False,
    **_kwargs: Any,
) -> WriteResult:
    del schema
    from connectors.writer_common import resolve_writer_backfill

    backfill_new_fields = resolve_writer_backfill(
        backfill_new_fields=backfill_new_fields,
        mappings=mappings,
        schema_policy=_kwargs.get("schema_policy"),
    )
    try:
        import pymysql
    except ImportError:
        pymysql = None
    if pymysql is None:
        from connectors.driver_guard import require_driver, stub_writes_allowed
        from connectors.stub_writer import simulate_stub_write

        if not stub_writes_allowed():
            return WriteResult(
                ok=False, rows_written=0, table_name=table_name, target_schema=database,
                checksum="", chunks_completed=0,
                error=require_driver("pymysql"),
                driver="none",
            )
        rows, checksum, chunks = simulate_stub_write(
            data_rows=data_rows, table_name=table_name, target_schema=database,
            on_checkpoint=on_checkpoint,
        )
        return WriteResult(
            ok=True, rows_written=rows, table_name=table_name, target_schema=database,
            checksum=checksum, chunks_completed=chunks, driver="stub",
        )

    from connectors.writer_common import sample_values_by_source_from_batch

    batch_samples = sample_values_by_source_from_batch(headers, data_rows, mappings)
    target_cols, logical_types = resolve_target_columns(
        mappings,
        column_types,
        preserve_case=True,
        sample_values_by_source=batch_samples,
        table_exists=False if create_table else None,
    )
    if not target_cols:
        return WriteResult(
            ok=False, rows_written=0, table_name=table_name, target_schema=database,
            checksum="", chunks_completed=0, error="No column mappings",
        )

    table_name = sanitize_identifier(table_name, preserve_case=True)
    target_types = [mysql_type(t) for t in logical_types]
    dest_types = {target_cols[i]: logical_types[i] for i in range(len(target_cols))}
    policy = transform_error_policy(error_policy)

    # Map before opening a socket so public proxies are not idle during transform.
    mapped_rows, transform_errors, rejected_details = build_mapped_rows_with_details(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=target_cols,
        column_types=column_types,
        dest_types=dest_types,
        error_policy=policy,
        preserve_case=True,
    )
    # Fail-closed DECIMAL(p,s) fit — never silently truncate/round into target.
    mapped_rows = quarantine_unfit_decimals(
        mapped_rows,
        target_cols,
        target_types,
        rejected_details,
        policy,
        dialect_label="MySQL DECIMAL",
    )
    mapped_rows = quarantine_unfit_specialty_types(
        mapped_rows, target_cols, target_types, rejected_details, policy
    )
    mapped_rows = quarantine_unfit_strings(
        mapped_rows,
        target_cols,
        target_types,
        rejected_details,
        policy,
        dialect_label="MySQL VARCHAR",
    )
    sparse_rows: list[tuple] = []
    if write_mode == "upsert" and conflict_columns:
        from connectors.writer_common import split_dense_sparse_rows

        mapped_rows, sparse_rows = split_dense_sparse_rows(mapped_rows)

    if write_mode == "upsert" and conflict_columns:
        if DF_LSN_COL in target_cols:
            mapped_rows = dedupe_rows_by_pk_and_lsn(
                mapped_rows, conflict_columns, target_cols
            )
        else:
            mapped_rows = dedupe_rows(mapped_rows, conflict_columns, target_cols)

    rejected_rows = _rejected_row_count(
        data_rows, mapped_rows, rejected_details, policy, sparse_rows=sparse_rows
    )
    coerced_null_rows = _coerced_null_row_count(rejected_details, policy)
    if transform_errors and policy == "fail":
        return WriteResult(
            ok=False, rows_written=0, table_name=table_name, target_schema=database,
            checksum="", chunks_completed=0,
            error=f"Transform errors: {'; '.join(transform_errors[:3])}",
            rejected_rows=rejected_rows,
            rejected_details=rejected_details,
            warnings=transform_errors,
        )

    chunk_size = write_chunk_size(host, connection_string=connection_string)
    total = len(mapped_rows)
    chunks = max(1, (total + chunk_size - 1) // chunk_size) if total else 1
    written = 0
    chunks_completed = 0
    placeholders = ", ".join(["%s"] * len(target_cols))
    table_q = quote_sql_identifier(table_name, "`")
    col_names = ", ".join(quote_sql_identifier(c, "`") for c in target_cols)
    if write_mode == "upsert" and conflict_columns:
        conflict = [c for c in conflict_columns if c in target_cols]
        if conflict:
            update_cols = [c for c in target_cols if c not in conflict]
            if update_cols:
                if DF_LSN_COL in target_cols:
                    from connectors.writer_common import mysql_lsn_values_newer_sql

                    newer = mysql_lsn_values_newer_sql(DF_LSN_COL, quote="`")
                    updates = ", ".join(
                        f"{quote_sql_identifier(c, '`')}=IF({newer}, VALUES({quote_sql_identifier(c, '`')}), {quote_sql_identifier(c, '`')})"
                        for c in update_cols
                    )
                else:
                    updates = ", ".join(
                        f"{quote_sql_identifier(c, '`')}=VALUES({quote_sql_identifier(c, '`')})"
                        for c in update_cols
                    )
                insert_sql = (
                    f"INSERT INTO {table_q} ({col_names}) VALUES ({placeholders}) "  # nosec B608
                    f"ON DUPLICATE KEY UPDATE {updates}"
                )
            else:
                insert_sql = (
                    f"INSERT IGNORE INTO {table_q} ({col_names}) VALUES ({placeholders})"
                )
        else:
            insert_sql = f"INSERT INTO {table_q} ({col_names}) VALUES ({placeholders})"  # nosec B608
    else:
        insert_sql = f"INSERT INTO {table_q} ({col_names}) VALUES ({placeholders})"  # nosec B608

    proxy_dest = is_public_proxy_host(host) or is_public_proxy_host(connection_string)
    job_id = str(_kwargs.get("job_id") or "").strip()
    write_batch_key = str(_kwargs.get("write_batch_key") or "").strip() or build_write_batch_key(
        table_name=table_name,
        file_batch_idx=_kwargs.get("file_batch_idx"),
    )
    use_ledger = bool(job_id)
    conn = None
    converted_rows: list[tuple] = []

    def _reconnect():
        nonlocal conn, cur
        close_quietly(conn)
        conn = _open_mysql(
            host=host, port=port, database=database,
            username=username, password=password,
            connection_string=connection_string, ssl=ssl,
        )
        cur = conn.cursor()

    def _run_setup(cursor) -> None:
        nonlocal target_types, converted_rows
        if use_ledger:
            ensure_mysql_write_ledger(cursor)
        if create_table:
            col_defs = ", ".join(
                f"{quote_sql_identifier(c, '`')} {t}" for c, t in zip(target_cols, target_types)
            )
            if write_mode == "upsert" and conflict_columns:
                conflict_cols = [c for c in conflict_columns if c in target_cols]
                if conflict_cols:
                    index_name = sanitize_identifier(
                        f"uidx_{table_name}_{'_'.join(conflict_cols)}"
                    )
                    cols = ", ".join(quote_sql_identifier(c, "`") for c in conflict_cols)
                    col_defs += f", UNIQUE KEY {quote_sql_identifier(index_name, '`')} ({cols})"
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_q} ({col_defs})")

        if backfill_new_fields:
            cursor.execute(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s",
                (table_name,),
            )
            existing = {row[0] for row in cursor.fetchall()}
            for col, typ in zip(target_cols, target_types):
                if col not in existing:
                    cursor.execute(
                        f"ALTER TABLE {table_q} ADD COLUMN {quote_sql_identifier(col, '`')} {typ}"
                    )

            # Pick the wider of the mapping-proposed target DDL and the freshly
            # introspected source DDL, then widen any destination columns that are
            # now too narrow for the source drift.
            desired_types: list[str] = []
            for mapping, target_type in zip(mappings, target_types):
                source = mapping.get("source") or ""
                source_type = (
                    column_types.get(source)
                    or mapping.get("source_type")
                    or "VARCHAR"
                )
                source_ddl = mysql_type(source_type)
                desired = (
                    source_ddl
                    if is_wider_type(target_type, source_ddl)
                    else target_type
                )
                desired_types.append(desired)

            widen_existing_columns_native(
                cursor,
                "mysql",
                database,
                table_name,
                target_cols,
                desired_types,
                backfill=backfill_new_fields,
                skip_cols=conflict_columns or [],
            )
            target_types = desired_types

        # Bind using physical types so ISO Z never hits a DATETIME column as TEXT.
        physical = _fetch_mysql_column_types(cursor, table_name)
        target_types = _apply_physical_temporal_types(target_cols, target_types, physical)
        converted_rows = [
            tuple(_to_mysql_value(v, target_types[i]) for i, v in enumerate(row))
            for row in mapped_rows
        ]
        conn.commit()

    try:
        conn = _open_mysql(
            host=host, port=port, database=database,
            username=username, password=password,
            connection_string=connection_string, ssl=ssl,
        )
        cur = conn.cursor()
        try:
            setup_attempt = 0
            setup_started = time.monotonic()
            while True:
                try:
                    _run_setup(cur)
                    break
                except Exception as setup_exc:
                    try:
                        conn.rollback()
                    except Exception as exc:
                        logger.debug("Cleanup exception suppressed: %s", exc, exc_info=exc)
                    setup_attempt += 1
                    if not is_connection_lost(setup_exc) or not should_retry_connection_lost(
                        attempt=setup_attempt, started_at=setup_started, proxy=proxy_dest
                    ):
                        raise
                    time.sleep(reconnect_backoff_seconds(setup_attempt))
                    _reconnect()

            # Defensive: if setup skipped conversion somehow, coerce with mapping types.
            if not converted_rows and mapped_rows:
                converted_rows = [
                    tuple(_to_mysql_value(v, target_types[i]) for i, v in enumerate(row))
                    for row in mapped_rows
                ]

            rows_skipped = 0
            sparse_written = 0
            if sparse_rows and write_mode == "upsert" and conflict_columns:
                # Convert sparse with physical types but keep DF_MISSING intact.
                sparse_converted = [
                    tuple(_to_mysql_value(v, target_types[i]) for i, v in enumerate(row))
                    for row in sparse_rows
                ]
                sparse_written = _mysql_apply_sparse_upsert(
                    cur,
                    table_q=table_q,
                    target_cols=target_cols,
                    conflict_columns=conflict_columns,
                    sparse_rows=sparse_converted,
                )
                conn.commit()
                written += sparse_written

            for chunk_idx in range(chunks):
                start = chunk_idx * chunk_size
                batch = converted_rows[start : start + chunk_size]
                if not batch:
                    break

                attempt = 0
                chunk_started = time.monotonic()
                chunk_written = 0
                while True:
                    try:
                        if use_ledger and mysql_chunk_committed(
                            cur,
                            job_id=job_id,
                            batch_key=write_batch_key,
                            chunk_idx=chunk_idx,
                        ):
                            chunk_written = len(batch)
                            break
                        write_batch = batch
                        if (
                            write_mode == "upsert"
                            and conflict_columns
                            and DF_LSN_COL in target_cols
                        ):
                            conflict_cols = [c for c in conflict_columns if c in target_cols]
                            write_batch, skipped = filter_stale_lsn_rows(
                                cur,
                                table_name,
                                None,
                                conflict_cols,
                                batch,
                                target_cols,
                                quote="`",
                                placeholder="%s",
                            )
                            rows_skipped += skipped
                        if write_batch:
                            cur.executemany(insert_sql, write_batch)
                        if use_ledger:
                            mark_mysql_chunk_committed(
                                cur,
                                job_id=job_id,
                                batch_key=write_batch_key,
                                chunk_idx=chunk_idx,
                                rows_written=len(write_batch),
                            )
                        conn.commit()
                        chunk_written = len(write_batch)
                        break
                    except Exception as chunk_exc:
                        try:
                            conn.rollback()
                        except Exception as exc:
                            logger.warning("Exception suppressed: %s", exc, exc_info=exc)
                        # Bad cells: write row-by-row and quarantine failures so one
                        # Incorrect datetime cannot abort a 100k-row transfer.
                        if is_sql_data_error(chunk_exc) and policy in {"quarantine", "coerce_null"}:
                            for row_i, row in enumerate(batch):
                                try:
                                    cur.execute(insert_sql, row)
                                    conn.commit()
                                    chunk_written += 1
                                except Exception as row_exc:
                                    try:
                                        conn.rollback()
                                    except Exception as exc:
                                        logger.debug("Cleanup exception suppressed: %s", exc, exc_info=exc)
                                    if is_connection_lost(row_exc):
                                        raise
                                    source_row = start + row_i
                                    col_name = extract_column_from_sql_error(row_exc) or "*"
                                    sample_val = ""
                                    if col_name != "*" and col_name in target_cols:
                                        try:
                                            sample_val = str(row[target_cols.index(col_name)])[:120]
                                        except Exception:
                                            sample_val = ""
                                    rejected_details.append({
                                        "row": source_row,
                                        "column": col_name,
                                        "value": sample_val,
                                        "reason": str(row_exc)[:300],
                                        "policy": policy,
                                    })
                                    transform_errors.append(str(row_exc)[:200])
                            if use_ledger and chunk_written:
                                try:
                                    mark_mysql_chunk_committed(
                                        cur,
                                        job_id=job_id,
                                        batch_key=write_batch_key,
                                        chunk_idx=chunk_idx,
                                        rows_written=chunk_written,
                                    )
                                    conn.commit()
                                except Exception as exc:
                                    logger.warning("Exception suppressed: %s", exc, exc_info=exc)
                            break
                        attempt += 1
                        if not is_connection_lost(chunk_exc) or not should_retry_connection_lost(
                            attempt=attempt, started_at=chunk_started, proxy=proxy_dest
                        ):
                            raise
                        time.sleep(reconnect_backoff_seconds(attempt))
                        _reconnect()

                written += chunk_written
                chunks_completed = chunk_idx + 1
                if on_checkpoint:
                    on_checkpoint(chunks_completed, chunks, written)
        finally:
            try:
                cur.close()
            except Exception as exc:
                logger.warning("Exception suppressed: %s", exc, exc_info=exc)

        close_quietly(conn)
        return WriteResult(
            ok=True, rows_written=written, table_name=table_name, target_schema=database,
            checksum=row_checksum(
                converted_rows or mapped_rows,
                target_cols,
                dest_db_type="mysql",
                dest_types={c: target_types[i] for i, c in enumerate(target_cols)},
            ),
            chunks_completed=chunks_completed or chunks,
            rejected_rows=max(rejected_rows, len(data_rows) - written),
            rejected_details=rejected_details,
            coerced_null_rows=coerced_null_rows,
            rows_skipped=rows_skipped,
            warnings=transform_errors,
        )
    except Exception as exc:
        close_quietly(conn)
        return WriteResult(
            ok=False,
            rows_written=written,
            table_name=table_name,
            target_schema=database,
            checksum=row_checksum(
                converted_rows or mapped_rows,
                target_cols,
                dest_db_type="mysql",
                dest_types={c: target_types[i] for i, c in enumerate(target_cols)}
                if target_types
                else None,
            )
            if written
            else "",
            chunks_completed=chunks_completed,
            error=str(exc),
            rejected_rows=rejected_rows,
            rejected_details=rejected_details,
            coerced_null_rows=coerced_null_rows,
            rows_skipped=rows_skipped if 'rows_skipped' in locals() else 0,
            warnings=transform_errors,
        )
