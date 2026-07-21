"""MySQL bulk writer — batched INSERT with checkpoint callbacks."""

from __future__ import annotations

import base64
import time
from dataclasses import dataclass
from typing import Any, Callable

from connectors.mysql_conn import get_connection
from connectors.sql_temporal import (
    coerce_sql_temporal,
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
    quote_sql_identifier,
    resolve_target_columns,
    row_checksum,
    sanitize_identifier,
    transform_error_policy,
)
from connectors.writer_common import (
    WriteResult as _WriteResult,
)
from services.type_system import ddl_type


@dataclass
class WriteResult(_WriteResult):
    driver: str = "pymysql"


def mysql_type(inferred: str) -> str:
    return ddl_type("mysql", inferred)


def _to_mysql_value(value: Any, source_type: str) -> Any:
    """Normalize transform-engine values to forms pymysql/MySQL can bind."""
    if value is None:
        return None
    temporal = coerce_sql_temporal(value, source_type)
    if temporal is not value:
        return temporal
    from connectors.sql_temporal import sql_base_type

    upper = sql_base_type(source_type)
    if upper in {"BINARY", "BLOB", "LONGBLOB", "VARBINARY", "BYTEA"}:
        if isinstance(value, bytes):
            return value
        if isinstance(value, str):
            try:
                return base64.b64decode(value, validate=True)
            except Exception:
                return value.encode("utf-8")
        return value
    return value

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

    if write_mode == "upsert" and conflict_columns:
        if DF_LSN_COL in target_cols:
            mapped_rows = dedupe_rows_by_pk_and_lsn(
                mapped_rows, conflict_columns, target_cols
            )
        else:
            mapped_rows = dedupe_rows(mapped_rows, conflict_columns, target_cols)

    rejected_rows = _rejected_row_count(data_rows, mapped_rows, rejected_details, policy)
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
                    lsn_q = quote_sql_identifier(DF_LSN_COL, "`")
                    newer = f"VALUES({lsn_q}) > COALESCE({lsn_q}, '')"
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
                    f"INSERT INTO {table_q} ({col_names}) VALUES ({placeholders}) "
                    f"ON DUPLICATE KEY UPDATE {updates}"
                )
            else:
                insert_sql = (
                    f"INSERT IGNORE INTO {table_q} ({col_names}) VALUES ({placeholders})"
                )
        else:
            insert_sql = f"INSERT INTO {table_q} ({col_names}) VALUES ({placeholders})"
    else:
        insert_sql = f"INSERT INTO {table_q} ({col_names}) VALUES ({placeholders})"

    converted_rows = [
        tuple(_to_mysql_value(v, target_types[i]) for i, v in enumerate(row))
        for row in mapped_rows
    ]
    proxy_dest = is_public_proxy_host(host) or is_public_proxy_host(connection_string)
    job_id = str(_kwargs.get("job_id") or "").strip()
    write_batch_key = str(_kwargs.get("write_batch_key") or "").strip() or build_write_batch_key(
        table_name=table_name,
        file_batch_idx=_kwargs.get("file_batch_idx"),
    )
    use_ledger = bool(job_id)
    conn = None

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
                    except Exception:
                        pass
                    setup_attempt += 1
                    if not is_connection_lost(setup_exc) or not should_retry_connection_lost(
                        attempt=setup_attempt, started_at=setup_started, proxy=proxy_dest
                    ):
                        raise
                    time.sleep(reconnect_backoff_seconds(setup_attempt))
                    _reconnect()

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
                        cur.executemany(insert_sql, batch)
                        if use_ledger:
                            mark_mysql_chunk_committed(
                                cur,
                                job_id=job_id,
                                batch_key=write_batch_key,
                                chunk_idx=chunk_idx,
                                rows_written=len(batch),
                            )
                        conn.commit()
                        chunk_written = len(batch)
                        break
                    except Exception as chunk_exc:
                        try:
                            conn.rollback()
                        except Exception:
                            pass
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
                                    except Exception:
                                        pass
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
                                except Exception:
                                    pass
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
            except Exception:
                pass

        close_quietly(conn)
        return WriteResult(
            ok=True, rows_written=written, table_name=table_name, target_schema=database,
            checksum=row_checksum(mapped_rows, target_cols),
            chunks_completed=chunks_completed or chunks,
            rejected_rows=max(rejected_rows, len(data_rows) - written),
            rejected_details=rejected_details,
            coerced_null_rows=coerced_null_rows,
            warnings=transform_errors,
        )
    except Exception as exc:
        close_quietly(conn)
        return WriteResult(
            ok=False,
            rows_written=written,
            table_name=table_name,
            target_schema=database,
            checksum=row_checksum(mapped_rows, target_cols) if written else "",
            chunks_completed=chunks_completed,
            error=str(exc),
            rejected_rows=rejected_rows,
            rejected_details=rejected_details,
            coerced_null_rows=coerced_null_rows,
            warnings=transform_errors,
        )
