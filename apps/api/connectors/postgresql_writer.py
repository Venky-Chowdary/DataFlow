"""PostgreSQL bulk writer — CSV file to table with checkpoint batches."""

from __future__ import annotations

import importlib.util
import io
import json
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable

from connectors.postgresql_conn import get_connection
from connectors.write_resilience import (
    build_write_batch_key,
    close_quietly,
    ensure_postgres_write_ledger,
    is_connection_lost,
    is_public_proxy_host,
    mark_postgres_chunk_committed,
    postgres_chunk_committed,
    reconnect_backoff_seconds,
    should_retry_connection_lost,
    write_chunk_size,
)
from services.value_serializer import json_default
from connectors.writer_common import (
    DF_LSN_COL,
    _coerced_null_row_count,
    _rejected_row_count,
    build_mapped_rows_with_details,
    dedupe_rows,
    dedupe_rows_by_pk_and_lsn,
    postgres_lsn_update_guard_sql,
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
    driver: str = "psycopg2"


def pg_type(inferred: str) -> str:
    return ddl_type("postgresql", inferred)


def _copy_text_value(value: Any) -> str:
    if value is None:
        return "\\N"
    if isinstance(value, bool):
        return "t" if value else "f"
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=json_default)
    if isinstance(value, bytes):
        return "\\x" + value.hex()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value)
    return text.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")


def _copy_rows(cur, schema: str, table_name: str, columns: list[str], rows: list[tuple]) -> None:
    from psycopg2 import sql

    cols_sql = sql.SQL(", ").join(map(sql.Identifier, columns))
    copy_sql = sql.SQL("COPY {}.{} ({}) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N')").format(
        sql.Identifier(schema),
        sql.Identifier(table_name),
        cols_sql,
    )
    buf = io.StringIO()
    for row in rows:
        buf.write("\t".join(_copy_text_value(v) for v in row))
        buf.write("\n")
    buf.seek(0)
    cur.copy_expert(copy_sql, buf)


def _open_pg(
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
    if importlib.util.find_spec("psycopg2") is None:
        from connectors.driver_guard import require_driver, stub_writes_allowed
        from connectors.stub_writer import simulate_stub_write

        if not stub_writes_allowed():
            return WriteResult(
                ok=False, rows_written=0, table_name=table_name, target_schema=schema or "public",
                checksum="", chunks_completed=0,
                error=require_driver("psycopg2", "psycopg2-binary"),
                driver="none",
            )
        rows, checksum, chunks = simulate_stub_write(
            data_rows=data_rows, table_name=table_name, target_schema=schema or "public",
            on_checkpoint=on_checkpoint,
        )
        return WriteResult(
            ok=True, rows_written=rows, table_name=table_name, target_schema=schema or "public",
            checksum=checksum, chunks_completed=chunks, driver="stub",
        )

    from psycopg2 import sql

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
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema=schema or "public",
            checksum="",
            chunks_completed=0,
            error="No column mappings",
        )

    schema = schema or "public"
    table_name = sanitize_identifier(table_name, preserve_case=True)
    target_types = [pg_type(t) for t in logical_types]
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

    if any(t == "BYTEA" for t in target_types):
        from base64 import b64decode

        bytea_positions = [i for i, t in enumerate(target_types) if t == "BYTEA"]
        converted: list[tuple] = []
        for row in mapped_rows:
            row_list = list(row)
            for idx in bytea_positions:
                val = row_list[idx]
                if isinstance(val, str):
                    try:
                        row_list[idx] = b64decode(val, validate=True)
                    except Exception:
                        row_list[idx] = val.encode("utf-8")
                elif isinstance(val, bytes):
                    row_list[idx] = val
                elif val is not None:
                    row_list[idx] = str(val).encode("utf-8")
            converted.append(tuple(row_list))
        mapped_rows = converted

    rejected_rows = _rejected_row_count(data_rows, mapped_rows, rejected_details, policy)
    coerced_null_rows = _coerced_null_row_count(rejected_details, policy)
    if transform_errors and policy == "fail":
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema=schema,
            checksum="",
            chunks_completed=0,
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
    proxy_dest = is_public_proxy_host(host) or is_public_proxy_host(connection_string)
    # COPY is faster locally but long COPY streams are a common Railway proxy kill.
    # Prefer chunked INSERT on public proxies so reconnect/ledger can resume cleanly.
    use_copy = (
        write_mode == "insert"
        and not conflict_columns
        and not any(t == "BYTEA" for t in target_types)
        and port != 5439
        and not proxy_dest
    )
    job_id = str(_kwargs.get("job_id") or "").strip()
    write_batch_key = str(_kwargs.get("write_batch_key") or "").strip() or build_write_batch_key(
        table_name=table_name,
        file_batch_idx=_kwargs.get("file_batch_idx"),
    )
    use_ledger = bool(job_id)
    conn = None

    def _build_insert():
        placeholders = sql.SQL(", ").join(sql.Placeholder() * len(target_cols))
        if write_mode == "upsert" and conflict_columns:
            conflict = [c for c in conflict_columns if c in target_cols]
            if conflict:
                update_cols = [c for c in target_cols if c not in conflict]
                if update_cols:
                    set_clause = sql.SQL(", ").join(
                        sql.SQL("{} = EXCLUDED.{}").format(
                            sql.Identifier(c), sql.Identifier(c)
                        )
                        for c in update_cols
                    )
                    if DF_LSN_COL in target_cols:
                        return sql.SQL(
                            "INSERT INTO {}.{} ({}) VALUES ({}) ON CONFLICT ({}) "
                            "DO UPDATE SET {} WHERE {}"
                        ).format(
                            sql.Identifier(schema),
                            sql.Identifier(table_name),
                            sql.SQL(", ").join(map(sql.Identifier, target_cols)),
                            placeholders,
                            sql.SQL(", ").join(map(sql.Identifier, conflict)),
                            set_clause,
                            sql.SQL(postgres_lsn_update_guard_sql(table_name)),
                        )
                    return sql.SQL(
                        "INSERT INTO {}.{} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}"
                    ).format(
                        sql.Identifier(schema),
                        sql.Identifier(table_name),
                        sql.SQL(", ").join(map(sql.Identifier, target_cols)),
                        placeholders,
                        sql.SQL(", ").join(map(sql.Identifier, conflict)),
                        set_clause,
                    )
                return sql.SQL(
                    "INSERT INTO {}.{} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING"
                ).format(
                    sql.Identifier(schema),
                    sql.Identifier(table_name),
                    sql.SQL(", ").join(map(sql.Identifier, target_cols)),
                    placeholders,
                    sql.SQL(", ").join(map(sql.Identifier, conflict)),
                )
        return sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
            sql.Identifier(schema),
            sql.Identifier(table_name),
            sql.SQL(", ").join(map(sql.Identifier, target_cols)),
            placeholders,
        )

    def _reconnect():
        nonlocal conn, cur
        close_quietly(conn)
        conn = _open_pg(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            connection_string=connection_string,
            ssl=ssl,
        )
        cur = conn.cursor()

    def _run_setup(cursor) -> None:
        if use_ledger:
            ensure_postgres_write_ledger(cursor, schema)
        if create_table:
            cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
            col_defs = sql.SQL(", ").join(
                sql.SQL("{} {}").format(sql.Identifier(c), sql.SQL(t))
                for c, t in zip(target_cols, target_types)
            )
            cursor.execute(
                sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
                    sql.Identifier(schema),
                    sql.Identifier(table_name),
                    col_defs,
                )
            )

        if backfill_new_fields:
            cursor.execute(
                """SELECT column_name FROM information_schema.columns
                   WHERE table_schema = %s AND table_name = %s""",
                (schema, table_name),
            )
            existing = {row[0] for row in cursor.fetchall()}
            for col, typ in zip(target_cols, target_types):
                if col not in existing:
                    cursor.execute(
                        sql.SQL("ALTER TABLE {}.{} ADD COLUMN {} {}").format(
                            sql.Identifier(schema),
                            sql.Identifier(table_name),
                            sql.Identifier(col),
                            sql.SQL(typ),
                        )
                    )

        if write_mode == "upsert" and conflict_columns:
            conflict_cols = [c for c in conflict_columns if c in target_cols]
            if conflict_cols:
                index_name = sanitize_identifier(
                    f"uidx_{table_name}_{'_'.join(conflict_cols)}"
                )
                cursor.execute(
                    sql.SQL(
                        "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {}.{} ({})"
                    ).format(
                        sql.Identifier(index_name),
                        sql.Identifier(schema),
                        sql.Identifier(table_name),
                        sql.SQL(", ").join(sql.Identifier(c) for c in conflict_cols),
                    )
                )
        conn.commit()

    try:
        conn = _open_pg(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            connection_string=connection_string,
            ssl=ssl,
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

            insert = None if use_copy else _build_insert()

            for chunk_idx in range(chunks):
                start = chunk_idx * chunk_size
                batch = mapped_rows[start : start + chunk_size]
                if not batch:
                    break

                attempt = 0
                chunk_started = time.monotonic()
                while True:
                    try:
                        if use_ledger and postgres_chunk_committed(
                            cur,
                            schema=schema,
                            job_id=job_id,
                            batch_key=write_batch_key,
                            chunk_idx=chunk_idx,
                        ):
                            break
                        if use_copy:
                            _copy_rows(cur, schema, table_name, target_cols, batch)
                        else:
                            cur.executemany(insert, batch)
                        if use_ledger:
                            mark_postgres_chunk_committed(
                                cur,
                                schema=schema,
                                job_id=job_id,
                                batch_key=write_batch_key,
                                chunk_idx=chunk_idx,
                                rows_written=len(batch),
                            )
                        conn.commit()
                        break
                    except Exception as chunk_exc:
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                        attempt += 1
                        if not is_connection_lost(chunk_exc) or not should_retry_connection_lost(
                            attempt=attempt, started_at=chunk_started, proxy=proxy_dest
                        ):
                            raise
                        time.sleep(reconnect_backoff_seconds(attempt))
                        _reconnect()
                        if not use_copy:
                            insert = _build_insert()

                written += len(batch)
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
            ok=True,
            rows_written=written,
            table_name=table_name,
            target_schema=schema,
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
            target_schema=schema or "public",
            checksum=row_checksum(mapped_rows, target_cols) if written else "",
            chunks_completed=chunks_completed,
            error=str(exc),
            rejected_rows=rejected_rows,
            rejected_details=rejected_details,
            coerced_null_rows=coerced_null_rows,
            warnings=transform_errors,
        )
