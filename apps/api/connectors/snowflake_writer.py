"""Snowflake bulk writer — staged batched INSERT with checkpoint callbacks."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

from connectors.snowflake_conn import get_connection, normalize_account
from connectors.writer_common import (
    CHUNK_SIZE,
    build_mapped_rows,
    resolve_target_columns,
    row_checksum,
    sanitize_identifier,
    transform_error_policy,
)
from services.type_system import ddl_type


@dataclass
class WriteResult:
    ok: bool
    rows_written: int
    table_name: str
    target_schema: str
    checksum: str
    chunks_completed: int
    error: str | None = None
    driver: str = "snowflake-connector-python"
    rejected_rows: int = 0
    warnings: list[str] = field(default_factory=list)


def sf_type(inferred: str) -> str:
    return ddl_type("snowflake", inferred)


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
    warehouse: str,
    table_name: str,
    headers: list[str],
    data_rows: list[list[str]],
    mappings: list[dict],
    column_types: dict[str, str],
    on_checkpoint: Callable[[int, int, int], None] | None = None,
    error_policy: str | None = None,
) -> WriteResult:
    del port, ssl
    try:
        import snowflake.connector  # noqa: F401
    except ImportError:
        from connectors.driver_guard import allow_stub_writes, require_driver
        from connectors.stub_writer import simulate_stub_write

        if not allow_stub_writes():
            return WriteResult(
                ok=False, rows_written=0, table_name=table_name, target_schema=schema or "PUBLIC",
                checksum="", chunks_completed=0,
                error=require_driver("snowflake.connector", "snowflake-connector-python"),
                driver="none",
            )
        rows, checksum, chunks = simulate_stub_write(
            data_rows=data_rows, table_name=table_name, target_schema=schema or "PUBLIC",
            on_checkpoint=on_checkpoint,
        )
        return WriteResult(
            ok=True, rows_written=rows, table_name=table_name, target_schema=schema or "PUBLIC",
            checksum=checksum, chunks_completed=chunks, driver="stub",
        )

    target_cols, source_types = resolve_target_columns(mappings, column_types)
    if not target_cols:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema=schema or "PUBLIC",
            checksum="",
            chunks_completed=0,
            error="No column mappings",
        )

    schema = (schema or "PUBLIC").upper()
    table_name = sanitize_identifier(table_name).upper()
    target_types = [sf_type(t) for t in source_types]
    account = normalize_account(host)
    policy = transform_error_policy(error_policy)

    try:
        conn = get_connection(
            account=account,
            username=username,
            password=password,
            database=database,
            schema=schema,
            warehouse=warehouse,
            connection_string=connection_string,
        )

        with conn.cursor() as cur:
            if warehouse:
                cur.execute(f"USE WAREHOUSE {warehouse}")
            if database:
                cur.execute(f"USE DATABASE {database}")
            cur.execute(f"USE SCHEMA {schema}")

            col_defs = ", ".join(f'"{c}" {t}' for c, t in zip(target_cols, target_types))
            cur.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({col_defs})')

            mapped_rows, transform_errors = build_mapped_rows(
                headers=headers,
                data_rows=data_rows,
                mappings=mappings,
                target_cols=target_cols,
                column_types=column_types,
                error_policy=policy,
            )
            rejected_rows = len(data_rows) - len(mapped_rows)
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
                    warnings=transform_errors,
                )
            total = len(mapped_rows)
            chunks = max(1, (total + CHUNK_SIZE - 1) // CHUNK_SIZE)
            written = 0
            col_list = ", ".join(f'"{c}"' for c in target_cols)
            placeholders = ", ".join(["%s"] * len(target_cols))
            insert_sql = f'INSERT INTO "{table_name}" ({col_list}) VALUES ({placeholders})'

            for chunk_idx in range(chunks):
                start = chunk_idx * CHUNK_SIZE
                batch = mapped_rows[start : start + CHUNK_SIZE]
                if not batch:
                    break
                cur.executemany(insert_sql, batch)
                written += len(batch)
                if on_checkpoint:
                    on_checkpoint(chunk_idx + 1, chunks, written)

        conn.close()
        return WriteResult(
            ok=True,
            rows_written=written,
            table_name=table_name,
            target_schema=schema,
            checksum=row_checksum(mapped_rows),
            chunks_completed=chunks,
            rejected_rows=len(data_rows) - written,
            warnings=transform_errors,
        )
    except Exception as exc:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema=schema,
            checksum="",
            chunks_completed=0,
            error=str(exc),
        )
