"""MySQL bulk writer — batched INSERT with checkpoint callbacks."""

from __future__ import annotations

import base64
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from connectors.mysql_conn import get_connection
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
    driver: str = "pymysql"
    rejected_rows: int = 0
    warnings: list[str] = field(default_factory=list)


def mysql_type(inferred: str) -> str:
    return ddl_type("mysql", inferred)


def _to_mysql_value(value: Any, source_type: str) -> Any:
    """Normalize transform-engine values to forms pymysql/MySQL can bind."""
    if value is None:
        return None
    upper = source_type.upper()
    if upper in {"DATETIME", "TIMESTAMP", "TIMESTAMP_TZ", "TIMESTAMPTZ"}:
        if isinstance(value, str):
            # ISO 8601 with Z -> naive UTC DATETIME
            text = value.strip()
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            try:
                dt = datetime.fromisoformat(text)
                if dt.tzinfo is not None:
                    dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
                return dt
            except ValueError:
                return value
        return value
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
) -> WriteResult:
    del schema
    try:
        import pymysql
    except ImportError:
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

    target_cols, source_types = resolve_target_columns(mappings, column_types)
    if not target_cols:
        return WriteResult(
            ok=False, rows_written=0, table_name=table_name, target_schema=database,
            checksum="", chunks_completed=0, error="No column mappings",
        )

    table_name = sanitize_identifier(table_name)
    target_types = [mysql_type(t) for t in source_types]
    policy = transform_error_policy(error_policy)

    try:
        conn = get_connection(
            host=host, port=port, database=database,
            username=username, password=password,
            connection_string=connection_string, ssl=ssl,
        )
        conn.autocommit = False

        with conn.cursor() as cur:
            if create_table:
                col_defs = ", ".join(f"`{c}` {t}" for c, t in zip(target_cols, target_types))
                cur.execute(f"CREATE TABLE IF NOT EXISTS `{table_name}` ({col_defs})")

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
                    ok=False, rows_written=0, table_name=table_name, target_schema=database,
                    checksum="", chunks_completed=0,
                    error=f"Transform errors: {'; '.join(transform_errors[:3])}",
                    rejected_rows=rejected_rows,
                    warnings=transform_errors,
                )

            total = len(mapped_rows)
            chunks = max(1, (total + CHUNK_SIZE - 1) // CHUNK_SIZE)
            written = 0
            placeholders = ", ".join(["%s"] * len(target_cols))
            col_names = ", ".join(f"`{c}`" for c in target_cols)
            if write_mode == "upsert" and conflict_columns:
                conflict = [c for c in conflict_columns if c in target_cols]
                if conflict:
                    update_cols = [c for c in target_cols if c not in conflict]
                    if update_cols:
                        updates = ", ".join(f"`{c}`=VALUES(`{c}`)" for c in update_cols)
                        insert_sql = (
                            f"INSERT INTO `{table_name}` ({col_names}) VALUES ({placeholders}) "
                            f"ON DUPLICATE KEY UPDATE {updates}"
                        )
                    else:
                        insert_sql = (
                            f"INSERT IGNORE INTO `{table_name}` ({col_names}) VALUES ({placeholders})"
                        )
                else:
                    insert_sql = f"INSERT INTO `{table_name}` ({col_names}) VALUES ({placeholders})"
            else:
                insert_sql = f"INSERT INTO `{table_name}` ({col_names}) VALUES ({placeholders})"

            converted_rows = [
                tuple(_to_mysql_value(v, source_types[i]) for i, v in enumerate(row))
                for row in mapped_rows
            ]

            for chunk_idx in range(chunks):
                start = chunk_idx * CHUNK_SIZE
                batch = converted_rows[start : start + CHUNK_SIZE]
                if not batch:
                    break
                cur.executemany(insert_sql, batch)
                conn.commit()
                written += len(batch)
                if on_checkpoint:
                    on_checkpoint(chunk_idx + 1, chunks, written)

        conn.close()
        return WriteResult(
            ok=True, rows_written=written, table_name=table_name, target_schema=database,
            checksum=row_checksum(mapped_rows), chunks_completed=chunks,
            rejected_rows=len(data_rows) - written,
            warnings=transform_errors,
        )
    except Exception as exc:
        return WriteResult(
            ok=False, rows_written=0, table_name=table_name, target_schema=database,
            checksum="", chunks_completed=0, error=str(exc),
        )
