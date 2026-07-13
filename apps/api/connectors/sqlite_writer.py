"""SQLite bulk writer — file-based SQL database with typed columns."""

from __future__ import annotations

import base64
import json
import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Callable

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
    driver: str = "sqlite3"
    rejected_rows: int = 0
    warnings: list[str] = field(default_factory=list)


def sqlite_type(inferred: str) -> str:
    return ddl_type("sqlite", inferred)


def _to_sqlite_value(value: Any, source_type: str) -> Any:
    if value is None:
        return None
    upper = source_type.upper()
    if upper in {"DECIMAL", "NUMERIC", "DOUBLE", "REAL", "FLOAT"}:
        if isinstance(value, Decimal):
            return float(value)
        return value
    if upper in {"JSON", "OBJECT", "ARRAY", "VARIANT"}:
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        return value
    if upper in {"BINARY", "BLOB", "BYTEA", "VARBINARY"}:
        if isinstance(value, bytes):
            return value
        if isinstance(value, str):
            try:
                return base64.b64decode(value, validate=True)
            except Exception:
                return value.encode("utf-8")
        return value
    if upper in {"DATETIME", "TIMESTAMP", "TIMESTAMP_TZ", "TIMESTAMPTZ", "TIMESTAMP_LTZ"}:
        if isinstance(value, datetime):
            return value.isoformat()
        return value
    if upper == "DATE":
        if isinstance(value, date):
            return value.isoformat()
        return value
    if upper == "TIME":
        if isinstance(value, time):
            return value.isoformat()
        return value
    if upper == "BOOLEAN":
        return 1 if value else 0
    return value


def _sqlite_upsert_batch(
    cur: Any,
    table_name: str,
    target_cols: list[str],
    batch: list[tuple],
    conflict_cols: list[str],
) -> None:
    """Delete rows matching conflict keys, then insert the deduplicated batch.

    Keeps the last occurrence of each conflict key so the final insert is unique.
    """
    indices = [target_cols.index(c) for c in conflict_cols]
    deduped: dict[tuple, tuple] = {}
    for row in batch:
        key = tuple(row[i] for i in indices)
        deduped[key] = row
    rows = list(deduped.values())

    col_sql = ", ".join(f"\"{c}\"" for c in conflict_cols)
    placeholders = ", ".join("(" + ", ".join("?" for _ in conflict_cols) + ")" for _ in deduped)
    delete_sql = f'DELETE FROM "{table_name}" WHERE ({col_sql}) IN ({placeholders})'
    delete_params = [v for key in deduped.keys() for v in key]
    cur.execute(delete_sql, delete_params)

    insert_sql = f'INSERT INTO "{table_name}" ({", ".join(f"\"{c}\"" for c in target_cols)}) VALUES ({", ".join("?" for _ in target_cols)})'
    cur.executemany(insert_sql, rows)


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
) -> WriteResult:
    """Write records to a SQLite database file."""
    del port, username, password, ssl
    path = connection_string or database or host
    if not path:
        return WriteResult(
            ok=False, rows_written=0, table_name=table_name, target_schema=schema or "main",
            checksum="", chunks_completed=0, error="SQLite path is required (database or connection_string).",
        )

    target_cols, source_types = resolve_target_columns(mappings, column_types, preserve_case=True)
    if not target_cols:
        return WriteResult(
            ok=False, rows_written=0, table_name=table_name, target_schema=schema or "main",
            checksum="", chunks_completed=0, error="No column mappings",
        )

    table_name = sanitize_identifier(table_name, preserve_case=True)
    target_types = [sqlite_type(t) for t in source_types]
    policy = transform_error_policy(error_policy)

    try:
        conn = sqlite3.connect(path, timeout=8)
        with conn:
            cur = conn.cursor()
            if create_table:
                col_defs = ", ".join(f'"{c}" {t}' for c, t in zip(target_cols, target_types))
                cur.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({col_defs})')

            if backfill_new_fields:
                existing = {row[1] for row in cur.execute(f'PRAGMA table_info("{table_name}")')}
                for col, typ in zip(target_cols, target_types):
                    if col not in existing:
                        try:
                            cur.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" {typ}')
                        except sqlite3.OperationalError:
                            pass

            mapped_rows, transform_errors = build_mapped_rows(
                headers=headers,
                data_rows=data_rows,
                mappings=mappings,
                target_cols=target_cols,
                column_types=column_types,
                error_policy=policy,
                preserve_case=True,
            )

            converted_rows = [
                tuple(_to_sqlite_value(v, source_types[i]) for i, v in enumerate(row))
                for row in mapped_rows
            ]

            rejected_rows = len(data_rows) - len(mapped_rows)
            if transform_errors and policy == "fail":
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=table_name,
                    target_schema=schema or "main",
                    checksum="",
                    chunks_completed=0,
                    error=f"Transform errors: {'; '.join(transform_errors[:3])}",
                    rejected_rows=rejected_rows,
                    warnings=transform_errors,
                )

            total = len(converted_rows)
            chunks = max(1, (total + CHUNK_SIZE - 1) // CHUNK_SIZE)
            written = 0
            placeholders = ", ".join("?" for _ in target_cols)
            insert = f'INSERT INTO "{table_name}" ({", ".join(f"\"{c}\"" for c in target_cols)}) VALUES ({placeholders})'
            conflict_cols = [c for c in (conflict_columns or []) if c in target_cols]
            for chunk_idx in range(chunks):
                start = chunk_idx * CHUNK_SIZE
                batch = converted_rows[start : start + CHUNK_SIZE]
                if not batch:
                    break

                if write_mode == "upsert" and conflict_cols:
                    _sqlite_upsert_batch(cur, table_name, target_cols, batch, conflict_cols)
                else:
                    cur.executemany(insert, batch)
                written += len(batch)
                if on_checkpoint:
                    on_checkpoint(chunk_idx + 1, chunks, written)

        conn.close()
        return WriteResult(
            ok=True,
            rows_written=written,
            table_name=table_name,
            target_schema=schema or "main",
            checksum=row_checksum(mapped_rows),
            chunks_completed=chunks,
            rejected_rows=len(data_rows) - written,
            warnings=transform_errors,
        )
    except Exception as exc:
        return WriteResult(
            ok=False, rows_written=0, table_name=table_name, target_schema=schema or "main",
            checksum="", chunks_completed=0, error=str(exc),
        )
