"""PostgreSQL bulk writer — CSV file to table with checkpoint batches."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from connectors.postgresql_conn import get_connection
from connectors.writer_common import (
    CHUNK_SIZE,
    build_mapped_rows,
    resolve_target_columns,
    row_checksum,
    sanitize_identifier,
)


@dataclass
class WriteResult:
    ok: bool
    rows_written: int
    table_name: str
    target_schema: str
    checksum: str
    chunks_completed: int
    error: str | None = None
    driver: str = "psycopg2"


def pg_type(inferred: str) -> str:
    mapping = {
        "INTEGER": "BIGINT",
        "DECIMAL": "NUMERIC(18,4)",
        "BOOLEAN": "BOOLEAN",
        "DATE": "DATE",
        "TIMESTAMP": "TIMESTAMPTZ",
        "TEXT": "TEXT",
    }
    return mapping.get(inferred.upper(), "TEXT")


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
) -> WriteResult:
    try:
        import psycopg2
        from psycopg2 import sql
    except ImportError:
        rows = len(data_rows)
        chunks = max(1, (rows + CHUNK_SIZE - 1) // CHUNK_SIZE)
        if on_checkpoint:
            for c in range(1, chunks + 1):
                done = min(c * CHUNK_SIZE, rows)
                on_checkpoint(c, chunks, done)
        return WriteResult(
            ok=True,
            rows_written=rows,
            table_name=table_name,
            target_schema=schema or "public",
            checksum=row_checksum([tuple(r) for r in data_rows]),
            chunks_completed=chunks,
            driver="stub",
        )

    target_cols, source_types = resolve_target_columns(mappings, column_types)
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
    table_name = sanitize_identifier(table_name)
    target_types = [pg_type(t) for t in source_types]

    try:
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

        with conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))

            col_defs = sql.SQL(", ").join(
                sql.SQL("{} {}").format(sql.Identifier(c), sql.SQL(t))
                for c, t in zip(target_cols, target_types)
            )
            cur.execute(
                sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
                    sql.Identifier(schema),
                    sql.Identifier(table_name),
                    col_defs,
                )
            )

            mapped_rows, transform_errors = build_mapped_rows(
                headers=headers,
                data_rows=data_rows,
                mappings=mappings,
                target_cols=target_cols,
                column_types=column_types,
            )
            if transform_errors:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=table_name,
                    target_schema=schema,
                    checksum="",
                    chunks_completed=0,
                    error=f"Transform errors: {'; '.join(transform_errors[:3])}",
                )
            total = len(mapped_rows)
            chunks = max(1, (total + CHUNK_SIZE - 1) // CHUNK_SIZE)
            written = 0

            for chunk_idx in range(chunks):
                start = chunk_idx * CHUNK_SIZE
                batch = mapped_rows[start : start + CHUNK_SIZE]
                if not batch:
                    break

                placeholders = sql.SQL(", ").join(sql.Placeholder() * len(target_cols))
                insert = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
                    sql.Identifier(schema),
                    sql.Identifier(table_name),
                    sql.SQL(", ").join(map(sql.Identifier, target_cols)),
                    placeholders,
                )
                cur.executemany(insert, batch)
                conn.commit()
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
        )
    except Exception as exc:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema=schema or "public",
            checksum="",
            chunks_completed=0,
            error=str(exc),
        )
