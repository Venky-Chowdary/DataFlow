"""PostgreSQL bulk writer — CSV file to table with checkpoint batches."""

from __future__ import annotations

import io
import json
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Callable

from connectors.postgresql_conn import get_connection
from connectors.writer_common import (
    CHUNK_SIZE,
    build_mapped_rows,
    dedupe_rows,
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
    driver: str = "psycopg2"
    rejected_rows: int = 0
    warnings: list[str] = field(default_factory=list)


def pg_type(inferred: str) -> str:
    return ddl_type("postgresql", inferred)


def _copy_text_value(value: Any) -> str:
    if value is None:
        return "\\N"
    if isinstance(value, bool):
        return "t" if value else "f"
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
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
    try:
        import psycopg2
        from psycopg2 import sql
    except ImportError:
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

    target_cols, source_types = resolve_target_columns(mappings, column_types, preserve_case=True)
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
    target_types = [pg_type(t) for t in source_types]
    policy = transform_error_policy(error_policy)

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
            if create_table:
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

            if backfill_new_fields:
                cur.execute(
                    """SELECT column_name FROM information_schema.columns
                       WHERE table_schema = %s AND table_name = %s""",
                    (schema, table_name),
                )
                existing = {row[0] for row in cur.fetchall()}
                for col, typ in zip(target_cols, target_types):
                    if col not in existing:
                        cur.execute(
                            sql.SQL("ALTER TABLE {}.{} ADD COLUMN {} {}").format(
                                sql.Identifier(schema),
                                sql.Identifier(table_name),
                                sql.Identifier(col),
                                sql.SQL(typ),
                            )
                        )

            # Upsert requires a unique constraint/index on the conflict columns.
            if write_mode == "upsert" and conflict_columns:
                conflict_cols = [c for c in conflict_columns if c in target_cols]
                if conflict_cols:
                    index_name = sanitize_identifier(
                        f"uidx_{table_name}_{'_'.join(conflict_cols)}"
                    )
                    cur.execute(
                        sql.SQL(
                            "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {}.{} ({})"
                        ).format(
                            sql.Identifier(index_name),
                            sql.Identifier(schema),
                            sql.Identifier(table_name),
                            sql.SQL(", ").join(sql.Identifier(c) for c in conflict_cols),
                        )
                    )

            mapped_rows, transform_errors = build_mapped_rows(
                headers=headers,
                data_rows=data_rows,
                mappings=mappings,
                target_cols=target_cols,
                column_types=column_types,
                error_policy=policy,
                preserve_case=True,
            )

            # Within a single batch, the last occurrence of an upsert key wins.
            # This avoids target-row count mismatches and ON CONFLICT churn.
            if write_mode == "upsert" and conflict_columns:
                mapped_rows = dedupe_rows(mapped_rows, conflict_columns, target_cols)

            # Convert base64-encoded strings to raw bytes for BYTEA columns
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
            use_copy = (
                write_mode == "insert"
                and not conflict_columns
                and not any(t == "BYTEA" for t in target_types)
                and port != 5439
            )

            for chunk_idx in range(chunks):
                start = chunk_idx * CHUNK_SIZE
                batch = mapped_rows[start : start + CHUNK_SIZE]
                if not batch:
                    break

                if use_copy:
                    _copy_rows(cur, schema, table_name, target_cols, batch)
                else:
                    placeholders = sql.SQL(", ").join(sql.Placeholder() * len(target_cols))
                    if write_mode == "upsert" and conflict_columns:
                        conflict = [c for c in conflict_columns if c in target_cols]
                        if conflict:
                            update_cols = [c for c in target_cols if c not in conflict]
                            if update_cols:
                                insert = sql.SQL(
                                    "INSERT INTO {}.{} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}"
                                ).format(
                                    sql.Identifier(schema),
                                    sql.Identifier(table_name),
                                    sql.SQL(", ").join(map(sql.Identifier, target_cols)),
                                    placeholders,
                                    sql.SQL(", ").join(map(sql.Identifier, conflict)),
                                    sql.SQL(", ").join(
                                        sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
                                        for c in update_cols
                                    ),
                                )
                            else:
                                insert = sql.SQL(
                                    "INSERT INTO {}.{} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING"
                                ).format(
                                    sql.Identifier(schema),
                                    sql.Identifier(table_name),
                                    sql.SQL(", ").join(map(sql.Identifier, target_cols)),
                                    placeholders,
                                    sql.SQL(", ").join(map(sql.Identifier, conflict)),
                                )
                        else:
                            insert = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
                                sql.Identifier(schema),
                                sql.Identifier(table_name),
                                sql.SQL(", ").join(map(sql.Identifier, target_cols)),
                                placeholders,
                            )
                    else:
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
            rejected_rows=len(data_rows) - written,
            warnings=transform_errors,
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
