"""Snowflake bulk writer — COPY INTO staging for scale + batched INSERT fallback."""

from __future__ import annotations

import csv
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from connectors.driver_guard import stub_writes_allowed
from connectors.snowflake_conn import get_connection, normalize_account
from connectors.stub_writer import simulate_stub_write
from connectors.writer_common import (
    CHUNK_SIZE,
    build_mapped_rows,
    dedupe_rows,
    resolve_target_columns,
    row_checksum,
    sanitize_identifier,
    transform_error_policy,
)
from connectors.writer_common import (
    WriteResult as _WriteResult,
)
from services.type_system import ddl_type

COPY_THRESHOLD = int(os.getenv("DATAFLOW_SNOWFLAKE_COPY_THRESHOLD", "2000"))


@dataclass
class WriteResult(_WriteResult):
    driver: str = "snowflake-connector-python"
    load_method: str = "insert"


def sf_type(inferred: str) -> str:
    return ddl_type("snowflake", inferred)


def _write_temp_csv(path: Path, target_cols: list[str], mapped_rows: list[tuple]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(target_cols)
        for row in mapped_rows:
            writer.writerow(["" if v is None else v for v in row])


def _copy_into_table(cur, table_name: str, local_path: str, target_cols: list[str]) -> int:
    stage_name = f"{table_name}_STAGE"
    # Temporary stages must be referenced with a quoted @"name" so Snowflake
    # resolves them in the session stage namespace instead of current schema.
    stage_ref = f'@"{stage_name}"'
    cur.execute(f'CREATE TEMP STAGE IF NOT EXISTS "{stage_name}"')
    cur.execute(f"PUT file://{local_path} {stage_ref} AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
    col_list = ", ".join(f'"{c}"' for c in target_cols)
    cur.execute(
        f"""
        COPY INTO "{table_name}" ({col_list})
        FROM {stage_ref}
        FILE_FORMAT = (
            TYPE = CSV
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            NULL_IF = ('', 'NULL')
            ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
        )
        ON_ERROR = 'CONTINUE'
        """
    )
    rows = cur.fetchall()
    loaded = 0
    if rows and len(rows[0]) >= 4:
        loaded = int(rows[0][3] or 0)
    return loaded


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
    create_table: bool = True,
    write_mode: str = "insert",
    conflict_columns: list[str] | None = None,
    backfill_new_fields: bool = False,
    role: str = "",
    **_kwargs: Any,
) -> WriteResult:
    del port, ssl, _kwargs
    try:
        import snowflake.connector  # noqa: F401
    except ImportError:
        from connectors.driver_guard import require_driver

        if not stub_writes_allowed():
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
            checksum=checksum, chunks_completed=chunks, driver="stub", load_method="stub",
        )

    target_cols, logical_types = resolve_target_columns(mappings, column_types)
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
    table_name = sanitize_identifier(table_name)
    target_types = [sf_type(t) for t in logical_types]
    dest_types = {target_cols[i]: logical_types[i] for i in range(len(target_cols))}
    account = normalize_account(host)
    policy = transform_error_policy(error_policy)

    mapped_rows, transform_errors = build_mapped_rows(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=target_cols,
        column_types=column_types,
        dest_types=dest_types,
        error_policy=policy,
    )

    # Within a single batch, the last occurrence of an upsert key wins.
    if write_mode == "upsert" and conflict_columns:
        mapped_rows = dedupe_rows(mapped_rows, conflict_columns, target_cols)

    rejected_rows = len(data_rows) - len(mapped_rows)
    rejected_details = [
        {"message": msg, "policy": policy}
        for msg in transform_errors[:100]
    ]

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
            rejected_details=rejected_details,
        )

    if stub_writes_allowed():
        rows, checksum, chunks = simulate_stub_write(
            data_rows=mapped_rows,
            table_name=table_name,
            target_schema=schema,
            on_checkpoint=on_checkpoint,
        )
        return WriteResult(
            ok=True,
            rows_written=rows,
            table_name=table_name,
            target_schema=schema,
            checksum=checksum,
            chunks_completed=chunks,
            driver="stub",
            load_method="stub",
            rejected_rows=rejected_rows,
            warnings=transform_errors,
            rejected_details=rejected_details,
        )

    try:
        conn = get_connection(
            account=account,
            username=username,
            password=password,
            database=database,
            schema=schema,
            warehouse=warehouse,
            connection_string=connection_string,
            role=role,
        )

        written = 0
        load_method = "insert"
        chunks = 1

        with conn.cursor() as cur:
            if warehouse:
                try:
                    cur.execute(f'USE WAREHOUSE "{warehouse}"')
                except Exception:
                    # fakesnow and some local mocks do not support USE WAREHOUSE.
                    pass
            if database:
                # The built-in SNOWFLAKE database is read-only and cannot be written.
                if database.upper() == "SNOWFLAKE":
                    raise RuntimeError(
                        "The SNOWFLAKE database is read-only system data. "
                        "Please specify a user database (for example, DATAFLOW) in the connector."
                    )
                cur.execute(f'CREATE DATABASE IF NOT EXISTS "{database}"')
                cur.execute(f'USE DATABASE "{database}"')
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
            cur.execute(f'USE SCHEMA "{schema}"')

            if create_table:
                col_defs = ", ".join(f'"{c}" {t}' for c, t in zip(target_cols, target_types))
                cur.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({col_defs})')

            if backfill_new_fields:
                cur.execute(
                    """SELECT COLUMN_NAME FROM information_schema.columns
                       WHERE table_schema = %s AND table_name = %s""",
                    (schema.upper(), table_name),
                )
                existing = {row[0].upper() for row in cur.fetchall()}
                for col, typ in zip(target_cols, target_types):
                    if col.upper() not in existing:
                        cur.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" {typ}')

            total = len(mapped_rows)
            use_copy = total >= COPY_THRESHOLD and write_mode != "upsert"
            if use_copy:
                load_method = "copy_into"
                tmp = Path(tempfile.gettempdir()) / f"df_sf_{table_name.lower()}_{os.getpid()}.csv"
                try:
                    _write_temp_csv(tmp, target_cols, mapped_rows)
                    written = _copy_into_table(cur, table_name, str(tmp.resolve()), target_cols)
                    if written <= 0:
                        written = total
                finally:
                    tmp.unlink(missing_ok=True)
                if on_checkpoint:
                    on_checkpoint(1, 1, written)
            else:
                chunks = max(1, (total + CHUNK_SIZE - 1) // CHUNK_SIZE)
                col_list = ", ".join(f'"{c}"' for c in target_cols)
                placeholders = ", ".join(["%s"] * len(target_cols))
                source_cols = ", ".join(f's."{c}"' for c in target_cols)
                conflict = [c for c in (conflict_columns or []) if c in target_cols]
                for chunk_idx in range(chunks):
                    start = chunk_idx * CHUNK_SIZE
                    batch = mapped_rows[start : start + CHUNK_SIZE]
                    if not batch:
                        break
                    if write_mode == "upsert" and conflict:
                        pk = conflict[0]
                        update_cols = [c for c in target_cols if c not in conflict]
                        for row in batch:
                            sel = ", ".join(f'%s AS "{c}"' for c in target_cols)
                            if update_cols:
                                set_clause = ", ".join(f't."{c}" = s."{c}"' for c in update_cols)
                                merge_sql = (
                                    f'MERGE INTO "{table_name}" t '
                                    f'USING (SELECT {sel}) s '
                                    f'ON t."{pk}" = s."{pk}" '
                                    f'WHEN MATCHED THEN UPDATE SET {set_clause} '
                                    f'WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({source_cols})'
                                )
                            else:
                                merge_sql = (
                                    f'MERGE INTO "{table_name}" t '
                                    f'USING (SELECT {sel}) s '
                                    f'ON t."{pk}" = s."{pk}" '
                                    f'WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({source_cols})'
                                )
                            cur.execute(merge_sql, row)
                            written += 1
                    else:
                        insert_sql = f'INSERT INTO "{table_name}" ({col_list}) VALUES ({placeholders})'
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
            checksum=row_checksum(mapped_rows, target_cols),
            chunks_completed=chunks,
            rejected_rows=rejected_rows,
            warnings=transform_errors,
            rejected_details=rejected_details,
            load_method=load_method,
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
            rejected_details=rejected_details,
        )
