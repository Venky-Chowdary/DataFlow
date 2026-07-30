"""PostgreSQL bulk writer — CSV file to table with checkpoint batches."""

from __future__ import annotations

import binascii
import importlib.util
import io
import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from services.schema_inference import infer_type
from services.type_system import ddl_type
from services.value_serializer import json_default

from connectors.postgresql_conn import get_connection
from connectors.schema_drift import is_wider_type, widen_existing_columns_native
from connectors.sql_temporal import (
    extract_column_from_sql_error,
    is_sql_data_error,
)
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
from connectors.writer_common import (
    DF_LSN_COL,
    _coerced_null_row_count,
    _rejected_row_count,
    assert_sparse_upsert_has_pk,
    build_mapped_rows_with_details,
    dedupe_rows,
    dedupe_rows_by_pk_and_lsn,
    filter_stale_lsn_rows,
    postgres_lsn_update_guard_sql,
    quarantine_unfit_decimals,
    quarantine_unfit_specialty_types,
    quarantine_unfit_strings,
    resolve_target_columns,
    row_checksum,
    sanitize_identifier,
    sparse_present_bindings,
    transform_error_policy,
)
from connectors.writer_common import (
    WriteResult as _WriteResult,
)

logger = logging.getLogger(__name__)



def _pg_apply_sparse_upsert(
    cursor: Any,
    sql: Any,
    *,
    schema: str,
    table_name: str,
    target_cols: list[str],
    conflict_columns: list[str],
    sparse_rows: list[tuple],
) -> int:
    """Per-row upsert omitting DF_MISSING — never SET col=NULL for absent CDC fields."""
    from services.cdc_effectively_once import should_apply_pk_row

    conflict = [c for c in conflict_columns if c in target_cols]
    if not conflict:
        raise ValueError("sparse PostgreSQL upsert requires conflict_columns")
    written = 0
    for row in sparse_rows:
        present = sparse_present_bindings(row, target_cols)
        assert_sparse_upsert_has_pk(present, conflict)
        non_pk = {k: v for k, v in present.items() if k not in conflict}
        pk_vals = [present[c] for c in conflict]
        where = sql.SQL(" AND ").join(
            sql.SQL("{} = %s").format(sql.Identifier(c)) for c in conflict
        )
        if DF_LSN_COL in present and DF_LSN_COL in target_cols:
            cursor.execute(
                sql.SQL("SELECT {} FROM {}.{} WHERE {}").format(
                    sql.Identifier(DF_LSN_COL),
                    sql.Identifier(schema),
                    sql.Identifier(table_name),
                    where,
                ),
                pk_vals,
            )
            existing = cursor.fetchone()
            if existing is not None:
                if not should_apply_pk_row(
                    existing_lsn=existing[0],
                    incoming_lsn=present[DF_LSN_COL],
                ).applied:
                    continue
        if non_pk:
            set_cols = list(non_pk.keys())
            set_clause = sql.SQL(", ").join(
                sql.SQL("{} = %s").format(sql.Identifier(c)) for c in set_cols
            )
            cursor.execute(
                sql.SQL("UPDATE {}.{} SET {} WHERE {}").format(
                    sql.Identifier(schema),
                    sql.Identifier(table_name),
                    set_clause,
                    where,
                ),
                [non_pk[c] for c in set_cols] + pk_vals,
            )
            if cursor.rowcount and cursor.rowcount > 0:
                written += 1
                continue
        cols = list(present.keys())
        try:
            cursor.execute(
                sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
                    sql.Identifier(schema),
                    sql.Identifier(table_name),
                    sql.SQL(", ").join(sql.Identifier(c) for c in cols),
                    sql.SQL(", ").join(sql.Placeholder() * len(cols)),
                ),
                [present[c] for c in cols],
            )
            written += 1
        except Exception:
            if not non_pk:
                raise
            set_cols = list(non_pk.keys())
            set_clause = sql.SQL(", ").join(
                sql.SQL("{} = %s").format(sql.Identifier(c)) for c in set_cols
            )
            cursor.execute(
                sql.SQL("UPDATE {}.{} SET {} WHERE {}").format(
                    sql.Identifier(schema),
                    sql.Identifier(table_name),
                    set_clause,
                    where,
                ),
                [non_pk[c] for c in set_cols] + pk_vals,
            )
            written += 1
    return written


def uses_pg_on_conflict_upsert(engine: str) -> bool:
    """Redshift rejects ``ON CONFLICT`` — never emit it for redshift engines."""
    return (engine or "postgresql").lower() not in {"redshift", "amazon_redshift", "redshift_serverless"}


def _redshift_delete_by_keys(
    cursor: Any,
    sql_mod: Any,
    *,
    schema: str,
    table_name: str,
    target_cols: list[str],
    conflict_cols: list[str],
    batch: list[tuple] | list[list],
) -> list[tuple] | list[list]:
    """Delete matching keys before insert (Redshift upsert), honoring ``_df_lsn``.

    Prefer a single set-based DELETE via a TEMP stage table (atomic within the
    open transaction) so a mid-batch crash cannot leave half-deleted keys.
    Falls back to per-row deletes when temp staging is unavailable.
    """
    from connectors.writer_common import DF_LSN_COL, compare_lsn

    if not batch or not conflict_cols:
        return list(batch)

    # Set-based path: stage → DELETE USING → return rows that should insert.
    try:
        return _redshift_stage_delete(
            cursor,
            sql_mod,
            schema=schema,
            table_name=table_name,
            target_cols=target_cols,
            conflict_cols=conflict_cols,
            batch=batch,
        )
    except Exception as exc:
        logger.warning("Exception suppressed: %s", exc, exc_info=exc)

    conflict_idxs = [target_cols.index(c) for c in conflict_cols]
    lsn_idx = target_cols.index(DF_LSN_COL) if DF_LSN_COL in target_cols else None
    to_write: list[Any] = []

    for row in batch:
        predicates = []
        values: list[Any] = []
        for col, idx in zip(conflict_cols, conflict_idxs):
            val = row[idx] if idx < len(row) else None
            if val is None:
                predicates.append(sql_mod.SQL("{} IS NULL").format(sql_mod.Identifier(col)))
            else:
                predicates.append(
                    sql_mod.SQL("{} = {}").format(sql_mod.Identifier(col), sql_mod.Placeholder())
                )
                values.append(val)
        where = sql_mod.SQL(" AND ").join(predicates)

        if lsn_idx is not None:
            cursor.execute(
                sql_mod.SQL("SELECT {} FROM {}.{} WHERE {} LIMIT 1").format(
                    sql_mod.Identifier(DF_LSN_COL),
                    sql_mod.Identifier(schema),
                    sql_mod.Identifier(table_name),
                    where,
                ),
                values,
            )
            existing = cursor.fetchone()
            incoming_lsn = row[lsn_idx] if lsn_idx < len(row) else None
            if existing is not None and compare_lsn(incoming_lsn, existing[0]) <= 0:
                continue

        cursor.execute(
            sql_mod.SQL("DELETE FROM {}.{} WHERE {}").format(
                sql_mod.Identifier(schema),
                sql_mod.Identifier(table_name),
                where,
            ),
            values,
        )
        to_write.append(row)
    return to_write


def _redshift_stage_delete(
    cursor: Any,
    sql_mod: Any,
    *,
    schema: str,
    table_name: str,
    target_cols: list[str],
    conflict_cols: list[str],
    batch: list[tuple] | list[list],
) -> list[tuple] | list[list]:
    """Stage batch keys and delete matches in one statement (txn-atomic)."""
    from connectors.writer_common import DF_LSN_COL, compare_lsn

    stage = f"_df_upsert_stage_{abs(hash((schema, table_name, tuple(conflict_cols)))) % 10_000_000}"
    conflict_idxs = [target_cols.index(c) for c in conflict_cols]
    lsn_idx = target_cols.index(DF_LSN_COL) if DF_LSN_COL in target_cols else None

    # Filter stale LSN rows client-side first (same honesty as per-row path).
    to_write: list[Any] = []
    for row in batch:
        if lsn_idx is not None:
            predicates = []
            values: list[Any] = []
            for col, idx in zip(conflict_cols, conflict_idxs):
                val = row[idx] if idx < len(row) else None
                if val is None:
                    predicates.append(sql_mod.SQL("{} IS NULL").format(sql_mod.Identifier(col)))
                else:
                    predicates.append(
                        sql_mod.SQL("{} = {}").format(sql_mod.Identifier(col), sql_mod.Placeholder())
                    )
                    values.append(val)
            where = sql_mod.SQL(" AND ").join(predicates)
            cursor.execute(
                sql_mod.SQL("SELECT {} FROM {}.{} WHERE {} LIMIT 1").format(
                    sql_mod.Identifier(DF_LSN_COL),
                    sql_mod.Identifier(schema),
                    sql_mod.Identifier(table_name),
                    where,
                ),
                values,
            )
            existing = cursor.fetchone()
            incoming_lsn = row[lsn_idx] if lsn_idx < len(row) else None
            if existing is not None and compare_lsn(incoming_lsn, existing[0]) <= 0:
                continue
        to_write.append(row)
    if not to_write:
        return []

    # Build TEMP table of conflict key columns only.
    col_defs = sql_mod.SQL(", ").join(
        sql_mod.SQL("{} VARCHAR(65535)").format(sql_mod.Identifier(c)) for c in conflict_cols
    )
    cursor.execute(
        sql_mod.SQL("CREATE TEMP TABLE {} ({})").format(sql_mod.Identifier(stage), col_defs)
    )
    insert_cols = sql_mod.SQL(", ").join(sql_mod.Identifier(c) for c in conflict_cols)
    placeholders = sql_mod.SQL(", ").join(sql_mod.Placeholder() for _ in conflict_cols)
    insert_sql = sql_mod.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql_mod.Identifier(stage), insert_cols, placeholders
    )
    key_rows = []
    for row in to_write:
        key_rows.append(tuple(row[idx] if idx < len(row) else None for idx in conflict_idxs))
    cursor.executemany(insert_sql, key_rows)

    join_pred = sql_mod.SQL(" AND ").join(
        sql_mod.SQL("t.{} IS NOT DISTINCT FROM s.{}").format(
            sql_mod.Identifier(c), sql_mod.Identifier(c)
        )
        for c in conflict_cols
    )
    # Redshift may lack IS NOT DISTINCT FROM — use equality + null-safe OR.
    try:
        cursor.execute(
            sql_mod.SQL(
                "DELETE FROM {}.{} USING {} AS s WHERE {}"
            ).format(
                sql_mod.Identifier(schema),
                sql_mod.Identifier(table_name),
                sql_mod.Identifier(stage),
                join_pred,
            )
        )
    except Exception:
        # Null-safe equality fallback
        eq_pred = sql_mod.SQL(" AND ").join(
            sql_mod.SQL(
                "((t.{c} = s.{c}) OR (t.{c} IS NULL AND s.{c} IS NULL))"
            ).format(c=sql_mod.Identifier(c))
            for c in conflict_cols
        )
        cursor.execute(
            sql_mod.SQL(
                "DELETE FROM {}.{} USING {} AS s WHERE {}"
            ).format(
                sql_mod.Identifier(schema),
                sql_mod.Identifier(table_name),
                sql_mod.Identifier(stage),
                eq_pred,
            )
        )
    try:
        cursor.execute(sql_mod.SQL("DROP TABLE IF EXISTS {}").format(sql_mod.Identifier(stage)))
    except Exception as exc:
        logger.warning("Exception suppressed: %s", exc, exc_info=exc)
    return to_write


@dataclass
class WriteResult(_WriteResult):
    driver: str = "psycopg2"


def pg_type(inferred: str, engine: str = "postgresql") -> str:
    """Map logical type to Postgres or Redshift DDL (never invent Redshift JSONB)."""
    db = "redshift" if (engine or "").lower() == "redshift" else "postgresql"
    return ddl_type(db, inferred)


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
    from connectors.writer_common import resolve_writer_backfill

    backfill_new_fields = resolve_writer_backfill(
        backfill_new_fields=backfill_new_fields,
        mappings=mappings,
        schema_policy=_kwargs.get("schema_policy"),
    )
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
    engine = str(_kwargs.get("engine") or _kwargs.get("db_type") or "postgresql").lower()
    target_types = [pg_type(t, engine=engine) for t in logical_types]
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
    # Fail-closed NUMERIC/DECIMAL(p,s) fit — never silently truncate/round into target.
    mapped_rows = quarantine_unfit_decimals(
        mapped_rows,
        target_cols,
        target_types,
        rejected_details,
        policy,
        dialect_label="PostgreSQL NUMERIC",
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
        dialect_label="PostgreSQL VARCHAR",
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

    if any(t == "BYTEA" for t in target_types):
        from base64 import b64decode

        from services.value_serializer import is_missing_sentinel

        bytea_positions = [i for i, t in enumerate(target_types) if t == "BYTEA"]

        def _coerce_bytea_row(row: tuple) -> tuple:
            row_list = list(row)
            for idx in bytea_positions:
                val = row_list[idx]
                if is_missing_sentinel(val):
                    continue
                if isinstance(val, str):
                    try:
                        row_list[idx] = b64decode(val, validate=True)
                    except (binascii.Error, ValueError):
                        row_list[idx] = val.encode("utf-8")
                elif isinstance(val, bytes):
                    row_list[idx] = val
                elif val is not None:
                    row_list[idx] = str(val).encode("utf-8")
            return tuple(row_list)

        mapped_rows = [_coerce_bytea_row(row) for row in mapped_rows]
        sparse_rows = [_coerce_bytea_row(row) for row in sparse_rows]

    # ISO-8601 / CSV timestamps → Python datetime so COPY/INSERT never send raw "…Z".
    # Boolean/JSON wire: Mongo cell_to_string ("true"/"false", JSON text, "") must
    # match MySQL's shared sql_bind path — never leave string bools for BOOLEAN.
    from connectors.sql_bind import normalize_sql_bind_value
    from connectors.sql_temporal import sql_base_type as _sql_base_type
    from services.value_serializer import is_missing_sentinel

    bind_positions = [
        i
        for i, t in enumerate(target_types)
        if _sql_base_type(t)
        in {
            "DATE",
            "TIME",
            "DATETIME",
            "TIMESTAMP",
            "TIMESTAMPTZ",
            "TIMESTAMP_TZ",
            "TIMESTAMP_LTZ",
            "BOOLEAN",
            "BOOL",
            "JSON",
            "JSONB",
        }
    ]
    if bind_positions:

        def _coerce_bind_row(row: tuple) -> tuple:
            row_list = list(row)
            for idx in bind_positions:
                if is_missing_sentinel(row_list[idx]):
                    continue
                row_list[idx] = normalize_sql_bind_value(
                    row_list[idx], target_types[idx], engine="postgresql"
                )
            return tuple(row_list)

        mapped_rows = [_coerce_bind_row(row) for row in mapped_rows]
        sparse_rows = [_coerce_bind_row(row) for row in sparse_rows]

    rejected_rows = _rejected_row_count(
        data_rows, mapped_rows, rejected_details, policy, sparse_rows=sparse_rows
    )
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
        # Redshift: no ON CONFLICT — plain INSERT after delete-by-key (see chunk loop).
        if (
            write_mode == "upsert"
            and conflict_columns
            and uses_pg_on_conflict_upsert(engine)
        ):
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
        nonlocal target_types
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

            # Pick the wider of the mapping-proposed target DDL and the freshly
            # inferred source DDL from the actual batch samples, then widen any
            # destination columns that are now too narrow for source drift.
            # Using the batch samples (with the active date_locale) instead of the
            # stale peek-file schema prevents MDY/DMY dates from being downgraded
            # to TEXT after the table is created.
            desired_types: list[str] = []
            for mapping, target_type in zip(mappings, target_types):
                source = mapping.get("source") or ""
                source_samples = batch_samples.get(source, []) if batch_samples else []
                if source_samples:
                    source_type = infer_type(source_samples, field_name=source)
                else:
                    source_type = (
                        column_types.get(source)
                        or mapping.get("source_type")
                        or "VARCHAR"
                    )
                source_ddl = pg_type(source_type, engine=engine)
                desired = (
                    source_ddl
                    if is_wider_type(target_type, source_ddl)
                    else target_type
                )
                desired_types.append(desired)

            widen_existing_columns_native(
                cursor,
                "postgresql",
                schema,
                table_name,
                target_cols,
                desired_types,
                backfill=backfill_new_fields,
                skip_cols=conflict_columns or [],
            )
            target_types = desired_types

        if write_mode == "upsert" and conflict_columns and uses_pg_on_conflict_upsert(engine):
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
                    except Exception as exc:
                        logger.debug("Cleanup exception suppressed: %s", exc, exc_info=exc)
                    setup_attempt += 1
                    if not is_connection_lost(setup_exc) or not should_retry_connection_lost(
                        attempt=setup_attempt, started_at=setup_started, proxy=proxy_dest
                    ):
                        raise
                    time.sleep(reconnect_backoff_seconds(setup_attempt))
                    _reconnect()

            if sparse_rows and write_mode == "upsert" and conflict_columns:
                from psycopg2 import sql as _psql

                written_sparse = _pg_apply_sparse_upsert(
                    cur,
                    _psql,
                    schema=schema,
                    table_name=table_name,
                    target_cols=target_cols,
                    conflict_columns=conflict_columns,
                    sparse_rows=sparse_rows,
                )
                conn.commit()
                written += written_sparse

            insert = None if use_copy else _build_insert()
            redshift_upsert_cols = (
                [c for c in (conflict_columns or []) if c in target_cols]
                if (
                    write_mode == "upsert"
                    and conflict_columns
                    and not uses_pg_on_conflict_upsert(engine)
                )
                else []
            )
            rows_skipped = 0

            for chunk_idx in range(chunks):
                start = chunk_idx * chunk_size
                batch = mapped_rows[start : start + chunk_size]
                if not batch:
                    break

                attempt = 0
                chunk_started = time.monotonic()
                chunk_written = 0
                while True:
                    try:
                        if use_ledger and postgres_chunk_committed(
                            cur,
                            schema=schema,
                            job_id=job_id,
                            batch_key=write_batch_key,
                            chunk_idx=chunk_idx,
                        ):
                            chunk_written = len(batch)
                            break
                        if use_copy:
                            _copy_rows(cur, schema, table_name, target_cols, batch)
                        else:
                            write_batch = batch
                            if redshift_upsert_cols:
                                write_batch = _redshift_delete_by_keys(
                                    cur,
                                    sql,
                                    schema=schema,
                                    table_name=table_name,
                                    target_cols=target_cols,
                                    conflict_cols=redshift_upsert_cols,
                                    batch=batch,
                                )
                                rows_skipped += max(0, len(batch) - len(write_batch))
                            elif (
                                write_mode == "upsert"
                                and conflict_columns
                                and DF_LSN_COL in target_cols
                                and uses_pg_on_conflict_upsert(engine)
                            ):
                                conflict_cols = [c for c in conflict_columns if c in target_cols]
                                write_batch, skipped = filter_stale_lsn_rows(
                                    cur,
                                    table_name,
                                    schema,
                                    conflict_cols,
                                    batch,
                                    target_cols,
                                    quote='"',
                                    placeholder="%s",
                                )
                                rows_skipped += skipped
                            if write_batch:
                                cur.executemany(insert, write_batch)
                        if use_ledger:
                            mark_postgres_chunk_committed(
                                cur,
                                schema=schema,
                                job_id=job_id,
                                batch_key=write_batch_key,
                                chunk_idx=chunk_idx,
                                rows_written=len(write_batch if not use_copy else batch),
                            )
                        conn.commit()
                        chunk_written = len(write_batch if not use_copy else batch)
                        break
                    except Exception as chunk_exc:
                        try:
                            conn.rollback()
                        except Exception as exc:
                            logger.warning("Exception suppressed: %s", exc, exc_info=exc)
                        if is_sql_data_error(chunk_exc) and policy in {"quarantine", "coerce_null"}:
                            if insert is None:
                                insert = _build_insert()
                            for row_i, row in enumerate(batch):
                                try:
                                    write_rows = [row]
                                    if redshift_upsert_cols:
                                        write_rows = _redshift_delete_by_keys(
                                            cur,
                                            sql,
                                            schema=schema,
                                            table_name=table_name,
                                            target_cols=target_cols,
                                            conflict_cols=redshift_upsert_cols,
                                            batch=[row],
                                        )
                                    if write_rows:
                                        cur.execute(insert, write_rows[0])
                                    conn.commit()
                                    chunk_written += 1
                                except Exception as row_exc:
                                    try:
                                        conn.rollback()
                                    except Exception as exc:
                                        logger.warning("Exception suppressed: %s", exc, exc_info=exc)
                                    if is_connection_lost(row_exc):
                                        raise
                                    col_name = extract_column_from_sql_error(row_exc) or "*"
                                    sample_val = ""
                                    if col_name != "*" and col_name in target_cols:
                                        try:
                                            sample_val = str(row[target_cols.index(col_name)])[:120]
                                        except (ValueError, IndexError, TypeError):
                                            sample_val = ""
                                    rejected_details.append({
                                        "row": start + row_i,
                                        "column": col_name,
                                        "value": sample_val,
                                        "reason": str(row_exc)[:300],
                                        "policy": policy,
                                    })
                                    transform_errors.append(str(row_exc)[:200])
                            if use_ledger and chunk_written:
                                try:
                                    mark_postgres_chunk_committed(
                                        cur,
                                        schema=schema,
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
                        if not use_copy:
                            insert = _build_insert()

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
            ok=True,
            rows_written=written,
            table_name=table_name,
            target_schema=schema,
            checksum=row_checksum(
                mapped_rows,
                target_cols,
                dest_db_type="postgresql",
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
            target_schema=schema or "public",
            checksum=row_checksum(
                mapped_rows,
                target_cols,
                dest_db_type="postgresql",
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
