"""PostgreSQL bulk writer — CSV file to table with checkpoint batches."""

from __future__ import annotations

import importlib.util
import io
import tempfile
import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from services.schema_inference import infer_type
from services.type_system import materialize_dest_ddl
from services.value_serializer import json_default

from connectors.postgresql_conn import get_connection
from connectors.schema_drift import widen_existing_columns_native
from connectors.sql_temporal import (
    extract_column_from_sql_error,
    is_sql_data_error,
)
from connectors.write_resilience import (
    build_write_batch_key,
    close_quietly,
    ensure_raw_write_ledger,
    is_connection_lost,
    is_public_proxy_host,
    mark_raw_chunk_committed,
    raw_chunk_rows_written,
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
    quarantine_currency_markers_into_numeric,
    quarantine_unfit_binaries,
    quarantine_unfit_bitstrings,
    quarantine_unfit_booleans,
    quarantine_unfit_decimals,
    quarantine_unfit_arrays,
    quarantine_unfit_enum_set,
    quarantine_unfit_integers,
    quarantine_unfit_json,
    quarantine_unfit_specialty_types,
    quarantine_unfit_strings,
    quarantine_unfit_temporals,
    quarantine_unfit_years,
    resolve_target_columns,
    row_checksum,
    sanitize_identifier,
    reject_on_strict_policy,
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
) -> tuple[int, int, list[tuple]]:
    """Per-row upsert omitting DF_MISSING — never SET col=NULL for absent CDC fields."""
    from connectors.writer_common import run_sparse_cdc_upsert

    conflict = [c for c in conflict_columns if c in target_cols]
    if not conflict:
        raise ValueError("sparse PostgreSQL upsert requires conflict_columns")
    where = sql.SQL(" AND ").join(
        sql.SQL("{} = %s").format(sql.Identifier(c)) for c in conflict
    )

    def fetch_existing(pk_vals: list[Any]) -> tuple | None:
        cursor.execute(
            sql.SQL("SELECT {} FROM {}.{} WHERE {}").format(
                sql.SQL(", ").join(sql.Identifier(c) for c in target_cols),
                sql.Identifier(schema),
                sql.Identifier(table_name),
                where,
            ),
            pk_vals,
        )
        return cursor.fetchone()

    def update_non_pk(non_pk: dict[str, Any], pk_vals: list[Any]) -> int:
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
        return int(cursor.rowcount or 0)

    def insert_present(present: dict[str, Any]) -> None:
        cols = list(present.keys())
        cursor.execute(
            sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
                sql.Identifier(schema),
                sql.Identifier(table_name),
                sql.SQL(", ").join(sql.Identifier(c) for c in cols),
                sql.SQL(", ").join(sql.Placeholder() * len(cols)),
            ),
            [present[c] for c in cols],
        )

    return run_sparse_cdc_upsert(
        target_cols=target_cols,
        conflict_columns=conflict,
        sparse_rows=sparse_rows,
        fetch_existing_row=fetch_existing,
        update_non_pk=update_non_pk,
        insert_present=insert_present,
    )


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
    """Upsert matching keys on Redshift (MERGE preferred, delete+insert fallback).

    Prefer native ``MERGE`` (AWS Redshift) so update+insert is one statement —
    Airbyte/Fivetran-class. Falls back to TEMP stage DELETE + caller INSERT when
    MERGE is unavailable. Honors ``_df_lsn`` (stale redelivery skip). Returns
    rows that still need INSERT (empty when MERGE applied the batch).
    """
    if not batch or not conflict_cols:
        return list(batch)

    try:
        return _redshift_merge_upsert(
            cursor,
            sql_mod,
            schema=schema,
            table_name=table_name,
            target_cols=target_cols,
            conflict_cols=conflict_cols,
            batch=batch,
        )
    except Exception as exc:
        logger.warning(
            "Redshift MERGE unavailable (%s); falling back to delete+insert",
            exc,
            exc_info=exc,
        )

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

    from connectors.writer_common import DF_LSN_COL, compare_lsn

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


def _redshift_null_safe_match(sql_mod: Any, conflict_cols: list[str], *, left: str, right: str):
    """Airbyte-class NULL-safe PK match: ``(a=b) OR (a IS NULL AND b IS NULL)``."""
    parts = []
    for c in conflict_cols:
        col = sql_mod.Identifier(c)
        parts.append(
            sql_mod.SQL(
                "(({l}.{c} = {r}.{c}) OR ({l}.{c} IS NULL AND {r}.{c} IS NULL))"
            ).format(l=sql_mod.Identifier(left), r=sql_mod.Identifier(right), c=col)
        )
    return sql_mod.SQL(" AND ").join(parts)


def _redshift_filter_stale_lsn_rows(
    cursor: Any,
    sql_mod: Any,
    *,
    schema: str,
    table_name: str,
    target_cols: list[str],
    conflict_cols: list[str],
    batch: list[tuple] | list[list],
) -> list[Any]:
    from connectors.writer_common import DF_LSN_COL, compare_lsn

    conflict_idxs = [target_cols.index(c) for c in conflict_cols]
    lsn_idx = target_cols.index(DF_LSN_COL) if DF_LSN_COL in target_cols else None
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
                        sql_mod.SQL("{} = {}").format(
                            sql_mod.Identifier(col), sql_mod.Placeholder()
                        )
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
    return to_write


def _redshift_merge_upsert(
    cursor: Any,
    sql_mod: Any,
    *,
    schema: str,
    table_name: str,
    target_cols: list[str],
    conflict_cols: list[str],
    batch: list[tuple] | list[list],
) -> list[tuple] | list[list]:
    """Apply batch via native Redshift MERGE; return [] (nothing left to INSERT)."""
    to_write = _redshift_filter_stale_lsn_rows(
        cursor,
        sql_mod,
        schema=schema,
        table_name=table_name,
        target_cols=target_cols,
        conflict_cols=conflict_cols,
        batch=batch,
    )
    if not to_write:
        return []

    stage = f"_df_merge_stage_{abs(hash((schema, table_name, tuple(conflict_cols)))) % 10_000_000}"
    # Clone target shape — avoids inventing VARCHAR widths for SUPER/VARBYTE.
    cursor.execute(
        sql_mod.SQL("CREATE TEMP TABLE {} AS SELECT * FROM {}.{} WHERE 0=1").format(
            sql_mod.Identifier(stage),
            sql_mod.Identifier(schema),
            sql_mod.Identifier(table_name),
        )
    )
    insert_cols = sql_mod.SQL(", ").join(sql_mod.Identifier(c) for c in target_cols)
    placeholders = sql_mod.SQL(", ").join(sql_mod.Placeholder() for _ in target_cols)
    insert_sql = sql_mod.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql_mod.Identifier(stage), insert_cols, placeholders
    )
    rows_out = []
    for row in to_write:
        rows_out.append(
            tuple(row[i] if i < len(row) else None for i in range(len(target_cols)))
        )
    cursor.executemany(insert_sql, rows_out)

    # Redshift MERGE: target without alias; source aliased as s.
    # NULL-safe ON (Airbyte destination-redshift class).
    tgt = sql_mod.SQL("{}.{}").format(
        sql_mod.Identifier(schema), sql_mod.Identifier(table_name)
    )
    set_clause = sql_mod.SQL(", ").join(
        sql_mod.SQL("{} = s.{}").format(sql_mod.Identifier(c), sql_mod.Identifier(c))
        for c in target_cols
    )
    insert_col_list = sql_mod.SQL(", ").join(sql_mod.Identifier(c) for c in target_cols)
    insert_val_list = sql_mod.SQL(", ").join(
        sql_mod.SQL("s.{}").format(sql_mod.Identifier(c)) for c in target_cols
    )
    on_parts = []
    for c in conflict_cols:
        col = sql_mod.Identifier(c)
        on_parts.append(
            sql_mod.SQL(
                "(({t}.{c} = s.{c}) OR ({t}.{c} IS NULL AND s.{c} IS NULL))"
            ).format(t=tgt, c=col)
        )
    on_sql = sql_mod.SQL(" AND ").join(on_parts)
    merge_sql = sql_mod.SQL(
        "MERGE INTO {} USING {} AS s ON {} "
        "WHEN MATCHED THEN UPDATE SET {} "
        "WHEN NOT MATCHED THEN INSERT ({}) VALUES ({})"
    ).format(tgt, sql_mod.Identifier(stage), on_sql, set_clause, insert_col_list, insert_val_list)
    cursor.execute(merge_sql)
    try:
        cursor.execute(sql_mod.SQL("DROP TABLE IF EXISTS {}").format(sql_mod.Identifier(stage)))
    except Exception as exc:
        logger.warning("Exception suppressed: %s", exc, exc_info=exc)
    # MERGE already wrote — caller must not INSERT again.
    return []


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
    """Map logical type to Postgres or Redshift DDL (never invent Redshift JSONB).

    Honors Map physical stamps via materialize_dest_ddl — never REAL→DOUBLE etc.
    """
    db = "redshift" if (engine or "").lower() == "redshift" else "postgresql"
    return materialize_dest_ddl(db, inferred)


def _copy_text_value(value: Any) -> str:
    from services.value_serializer import is_missing_sentinel

    # Dense COPY: absent schemaless fields → SQL NULL (never bind sentinel text).
    if value is None or is_missing_sentinel(value):
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



def _copy_buffer():
    """Memory-bounded buffer for COPY — spills to disk when wide batches exceed 1 MiB."""
    return tempfile.SpooledTemporaryFile(max_size=1 * 1024 * 1024, mode="w+", encoding="utf-8", newline="")

def _copy_rows(cur, schema: str, table_name: str, columns: list[str], rows: list[tuple]) -> None:
    from psycopg2 import sql

    cols_sql = sql.SQL(", ").join(map(sql.Identifier, columns))
    copy_sql = sql.SQL("COPY {}.{} ({}) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N')").format(
        sql.Identifier(schema),
        sql.Identifier(table_name),
        cols_sql,
    )
    buf = _copy_buffer()
    try:
        for row in rows:
            buf.write("\t".join(_copy_text_value(v) for v in row))
            buf.write("\n")
        buf.seek(0)
        cur.copy_expert(copy_sql, buf)
    finally:
        buf.close()


def _copy_rows_temp(cur, table_name: str, columns: list[str], rows: list[tuple]) -> None:
    """COPY into a session TEMP table (unqualified identifier)."""
    from psycopg2 import sql

    cols_sql = sql.SQL(", ").join(map(sql.Identifier, columns))
    copy_sql = sql.SQL(
        "COPY {} ({}) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N')"
    ).format(sql.Identifier(table_name), cols_sql)
    buf = _copy_buffer()
    try:
        for row in rows:
            buf.write("\t".join(_copy_text_value(v) for v in row))
            buf.write("\n")
        buf.seek(0)
        cur.copy_expert(copy_sql, buf)
    finally:
        buf.close()


def _execute_values_insert(cur, insert_sql: Any, rows: list[tuple] | list[list], *, page_size: int = 1000) -> None:
    """Bulk INSERT via ``execute_values`` (one round-trip per page, not per row)."""
    try:
        from psycopg2.extras import execute_values
    except ImportError:
        cur.executemany(insert_sql, rows)
        return
    # ``execute_values`` expects ``INSERT ... VALUES %s`` template form.
    sql_text = insert_sql.as_string(cur) if hasattr(insert_sql, "as_string") else str(insert_sql)
    # Convert ``VALUES (%s, %s, ...)`` → ``VALUES %s`` for execute_values.
    marker = " VALUES "
    idx = sql_text.upper().rfind(marker)
    if idx < 0:
        cur.executemany(insert_sql, rows)
        return
    template_sql = sql_text[: idx + len(marker)] + "%s"
    # Strip trailing ON CONFLICT clause for the values template — keep it after %s.
    on_conflict = ""
    upper = sql_text.upper()
    oc_idx = upper.find(" ON CONFLICT ")
    if oc_idx > idx:
        on_conflict = " " + sql_text[oc_idx:].strip()
        # Rebuild: INSERT ... VALUES %s ON CONFLICT ...
        head = sql_text[:idx + len(marker)]
        template_sql = head + "%s" + on_conflict
    execute_values(cur, template_sql, rows, page_size=page_size)


def _copy_upsert_batch(
    cur: Any,
    sql_mod: Any,
    *,
    schema: str,
    table_name: str,
    target_cols: list[str],
    conflict_cols: list[str],
    batch: list[tuple] | list[list],
    insert_sql: Any,
) -> int:
    """Stage via COPY into a TEMP table, then INSERT…ON CONFLICT from the stage.

    Avoids per-row RTTs on upsert paths while preserving conflict semantics.
    Falls back to ``execute_values`` when COPY staging is unavailable.
    """
    import uuid

    if not batch or not conflict_cols:
        if batch:
            _execute_values_insert(cur, insert_sql, [tuple(r) for r in batch])
        return len(batch)

    # Per-call UUID so a failed prior stage left on a reused session cannot collide.
    stage = f"_df_copy_ups_{uuid.uuid4().hex[:16]}"
    conn = getattr(cur, "connection", None)

    def _drop_stage_best_effort() -> None:
        drop = sql_mod.SQL("DROP TABLE IF EXISTS {}").format(sql_mod.Identifier(stage))
        try:
            cur.execute(drop)
            return
        except Exception:
            pass
        if conn is None:
            return
        # Aborted transaction blocks DDL until rollback.
        try:
            conn.rollback()
        except Exception:
            return
        try:
            cur.execute(drop)
        except Exception as exc:
            logger.debug("TEMP upsert stage drop skipped: %s", exc)

    try:
        cur.execute(
            sql_mod.SQL("CREATE TEMP TABLE {} AS SELECT * FROM {}.{} WHERE 0=1").format(
                sql_mod.Identifier(stage),
                sql_mod.Identifier(schema),
                sql_mod.Identifier(table_name),
            )
        )
        _copy_rows_temp(cur, stage, target_cols, [tuple(r) for r in batch])
        col_list = sql_mod.SQL(", ").join(sql_mod.Identifier(c) for c in target_cols)
        conflict = sql_mod.SQL(", ").join(sql_mod.Identifier(c) for c in conflict_cols)
        update_cols = [c for c in target_cols if c not in conflict_cols]
        if update_cols:
            set_clause = sql_mod.SQL(", ").join(
                sql_mod.SQL("{} = EXCLUDED.{}").format(
                    sql_mod.Identifier(c), sql_mod.Identifier(c)
                )
                for c in update_cols
            )
            merge = sql_mod.SQL(
                "INSERT INTO {}.{} ({}) SELECT {} FROM {} "
                "ON CONFLICT ({}) DO UPDATE SET {}"
            ).format(
                sql_mod.Identifier(schema),
                sql_mod.Identifier(table_name),
                col_list,
                col_list,
                sql_mod.Identifier(stage),
                conflict,
                set_clause,
            )
        else:
            merge = sql_mod.SQL(
                "INSERT INTO {}.{} ({}) SELECT {} FROM {} "
                "ON CONFLICT ({}) DO NOTHING"
            ).format(
                sql_mod.Identifier(schema),
                sql_mod.Identifier(table_name),
                col_list,
                col_list,
                sql_mod.Identifier(stage),
                conflict,
            )
        cur.execute(merge)
        _drop_stage_best_effort()
        return len(batch)
    except Exception:
        # Clear aborted txn + orphan stage, then values-based upsert.
        _drop_stage_best_effort()
        _execute_values_insert(cur, insert_sql, [tuple(r) for r in batch])
        return len(batch)


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
    connection: Any | None = None,
    close_connection: bool | None = None,
    connection_holder: dict[str, Any] | None = None,
    **_kwargs: Any,
) -> WriteResult:
    from connectors.writer_common import resolve_writer_backfill

    backfill_new_fields = resolve_writer_backfill(
        backfill_new_fields=backfill_new_fields,
        mappings=mappings,
        schema_policy=_kwargs.get("schema_policy"),
    )
    # Shared connection (stream reuse): default to not closing the caller's conn.
    if close_connection is None:
        close_connection = connection is None
    # Prefer explicit holder; also accept kwargs for older call sites.
    if connection_holder is None:
        raw_holder = _kwargs.get("connection_holder")
        connection_holder = raw_holder if isinstance(raw_holder, dict) else None
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

    # GENERATED ALWAYS must not appear in INSERT — DB assigns values.
    from connectors.writer_common import omit_generated_always_columns

    target_cols, logical_types, _, _omitted_identity = omit_generated_always_columns(
        target_cols, logical_types, []
    )
    if not target_cols:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema=schema or "public",
            checksum="",
            chunks_completed=0,
            error="All mapped columns are GENERATED ALWAYS — nothing to insert",
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
        dest_kind="postgresql",
        # Upsert conflict cols / dest PK — full composite for quarantine replay identity.
        destination_pk_columns=list(conflict_columns or []) or None,
    )
    mapped_rows = quarantine_currency_markers_into_numeric(
        mapped_rows, target_cols, target_types, rejected_details, policy
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
    mapped_rows = quarantine_unfit_years(
        mapped_rows, target_cols, target_types, rejected_details, policy
    )
    mapped_rows = quarantine_unfit_booleans(
        mapped_rows, target_cols, target_types, rejected_details, policy
    )
    mapped_rows = quarantine_unfit_temporals(
        mapped_rows, target_cols, target_types, rejected_details, policy
    )
    mapped_rows = quarantine_unfit_specialty_types(
        mapped_rows, target_cols, target_types, rejected_details, policy
    )
    mapped_rows = quarantine_unfit_integers(
        mapped_rows,
        target_cols,
        target_types,
        rejected_details,
        policy,
        dialect_label="PostgreSQL INTEGER",
    )
    mapped_rows = quarantine_unfit_bitstrings(
        mapped_rows, target_cols, target_types, rejected_details, policy
    )
    mapped_rows = quarantine_unfit_binaries(
        mapped_rows,
        target_cols,
        target_types,
        rejected_details,
        policy,
        dialect_label="PostgreSQL BYTEA",
    )
    mapped_rows = quarantine_unfit_enum_set(
        mapped_rows, target_cols, logical_types, rejected_details, policy
    )
    string_dialect = (
        "Redshift VARCHAR"
        if engine in {"redshift", "amazon_redshift", "redshift_serverless"}
        else "PostgreSQL VARCHAR"
    )
    mapped_rows = quarantine_unfit_strings(
        mapped_rows,
        target_cols,
        target_types,
        rejected_details,
        policy,
        dialect_label=string_dialect,
    )
    mapped_rows = quarantine_unfit_arrays(
        mapped_rows,
        target_cols,
        target_types,
        rejected_details,
        policy,
        dialect_label="Redshift" if engine.startswith("redshift") else "PostgreSQL",
    )
    mapped_rows = quarantine_unfit_json(
        mapped_rows,
        target_cols,
        target_types,
        rejected_details,
        policy,
        dialect_label=string_dialect.split()[0] + " JSON",
    )
    sparse_rows: list[tuple] = []
    rows_for_checksum: list[tuple] = list(mapped_rows)
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
        from connectors.sql_bind import coerce_binary_wire
        from services.value_serializer import is_missing_sentinel

        bytea_positions = [i for i, t in enumerate(target_types) if t == "BYTEA"]

        def _coerce_bytea_row(row: tuple) -> tuple:
            row_list = list(row)
            for idx in bytea_positions:
                val = row_list[idx]
                if is_missing_sentinel(val) or val is None:
                    continue
                # Airbyte/Fivetran class: base64 → bytes; never invent UTF-8 payload.
                row_list[idx] = coerce_binary_wire(val)
            return tuple(row_list)

        mapped_rows = [_coerce_bytea_row(row) for row in mapped_rows]
        sparse_rows = [_coerce_bytea_row(row) for row in sparse_rows]

    # ISO-8601 / CSV timestamps → Python datetime so COPY/INSERT never send raw "…Z".
    # Boolean/JSON wire: Mongo cell_to_string ("true"/"false", JSON text, "") must
    # match MySQL's shared sql_bind path — never leave string bools for BOOLEAN.
    from connectors.sql_bind import normalize_sql_bind_value
    from services.type_system import parse_enum_or_set_ordered_members
    from services.value_serializer import is_missing_sentinel

    # Wave 64: normalize every column through SSOT — ENUM/SET use logical
    # carriers (domain + SET→list for TEXT[]), other columns use target DDL.

    def _bind_ddl(idx: int) -> str:
        logical = logical_types[idx] if idx < len(logical_types) else ""
        target = target_types[idx] if idx < len(target_types) else ""
        if logical and parse_enum_or_set_ordered_members(logical) is not None:
            return logical
        # ROWVERSION / HIERARCHYID carriers must bind via logical polarity
        # (binary concurrency / slash→ltree) even when target DDL is BYTEA/LTREE.
        logical_u = (logical or "").strip().upper()
        if logical_u in {"ROWVERSION", "HIERARCHYID", "SQL_VARIANT", "ROWID", "UROWID"}:
            return logical
        return target or logical

    def _coerce_bind_row(row: tuple) -> tuple:
        row_list = list(row)
        for idx in range(len(row_list)):
            if is_missing_sentinel(row_list[idx]):
                continue
            ddl = _bind_ddl(idx)
            if not ddl:
                continue
            row_list[idx] = normalize_sql_bind_value(
                row_list[idx], ddl, engine="postgresql"
            )
        return tuple(row_list)

    mapped_rows = [_coerce_bind_row(row) for row in mapped_rows]
    sparse_rows = [_coerce_bind_row(row) for row in sparse_rows]
    # Dense INSERT/COPY: absent schemaless fields → SQL NULL (sparse keeps sentinel).
    from connectors.writer_common import materialize_missing_as_null_for_dense_write

    mapped_rows = materialize_missing_as_null_for_dense_write(mapped_rows)

    rejected_rows = _rejected_row_count(
        data_rows, mapped_rows, rejected_details, policy, sparse_rows=sparse_rows
    )
    coerced_null_rows = _coerced_null_row_count(rejected_details, policy)
    _map_abort = reject_on_strict_policy(policy, rejected_details, 'PostgreSQL')
    if _map_abort or (transform_errors and policy == "fail"):
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema=schema,
            checksum="",
            chunks_completed=0,
            error=_map_abort or f"Transform errors: {'; '.join(transform_errors[:3])}",
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
    # Chunked COPY (PROXY_CHUNK_SIZE / CHUNK_SIZE) + per-chunk commit + ledger
    # resume. Blanket proxy COPY-off forced executemany at ~1k rows and made
    # 1M CSV→PG loads look like multi-hour jobs vs competitors using COPY.
    use_copy = (
        write_mode == "insert"
        and not conflict_columns
        and not any(t == "BYTEA" for t in target_types)
        and port != 5439
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
        nonlocal conn, cur, close_connection
        # Drop the dead handle.
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
        if connection_holder is not None:
            # Hand the replacement back to the stream so later chunks reuse it
            # and we do not close the live socket at the end of this write.
            connection_holder["conn"] = conn
            close_connection = False
        else:
            close_connection = True
        cur = conn.cursor()

    def _run_setup(cursor) -> None:
        nonlocal target_types
        if use_ledger:
            ensure_raw_write_ledger(cursor, dialect="postgresql", schema=schema)
        if create_table:
            cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
            if engine not in {"redshift", "amazon_redshift", "redshift_serverless"}:
                from services.type_system import collect_pg_enum_prerequisites

                for stmt in collect_pg_enum_prerequisites(logical_types):
                    cursor.execute(stmt)
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

            # Map≡ALTER: source/batch DDL may propose a wider type, but an
            # explicit Map target_type is a hard ceiling — never ALTER past it.
            # Unfit cells quarantine on write (overflow), not silent widen.
            from connectors.writer_common import desired_types_honoring_map_stamps
            from services.mapping_constraints import write_mappings

            active_by_tgt: dict[str, dict] = {}
            for mapping in write_mappings(mappings):
                tgt = sanitize_identifier(str(mapping.get("target") or ""), preserve_case=False)
                if tgt and tgt not in active_by_tgt:
                    active_by_tgt[tgt] = mapping
            candidate_by_col: dict[str, str] = {}
            for col in target_cols:
                mapping = active_by_tgt.get(col) or {}
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
                candidate_by_col[col] = pg_type(source_type, engine=engine)

            desired_types, alter_refusals = desired_types_honoring_map_stamps(
                target_cols=target_cols,
                current_target_types=target_types,
                mappings=mappings,
                candidate_by_col=candidate_by_col,
            )
            if alter_refusals:
                logger.info(
                    "postgresql Map≡ALTER refusals (stamp ceiling): %s",
                    alter_refusals,
                )

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
        if connection is not None:
            conn = connection
            try:
                conn.autocommit = False
            except Exception:
                pass
        else:
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

            rows_skipped = 0
            if sparse_rows and write_mode == "upsert" and conflict_columns:
                from psycopg2 import sql as _psql
                from connectors.writer_common import reject_on_strict_policy, row_has_missing_sentinel

                written_sparse, sparse_skipped, sparse_checksum = _pg_apply_sparse_upsert(
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
                rows_skipped += sparse_skipped
                rows_for_checksum = [
                    r for r in rows_for_checksum if not row_has_missing_sentinel(r)
                ] + list(sparse_checksum)

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
                        if use_ledger:
                            already = raw_chunk_rows_written(
                                cur,
                                dialect="postgresql",
                                schema=schema,
                                job_id=job_id,
                                batch_key=write_batch_key,
                                chunk_idx=chunk_idx,
                            )
                            if already is not None:
                                # Credit what the first attempt actually landed.
                                # The chunk may have quarantined rows, so
                                # len(batch) would over-report and make the
                                # reconcile checksum disagree with the table.
                                chunk_written = already
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
                                conflict_for_copy = [
                                    c for c in (conflict_columns or []) if c in target_cols
                                ]
                                if (
                                    write_mode == "upsert"
                                    and conflict_for_copy
                                    and uses_pg_on_conflict_upsert(engine)
                                    and DF_LSN_COL not in target_cols
                                ):
                                    _copy_upsert_batch(
                                        cur,
                                        sql,
                                        schema=schema,
                                        table_name=table_name,
                                        target_cols=target_cols,
                                        conflict_cols=conflict_for_copy,
                                        batch=write_batch,
                                        insert_sql=insert,
                                    )
                                else:
                                    _execute_values_insert(
                                        cur, insert, [tuple(r) for r in write_batch]
                                    )
                        landed = len(batch if use_copy else write_batch)
                        if use_ledger:
                            mark_raw_chunk_committed(
                                cur,
                                dialect="postgresql",
                                schema=schema,
                                job_id=job_id,
                                batch_key=write_batch_key,
                                chunk_idx=chunk_idx,
                                rows_written=landed,
                            )
                        conn.commit()
                        chunk_written = landed
                        break
                    except Exception as chunk_exc:
                        try:
                            conn.rollback()
                        except Exception as exc:
                            logger.warning("Exception suppressed: %s", exc, exc_info=exc)
                        if is_sql_data_error(chunk_exc) and policy in {"quarantine", "coerce_null"}:
                            if insert is None:
                                insert = _build_insert()
                            # SAVEPOINT per row + single commit — avoids one RTT/commit
                            # per rejected cell (was the CDC/quarantine throughput cliff).
                            for row_i, row in enumerate(batch):
                                try:
                                    cur.execute("SAVEPOINT df_row_sp")
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
                                    cur.execute("RELEASE SAVEPOINT df_row_sp")
                                    chunk_written += 1
                                except Exception as row_exc:
                                    try:
                                        cur.execute("ROLLBACK TO SAVEPOINT df_row_sp")
                                    except Exception as exc:
                                        logger.warning("Exception suppressed: %s", exc, exc_info=exc)
                                        try:
                                            conn.rollback()
                                        except Exception as exc2:
                                            logger.warning(
                                                "Exception suppressed: %s", exc2, exc_info=exc2
                                            )
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
                            try:
                                if use_ledger and chunk_written:
                                    mark_raw_chunk_committed(
                                        cur,
                                        dialect="postgresql",
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

        if close_connection:
            close_quietly(conn)
        return WriteResult(
            ok=True,
            rows_written=written,
            table_name=table_name,
            target_schema=schema,
            checksum=row_checksum(
                rows_for_checksum if "rows_for_checksum" in locals() else mapped_rows,
                target_cols,
                dest_db_type="postgresql",
                dest_types={c: target_types[i] for i, c in enumerate(target_cols)},
            ),
            chunks_completed=chunks_completed or chunks,
            rejected_rows=max(rejected_rows, len(data_rows) - written - rows_skipped),
            rejected_details=rejected_details,
            coerced_null_rows=coerced_null_rows,
            rows_skipped=rows_skipped,
            warnings=transform_errors,
            load_method="copy" if use_copy else "insert",
        )
    except Exception as exc:
        if close_connection:
            close_quietly(conn)
        return WriteResult(
            ok=False,
            rows_written=written,
            table_name=table_name,
            target_schema=schema or "public",
            checksum=row_checksum(
                rows_for_checksum if "rows_for_checksum" in locals() else mapped_rows,
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
