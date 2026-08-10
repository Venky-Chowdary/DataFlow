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
from services.decision_kernel import materialize_dest_ddl
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
    flush_normalized_child_batches,
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
    bind_sql_mapped_rows_with_quarantine,
    overlay_physical_bind_types,
    require_physical_types_for_existing_table,
    resolve_conflict_targets,
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
from services.transform_resolver import LiveDestTypes

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
    rejected_details: list[dict[str, Any]] | None = None,
    policy: str = "quarantine",
) -> tuple[int, int, list[tuple]]:
    """Per-row upsert omitting DF_MISSING — never SET col=NULL for absent CDC fields."""
    from connectors.writer_common import resolve_conflict_targets, run_sparse_cdc_upsert

    conflict = resolve_conflict_targets(conflict_columns, target_cols, strict=True)
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
        rejected_details=rejected_details,
        policy=policy,
    )


def _fetch_pg_column_types(cursor: Any, schema: str, table_name: str) -> dict[str, str]:
    """Live PostgreSQL/Redshift column DDL for physical bind overlay."""
    try:
        cursor.execute(
            """
            SELECT column_name, data_type, udt_name,
                   character_maximum_length, numeric_precision, numeric_scale
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            """,
            (schema, table_name),
        )
        out: dict[str, str] = {}
        _udt_scalar = {
            "INT2": "SMALLINT",
            "INT4": "INTEGER",
            "INT8": "BIGINT",
            "FLOAT4": "REAL",
            "FLOAT8": "DOUBLE PRECISION",
            "BOOL": "BOOLEAN",
            "UUID": "UUID",
            "JSON": "JSON",
            "JSONB": "JSONB",
            "TEXT": "TEXT",
            "VARCHAR": "VARCHAR",
            "BPCHAR": "CHAR",
            "NAME": "NAME",
            "OID": "OID",
            "INET": "INET",
            "CIDR": "CIDR",
            "MACADDR": "MACADDR",
            "MACADDR8": "MACADDR8",
            "BYTEA": "BYTEA",
            "XML": "XML",
            "HSTORE": "HSTORE",
            "LTREE": "LTREE",
        }
        for name, data_type, udt_name, char_len, precision, scale in cursor.fetchall():
            udt = str(udt_name or "").upper()
            data = str(data_type or "").upper()
            # Array udts are ``_INT4`` / ``_TEXT`` — never invent scalar INT4 over
            # live INTEGER[] (Map VARCHAR rematerialize polarity cliff).
            if data == "ARRAY" or udt.startswith("_"):
                elem_udt = udt[1:] if udt.startswith("_") else udt
                elem = _udt_scalar.get(elem_udt, elem_udt or "TEXT")
                ddl = f"{elem}[]"
            elif data in {"CHARACTER VARYING", "VARCHAR"} and char_len:
                ddl = f"VARCHAR({int(char_len)})"
            elif data in {"CHARACTER", "CHAR"} and char_len:
                ddl = f"CHAR({int(char_len)})"
            elif udt in _udt_scalar:
                ddl = _udt_scalar[udt]
            elif data in {
                "DATE",
                "TIME",
                "TIMESTAMP",
                "TIMESTAMP WITHOUT TIME ZONE",
                "TIMESTAMP WITH TIME ZONE",
                "TIME WITHOUT TIME ZONE",
                "TIME WITH TIME ZONE",
            }:
                ddl = data
            elif data in {"NUMERIC", "DECIMAL"} and precision is not None:
                ddl = f"NUMERIC({int(precision)},{int(scale or 0)})"
            else:
                ddl = udt or data
            key = str(name)
            out[key] = ddl
            out[key.lower()] = ddl
            out[key.upper()] = ddl
        return out
    except Exception:
        logger.debug("postgresql physical column introspection failed", exc_info=True)
        return {}


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
    rejected_details: list[dict[str, Any]] | None = None,
    policy: str = "quarantine",
) -> list[tuple] | list[list]:
    """Upsert matching keys on Redshift (MERGE preferred, delete+insert fallback).

    Prefer native ``MERGE`` (AWS Redshift) so update+insert is one statement —
    Airbyte/Fivetran-class. Falls back to TEMP stage DELETE + caller INSERT when
    MERGE is unavailable. Honors ``_df_lsn`` (stale redelivery skip). Returns
    rows that still need INSERT (empty when MERGE applied the batch).
    """
    if not batch or not conflict_cols:
        return list(batch)

    from connectors.writer_common import partition_dense_upsert_rows

    # Quarantine null keys — never abort the whole Redshift upsert chunk.
    batch = partition_dense_upsert_rows(
        list(batch),
        conflict_cols,
        target_cols=target_cols,
        rejected_details=rejected_details,
        policy=policy,
    )
    if not batch:
        return []

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
    except ValueError:
        raise
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
    except ValueError:
        raise
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
            if val is None or (isinstance(val, str) and str(val).strip() == ""):
                raise ValueError(
                    f"Redshift upsert delete refused null/empty conflict key {col!r} — "
                    "IS NULL predicates would mass-delete destination rows"
                )
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

    for row in to_write:
        for col, idx in zip(conflict_cols, conflict_idxs):
            val = row[idx] if idx < len(row) else None
            if val is None or (isinstance(val, str) and str(val).strip() == ""):
                raise ValueError(
                    f"Redshift upsert stage-delete refused null/empty conflict key "
                    f"{col!r} — NULL-safe join would mass-delete destination rows"
                )

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


@dataclass
class _PgMaterializedBatch:
    """Map + quarantine + bind output for Postgres/Redshift writes."""

    mapped_rows: list[tuple]
    sparse_rows: list[tuple]
    transform_errors: list[str]
    rejected_details: list
    target_types: list[str]
    bind_types: list[str]
    rows_for_checksum: list[tuple]


def _pg_materialize_mapped_batch(
    *,
    headers: list[str],
    data_rows: list,
    mappings: list,
    target_cols: list[str],
    column_types: dict[str, str] | None,
    dest_types: dict[str, str],
    logical_types: list[str],
    policy: Any,
    engine: str,
    conflict_columns: list[str] | None,
    write_mode: str,
    destination_pk_columns: list[str] | None = None,
    destination_column_nullability: Any = None,
    allow_logical_fallback: bool = True,
    empty_cells_as_null: bool = False,
) -> _PgMaterializedBatch:
    """Build mapped rows against ``dest_types`` then quarantine/bind.

    Call again after live DDL overlay so Map stamps cannot coerce before
    physical types win (BQ-class reorder — existing-table invent cliff).
    """
    target_types = []
    for i, c in enumerate(target_cols):
        carrier = str(dest_types.get(c) or "").strip()
        if not carrier and allow_logical_fallback:
            carrier = str(logical_types[i] if i < len(logical_types) else "").strip()
        target_types.append(pg_type(carrier, engine=engine) if carrier else "")
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
        destination_pk_columns=list(destination_pk_columns or []) or None,
        destination_column_nullability=destination_column_nullability,
        empty_cells_as_null=bool(empty_cells_as_null),
    )
    mapped_rows = quarantine_currency_markers_into_numeric(
        mapped_rows, target_cols, target_types, rejected_details, policy
    )
    _decimal_dest = (
        "redshift"
        if engine.startswith("redshift") or engine in {"amazon_redshift", "redshift_serverless"}
        else "postgresql"
    )
    mapped_rows = quarantine_unfit_decimals(
        mapped_rows,
        target_cols,
        target_types,
        rejected_details,
        policy,
        dialect_label=(
            "PostgreSQL NUMERIC" if _decimal_dest == "postgresql" else "Redshift NUMERIC"
        ),
        dest_db=_decimal_dest,
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
        dest_db="postgresql",
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
        dialect_label="Redshift" if _decimal_dest == "redshift" else "PostgreSQL",
        dest_db=_decimal_dest,
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
                row_list[idx] = coerce_binary_wire(val)
            return tuple(row_list)

        mapped_rows = [_coerce_bytea_row(row) for row in mapped_rows]
        sparse_rows = [_coerce_bytea_row(row) for row in sparse_rows]

    from services.type_system import parse_enum_or_set_ordered_members

    def _bind_ddl(idx: int) -> str:
        logical = logical_types[idx] if idx < len(logical_types) else ""
        target = target_types[idx] if idx < len(target_types) else ""
        if logical and parse_enum_or_set_ordered_members(logical) is not None:
            return logical
        logical_u = (logical or "").strip().upper()
        if logical_u in {"ROWVERSION", "HIERARCHYID", "SQL_VARIANT", "ROWID", "UROWID"}:
            return logical
        return target or logical

    bind_types = [_bind_ddl(i) for i in range(len(target_cols))]
    mapped_rows = bind_sql_mapped_rows_with_quarantine(
        mapped_rows,
        target_cols,
        bind_types,
        rejected_details,
        policy,
        engine="postgresql",
        dialect_label="PostgreSQL",
        mappings=mappings,
    )
    sparse_rows = bind_sql_mapped_rows_with_quarantine(
        sparse_rows,
        target_cols,
        bind_types,
        rejected_details,
        policy,
        engine="postgresql",
        dialect_label="PostgreSQL",
        mappings=mappings,
    )
    from connectors.writer_common import materialize_missing_as_null_for_dense_write

    mapped_rows = materialize_missing_as_null_for_dense_write(mapped_rows)
    return _PgMaterializedBatch(
        mapped_rows=mapped_rows,
        sparse_rows=sparse_rows,
        transform_errors=list(transform_errors or []),
        rejected_details=rejected_details,
        target_types=target_types,
        bind_types=bind_types,
        rows_for_checksum=rows_for_checksum,
    )


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

    # Normalize upsert identity onto Map targets (casefold + refuse partial composite).
    if conflict_columns:
        try:
            conflict_columns = resolve_conflict_targets(
                conflict_columns, target_cols, strict=True
            )
        except ValueError as exc:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=table_name,
                target_schema=schema or "public",
                checksum="",
                chunks_completed=0,
                error=str(exc),
            )

    schema = schema or "public"
    table_name = sanitize_identifier(table_name, preserve_case=True)
    engine = str(_kwargs.get("engine") or _kwargs.get("db_type") or "postgresql").lower()
    # Prefer Studio-probed live DDL over Map stamps (BOOLEAN→VARCHAR invent cliff).
    from connectors.writer_common import resolve_studio_or_map_dest_types

    live_dest = _kwargs.get("destination_column_types")
    dest_types, studio_err = resolve_studio_or_map_dest_types(
        target_cols,
        mappings,
        column_types,
        logical_types=logical_types,
        studio_types=live_dest if isinstance(live_dest, dict) else None,
        product="PostgreSQL",
    )
    policy = transform_error_policy(error_policy)

    # Partial Studio: defer Map + strict abort until live DDL rematerialize
    # (Map-blank invent must not fail batches that succeed against physical carriers).
    # Matches generic_sql / BigQuery. Create-new already refused below on studio_err.
    mapped_rows: list = []
    sparse_rows: list = []
    transform_errors: list[str] = []
    rejected_details: list = []
    target_types: list[str] = []
    bind_types: list[str] = []
    rows_for_checksum: list = []
    rejected_rows = 0
    coerced_null_rows = 0
    # Strict-policy abort withheld from the Map-projected pass until live DDL
    # either confirms it or rematerializes it away.
    deferred_map_abort: str | None = None
    if not studio_err:
        # Map before opening a socket so public proxies are not idle during transform.
        _batch = _pg_materialize_mapped_batch(
            headers=headers,
            data_rows=data_rows,
            mappings=mappings,
            target_cols=target_cols,
            column_types=column_types,
            dest_types=dest_types,
            logical_types=logical_types,
            policy=policy,
            engine=engine,
            conflict_columns=conflict_columns,
            write_mode=write_mode,
            destination_pk_columns=list(conflict_columns or []) or None,
            destination_column_nullability=_kwargs.get("destination_column_nullability"),
            allow_logical_fallback=True,
            empty_cells_as_null=bool(_kwargs.get("empty_cells_as_null")),
        )
        mapped_rows = _batch.mapped_rows
        sparse_rows = _batch.sparse_rows
        transform_errors = _batch.transform_errors
        rejected_details = _batch.rejected_details
        target_types = _batch.target_types
        bind_types = _batch.bind_types
        rows_for_checksum = _batch.rows_for_checksum

        rejected_rows = _rejected_row_count(
            data_rows, mapped_rows, rejected_details, policy, sparse_rows=sparse_rows
        )
        coerced_null_rows = _coerced_null_row_count(rejected_details, policy)
        _map_abort = reject_on_strict_policy(
            policy, rejected_details, "PostgreSQL", transform_errors
        )
        # Map stamps only *project* the carriers create-new would invent. An
        # existing table's physical DDL is read below and rematerializes the
        # batch, so aborting here fails rows the real sink accepts (BOOLEAN
        # stamp over a live TEXT column rejected every 'Y'). Hold the verdict:
        # the post-overlay pass re-decides, and the no-physical path re-raises
        # it unchanged.
        if _map_abort and not isinstance(dest_types, LiveDestTypes):
            deferred_map_abort = _map_abort
            _map_abort = None
        if _map_abort:
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
    chunks = max(1, (total + chunk_size - 1) // chunk_size) if total else 0
    written = 0
    chunks_completed = 0
    child_flush_error: str | None = None
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
    # Ledger dedupes insert retries. Upserts already converge on conflict keys —
    # skipping a ledgered upsert would suppress a legitimate value update.
    use_ledger = bool(job_id) and not (
        write_mode == "upsert" and conflict_columns
    )
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

    additive_refuse: str | None = None

    def _run_setup(cursor) -> None:
        nonlocal target_types, additive_refuse
        if use_ledger:
            ensure_raw_write_ledger(cursor, dialect="postgresql", schema=schema)
        if create_table:
            cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
            if engine not in {"redshift", "amazon_redshift", "redshift_serverless"}:
                from services.type_system import collect_pg_enum_prerequisites

                for stmt in collect_pg_enum_prerequisites(logical_types):
                    cursor.execute(stmt)
            fidelity_plan = None
            # None = existence never established; only a proven False allows the
            # orphan-rollback registration below to drop the object.
            pg_table_existed: bool | None = None
            from services.schema_fidelity import (
                empty_unsupported_report,
                render_create_column_defs,
                resolve_create_fidelity_plan,
            )

            try:
                # Probe existence so we do not claim PK carry on IF NOT EXISTS no-op.
                cursor.execute(
                    """
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = %s
                    LIMIT 1
                    """,
                    (schema, table_name),
                )
                pg_table_existed = cursor.fetchone() is not None
                fidelity_plan = resolve_create_fidelity_plan(
                    source_schema_catalog=_kwargs.get("source_schema_catalog"),
                    mappings=mappings,
                    target_columns=target_cols,
                    target_types=target_types,
                    dest_dialect="postgresql",
                    table_already_exists=pg_table_existed,
                )
                if fidelity_plan.column_renames and fidelity_plan.dest_columns:
                    target_cols[:] = list(fidelity_plan.dest_columns)
                body = render_create_column_defs(
                    columns=target_cols,
                    types=target_types,
                    plan=(None if pg_table_existed else fidelity_plan),
                    dialect="postgresql",
                )
                cursor.execute(
                    sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
                        sql.Identifier(schema),
                        sql.Identifier(table_name),
                        sql.SQL(body),
                    )
                )
                _kwargs["_schema_fidelity_report"] = fidelity_plan.report.to_dict()
            except Exception as exc:
                logger.warning(
                    "PostgreSQL schema fidelity plan failed; falling back to types-only CREATE: %s",
                    exc,
                )
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
                # Never silence a types-only fallback — Property 6 certificate required.
                _kwargs["_schema_fidelity_report"] = empty_unsupported_report(
                    source_dialect="",
                    dest_dialect="postgresql",
                    reason=(
                        f"Schema fidelity CREATE failed ({type(exc).__name__}); "
                        "fell back to types-only CREATE TABLE — constraints not carried."
                    ),
                ).to_dict()
            # Track empty shell for orphan rollback if the job fails before the
            # first ack. Only a table this run actually created may be dropped:
            # ``CREATE TABLE IF NOT EXISTS`` is a no-op on an operator's existing
            # table, and registering that made a failed write (a NOT NULL
            # violation on row 1) drop a populated destination.
            if pg_table_existed is False:
                try:
                    from services.auto_create_lifecycle import register_auto_create

                    register_auto_create(
                        db_type=(
                            "postgresql"
                            if engine not in {"redshift", "amazon_redshift"}
                            else "redshift"
                        ),
                        table=table_name,
                        schema=schema,
                        config={
                            "host": host,
                            "port": port,
                            "user": username,
                            "username": username,
                            "password": password,
                            "database": database,
                            "connection_string": connection_string,
                        },
                        job_id=job_id,
                    )
                except Exception:
                    logger.debug("auto_create register skipped", exc_info=True)

        if backfill_new_fields:
            cursor.execute(
                """SELECT column_name FROM information_schema.columns
                   WHERE table_schema = %s AND table_name = %s""",
                (schema, table_name),
            )
            existing = {row[0] for row in cursor.fetchall()}
            from connectors.writer_common import gate_additive_types_under_partial_studio

            target_types, add_err = gate_additive_types_under_partial_studio(
                target_cols=target_cols,
                target_types=target_types,
                existing=existing,
                mappings=mappings,
                studio_err=studio_err,
                product="PostgreSQL",
                materialize_stamp=lambda stamp: pg_type(stamp, engine=engine),
            )
            if add_err:
                additive_refuse = add_err
                return
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
            source_type_by_col: dict[str, str] = {}
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
                        or ""
                    )
                # Unknown source DDL: do not invent VARCHAR widen candidate —
                # keep Map/current ceiling (desired_types falls back to cur_type).
                if not str(source_type or "").strip():
                    continue
                source_type_by_col[col] = str(source_type)
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

            suppressed_widens: dict[str, str] = {}
            widen_existing_columns_native(
                cursor,
                "postgresql",
                schema,
                table_name,
                target_cols,
                desired_types,
                backfill=backfill_new_fields,
                skip_cols=conflict_columns or [],
                source_types=source_type_by_col,
                suppressed_out=suppressed_widens,
            )
            if suppressed_widens:
                desired_types = [
                    suppressed_widens.get(col, typ)
                    for col, typ in zip(target_cols, desired_types)
                ]
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
            # Existence before CREATE — create-new may skip overlay require.
            table_existed = False
            try:
                cur.execute(
                    """
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = %s
                    LIMIT 1
                    """,
                    (schema, table_name),
                )
                table_existed = cur.fetchone() is not None
            except Exception:
                table_existed = not create_table

            # Create-new: partial Studio must not soft-bind Map VARCHAR.
            if not table_existed and studio_err:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=table_name,
                    target_schema=schema,
                    checksum="",
                    chunks_completed=0,
                    error=studio_err,
                    rejected_details=rejected_details,
                    warnings=transform_errors,
                )

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

            if additive_refuse:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=table_name,
                    target_schema=schema,
                    checksum="",
                    chunks_completed=0,
                    error=additive_refuse,
                    rejected_details=rejected_details,
                    warnings=transform_errors,
                )

            # Live DDL must win over Map stamps before values are coerced.
            # Rematerialize map/quarantine when physical carriers differ (BQ-class).
            # Do NOT late-import bind_sql_mapped_rows_with_quarantine — UnboundLocal.
            physical = _fetch_pg_column_types(cur, schema, table_name)
            overlay_err = require_physical_types_for_existing_table(
                table_existed=table_existed,
                physical=physical,
                dialect_label="PostgreSQL" if port != 5439 else "Redshift",
                target_cols=target_cols,
            )
            if overlay_err:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=table_name,
                    target_schema=schema,
                    checksum="",
                    chunks_completed=0,
                    error=overlay_err,
                    rejected_details=rejected_details,
                    warnings=transform_errors,
                )
            if not physical and deferred_map_abort:
                # No physical carriers to overturn the projection (create-new /
                # unreadable DDL): the Map verdict stands exactly as computed.
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=table_name,
                    target_schema=schema,
                    checksum="",
                    chunks_completed=0,
                    error=deferred_map_abort,
                    rejected_rows=rejected_rows,
                    rejected_details=rejected_details,
                    warnings=transform_errors,
                )
            if physical:
                from connectors.writer_common import rematerialize_live_dest_types

                live_dest_types = rematerialize_live_dest_types(
                    physical, list(target_cols or []), product="PostgreSQL"
                )
                if live_dest_types is None:
                    return WriteResult(
                        ok=False,
                        rows_written=0,
                        table_name=table_name,
                        target_schema=schema,
                        checksum="",
                        chunks_completed=0,
                        error=(
                            "PostgreSQL live DDL incomplete for mapped columns — "
                            "refuse Map VARCHAR rematerialize invent. Re-run "
                            "destination schema introspect and retry."
                        ),
                        rejected_details=rejected_details,
                        warnings=transform_errors,
                    )
                carriers_differ = any(
                    str(dest_types.get(c) or "").strip().upper()
                    != str(live_dest_types.get(c) or "").strip().upper()
                    for c in target_cols
                )
                need_remap = carriers_differ or (bool(studio_err) and not mapped_rows)
                if need_remap:
                    dest_types = live_dest_types
                    _batch = _pg_materialize_mapped_batch(
                        headers=headers,
                        data_rows=data_rows,
                        mappings=mappings,
                        target_cols=target_cols,
                        column_types=column_types,
                        dest_types=dest_types,
                        logical_types=logical_types,
                        policy=policy,
                        engine=engine,
                        conflict_columns=conflict_columns,
                        write_mode=write_mode,
                        destination_pk_columns=list(conflict_columns or []) or None,
                        destination_column_nullability=_kwargs.get(
                            "destination_column_nullability"
                        ),
                        empty_cells_as_null=bool(_kwargs.get("empty_cells_as_null")),
                    )
                    mapped_rows = _batch.mapped_rows
                    sparse_rows = _batch.sparse_rows
                    transform_errors = _batch.transform_errors
                    rejected_details = _batch.rejected_details
                    target_types = _batch.target_types
                    bind_types = _batch.bind_types
                    rows_for_checksum = _batch.rows_for_checksum
                    total = len(mapped_rows)
                    chunks = (
                        max(1, (total + chunk_size - 1) // chunk_size) if total else 0
                    )
                    rejected_rows = _rejected_row_count(
                        data_rows,
                        mapped_rows,
                        rejected_details,
                        policy,
                        sparse_rows=sparse_rows,
                    )
                    coerced_null_rows = _coerced_null_row_count(
                        rejected_details, policy
                    )
                else:
                    bind_types = overlay_physical_bind_types(
                        target_cols, bind_types, physical
                    )
                    target_types = list(bind_types)
                    mapped_rows = bind_sql_mapped_rows_with_quarantine(
                        mapped_rows,
                        target_cols,
                        bind_types,
                        rejected_details,
                        policy,
                        engine="postgresql",
                        dialect_label="PostgreSQL",
                        mappings=mappings,
                    )
                    sparse_rows = bind_sql_mapped_rows_with_quarantine(
                        sparse_rows,
                        target_cols,
                        bind_types,
                        rejected_details,
                        policy,
                        engine="postgresql",
                        dialect_label="PostgreSQL",
                        mappings=mappings,
                    )
                    from connectors.writer_common import (
                        materialize_missing_as_null_for_dense_write,
                    )

                    mapped_rows = materialize_missing_as_null_for_dense_write(
                        mapped_rows
                    )
                    rows_for_checksum = list(mapped_rows)
                deferred_map_abort = None
                _phys_abort = reject_on_strict_policy(
                    policy, rejected_details, "PostgreSQL", transform_errors
                )
                if _phys_abort:
                    return WriteResult(
                        ok=False,
                        rows_written=0,
                        table_name=table_name,
                        target_schema=schema,
                        checksum="",
                        chunks_completed=0,
                        error=_phys_abort,
                        rejected_details=rejected_details,
                        warnings=transform_errors,
                    )

            rows_skipped = 0
            if sparse_rows and write_mode == "upsert" and conflict_columns:
                from psycopg2 import sql as _psql
                from connectors.writer_common import row_has_missing_sentinel

                written_sparse, sparse_skipped, sparse_checksum = _pg_apply_sparse_upsert(
                    cur,
                    _psql,
                    schema=schema,
                    table_name=table_name,
                    target_cols=target_cols,
                    conflict_columns=conflict_columns,
                    sparse_rows=sparse_rows,
                    rejected_details=rejected_details,
                    policy=policy,
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
                            if (
                                write_mode == "upsert"
                                and conflict_columns
                                and uses_pg_on_conflict_upsert(engine)
                                and not redshift_upsert_cols
                            ):
                                from connectors.writer_common import (
                                    partition_dense_upsert_rows,
                                )

                                conflict_for_part = [
                                    c for c in conflict_columns if c in target_cols
                                ]
                                if conflict_for_part:
                                    before = len(write_batch)
                                    write_batch = partition_dense_upsert_rows(
                                        write_batch,
                                        conflict_for_part,
                                        target_cols=target_cols,
                                        rejected_details=rejected_details,
                                        policy=policy,
                                        row_offset=start,
                                    )
                                    rows_skipped += before - len(write_batch)
                            if redshift_upsert_cols:
                                write_batch = _redshift_delete_by_keys(
                                    cur,
                                    sql,
                                    schema=schema,
                                    table_name=table_name,
                                    target_cols=target_cols,
                                    conflict_cols=redshift_upsert_cols,
                                    batch=batch,
                                    rejected_details=rejected_details,
                                    policy=policy,
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
                                    write_batch,
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
                                row_start=start,
                                row_end=start + max(landed - 1, 0),
                                attempt=1,
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
                                            rejected_details=rejected_details,
                                            policy=policy,
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
                                    from connectors.writer_common import (
                                        append_write_quarantine_detail,
                                    )

                                    append_write_quarantine_detail(
                                        rejected_details,
                                        {
                                            "row": start + row_i,
                                            "column": col_name,
                                            "value": sample_val,
                                            "reason": str(row_exc)[:300],
                                            "policy": policy,
                                        },
                                        mapped_row=row,
                                        target_cols=target_cols,
                                        mappings=mappings,
                                    )
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
                                        row_start=start,
                                        row_end=start + max(chunk_written - 1, 0),
                                        attempt=1,
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
                if written > 0 and job_id:
                    try:
                        from services.auto_create_lifecycle import mark_auto_create_committed

                        mark_auto_create_committed(job_id)
                    except Exception:
                        logger.debug("auto_create commit mark skipped", exc_info=True)
                if on_checkpoint:
                    on_checkpoint(chunks_completed, chunks, written)

            child_flush = flush_normalized_child_batches(
                headers=headers,
                data_rows=data_rows,
                mappings=mappings,
                dest_db="postgresql",
                create_table=create_table,
                cursor=cur,
                quote='"',
                placeholder="%s",
                schema=schema or "public",
            )
            if not child_flush.get("ok", True):
                try:
                    conn.rollback()
                except Exception as exc:
                    logger.warning("Exception suppressed: %s", exc, exc_info=exc)
                child_flush_error = "; ".join(
                    child_flush.get("errors") or ["child table flush failed"]
                )
            elif child_flush.get("rows_written"):
                try:
                    conn.commit()
                except Exception as exc:
                    logger.warning("Exception suppressed: %s", exc, exc_info=exc)
                for t in child_flush.get("tables") or []:
                    transform_errors.append(f"normalized child table wrote {t}")
        finally:
            try:
                cur.close()
            except Exception as exc:
                logger.warning("Exception suppressed: %s", exc, exc_info=exc)

        if close_connection:
            close_quietly(conn)
        if child_flush_error:
            return WriteResult(
                ok=False,
                rows_written=written,
                table_name=table_name,
                target_schema=schema,
                checksum="",
                chunks_completed=chunks_completed or chunks,
                error=child_flush_error,
                rejected_rows=max(rejected_rows, len(data_rows) - written - rows_skipped),
                rejected_details=rejected_details,
                coerced_null_rows=coerced_null_rows,
                rows_skipped=rows_skipped,
                warnings=transform_errors,
                load_method="copy" if use_copy else "insert",
            )
        _final_abort = reject_on_strict_policy(policy, rejected_details, "PostgreSQL")
        if _final_abort:
            return WriteResult(
                ok=False,
                rows_written=written,
                table_name=table_name,
                target_schema=schema,
                checksum="",
                chunks_completed=chunks_completed or chunks,
                error=_final_abort,
                rejected_rows=max(rejected_rows, len(data_rows) - written - rows_skipped),
                rejected_details=rejected_details,
                coerced_null_rows=coerced_null_rows,
                rows_skipped=rows_skipped,
                warnings=transform_errors,
                load_method="copy" if use_copy else "insert",
            )
        from connectors.writer_common import gate8_writer_meta

        _checksum_rows = (
            rows_for_checksum if "rows_for_checksum" in locals() else mapped_rows
        )
        meta_out = gate8_writer_meta(
            _checksum_rows,
            target_cols,
            conflict_columns=conflict_columns or None,
        )
        fid_report = _kwargs.get("_schema_fidelity_report")
        if isinstance(fid_report, dict):
            meta_out = dict(meta_out or {})
            meta_out["schema_fidelity"] = fid_report
        return WriteResult(
            ok=True,
            rows_written=written,
            table_name=table_name,
            target_schema=schema,
            checksum=row_checksum(
                _checksum_rows,
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
            meta=meta_out,
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
