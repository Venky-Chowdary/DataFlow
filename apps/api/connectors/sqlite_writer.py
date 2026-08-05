"""SQLite bulk writer — file-based SQL database with typed columns."""

from __future__ import annotations

import base64
import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Callable

from services.type_system import ddl_type, materialize_dest_ddl
from services.value_serializer import json_default

from connectors.sqlite_common import sqlite_file_path
from connectors.write_resilience import (
    ensure_raw_write_ledger,
    mark_raw_chunk_committed,
    raw_chunk_rows_written,
)
from connectors.writer_common import (
    CHUNK_SIZE,
    _coerced_null_row_count,
    _rejected_row_count,
    build_mapped_rows_with_details,
    filter_stale_lsn_rows,
    quarantine_currency_markers_into_numeric,
    quarantine_unfit_binaries,
    quarantine_unfit_bitstrings,
    quarantine_unfit_booleans,
    quarantine_unfit_decimals,
    quarantine_unfit_enum_set,
    quarantine_unfit_integers,
    quarantine_unfit_specialty_types,
    quarantine_unfit_strings,
    quarantine_unfit_temporals,
    quarantine_unfit_years,
    quote_sql_identifier,
    resolve_target_columns,
    row_checksum,
    sanitize_identifier,
    split_dense_sparse_rows,
    transform_error_policy,
)
from connectors.writer_common import (
    WriteResult as _WriteResult,
)

logger = logging.getLogger(__name__)


@dataclass
class WriteResult(_WriteResult):
    driver: str = "sqlite3"


def sqlite_type(inferred: str) -> str:
    """CREATE DDL for SQLite — rematerializes DECIMAL/MONEY to TEXT (no affinity invent)."""
    return materialize_dest_ddl("sqlite", inferred)


def _to_sqlite_value(value: Any, source_type: str) -> Any:
    from services.value_serializer import is_missing_sentinel

    # Sparse CDC: never coerce DF_MISSING → NULL (would wipe present destination cols).
    if is_missing_sentinel(value):
        return value
    if value is None:
        return None
    upper = source_type.upper()
    if upper in {"DECIMAL", "NUMERIC", "DOUBLE", "REAL", "FLOAT"}:
        if isinstance(value, Decimal):
            return str(value)
        return value
    if upper in {"JSON", "OBJECT", "ARRAY", "VARIANT"}:
        if isinstance(value, (dict, list)):
            return json.dumps(
                value, ensure_ascii=False, separators=(",", ":"), default=json_default
            )
        return value
    if upper in {"BINARY", "BLOB", "BYTEA", "VARBINARY"} or upper.startswith(
        ("BINARY(", "VARBINARY(", "BLOB(")
    ):
        if isinstance(value, bytes):
            return value
        if isinstance(value, str):
            try:
                return base64.b64decode(value, validate=True)
            except Exception as exc:
                # Same honesty as quarantine_unfit_binaries — never invent UTF-8
                # bytes from invalid base64 (silent payload mutation).
                raise ValueError(
                    "binary wire is not valid base64 — refuse silent UTF-8 encode"
                ) from exc
        return value
    if upper in {
        "DATETIME",
        "TIMESTAMP",
        "TIMESTAMP_TZ",
        "TIMESTAMPTZ",
        "TIMESTAMP_LTZ",
        "TIMESTAMP_NTZ",
        "DATE",
        "TIME",
    }:
        from connectors.sql_temporal import coerce_sql_temporal, format_wire_value

        coerced = coerce_sql_temporal(
            value, upper if upper != "TIMESTAMP_NTZ" else "TIMESTAMP"
        )
        wire = format_wire_value(
            value, upper if upper != "TIMESTAMP_NTZ" else "TIMESTAMP"
        )
        if wire is not None:
            return wire
        if isinstance(coerced, datetime):
            return coerced.isoformat(sep=" ")
        if isinstance(coerced, date) and not isinstance(coerced, datetime):
            return coerced.isoformat()
        if isinstance(coerced, time):
            return coerced.isoformat()
        return value
    if upper == "BOOLEAN":
        return 1 if value else 0
    return value


def _sqlite_apply_sparse_upsert(
    cur: Any,
    table_name: str,
    target_cols: list[str],
    conflict_columns: list[str],
    sparse_rows: list[tuple],
) -> tuple[int, int, list[tuple]]:
    """Per-row upsert omitting DF_MISSING — never SET col=NULL for absent CDC fields.

    Returns ``(rows_written, rows_skipped, checksum_rows)`` where checksum_rows are
    post-apply images (destination values preserved for omitted columns).
    """
    from connectors.writer_common import (
        DF_LSN_COL,
        assert_sparse_upsert_has_pk,
        materialize_sparse_row_for_checksum,
        sparse_present_bindings,
    )
    from services.cdc_effectively_once import should_apply_pk_row

    conflict = [c for c in conflict_columns if c in target_cols]
    if not conflict:
        raise ValueError("sparse SQLite upsert requires conflict_columns")
    table_q = quote_sql_identifier(table_name)
    written = 0
    skipped = 0
    checksum_rows: list[tuple] = []
    select_sql = ", ".join(quote_sql_identifier(c) for c in target_cols)
    for row in sparse_rows:
        present = sparse_present_bindings(row, target_cols)
        assert_sparse_upsert_has_pk(present, conflict)
        non_pk = {k: v for k, v in present.items() if k not in conflict}
        pk_vals = [present[c] for c in conflict]
        where_sql = " AND ".join(
            f"{quote_sql_identifier(c)}=?" for c in conflict
        )
        cur.execute(
            f"SELECT {select_sql} FROM {table_q} WHERE {where_sql}",  # nosec B608
            pk_vals,
        )
        existing_tuple = cur.fetchone()
        existing = (
            dict(zip(target_cols, existing_tuple)) if existing_tuple is not None else None
        )
        if (
            existing is not None
            and DF_LSN_COL in present
            and DF_LSN_COL in target_cols
        ):
            if not should_apply_pk_row(
                existing_lsn=existing.get(DF_LSN_COL),
                incoming_lsn=present[DF_LSN_COL],
            ).applied:
                skipped += 1
                continue
        if non_pk:
            set_cols = list(non_pk.keys())
            set_sql = ", ".join(
                f"{quote_sql_identifier(c)}=?" for c in set_cols
            )
            cur.execute(
                f"UPDATE {table_q} SET {set_sql} WHERE {where_sql}",  # nosec B608
                [non_pk[c] for c in set_cols] + pk_vals,
            )
            if cur.rowcount and cur.rowcount > 0:
                written += 1
                checksum_rows.append(
                    materialize_sparse_row_for_checksum(present, existing, target_cols)
                )
                continue
        cols = list(present.keys())
        col_sql = ", ".join(quote_sql_identifier(c) for c in cols)
        ph = ", ".join("?" for _ in cols)
        try:
            cur.execute(
                f"INSERT INTO {table_q} ({col_sql}) VALUES ({ph})",  # nosec B608
                [present[c] for c in cols],
            )
            written += 1
            checksum_rows.append(
                materialize_sparse_row_for_checksum(present, existing, target_cols)
            )
        except Exception:
            if not non_pk:
                raise
            set_cols = list(non_pk.keys())
            set_sql = ", ".join(
                f"{quote_sql_identifier(c)}=?" for c in set_cols
            )
            cur.execute(
                f"UPDATE {table_q} SET {set_sql} WHERE {where_sql}",  # nosec B608
                [non_pk[c] for c in set_cols] + pk_vals,
            )
            written += 1
            # Re-read after race: prefer merged image from pre-insert existing.
            checksum_rows.append(
                materialize_sparse_row_for_checksum(present, existing, target_cols)
            )
    return written, skipped, checksum_rows


def _sqlite_upsert_batch(
    cur: Any,
    table_name: str,
    target_cols: list[str],
    batch: list[tuple],
    conflict_cols: list[str],
    schema: str | None = None,
) -> tuple[int, int]:
    """Upsert with optional ``_df_lsn`` monotonic guard (at-least-once CDC).

    Prefetch existing LSNs for the batch, drop stale rows, then either use
    ``INSERT … ON CONFLICT DO UPDATE WHERE`` or delete+insert fallback. Returns
    (rows_written, rows_skipped) for accurate reconciliation accounting.
    """
    from connectors.writer_common import DF_LSN_COL, dedupe_rows_by_pk_and_lsn, sqlite_lsn_update_guard_sql

    rows = dedupe_rows_by_pk_and_lsn(batch, conflict_cols, target_cols)
    if not rows:
        return 0, 0

    original_count = len(rows)
    lsn_guarded = DF_LSN_COL in target_cols and conflict_cols
    if lsn_guarded:
        rows, skipped = filter_stale_lsn_rows(
            cur,
            table_name,
            schema,
            conflict_cols,
            rows,
            target_cols,
            quote='"',
            placeholder="?",
        )
    else:
        skipped = 0

    if not rows:
        return 0, skipped + (original_count - len(rows))

    table_quoted = quote_sql_identifier(table_name)
    cols_sql = ", ".join(quote_sql_identifier(c) for c in target_cols)
    placeholders = ", ".join("?" for _ in target_cols)
    conflict_sql = ", ".join(quote_sql_identifier(c) for c in conflict_cols)
    update_cols = [c for c in target_cols if c not in conflict_cols]

    if lsn_guarded and update_cols:
        where_sql = sqlite_lsn_update_guard_sql(table_name)
        set_sql = ", ".join(
            f"{quote_sql_identifier(c)}=excluded.{quote_sql_identifier(c)}"
            for c in update_cols
        )
        insert_sql = (
            f"INSERT INTO {table_quoted} ({cols_sql}) VALUES ({placeholders}) "  # nosec B608
            f"ON CONFLICT ({conflict_sql}) DO UPDATE SET {set_sql} WHERE {where_sql}"
        )
        try:
            cur.executemany(insert_sql, rows)
            return len(rows), skipped + (original_count - len(rows))
        except Exception as exc:
            # Missing UNIQUE on conflict cols — fall through to delete+insert.
            logger.warning("Exception suppressed: %s", exc, exc_info=exc)

    # delete+insert fallback (already deduped + LSN filtered).
    indices = [target_cols.index(c) for c in conflict_cols]
    deduped = {tuple(row[i] for i in indices): row for row in rows}
    rows = list(deduped.values())
    col_sql = ", ".join(quote_sql_identifier(c) for c in conflict_cols)
    del_placeholders = ", ".join(
        "(" + ", ".join("?" for _ in conflict_cols) + ")" for _ in deduped
    )
    delete_sql = f"DELETE FROM {table_quoted} WHERE ({col_sql}) IN ({del_placeholders})"  # nosec B608
    delete_params = [v for key in deduped.keys() for v in key]
    cur.execute(delete_sql, delete_params)

    insert_sql = f"INSERT INTO {table_quoted} ({cols_sql}) VALUES ({placeholders})"  # nosec B608
    cur.executemany(insert_sql, rows)
    return len(rows), skipped + (original_count - len(rows))


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
    """Write records to a SQLite database file."""
    del port, username, password, ssl
    from connectors.writer_common import resolve_writer_backfill

    backfill_new_fields = resolve_writer_backfill(
        backfill_new_fields=backfill_new_fields,
        mappings=mappings,
        schema_policy=_kwargs.get("schema_policy"),
    )
    path = sqlite_file_path(database, connection_string, host)
    if not path:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema=schema or "main",
            checksum="",
            chunks_completed=0,
            error="SQLite path is required (database or connection_string).",
        )

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
            target_schema=schema or "main",
            checksum="",
            chunks_completed=0,
            error="No column mappings",
        )

    table_name = sanitize_identifier(table_name, preserve_case=True)
    table_quoted = quote_sql_identifier(table_name)
    target_types = [sqlite_type(t) for t in logical_types]
    dest_types = {target_cols[i]: logical_types[i] for i in range(len(target_cols))}
    policy = transform_error_policy(error_policy)

    mapped_rows: list[tuple] = []
    converted_rows: list[tuple] = []
    chunks = 0
    written = 0
    rows_skipped = 0
    transform_errors: list[str] = []

    try:
        mapped_rows, transform_errors, rejected_details = (
            build_mapped_rows_with_details(
                headers=headers,
                data_rows=data_rows,
                mappings=mappings,
                target_cols=target_cols,
                column_types=column_types,
                dest_types=dest_types,
                error_policy=policy,
                preserve_case=True,
            )
        )
        # Shared quarantine matrix — SQLite is PRODUCTION_SKU; never skip fit
        # checks that generic_sql / Postgres / BQ run (silent truncate / invent).
        tgt_types = [str(logical_types[i] if i < len(logical_types) else "") for i in range(len(target_cols))]
        mapped_rows = quarantine_currency_markers_into_numeric(
            mapped_rows, target_cols, tgt_types, rejected_details, policy
        )
        mapped_rows = quarantine_unfit_decimals(
            mapped_rows,
            target_cols,
            tgt_types,
            rejected_details,
            policy,
            dialect_label="SQLite NUMERIC",
        )
        mapped_rows = quarantine_unfit_years(
            mapped_rows, target_cols, tgt_types, rejected_details, policy
        )
        mapped_rows = quarantine_unfit_booleans(
            mapped_rows, target_cols, tgt_types, rejected_details, policy
        )
        mapped_rows = quarantine_unfit_temporals(
            mapped_rows, target_cols, tgt_types, rejected_details, policy
        )
        mapped_rows = quarantine_unfit_specialty_types(
            mapped_rows, target_cols, tgt_types, rejected_details, policy
        )
        mapped_rows = quarantine_unfit_integers(
            mapped_rows,
            target_cols,
            tgt_types,
            rejected_details,
            policy,
            dialect_label="SQLite INTEGER",
        )
        mapped_rows = quarantine_unfit_bitstrings(
            mapped_rows, target_cols, tgt_types, rejected_details, policy
        )
        mapped_rows = quarantine_unfit_binaries(
            mapped_rows,
            target_cols,
            tgt_types,
            rejected_details,
            policy,
            dialect_label="SQLite BLOB",
        )
        mapped_rows = quarantine_unfit_enum_set(
            mapped_rows, target_cols, tgt_types, rejected_details, policy
        )
        mapped_rows = quarantine_unfit_strings(
            mapped_rows,
            target_cols,
            tgt_types,
            rejected_details,
            policy,
            dialect_label="SQLite TEXT",
        )

        rows_for_checksum: list[tuple] = []
        sparse_rows: list[tuple] = []
        conflict_cols = [c for c in (conflict_columns or []) if c in target_cols]
        if write_mode == "upsert" and conflict_cols:
            mapped_rows, sparse_rows = split_dense_sparse_rows(mapped_rows)

        converted_rows = [
            tuple(_to_sqlite_value(v, logical_types[i]) for i, v in enumerate(row))
            for row in mapped_rows
        ]
        sparse_converted = [
            tuple(_to_sqlite_value(v, logical_types[i]) for i, v in enumerate(row))
            for row in sparse_rows
        ]
        # Dense rows are fully written — include them in writer-ack checksum.
        rows_for_checksum = list(converted_rows)

        rejected_rows = _rejected_row_count(
            data_rows, mapped_rows, rejected_details, policy, sparse_rows=sparse_rows
        )
        coerced_null_rows = _coerced_null_row_count(rejected_details, policy)
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
                rejected_details=rejected_details,
                warnings=transform_errors,
            )

        total = len(converted_rows)
        chunks = max(1, (total + CHUNK_SIZE - 1) // CHUNK_SIZE) if total else 0
        placeholders = ", ".join("?" for _ in target_cols)
        insert = f"INSERT INTO {table_quoted} ({', '.join(quote_sql_identifier(c) for c in target_cols)}) VALUES ({placeholders})"  # nosec B608

        # Insert-mode writes are the only ones a retry can duplicate; upserts
        # converge on their conflict key. The ledger needs a job id to tell an
        # interrupted attempt apart from a fresh one.
        ledger_job_id = str(_kwargs.get("job_id") or "")
        ledger_batch_key = str(_kwargs.get("write_batch_key") or "")
        use_ledger = bool(
            ledger_job_id
            and ledger_batch_key
            and not (write_mode == "upsert" and conflict_cols)
        )
        ledger_chunks_skipped = 0

        conn = sqlite3.connect(path, timeout=8)
        try:
            # Schema setup in its own transaction.
            with conn:
                cur = conn.cursor()
                if use_ledger:
                    try:
                        ensure_raw_write_ledger(cur, dialect="sqlite")
                    except sqlite3.Error as exc:
                        logger.warning(
                            "SQLite write ledger unavailable, retries of this "
                            "insert cannot be de-duplicated: %s",
                            exc,
                        )
                        use_ledger = False
                if create_table:
                    col_defs = ", ".join(
                        f"{quote_sql_identifier(c)} {t}"
                        for c, t in zip(target_cols, target_types)
                    )
                    cur.execute(
                        f"CREATE TABLE IF NOT EXISTS {table_quoted} ({col_defs})"
                    )

                if backfill_new_fields:
                    existing = {
                        row[1]
                        for row in cur.execute(f"PRAGMA table_info({table_quoted})")
                    }
                    for col, typ in zip(target_cols, target_types):
                        if col not in existing:
                            try:
                                cur.execute(
                                    f"ALTER TABLE {table_quoted} ADD COLUMN {quote_sql_identifier(col)} {typ}"
                                )
                            except sqlite3.OperationalError as exc:
                                logger.debug(
                                    "sqlite add column skipped for %s: %s",
                                    col,
                                    exc,
                                    exc_info=exc,
                                )

            if sparse_converted and write_mode == "upsert" and conflict_cols:
                with conn:
                    cur = conn.cursor()
                    sparse_written, sparse_skipped, sparse_checksum = (
                        _sqlite_apply_sparse_upsert(
                            cur,
                            table_name,
                            target_cols,
                            conflict_cols,
                            sparse_converted,
                        )
                    )
                    written += sparse_written
                    rows_skipped += sparse_skipped
                    rows_for_checksum.extend(sparse_checksum)

            # Each chunk is a separate transaction so checkpoints are durable
            # and a failed chunk can be retried without writing partial data.
            for chunk_idx in range(chunks):
                start = chunk_idx * CHUNK_SIZE
                batch = converted_rows[start : start + CHUNK_SIZE]
                if not batch:
                    break

                with conn:
                    cur = conn.cursor()
                    already = (
                        raw_chunk_rows_written(
                            cur,
                            dialect="sqlite",
                            job_id=ledger_job_id,
                            batch_key=ledger_batch_key,
                            chunk_idx=chunk_idx,
                        )
                        if use_ledger
                        else None
                    )
                    if already is not None:
                        # A previous attempt already committed this chunk.
                        # Credit the recorded count, not len(batch), so a chunk
                        # that quarantined rows is not over-reported on replay.
                        written += already
                        ledger_chunks_skipped += 1
                    elif write_mode == "upsert" and conflict_cols:
                        chunk_written, chunk_skipped = _sqlite_upsert_batch(
                            cur, table_name, target_cols, batch, conflict_cols, schema=schema or None
                        )
                        written += chunk_written
                        rows_skipped += chunk_skipped
                    else:
                        cur.executemany(insert, batch)
                        written += len(batch)
                        if use_ledger:
                            # Same transaction as the rows it vouches for, so
                            # the ledger entry cannot outlive a rolled-back write.
                            mark_raw_chunk_committed(
                                cur,
                                dialect="sqlite",
                                job_id=ledger_job_id,
                                batch_key=ledger_batch_key,
                                chunk_idx=chunk_idx,
                                rows_written=len(batch),
                            )

                if on_checkpoint:
                    on_checkpoint(chunk_idx + 1, max(chunks, 1), written)

            if ledger_chunks_skipped:
                transform_errors.append(
                    f"Skipped {ledger_chunks_skipped} chunk(s) already committed by a "
                    "previous attempt (write ledger prevented duplicate rows)"
                )

            return WriteResult(
                ok=True,
                rows_written=written,
                table_name=table_name,
                target_schema=schema or "main",
                # Checksum must reflect values as stored (sparse preserves dest cells).
                checksum=row_checksum(
                    rows_for_checksum,
                    target_cols,
                    dest_db_type="sqlite",
                    dest_types=dest_types,
                ),
                chunks_completed=chunks or (1 if sparse_converted else 0),
                rejected_rows=max(
                    rejected_rows, len(data_rows) - written - rows_skipped
                ),
                rejected_details=rejected_details,
                coerced_null_rows=coerced_null_rows,
                rows_skipped=rows_skipped,
                warnings=transform_errors,
            )
        finally:
            conn.close()
    except Exception as exc:
        return WriteResult(
            ok=False,
            rows_written=written,
            table_name=table_name,
            target_schema=schema or "main",
            checksum="",
            chunks_completed=chunks,
            error=str(exc),
            rejected_details=rejected_details if "rejected_details" in locals() else [],
            rows_skipped=rows_skipped if "rows_skipped" in locals() else 0,
        )
