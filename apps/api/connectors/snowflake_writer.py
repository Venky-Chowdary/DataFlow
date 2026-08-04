"""Snowflake bulk writer — COPY INTO staging for scale + batched INSERT fallback."""

from __future__ import annotations

import csv
import logging
import os
from services.brand_env import getenv_brand
import tempfile
import uuid
from dataclasses import dataclass
from decimal import Overflow
from pathlib import Path
from typing import Any, Callable

from services.type_system import ddl_type, materialize_dest_ddl, normalize_logical_type
from services.value_serializer import cell_to_string

from connectors.driver_guard import stub_writes_allowed
from connectors.snowflake_conn import (
    _is_local_account,
    get_connection,
    normalize_account,
    resolve_snowflake_table_name,
)
from connectors.stub_writer import simulate_stub_write
from connectors.writer_common import (
    DF_LSN_COL,
    _coerced_null_row_count,
    _rejected_row_count,
    assert_sparse_upsert_has_pk,
    build_mapped_rows_with_details,
    dedupe_rows,
    dedupe_rows_by_pk_and_lsn,
    null_safe_merge_on,
    quarantine_unfit_decimals,
    quarantine_unfit_specialty_types,
    quarantine_unfit_strings,
    quote_sql_identifier,
    resolve_target_columns,
    row_checksum,
    sample_values_by_source_from_batch,
    sanitize_identifier,
    snowflake_lsn_match_predicate,
    sparse_present_bindings,
    split_dense_sparse_rows,
    transform_error_policy,
)
from connectors.writer_common import (
    WriteResult as _WriteResult,
)

logger = logging.getLogger(__name__)

# Prefer COPY INTO for modest stream batches — 2000 was too high when wide Mongo
# rows shrink stream chunks below the threshold and force slow INSERT loops.
COPY_THRESHOLD = int(getenv_brand("SNOWFLAKE_COPY_THRESHOLD", "200"))
MAX_BIND_INSERT_ROWS = int(getenv_brand("SF_BIND_INSERT_ROWS", "1000"))

@dataclass
class WriteResult(_WriteResult):
    driver: str = "snowflake-connector-python"
    load_method: str = "insert"


def sf_type(inferred: str) -> str:
    return materialize_dest_ddl("snowflake", inferred)


def _is_fakesnow_connection(conn: Any) -> bool:
    """Return True for the local fakesnow emulator — it does not support PUT/COPY."""
    return (
        getattr(conn, "__class__", None) is not None
        and conn.__class__.__name__ == "FakeSnowflakeConnection"
    )


def _parse_number_type(sf_type_str: str) -> tuple[int, int] | None:
    from connectors.writer_common import parse_decimal_precision_scale

    return parse_decimal_precision_scale(sf_type_str)


def _decimal_scale_and_int_digits(value: Any) -> tuple[int, int]:
    from connectors.writer_common import decimal_int_digits_and_scale

    return decimal_int_digits_and_scale(value)


def _fits_snowflake_number(value: Any, precision: int, scale: int) -> bool:
    """True if value can be stored in Snowflake NUMBER(precision, scale)."""
    from connectors.writer_common import fits_decimal

    return fits_decimal(value, precision, scale)


def _snowflake_decimal_type(col_idx: int, mapped_rows: list[tuple]) -> str:
    """Pick a NUMBER(p,s) type wide enough for the actual data in this batch.

    Snowflake requires p <= 38 and p >= s. Prefer preserving integer magnitude
    over fractional digits when the value would otherwise overflow NUMBER(38,*).
    """
    max_int = 0
    max_scale = 0
    for row in mapped_rows:
        if col_idx >= len(row) or row[col_idx] is None:
            continue
        int_digits, scale = _decimal_scale_and_int_digits(row[col_idx])
        max_int = max(max_int, int_digits)
        max_scale = max(max_scale, scale)

    if max_scale == 0 and max_int == 0:
        return "NUMBER(38,10)"

    # Prefer observed scale; keep a small buffer when data is modest.
    scale = min(38, max_scale + (2 if max_scale > 0 else 0))
    int_digits = max(1, max_int + (1 if max_int > 0 else 0))
    if int_digits + scale > 38:
        scale = max(0, 38 - int_digits)
    if int_digits + scale > 38:
        int_digits = 38 - scale
    precision = max(scale, min(38, int_digits + scale))
    if precision < 1:
        return "NUMBER(38,10)"
    return f"NUMBER({precision},{scale})"


def _quarantine_unfit_decimals(
    mapped_rows: list[tuple],
    target_cols: list[str],
    target_types: list[str],
    rejected_details: list[dict[str, Any]],
    policy: str,
) -> list[tuple]:
    """Hold out / NULL cells that cannot fit NUMBER(p,s)."""
    from connectors.writer_common import quarantine_unfit_decimals

    return quarantine_unfit_decimals(
        mapped_rows,
        target_cols,
        target_types,
        rejected_details,
        policy,
        dialect_label="Snowflake NUMBER",
    )


def _widen_existing_number_columns(
    cur: Any,
    schema: str,
    table_name: str,
    target_cols: list[str],
    target_types: list[str],
) -> None:
    """Widen existing NUMBER columns when a later batch needs more capacity.

    CREATE TABLE IF NOT EXISTS freezes the first batch's sizing; without this,
    later chunks raise decimal.Overflow after tens of thousands of rows succeed.
    """
    try:
        cur.execute(
            """
            SELECT COLUMN_NAME, NUMERIC_PRECISION, NUMERIC_SCALE
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            """,
            (schema.upper(), table_name.upper()),
        )
        existing = {
            str(row[0]).upper(): (int(row[1] or 0), int(row[2] or 0))
            for row in cur.fetchall()
        }
    except Exception as exc:
        logger.warning(
            "snowflake widen column introspection failed: %s", exc, exc_info=exc
        )
        return

    for col, typ in zip(target_cols, target_types):
        parsed = _parse_number_type(typ)
        if not parsed:
            continue
        want_p, want_s = parsed
        cur_p, cur_s = existing.get(col.upper(), (0, 0))
        if cur_p <= 0:
            continue
        want_int = max(0, want_p - want_s)
        cur_int = max(0, cur_p - cur_s)
        final_int = max(want_int, cur_int)
        final_scale = max(want_s, cur_s)
        if final_int + final_scale > 38:
            final_scale = max(0, 38 - final_int)
        final_p = min(38, final_int + final_scale)
        if final_p <= cur_p and final_scale <= cur_s and final_int <= cur_int:
            continue
        try:
            cur.execute(
                f'ALTER TABLE "{table_name}" ALTER COLUMN "{col}" '
                f"SET DATA TYPE NUMBER({final_p},{final_scale})"
            )
        except Exception as exc:
            logger.warning(
                "snowflake alter column %s failed: %s", col, exc, exc_info=exc
            )


def _format_write_error(exc: BaseException) -> str:
    """Human-readable write error — never bare ``[<class 'decimal.Overflow'>]``."""
    if isinstance(exc, Overflow) or type(exc).__name__ == "Overflow":
        return (
            "decimal.Overflow: a numeric value exceeded Snowflake NUMBER capacity. "
            "Bad cells are quarantined when error policy allows; widen the column "
            "or map overflow fields to VARCHAR."
        )
    msg = str(exc).strip()
    if not msg or msg.startswith("[<class"):
        return f"{type(exc).__module__}.{type(exc).__name__}: numeric overflow or bind failure during Snowflake write"
    return msg


def _bind_rows_for_snowflake(
    mapped_rows: list[tuple],
    target_types: list[str],
) -> list[tuple]:
    """Normalize every cell with shared sql_bind before COPY / INSERT / MERGE.

    Production volume uses COPY (≥ COPY_THRESHOLD); without this, BOOLEAN/VARIANT
    bind only on the small JSON INSERT path and Mongo ``\"true\"`` wire drifts.

    Dense load only — callers must route sparse CDC rows to omit-from-SET upsert.
    ``DF_MISSING`` becomes SQL NULL so BOOL/NUMBER never see the sentinel string.
    """
    from connectors.sql_bind import normalize_sql_bind_value
    from connectors.writer_common import materialize_missing_as_null_for_dense_write

    mapped_rows = materialize_missing_as_null_for_dense_write(mapped_rows)
    bound: list[tuple] = []
    for row in mapped_rows:
        converted: list[Any] = []
        for v, t in zip(row, target_types):
            ddl = (t or "VARCHAR").strip() or "VARCHAR"
            converted.append(normalize_sql_bind_value(v, ddl, engine="snowflake"))
        # Preserve trailing columns if types list is shorter (defensive).
        if len(row) > len(target_types):
            converted.extend(row[len(target_types) :])
        bound.append(tuple(converted))
    return bound


def _write_temp_csv(
    path: Path, target_cols: list[str], mapped_rows: list[tuple]
) -> None:
    from services.value_serializer import is_missing_sentinel

    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, quoting=csv.QUOTE_ALL)
        writer.writerow(target_cols)
        for row in mapped_rows:
            if any(is_missing_sentinel(v) for v in row):
                raise ValueError(
                    "Snowflake COPY path refused DF_MISSING sentinel — "
                    "sparse CDC rows must use omit-from-SET upsert, not CSV stage"
                )
            # Temporal cells are already warehouse-normalized strings; other
            # types still go through cell_to_string for CSV safety.
            writer.writerow(
                [
                    ""
                    if v is None
                    else (v if isinstance(v, str) else cell_to_string(v))
                    for v in row
                ]
            )


def _sf_apply_sparse_upsert(
    cur: Any,
    table_name: str,
    target_cols: list[str],
    target_types: list[str],
    conflict: list[str],
    sparse_rows: list[tuple],
) -> tuple[int, int, list[tuple]]:
    """Per-row Snowflake upsert omitting DF_MISSING — never SET col=NULL for absent."""
    from connectors.sql_bind import normalize_sql_bind_value
    from connectors.writer_common import (
        DF_LSN_COL,
        assert_sparse_upsert_has_pk,
        materialize_sparse_row_for_checksum,
        sparse_present_bindings,
    )
    from services.cdc_effectively_once import should_apply_pk_row

    if not conflict:
        raise ValueError("sparse Snowflake upsert requires conflict_columns")
    type_by_col = {c: t for c, t in zip(target_cols, target_types)}
    tbl = quote_sql_identifier(table_name)
    written = 0
    skipped = 0
    checksum_rows: list[tuple] = []
    select_sql = ", ".join(quote_sql_identifier(c) for c in target_cols)
    where_sql = " AND ".join(f"{quote_sql_identifier(c)} = %s" for c in conflict)

    for row in sparse_rows:
        raw_present = sparse_present_bindings(row, target_cols)
        present = {
            k: normalize_sql_bind_value(
                v, type_by_col.get(k, "VARCHAR"), engine="snowflake"
            )
            for k, v in raw_present.items()
        }
        assert_sparse_upsert_has_pk(present, conflict)
        non_pk = {k: v for k, v in present.items() if k not in conflict}
        pk_vals = [present[c] for c in conflict]
        cur.execute(
            f"SELECT {select_sql} FROM {tbl} WHERE {where_sql}",  # nosec B608
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
                f"{quote_sql_identifier(c)} = %s" for c in set_cols
            )
            cur.execute(
                f"UPDATE {tbl} SET {set_sql} WHERE {where_sql}",  # nosec B608
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
        ph = ", ".join(["%s"] * len(cols))
        try:
            cur.execute(
                f"INSERT INTO {tbl} ({col_sql}) VALUES ({ph})",  # nosec B608
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
                f"{quote_sql_identifier(c)} = %s" for c in set_cols
            )
            cur.execute(
                f"UPDATE {tbl} SET {set_sql} WHERE {where_sql}",  # nosec B608
                [non_pk[c] for c in set_cols] + pk_vals,
            )
            written += 1
            checksum_rows.append(
                materialize_sparse_row_for_checksum(present, existing, target_cols)
            )
    return written, skipped, checksum_rows


def _is_json_type(sf_type: str) -> bool:
    return sf_type and sf_type.split("(")[0].upper() in {
        "VARIANT",
        "JSON",
        "OBJECT",
        "ARRAY",
    }


def _batch_insert_rows(
    cur: Any,
    table_name: str,
    target_cols: list[str],
    target_types: list[str],
    batch: list[tuple],
) -> int:
    """Batch INSERT for rows below the COPY threshold, including VARIANT/JSON.

    Snowflake's Python connector does not support array binds inside a
    ``SELECT ... FROM VALUES`` subquery (SNOW-940628) and ``VALUES`` clauses do
    not allow function calls such as ``PARSE_JSON``.  We build one
    ``INSERT INTO ... SELECT ... FROM VALUES (%s,...), (%s,...)`` statement per
    sub-batch, stringify JSON-typed values so ``PARSE_JSON(columnN)`` can parse
    them, and bind all values as positional parameters.
    """
    col_list = ", ".join(f'"{c}"' for c in target_cols)
    select_items = []
    for i, t in enumerate(target_types, start=1):
        if _is_json_type(t):
            select_items.append(f"PARSE_JSON(column{i})")
        else:
            select_items.append(f"column{i}")
    select_sql = ", ".join(select_items)

    written = 0
    for offset in range(0, len(batch), MAX_BIND_INSERT_ROWS):
        sub = batch[offset : offset + MAX_BIND_INSERT_ROWS]
        row_placeholders: list[str] = []
        params: list[Any] = []
        from connectors.sql_bind import normalize_sql_bind_value

        for row in sub:
            converted: list[Any] = []
            for v, t in zip(row, target_types):
                if _is_json_type(t):
                    # JSON/VARIANT: empty → SQL NULL; scalars wrapped as JSON text.
                    bound = normalize_sql_bind_value(v, t or "VARIANT", engine="snowflake")
                    converted.append(bound)
                else:
                    base = (t or "").split("(")[0].upper()
                    if base in {"BOOLEAN", "BOOL"}:
                        converted.append(
                            normalize_sql_bind_value(v, "BOOLEAN", engine="snowflake")
                        )
                    else:
                        converted.append(v)
            params.extend(converted)
            row_placeholders.append(f"({', '.join(['%s'] * len(target_cols))})")
        values_sql = ", ".join(row_placeholders)
        sql = (
            f"INSERT INTO {quote_sql_identifier(table_name)} ({col_list}) "  # nosec B608
            f"SELECT {select_sql} FROM VALUES {values_sql}"
        )
        cur.execute(sql, params)
        written += len(sub)
    return written


def _load_rows_into_table(
    cur: Any,
    table_name: str,
    target_cols: list[str],
    target_types: list[str],
    mapped_rows: list[tuple],
    *,
    prefer_copy: bool,
    conn: Any,
) -> str:
    """Load rows into ``table_name`` via COPY INTO when possible; else INSERT.

    Returns the load method used: ``copy_into`` or ``insert``.
    """
    # Bind once for all load paths (COPY, plain INSERT, JSON INSERT).
    mapped_rows = _bind_rows_for_snowflake(mapped_rows, target_types)
    total = len(mapped_rows)
    use_copy = (
        prefer_copy and total >= COPY_THRESHOLD and not _is_fakesnow_connection(conn)
    )
    if use_copy:
        fd, tmp_path = tempfile.mkstemp(
            suffix=".csv", prefix=f"df_sf_{table_name.lower()}_"
        )
        os.close(fd)
        tmp = Path(tmp_path)
        try:
            _write_temp_csv(tmp, target_cols, mapped_rows)
            written = _copy_into_table(
                cur, table_name, str(tmp.resolve()), target_cols, target_types
            )
            if written <= 0:
                written = total
            return "copy_into"
        finally:
            tmp.unlink(missing_ok=True)

    has_json = any(_is_json_type(t) for t in target_types)
    if has_json:
        # Already bound — _batch_insert_rows still stringifies VARIANT safely.
        _batch_insert_rows(cur, table_name, target_cols, target_types, mapped_rows)
    else:
        col_list = ", ".join(quote_sql_identifier(c) for c in target_cols)
        value_placeholders = ", ".join(["%s"] * len(target_cols))
        insert_sql = (
            f"INSERT INTO {quote_sql_identifier(table_name)} ({col_list}) "  # nosec B608
            f"VALUES ({value_placeholders})"
        )
        for offset in range(0, total, MAX_BIND_INSERT_ROWS):
            sub = mapped_rows[offset : offset + MAX_BIND_INSERT_ROWS]
            cur.executemany(insert_sql, sub)
    return "insert"


def _merge_batch_via_temp(
    cur: Any,
    table_name: str,
    target_cols: list[str],
    target_types: list[str],
    mapped_rows: list[tuple],
    conflict: list[str],
    *,
    prefer_copy: bool,
    conn: Any,
) -> int:
    """Stage the batch into a temp table, then run a single MERGE into the target."""
    if not mapped_rows:
        return 0
    temp = f"_DF_UPSERT_{uuid.uuid4().hex[:12]}"
    col_defs = ", ".join(
        f"{quote_sql_identifier(c)} {t}" for c, t in zip(target_cols, target_types)
    )
    cur.execute(f"CREATE TEMPORARY TABLE {quote_sql_identifier(temp)} ({col_defs})")
    try:
        _load_rows_into_table(
            cur,
            temp,
            target_cols,
            target_types,
            mapped_rows,
            prefer_copy=prefer_copy,
            conn=conn,
        )
        on_clause = null_safe_merge_on(
            conflict,
            left_alias="t",
            right_alias="s",
            quote_column=quote_sql_identifier,
        )
        col_list = ", ".join(quote_sql_identifier(c) for c in target_cols)
        source_cols = ", ".join(f"s.{quote_sql_identifier(c)}" for c in target_cols)
        update_cols = [c for c in target_cols if c not in conflict]
        lsn_guard = (
            f" AND {snowflake_lsn_match_predicate()}"
            if DF_LSN_COL in target_cols
            else ""
        )
        tgt_q = quote_sql_identifier(table_name)
        tmp_q = quote_sql_identifier(temp)
        if update_cols:
            set_clause = ", ".join(
                f"t.{quote_sql_identifier(c)} = s.{quote_sql_identifier(c)}"
                for c in update_cols
            )
            merge_sql = (
                f"MERGE INTO {tgt_q} t "  # nosec B608
                f"USING {tmp_q} s "
                f"ON {on_clause} "
                f"WHEN MATCHED{lsn_guard} THEN UPDATE SET {set_clause} "
                f"WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({source_cols})"
            )
        else:
            merge_sql = (
                f"MERGE INTO {tgt_q} t "
                f"USING {tmp_q} s "
                f"ON {on_clause} "
                f"WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({source_cols})"
            )
        cur.execute(merge_sql)
        return len(mapped_rows)
    finally:
        try:
            cur.execute(f'DROP TABLE IF EXISTS "{temp}"')
        except Exception as exc:
            logger.warning("Exception suppressed: %s", exc, exc_info=exc)


def _copy_into_table(
    cur,
    table_name: str,
    local_path: str,
    target_cols: list[str],
    target_types: list[str],
) -> int:
    # Use a unique stage per call so parallel threads/processes cannot overwrite
    # each other's staged files and load each other's data.
    stage_name = f"{table_name}_STAGE_{uuid.uuid4().hex}"
    stage_ref = f'@"{stage_name}"'
    try:
        cur.execute(f'CREATE TEMP STAGE IF NOT EXISTS "{stage_name}"')
        cur.execute(f"PUT file://{local_path} {stage_ref} AUTO_COMPRESS=TRUE")
        col_list = ", ".join(f'"{c}"' for c in target_cols)
        # VARIANT/JSON columns are parsed from the CSV string using PARSE_JSON($i);
        # all other columns are loaded directly by position.
        select_items = []
        for i, (c, t) in enumerate(zip(target_cols, target_types), start=1):
            if _is_json_type(t):
                select_items.append(f"PARSE_JSON(${i})")
            else:
                select_items.append(f"${i}")
        select_sql = ", ".join(select_items)
        cur.execute(
            f"""
            COPY INTO "{table_name}" ({col_list})
            FROM (SELECT {select_sql} FROM {stage_ref})
            FILE_FORMAT = (
                TYPE = CSV
                SKIP_HEADER = 1
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                NULL_IF = ('', 'NULL')
                ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
            )
            ON_ERROR = 'ABORT_STATEMENT'
            """  # nosec B608
        )
        rows = cur.fetchall()
        loaded = 0
        errors_seen = 0
        first_error = None
        for row in rows:
            if len(row) >= 4:
                loaded += int(row[3] or 0)
            if len(row) >= 6 and row[5]:
                errors_seen += int(row[5] or 0)
                if first_error is None and len(row) >= 7:
                    first_error = row[6]
        if errors_seen:
            raise RuntimeError(
                f"COPY INTO loaded {loaded} rows with {errors_seen} errors: {first_error or 'unknown'}"
            )
        return loaded
    finally:
        try:
            cur.execute(f'DROP STAGE IF EXISTS "{stage_name}"')
        except Exception as exc:
            logger.warning("Exception suppressed: %s", exc, exc_info=exc)


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
    connection: Any | None = None,
    close_connection: bool | None = None,
    skip_session_setup: bool = False,
    **_kwargs: Any,
) -> WriteResult:
    del port, ssl, _kwargs
    from connectors.writer_common import resolve_writer_backfill

    backfill_new_fields = resolve_writer_backfill(
        backfill_new_fields=backfill_new_fields,
        mappings=mappings,
    )
    # When a shared connection is passed (stream reuse), default to not closing it.
    if close_connection is None:
        close_connection = connection is None
    try:
        import snowflake.connector  # noqa: F401
    except ImportError:
        from connectors.driver_guard import require_driver

        if not stub_writes_allowed():
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=table_name,
                target_schema=schema or "PUBLIC",
                checksum="",
                chunks_completed=0,
                error=require_driver(
                    "snowflake.connector", "snowflake-connector-python"
                ),
                driver="none",
            )
        if not create_table:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=table_name,
                target_schema=schema or "PUBLIC",
                checksum="",
                chunks_completed=0,
                error="Snowflake table may not exist and create_table is disabled",
                driver="none",
            )
        rows, checksum, chunks = simulate_stub_write(
            data_rows=data_rows,
            table_name=table_name,
            target_schema=schema or "PUBLIC",
            on_checkpoint=on_checkpoint,
        )
        return WriteResult(
            ok=True,
            rows_written=rows,
            table_name=table_name,
            target_schema=schema or "PUBLIC",
            checksum=checksum,
            chunks_completed=chunks,
            driver="stub",
            load_method="stub",
        )

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
            target_schema=schema or "PUBLIC",
            checksum="",
            chunks_completed=0,
            error="No column mappings",
        )

    schema = (schema or "PUBLIC").upper()
    # Sanitize only here; after USE SCHEMA we resolve against information_schema
    # so legacy quoted-lowercase tables (e.g. "csvtestfile") are reused instead of
    # creating a parallel "CSVTESTFILE".
    table_name = sanitize_identifier(table_name)
    target_types = [sf_type(t) for t in logical_types]
    dest_types = {target_cols[i]: logical_types[i] for i in range(len(target_cols))}
    account = normalize_account(host)
    policy = transform_error_policy(error_policy)

    mapped_rows, transform_errors, rejected_details = build_mapped_rows_with_details(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=target_cols,
        column_types=column_types,
        dest_types=dest_types,
        error_policy=policy,
    )

    # Size Snowflake NUMBER columns from the actual batch data.  Prefer
    # integer capacity over fractional digits so NUMBER(38,s) never under-fits.
    target_types = [
        _snowflake_decimal_type(i, mapped_rows)
        if normalize_logical_type(t) == "decimal"
        else sf_type(t)
        for i, t in enumerate(logical_types)
    ]
    mapped_rows = _quarantine_unfit_decimals(
        mapped_rows, target_cols, target_types, rejected_details, policy
    )
    from connectors.writer_common import (
        normalize_temporal_cells,
        quarantine_currency_markers_into_numeric,
        quarantine_unfit_arrays,
        quarantine_unfit_binaries,
        quarantine_unfit_bitstrings,
        quarantine_unfit_booleans,
        quarantine_unfit_enum_set,
        quarantine_unfit_integers,
        quarantine_unfit_json,
        quarantine_unfit_specialty_types,
        quarantine_unfit_strings,
        quarantine_unfit_temporals,
        quarantine_unfit_years,
    )

    mapped_rows = quarantine_currency_markers_into_numeric(
        mapped_rows, target_cols, target_types, rejected_details, policy
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
        dialect_label="Snowflake INTEGER",
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
        dialect_label="Snowflake BINARY",
    )
    mapped_rows = quarantine_unfit_enum_set(
        mapped_rows, target_cols, target_types, rejected_details, policy
    )
    mapped_rows = quarantine_unfit_strings(
        mapped_rows,
        target_cols,
        target_types,
        rejected_details,
        policy,
        dialect_label="Snowflake VARCHAR",
    )
    mapped_rows = quarantine_unfit_arrays(
        mapped_rows,
        target_cols,
        target_types,
        rejected_details,
        policy,
        dialect_label="Snowflake",
    )
    mapped_rows = quarantine_unfit_json(
        mapped_rows,
        target_cols,
        target_types,
        rejected_details,
        policy,
        dialect_label="Snowflake VARIANT",
    )
    # Destination-native temporal normalize (ISO-Z → TIMESTAMP_NTZ wall clock).
    mapped_rows = normalize_temporal_cells(
        mapped_rows, target_types, target_cols, engine="snowflake"
    )

    sparse_rows: list[tuple] = []
    rows_for_checksum: list[tuple] = list(mapped_rows)
    # Within a single batch, the last occurrence of an upsert key wins.
    if write_mode == "upsert" and conflict_columns:
        mapped_rows, sparse_rows = split_dense_sparse_rows(mapped_rows)
        if DF_LSN_COL in target_cols:
            mapped_rows = dedupe_rows_by_pk_and_lsn(
                mapped_rows, conflict_columns, target_cols
            )
        else:
            mapped_rows = dedupe_rows(mapped_rows, conflict_columns, target_cols)

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
            warnings=transform_errors,
            rejected_details=rejected_details,
        )

    # Never stub local/fakesnow accounts when snowflake.connector is installed —
    # stub writes skip real load and break strict reconciliation (no read-back).
    if stub_writes_allowed() and not _is_local_account(str(account or "")):
        if not create_table:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=table_name,
                target_schema=schema,
                checksum="",
                chunks_completed=0,
                error="Snowflake table may not exist and create_table is disabled",
                rejected_rows=rejected_rows,
                warnings=transform_errors,
                rejected_details=rejected_details,
                coerced_null_rows=coerced_null_rows,
            )
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
            coerced_null_rows=coerced_null_rows,
        )

    conn = connection
    written = 0
    rows_skipped = 0
    load_method = "insert"
    chunks = 1
    try:
        if conn is None:
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

        with conn.cursor() as cur:
            if not skip_session_setup:
                if warehouse:
                    try:
                        wh_q = quote_sql_identifier(
                            sanitize_identifier(warehouse, preserve_case=True)
                        )
                        cur.execute(f"USE WAREHOUSE {wh_q}")
                    except Exception:
                        # fakesnow and some local mocks do not support USE WAREHOUSE.
                        logger.debug(
                            "USE WAREHOUSE skipped (driver/mock limitation)",
                            exc_info=True,
                        )
                if database:
                    # The built-in SNOWFLAKE database is read-only and cannot be written.
                    if database.upper() == "SNOWFLAKE":
                        raise RuntimeError(
                            "The SNOWFLAKE database is read-only system data. "
                            "Please specify a user database (for example, DATAFLOW) in the connector."
                        )
                    db_q = quote_sql_identifier(
                        sanitize_identifier(database, preserve_case=True)
                    )
                    if create_table:
                        cur.execute(f"CREATE DATABASE IF NOT EXISTS {db_q}")
                    cur.execute(f"USE DATABASE {db_q}")
                sch_q = quote_sql_identifier(
                    sanitize_identifier(schema, preserve_case=True)
                )
                if create_table:
                    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {sch_q}")
                cur.execute(f"USE SCHEMA {sch_q}")

            # Bind to the real stored table name (case) before DDL/DML.
            from connectors.sql_identifiers import snowflake_fold_identifier

            found = resolve_snowflake_table_name(cur, schema, table_name)
            if found is None and not create_table:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=table_name,
                    target_schema=schema,
                    checksum="",
                    chunks_completed=0,
                    error=(
                        f"Snowflake table {table_name!r} is missing and "
                        "create_table is disabled"
                    ),
                )
            table_name = (
                found if found is not None else snowflake_fold_identifier(table_name)
            )

            if create_table:
                col_defs = ", ".join(
                    f"{quote_sql_identifier(c)} {t}"
                    for c, t in zip(target_cols, target_types)
                )
                cur.execute(
                    f"CREATE TABLE IF NOT EXISTS {quote_sql_identifier(table_name)} ({col_defs})"
                )

            # Later stream chunks may need wider NUMBER than the first CREATE.
            _widen_existing_number_columns(
                cur, schema, table_name, target_cols, target_types
            )

            if backfill_new_fields:
                cur.execute(
                    """SELECT COLUMN_NAME FROM information_schema.columns
                       WHERE UPPER(table_schema) = UPPER(%s) AND table_name = %s""",
                    (schema, table_name),
                )
                existing = {row[0].upper() for row in cur.fetchall()}
                tbl_q = quote_sql_identifier(table_name)
                for col, typ in zip(target_cols, target_types):
                    if col.upper() not in existing:
                        col_q = quote_sql_identifier(col)
                        cur.execute(f"ALTER TABLE {tbl_q} ADD COLUMN {col_q} {typ}")

            total = len(mapped_rows)
            target_cols_lower = {t.lower(): t for t in target_cols}
            conflict = [
                target_cols_lower[c.lower()]
                for c in (conflict_columns or [])
                if c.lower() in target_cols_lower
            ]
            if write_mode == "upsert" and conflict:
                load_method = "merge_batch"
                written = 0
                if sparse_rows:
                    from connectors.writer_common import row_has_missing_sentinel

                    sparse_written, sparse_skipped, sparse_checksum = (
                        _sf_apply_sparse_upsert(
                            cur,
                            table_name,
                            target_cols,
                            target_types,
                            conflict,
                            sparse_rows,
                        )
                    )
                    written += sparse_written
                    rows_skipped += sparse_skipped
                    rows_for_checksum = [
                        r for r in rows_for_checksum if not row_has_missing_sentinel(r)
                    ] + list(sparse_checksum)
                # Stage once (COPY when large enough) and MERGE the dense batch.
                written += _merge_batch_via_temp(
                    cur,
                    table_name,
                    target_cols,
                    target_types,
                    mapped_rows,
                    conflict,
                    prefer_copy=True,
                    conn=conn,
                )
                if on_checkpoint:
                    on_checkpoint(1, 1, written)
            else:
                # Prefer COPY INTO for insert / full_refresh when the batch is large enough.
                # fakesnow does not support PUT/COPY — falls back to INSERT.
                load_method = _load_rows_into_table(
                    cur,
                    table_name,
                    target_cols,
                    target_types,
                    mapped_rows,
                    prefer_copy=True,
                    conn=conn,
                )
                written = total
                if on_checkpoint:
                    on_checkpoint(1, 1, written)

        return WriteResult(
            ok=True,
            rows_written=written,
            table_name=table_name,
            target_schema=schema,
            checksum=row_checksum(
                rows_for_checksum,
                target_cols,
                dest_db_type="snowflake",
                dest_types={c: target_types[i] for i, c in enumerate(target_cols)}
                if target_types
                else None,
            ),
            chunks_completed=chunks,
            rejected_rows=max(rejected_rows, len(data_rows) - written - rows_skipped),
            warnings=transform_errors,
            rejected_details=rejected_details,
            coerced_null_rows=coerced_null_rows,
            load_method=load_method,
            rows_skipped=rows_skipped,
        )
    except Exception as exc:
        return WriteResult(
            ok=False,
            rows_written=written,
            table_name=table_name,
            target_schema=schema,
            checksum=row_checksum(
                rows_for_checksum[:written] if written else [],
                target_cols,
                dest_db_type="snowflake",
                dest_types={c: target_types[i] for i, c in enumerate(target_cols)}
                if target_types
                else None,
            )
            if written
            else "",
            chunks_completed=chunks if written else 0,
            error=_format_write_error(exc),
            rejected_rows=rejected_rows,
            warnings=transform_errors,
            rejected_details=rejected_details,
            coerced_null_rows=coerced_null_rows,
            load_method=load_method,
        )
    finally:
        if close_connection and conn is not None:
            try:
                conn.close()
            except Exception as exc:
                logger.warning("Exception suppressed: %s", exc, exc_info=exc)
