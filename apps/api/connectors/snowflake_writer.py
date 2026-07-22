"""Snowflake bulk writer — COPY INTO staging for scale + batched INSERT fallback."""

from __future__ import annotations

import csv
import logging
import os
import re
import tempfile
import uuid
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, Overflow
from pathlib import Path
from typing import Any, Callable

logger = logging.getLogger(__name__)

from connectors.driver_guard import stub_writes_allowed
from connectors.snowflake_conn import _is_local_account, get_connection, normalize_account
from connectors.stub_writer import simulate_stub_write
from connectors.writer_common import (
    CHUNK_SIZE,
    DF_LSN_COL,
    _coerced_null_row_count,
    _rejected_row_count,
    build_mapped_rows_with_details,
    dedupe_rows,
    dedupe_rows_by_pk_and_lsn,
    resolve_target_columns,
    row_checksum,
    sample_values_by_source_from_batch,
    quote_sql_identifier,
    sanitize_identifier,
    snowflake_lsn_match_predicate,
    transform_error_policy,
)
from services.value_serializer import cell_to_string
from connectors.writer_common import (
    WriteResult as _WriteResult,
)
from services.type_system import ddl_type, normalize_logical_type

# Prefer COPY INTO for modest stream batches — 2000 was too high when wide Mongo
# rows shrink stream chunks below the threshold and force slow INSERT loops.
COPY_THRESHOLD = int(os.getenv("DATAFLOW_SNOWFLAKE_COPY_THRESHOLD", "200"))
MAX_BIND_INSERT_ROWS = int(os.getenv("DATAFLOW_SF_BIND_INSERT_ROWS", "1000"))
_NUMBER_TYPE_RE = re.compile(r"^NUMBER\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)$", re.I)


@dataclass
class WriteResult(_WriteResult):
    driver: str = "snowflake-connector-python"
    load_method: str = "insert"


def sf_type(inferred: str) -> str:
    return ddl_type("snowflake", inferred)


def _is_fakesnow_connection(conn: Any) -> bool:
    """Return True for the local fakesnow emulator — it does not support PUT/COPY."""
    return getattr(conn, "__class__", None) is not None and conn.__class__.__name__ == "FakeSnowflakeConnection"


def _parse_number_type(sf_type_str: str) -> tuple[int, int] | None:
    m = _NUMBER_TYPE_RE.match((sf_type_str or "").strip())
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def _decimal_scale_and_int_digits(value: Any) -> tuple[int, int]:
    """Return (integer_digits, fractional_scale) for a decimal cell value."""
    try:
        text = str(value).strip() if value is not None else ""
        if not text:
            return 0, 0
        d = Decimal(text)
        if not d.is_finite():
            return 0, 0
        _sign, digits, exponent = d.as_tuple()
        scale = -exponent if exponent < 0 else 0
        int_digits = max(0, len(digits) + exponent)
        return int_digits, scale
    except (InvalidOperation, Overflow, ValueError, TypeError):
        return 0, 0


def _fits_snowflake_number(value: Any, precision: int, scale: int) -> bool:
    """True if value can be stored in Snowflake NUMBER(precision, scale).

    Scale overflow is fail-closed (quarantine) — never silently quantize/round.
    """
    if value is None:
        return True
    try:
        d = Decimal(str(value).strip())
        if not d.is_finite():
            return False
        int_digits, value_scale = _decimal_scale_and_int_digits(d)
        # Do not silently round fractional digits into an existing NUMBER(p,s).
        if value_scale > scale:
            return False
        max_int = max(0, precision - scale)
        return int_digits <= max_int and value_scale <= scale
    except (InvalidOperation, Overflow, ValueError, TypeError):
        return False


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
    """NULL cells that cannot fit their NUMBER(p,s); never abort the whole load."""
    if policy == "fail":
        return mapped_rows
    number_cols: list[tuple[int, int, int]] = []
    for i, typ in enumerate(target_types):
        parsed = _parse_number_type(typ)
        if parsed:
            number_cols.append((i, parsed[0], parsed[1]))
    if not number_cols:
        return mapped_rows

    out: list[tuple] = []
    for row_idx, row in enumerate(mapped_rows):
        cells = list(row)
        changed = False
        for col_idx, precision, scale in number_cols:
            if col_idx >= len(cells) or cells[col_idx] is None:
                continue
            if _fits_snowflake_number(cells[col_idx], precision, scale):
                continue
            sample = cell_to_string(cells[col_idx])[:120]
            rejected_details.append({
                "row": row_idx + 1,
                "column": target_cols[col_idx],
                "target": target_cols[col_idx],
                "value": sample,
                "reason": (
                    f"decimal does not fit Snowflake NUMBER({precision},{scale}) "
                    "— quarantined (would raise decimal.Overflow)"
                ),
                "policy": "write_quarantine",
                "chars": [],
            })
            cells[col_idx] = None
            changed = True
        out.append(tuple(cells) if changed else row)
    return out


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
    except Exception:
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
        except Exception:
            pass


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


def _write_temp_csv(path: Path, target_cols: list[str], mapped_rows: list[tuple]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, quoting=csv.QUOTE_ALL)
        writer.writerow(target_cols)
        for row in mapped_rows:
            # Temporal cells are already warehouse-normalized strings; other
            # types still go through cell_to_string for CSV safety.
            writer.writerow(["" if v is None else (v if isinstance(v, str) else cell_to_string(v)) for v in row])


def _is_json_type(sf_type: str) -> bool:
    return sf_type and sf_type.split("(")[0].upper() in {"VARIANT", "JSON", "OBJECT", "ARRAY"}


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
        for row in sub:
            converted: list[Any] = []
            for v, t in zip(row, target_types):
                if _is_json_type(t):
                    # JSON-typed cells must be valid JSON strings (or SQL NULL).
                    # cell_to_string(None) would produce '', which PARSE_JSON('')
                    # treats as NULL in real Snowflake but errors in DuckDB/fakesnow.
                    s = cell_to_string(v)
                    converted.append(None if s == "" else s)
                else:
                    converted.append(v)
            params.extend(converted)
            row_placeholders.append(f"({', '.join(['%s'] * len(target_cols))})")
        values_sql = ", ".join(row_placeholders)
        sql = (
            f"INSERT INTO {quote_sql_identifier(table_name)} ({col_list}) "
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
    total = len(mapped_rows)
    use_copy = (
        prefer_copy
        and total >= COPY_THRESHOLD
        and not _is_fakesnow_connection(conn)
    )
    if use_copy:
        fd, tmp_path = tempfile.mkstemp(suffix=".csv", prefix=f"df_sf_{table_name.lower()}_")
        os.close(fd)
        tmp = Path(tmp_path)
        try:
            _write_temp_csv(tmp, target_cols, mapped_rows)
            written = _copy_into_table(cur, table_name, str(tmp.resolve()), target_cols, target_types)
            if written <= 0:
                written = total
            return "copy_into"
        finally:
            tmp.unlink(missing_ok=True)

    has_json = any(_is_json_type(t) for t in target_types)
    if has_json:
        _batch_insert_rows(cur, table_name, target_cols, target_types, mapped_rows)
    else:
        col_list = ", ".join(quote_sql_identifier(c) for c in target_cols)
        value_placeholders = ", ".join(["%s"] * len(target_cols))
        insert_sql = (
            f"INSERT INTO {quote_sql_identifier(table_name)} ({col_list}) "
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
            cur, temp, target_cols, target_types, mapped_rows,
            prefer_copy=prefer_copy, conn=conn,
        )
        on_clause = " AND ".join(
            f"t.{quote_sql_identifier(c)} = s.{quote_sql_identifier(c)}" for c in conflict
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
                f"t.{quote_sql_identifier(c)} = s.{quote_sql_identifier(c)}" for c in update_cols
            )
            merge_sql = (
                f"MERGE INTO {tgt_q} t "
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
        except Exception:
            pass


def _copy_into_table(cur, table_name: str, local_path: str, target_cols: list[str], target_types: list[str]) -> int:
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
                ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
            )
            ON_ERROR = 'CONTINUE'
            """
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
            raise RuntimeError(f"COPY INTO loaded {loaded} rows with {errors_seen} errors: {first_error or 'unknown'}")
        return loaded
    finally:
        try:
            cur.execute(f'DROP STAGE IF EXISTS "{stage_name}"')
        except Exception:
            pass


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

    batch_samples = sample_values_by_source_from_batch(headers, data_rows, mappings)
    target_cols, logical_types = resolve_target_columns(
        mappings,
        column_types,
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
    # Destination-native temporal normalize (ISO-Z → TIMESTAMP_NTZ wall clock).
    from connectors.writer_common import normalize_temporal_cells

    mapped_rows = normalize_temporal_cells(
        mapped_rows, target_types, target_cols, engine="snowflake"
    )

    # Within a single batch, the last occurrence of an upsert key wins.
    if write_mode == "upsert" and conflict_columns:
        if DF_LSN_COL in target_cols:
            mapped_rows = dedupe_rows_by_pk_and_lsn(
                mapped_rows, conflict_columns, target_cols
            )
        else:
            mapped_rows = dedupe_rows(mapped_rows, conflict_columns, target_cols)

    rejected_rows = _rejected_row_count(data_rows, mapped_rows, rejected_details, policy)
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
                        wh_q = quote_sql_identifier(sanitize_identifier(warehouse, preserve_case=True))
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
                    db_q = quote_sql_identifier(sanitize_identifier(database, preserve_case=True))
                    cur.execute(f"CREATE DATABASE IF NOT EXISTS {db_q}")
                    cur.execute(f"USE DATABASE {db_q}")
                sch_q = quote_sql_identifier(sanitize_identifier(schema, preserve_case=True))
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {sch_q}")
                cur.execute(f"USE SCHEMA {sch_q}")

            # Bind to the real stored table name (case) before DDL/DML.
            from connectors.snowflake_conn import resolve_snowflake_table_name
            from connectors.sql_identifiers import snowflake_fold_identifier

            found = resolve_snowflake_table_name(cur, schema, table_name)
            table_name = found if found is not None else snowflake_fold_identifier(table_name)

            if create_table:
                col_defs = ", ".join(
                    f"{quote_sql_identifier(c)} {t}" for c, t in zip(target_cols, target_types)
                )
                cur.execute(
                    f"CREATE TABLE IF NOT EXISTS {quote_sql_identifier(table_name)} ({col_defs})"
                )

            # Later stream chunks may need wider NUMBER than the first CREATE.
            _widen_existing_number_columns(cur, schema, table_name, target_cols, target_types)

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
            conflict = [c for c in (conflict_columns or []) if c in target_cols]
            if write_mode == "upsert" and conflict:
                load_method = "merge_batch"
                # Stage once (COPY when large enough) and MERGE the whole batch.
                written = _merge_batch_via_temp(
                    cur, table_name, target_cols, target_types, mapped_rows, conflict,
                    prefer_copy=True, conn=conn,
                )
                if on_checkpoint:
                    on_checkpoint(1, 1, written)
            else:
                # Prefer COPY INTO for insert / full_refresh when the batch is large enough.
                # fakesnow does not support PUT/COPY — falls back to INSERT.
                load_method = _load_rows_into_table(
                    cur, table_name, target_cols, target_types, mapped_rows,
                    prefer_copy=True, conn=conn,
                )
                written = total
                if on_checkpoint:
                    on_checkpoint(1, 1, written)

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
            coerced_null_rows=coerced_null_rows,
            load_method=load_method,
        )
    except Exception as exc:
        return WriteResult(
            ok=False,
            rows_written=written,
            table_name=table_name,
            target_schema=schema,
            checksum=row_checksum(mapped_rows[:written], target_cols) if written else "",
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
            except Exception:
                pass
