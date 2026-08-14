"""MySQL bulk writer — batched INSERT with checkpoint callbacks."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from services import reflection_cache
from services.decision_kernel import ddl_type, materialize_dest_ddl

from connectors.mysql_conn import get_connection
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
    build_mapped_rows_with_details,
    flush_normalized_child_batches,
    dedupe_rows,
    dedupe_rows_by_pk_and_lsn,
    filter_stale_lsn_rows,
    quarantine_currency_markers_into_numeric,
    quarantine_unfit_binaries,
    quarantine_unfit_bitstrings,
    quarantine_unfit_booleans,
    quarantine_unfit_decimals,
    quarantine_unfit_enum_set,
    quarantine_unfit_integers,
    quarantine_unfit_json,
    quarantine_unfit_specialty_types,
    quarantine_unfit_strings,
    quarantine_unfit_temporals,
    quarantine_unfit_years,
    bind_sql_mapped_rows_with_quarantine,
    quote_sql_identifier,
    resolve_conflict_targets,
    resolve_target_columns,
    row_checksum,
    reject_on_strict_policy,
    sanitize_identifier,
    transform_error_policy,
)
from connectors.writer_common import (
    WriteResult as _WriteResult,
)

logger = logging.getLogger(__name__)


@dataclass
class WriteResult(_WriteResult):
    driver: str = "pymysql"


def mysql_type(inferred: str) -> str:
    return materialize_dest_ddl("mysql", inferred)


def _fetch_mysql_column_types(
    cursor: Any,
    table_name: str,
    identity: str = "",
    required_columns: list[str] | None = None,
) -> dict[str, str]:
    """Return physical ``COLUMN_TYPE`` for an existing table (empty if missing).

    Cached per table when ``identity`` is supplied. This runs once per write
    chunk, so on a large load it was thousands of identical
    ``INFORMATION_SCHEMA.COLUMNS`` scans describing a table that only changes
    when the drift path below alters it — and that path invalidates.

    An empty result is never cached. Empty means "table missing or not
    readable", which is precisely the answer that goes stale the moment the
    ``CREATE TABLE IF NOT EXISTS`` above succeeds.

    ``required_columns`` are the mapped targets whose absence would *refuse*
    the write. The cache is only coherent with DDL this process ran, so an
    operator who recreated or altered the destination between runs would be
    told "physical DDL missing for mapped column(s) …" about a column that is
    there — an unactionable error against a stale answer. A cached answer that
    fails to cover them is therefore re-read from the catalog before it can
    block anything.
    """

    def _load() -> dict[str, str]:
        try:
            cursor.execute(
                "SELECT COLUMN_NAME, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s",
                (table_name,),
            )
            return {str(name): str(ctype or "") for name, ctype in cursor.fetchall()}
        except Exception:
            return {}

    if not identity:
        return _load()

    cached = reflection_cache.peek_by_identity(identity, "", table_name, "mysql_col_types")
    if cached and _covers_columns(dict(cached), required_columns):
        return dict(cached)
    if cached:
        reflection_cache.invalidate_by_identity(identity, "", table_name)
    fresh = _load()
    if fresh:
        reflection_cache.put_by_identity(identity, "", table_name, "mysql_col_types", fresh)
    return fresh


def _covers_columns(physical: dict[str, str], columns: list[str] | None) -> bool:
    """True when every requested column has a physical type (any case)."""
    for col in columns or []:
        name = str(col)
        if not (
            physical.get(name) or physical.get(name.lower()) or physical.get(name.upper())
        ):
            return False
    return True


def _apply_physical_temporal_types(
    target_cols: list[str],
    target_types: list[str],
    physical: dict[str, str],
) -> list[str]:
    """Prefer live MySQL column types for temporal coercion.

    Mapping/schema may label a TIMESTAMP source as TEXT after cell serialize, while
    the destination column is DATETIME — without this, ISO ``…T…Z`` literals hit
    MySQL 1292 Incorrect datetime value.
    """
    from connectors.sql_temporal import is_temporal_ddl, sql_base_type

    if not physical:
        return target_types
    out = list(target_types)
    for i, col in enumerate(target_cols):
        phys = physical.get(col) or physical.get(col.lower()) or physical.get(col.upper())
        if not phys:
            continue
        base = sql_base_type(phys)
        if is_temporal_ddl(base) or base in {
            "DATETIME", "TIMESTAMP", "DATE", "TIME", "YEAR",
        }:
            out[i] = phys
    return out


def _to_mysql_value(value: Any, source_type: str) -> Any:
    """Normalize transform-engine values to forms pymysql/MySQL can bind."""
    from connectors.sql_bind import normalize_sql_bind_value
    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        return value
    return normalize_sql_bind_value(value, source_type, engine="mysql")


def _mysql_apply_sparse_upsert(
    cursor: Any,
    *,
    table_q: str,
    target_cols: list[str],
    conflict_columns: list[str],
    sparse_rows: list[tuple],
    rejected_details: list[dict[str, Any]] | None = None,
    policy: str = "quarantine",
) -> tuple[int, int, list[tuple]]:
    """Per-row upsert that omits DF_MISSING columns (never SET col=NULL for absent)."""
    from connectors.writer_common import resolve_conflict_targets, run_sparse_cdc_upsert

    conflict = resolve_conflict_targets(conflict_columns, target_cols, strict=True)
    if not conflict:
        raise ValueError("sparse MySQL upsert requires conflict_columns")
    select_sql = ", ".join(quote_sql_identifier(c, "`") for c in target_cols)
    where_sql = " AND ".join(
        f"{quote_sql_identifier(c, '`')}=%s" for c in conflict
    )

    def fetch_existing(pk_vals: list[Any]) -> tuple | None:
        cursor.execute(
            f"SELECT {select_sql} FROM {table_q} WHERE {where_sql}",  # nosec B608
            pk_vals,
        )
        return cursor.fetchone()

    def update_non_pk(non_pk: dict[str, Any], pk_vals: list[Any]) -> int:
        from services.mirror_engine import upsert_set_columns

        set_cols = upsert_set_columns(list(non_pk.keys()), [])
        if not set_cols:
            return 0
        set_sql = ", ".join(
            f"{quote_sql_identifier(c, '`')}=%s" for c in set_cols
        )
        cursor.execute(
            f"UPDATE {table_q} SET {set_sql} WHERE {where_sql}",  # nosec B608
            [non_pk[c] for c in set_cols] + pk_vals,
        )
        return int(cursor.rowcount or 0)

    def insert_present(present: dict[str, Any]) -> None:
        from services.mirror_engine import upsert_insert_columns

        cols = upsert_insert_columns(list(present.keys()))
        if not cols:
            return
        col_sql = ", ".join(quote_sql_identifier(c, "`") for c in cols)
        ph = ", ".join(["%s"] * len(cols))
        cursor.execute(
            f"INSERT INTO {table_q} ({col_sql}) VALUES ({ph})",  # nosec B608
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

def _open_mysql(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    connection_string: str,
    ssl: bool,
    purpose: str = "write",
):
    conn = get_connection(
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        connection_string=connection_string,
        ssl=ssl,
        purpose=purpose,
    )
    conn.autocommit = False
    return conn


def _mysql_insert_sql(
    *,
    table_q: str,
    target_cols: list[str],
    write_mode: str,
    conflict_columns: list[str] | None,
) -> str:
    """INSERT / ON DUPLICATE KEY SQL for the current mapped column list."""
    placeholders = ", ".join(["%s"] * len(target_cols))
    col_names = ", ".join(quote_sql_identifier(c, "`") for c in target_cols)
    if write_mode == "upsert" and conflict_columns:
        conflict = [c for c in conflict_columns if c in target_cols]
        if conflict:
            from services.mirror_engine import upsert_set_columns

            update_cols = upsert_set_columns(target_cols, conflict)
            if update_cols:
                if DF_LSN_COL in target_cols:
                    from connectors.writer_common import mysql_lsn_values_newer_sql

                    newer = mysql_lsn_values_newer_sql(DF_LSN_COL, quote="`")
                    updates = ", ".join(
                        f"{quote_sql_identifier(c, '`')}=IF({newer}, VALUES({quote_sql_identifier(c, '`')}), {quote_sql_identifier(c, '`')})"
                        for c in update_cols
                    )
                else:
                    updates = ", ".join(
                        f"{quote_sql_identifier(c, '`')}=VALUES({quote_sql_identifier(c, '`')})"
                        for c in update_cols
                    )
                return (
                    f"INSERT INTO {table_q} ({col_names}) VALUES ({placeholders}) "  # nosec B608
                    f"ON DUPLICATE KEY UPDATE {updates}"
                )
            return (
                f"INSERT IGNORE INTO {table_q} ({col_names}) VALUES ({placeholders})"
            )
    return f"INSERT INTO {table_q} ({col_names}) VALUES ({placeholders})"  # nosec B608


@dataclass
class _MysqlMaterializedBatch:
    mapped_rows: list[tuple]
    sparse_rows: list[tuple]
    transform_errors: list[str]
    rejected_details: list
    target_types: list[str]
    rows_for_checksum: list[tuple]


def _mysql_materialize_mapped_batch(
    *,
    headers: list[str],
    data_rows: list,
    mappings: list,
    target_cols: list[str],
    column_types: dict[str, str] | None,
    dest_types: dict[str, str],
    logical_types: list[str],
    policy: Any,
    conflict_columns: list[str] | None,
    write_mode: str,
    destination_pk_columns: list[str] | None = None,
    destination_column_nullability: Any = None,
    allow_logical_fallback: bool = True,
    empty_cells_as_null: bool = False,
) -> _MysqlMaterializedBatch:
    """Map + quarantine against ``dest_types`` (call again after live DDL overlay)."""
    target_types = []
    for i, c in enumerate(target_cols):
        carrier = str(dest_types.get(c) or "").strip()
        if not carrier and allow_logical_fallback:
            carrier = str(logical_types[i] if i < len(logical_types) else "").strip()
        target_types.append(mysql_type(carrier) if carrier else "")
    mapped_rows, transform_errors, rejected_details = build_mapped_rows_with_details(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=target_cols,
        column_types=column_types,
        dest_types=dest_types,
        error_policy=policy,
        preserve_case=True,
        dest_kind="mysql",
        destination_pk_columns=list(destination_pk_columns or []) or None,
        destination_column_nullability=destination_column_nullability,
        empty_cells_as_null=bool(empty_cells_as_null),
    )
    mapped_rows = quarantine_currency_markers_into_numeric(
        mapped_rows, target_cols, target_types, rejected_details, policy
    )
    mapped_rows = quarantine_unfit_decimals(
        mapped_rows,
        target_cols,
        target_types,
        rejected_details,
        policy,
        dialect_label="MySQL DECIMAL",
        dest_db="mysql",
    )
    mapped_rows = quarantine_unfit_years(
        mapped_rows, target_cols, target_types, rejected_details, policy
    )
    mapped_rows = quarantine_unfit_booleans(
        mapped_rows, target_cols, target_types, rejected_details, policy
    )
    mapped_rows = quarantine_unfit_temporals(
        mapped_rows,
        target_cols,
        target_types,
        rejected_details,
        policy,
        dest_db="mysql",
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
        dialect_label="MySQL INTEGER",
        dest_db="mysql",
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
        dialect_label="MySQL VARBINARY",
    )
    mapped_rows = quarantine_unfit_enum_set(
        mapped_rows, target_cols, logical_types, rejected_details, policy
    )
    mapped_rows = quarantine_unfit_strings(
        mapped_rows,
        target_cols,
        target_types,
        rejected_details,
        policy,
        dialect_label="MySQL VARCHAR",
        dest_db="mysql",
    )
    mapped_rows = quarantine_unfit_json(
        mapped_rows,
        target_cols,
        target_types,
        rejected_details,
        policy,
        dialect_label="MySQL JSON",
    )
    sparse_rows: list[tuple] = []
    rows_for_checksum: list[tuple] = list(mapped_rows)
    if write_mode == "upsert" and conflict_columns:
        from connectors.writer_common import split_dense_sparse_rows

        mapped_rows, sparse_rows = split_dense_sparse_rows(mapped_rows)
        if DF_LSN_COL in target_cols:
            mapped_rows = dedupe_rows_by_pk_and_lsn(
                mapped_rows, conflict_columns, target_cols
            )
        else:
            mapped_rows = dedupe_rows(mapped_rows, conflict_columns, target_cols)
    return _MysqlMaterializedBatch(
        mapped_rows=mapped_rows,
        sparse_rows=sparse_rows,
        transform_errors=list(transform_errors or []),
        rejected_details=rejected_details,
        target_types=target_types,
        rows_for_checksum=rows_for_checksum,
    )


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
    del schema
    from connectors.writer_common import resolve_writer_backfill

    backfill_new_fields = resolve_writer_backfill(
        backfill_new_fields=backfill_new_fields,
        mappings=mappings,
        schema_policy=_kwargs.get("schema_policy"),
    )
    try:
        import pymysql
    except ImportError:
        pymysql = None
    if pymysql is None:
        from connectors.driver_guard import require_driver, stub_writes_allowed
        from connectors.stub_writer import simulate_stub_write

        if not stub_writes_allowed():
            return WriteResult(
                ok=False, rows_written=0, table_name=table_name, target_schema=database,
                checksum="", chunks_completed=0,
                error=require_driver("pymysql"),
                driver="none",
            )
        rows, checksum, chunks = simulate_stub_write(
            data_rows=data_rows, table_name=table_name, target_schema=database,
            on_checkpoint=on_checkpoint,
        )
        return WriteResult(
            ok=True, rows_written=rows, table_name=table_name, target_schema=database,
            checksum=checksum, chunks_completed=chunks, driver="stub",
        )

    from connectors.writer_common import sample_values_by_source_from_batch

    batch_samples = sample_values_by_source_from_batch(headers, data_rows, mappings)
    target_cols, logical_types = resolve_target_columns(
        mappings,
        column_types,
        preserve_case=True,
        sample_values_by_source=batch_samples,
        table_exists=False if create_table else None,
        dest_db="mysql",
    )
    if not target_cols:
        return WriteResult(
            ok=False, rows_written=0, table_name=table_name, target_schema=database,
            checksum="", chunks_completed=0, error="No column mappings",
        )

    from connectors.writer_common import omit_generated_always_columns

    target_cols, logical_types, _, _ = omit_generated_always_columns(
        target_cols, logical_types, []
    )
    if not target_cols:
        return WriteResult(
            ok=False, rows_written=0, table_name=table_name, target_schema=database,
            checksum="", chunks_completed=0,
            error="All mapped columns are GENERATED ALWAYS — nothing to insert",
        )

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
                target_schema=database,
                checksum="",
                chunks_completed=0,
                error=str(exc),
            )

    table_name = sanitize_identifier(table_name, preserve_case=True)
    # Prefer Studio-probed live DDL over Map stamps (BOOLEAN→VARCHAR invent cliff).
    from connectors.writer_common import resolve_studio_or_map_dest_types

    live_dest = _kwargs.get("destination_column_types")
    dest_types, studio_err = resolve_studio_or_map_dest_types(
        target_cols,
        mappings,
        column_types,
        logical_types=logical_types,
        studio_types=live_dest if isinstance(live_dest, dict) else None,
        product="MySQL",
        dest_db="mysql",
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
    rows_for_checksum: list = []
    rejected_rows = 0
    coerced_null_rows = 0
    if not studio_err:
        # Map before opening a socket so public proxies are not idle during transform.
        _batch = _mysql_materialize_mapped_batch(
            headers=headers,
            data_rows=data_rows,
            mappings=mappings,
            target_cols=target_cols,
            column_types=column_types,
            dest_types=dest_types,
            logical_types=logical_types,
            policy=policy,
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
        rows_for_checksum = _batch.rows_for_checksum

        rejected_rows = _rejected_row_count(
            data_rows, mapped_rows, rejected_details, policy, sparse_rows=sparse_rows
        )
        coerced_null_rows = _coerced_null_row_count(rejected_details, policy)
        _map_abort = reject_on_strict_policy(
            policy, rejected_details, "MySQL", transform_errors
        )
        if _map_abort:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=table_name,
                target_schema=database,
                checksum="",
                chunks_completed=0,
                error=_map_abort
                or f"Transform errors: {'; '.join(transform_errors[:3])}",
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
    table_q = quote_sql_identifier(table_name, "`")
    insert_sql = _mysql_insert_sql(
        table_q=table_q,
        target_cols=target_cols,
        write_mode=write_mode,
        conflict_columns=conflict_columns,
    )

    proxy_dest = is_public_proxy_host(host) or is_public_proxy_host(connection_string)
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
    converted_rows: list[tuple] = []
    # Probed before CREATE so fail-closed overlay can distinguish create-new.
    table_existed = False
    additive_refuse: str | None = None

    # CREATE/ALTER opens with purpose="setup" (short lock wait) so contended
    # metadata locks fail fast. After setup we reconnect with purpose="write"
    # for the proxy-friendly INSERT budget. DROP uses purpose="ddl" in
    # table_manager separately.
    def _reconnect(*, purpose: str = "write"):
        nonlocal conn, cur
        close_quietly(conn)
        conn = _open_mysql(
            host=host, port=port, database=database,
            username=username, password=password,
            connection_string=connection_string, ssl=ssl,
            purpose=purpose,
        )
        cur = conn.cursor()

    _identity = reflection_cache.dsn_identity(
        driver="mysql",
        host=host,
        port=port,
        database=database,
        username=username,
        connection_string=connection_string,
    )

    def _run_setup(cursor) -> None:
        nonlocal target_types, converted_rows, dest_types, additive_refuse
        nonlocal mapped_rows, sparse_rows, transform_errors, rejected_details, rows_for_checksum
        nonlocal insert_sql
        if use_ledger:
            ensure_raw_write_ledger(cursor, dialect="mysql")
        if create_table:
            from services.identity_carry import dbapi_percent_fetchall
            from services.schema_fidelity import (
                empty_unsupported_report,
                plan_covers_unique,
                render_create_column_defs,
                resolve_create_fidelity_plan,
                settle_create_new_on_destination,
            )

            fidelity_plan = None
            try:
                fidelity_plan = resolve_create_fidelity_plan(
                    source_schema_catalog=_kwargs.get("source_schema_catalog"),
                    mappings=mappings,
                    target_columns=target_cols,
                    target_types=target_types,
                    dest_dialect="mysql",
                    table_already_exists=bool(table_existed),
                    dest_table=table_name,
                    dest_schema="",
                )
            except Exception as exc:  # noqa: BLE001 — planner failure is types-only + certificate
                logger.warning(
                    "MySQL schema fidelity plan failed (types-only CREATE): %s",
                    exc,
                )
                fidelity_plan = None
                _kwargs["_schema_fidelity_report"] = empty_unsupported_report(
                    source_dialect="",
                    dest_dialect="mysql",
                    reason=(
                        f"Schema fidelity planner raised ({type(exc).__name__}); "
                        "create-new emitted column types only."
                    ),
                ).to_dict()
            if fidelity_plan is not None:
                if fidelity_plan.column_renames and fidelity_plan.dest_columns:
                    target_cols[:] = list(fidelity_plan.dest_columns)
                    insert_sql = _mysql_insert_sql(
                        table_q=table_q,
                        target_cols=target_cols,
                        write_mode=write_mode,
                        conflict_columns=conflict_columns,
                    )
                col_defs = render_create_column_defs(
                    columns=target_cols,
                    types=target_types,
                    plan=(None if table_existed else fidelity_plan),
                    dialect="mysql",
                )
                if (
                    write_mode == "upsert"
                    and conflict_columns
                    and not table_existed
                    and not plan_covers_unique(fidelity_plan, list(conflict_columns))
                ):
                    conflict_cols = [c for c in conflict_columns if c in target_cols]
                    if conflict_cols:
                        index_name = sanitize_identifier(
                            f"uidx_{table_name}_{'_'.join(conflict_cols)}"
                        )
                        cols = ", ".join(
                            quote_sql_identifier(c, "`") for c in conflict_cols
                        )
                        col_defs += (
                            f", UNIQUE KEY {quote_sql_identifier(index_name, '`')} ({cols})"
                        )
                suffix = ""
                if not table_existed:
                    suffix = (fidelity_plan.create_suffix or "").strip()
                create_sql = f"CREATE TABLE IF NOT EXISTS {table_q} ({col_defs})"
                if suffix:
                    create_sql = f"{create_sql} {suffix}"
                cursor.execute(create_sql)
                _kwargs["_schema_fidelity_report"] = settle_create_new_on_destination(
                    fidelity_plan,
                    dest_dialect="mysql",
                    dest_schema="",
                    dest_table=table_name,
                    table_already_exists=bool(table_existed),
                    execute=cursor.execute,
                    fetchall=dbapi_percent_fetchall(cursor),
                    cursor=cursor,
                )
            else:
                col_defs = ", ".join(
                    f"{quote_sql_identifier(c, '`')} {t}"
                    for c, t in zip(target_cols, target_types)
                )
                if write_mode == "upsert" and conflict_columns:
                    conflict_cols = [c for c in conflict_columns if c in target_cols]
                    if conflict_cols:
                        index_name = sanitize_identifier(
                            f"uidx_{table_name}_{'_'.join(conflict_cols)}"
                        )
                        cols = ", ".join(
                            quote_sql_identifier(c, "`") for c in conflict_cols
                        )
                        col_defs += (
                            f", UNIQUE KEY {quote_sql_identifier(index_name, '`')} ({cols})"
                        )
                cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_q} ({col_defs})")
                _kwargs.setdefault(
                    "_schema_fidelity_report",
                    empty_unsupported_report(
                        source_dialect="",
                        dest_dialect="mysql",
                        reason=(
                            "Schema fidelity plan unavailable; "
                            "create-new emitted column types only."
                        ),
                    ).to_dict(),
                )
            # Empty physical-type answers are never cached (see
            # ``_fetch_mysql_column_types``), so a just-created table has no
            # stale entry to drop — only real ALTER paths invalidate.

        if backfill_new_fields:
            cursor.execute(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s",
                (table_name,),
            )
            existing = {row[0] for row in cursor.fetchall()}
            from connectors.writer_common import gate_additive_types_under_partial_studio

            target_types, add_err = gate_additive_types_under_partial_studio(
                target_cols=target_cols,
                target_types=target_types,
                existing=existing,
                mappings=mappings,
                studio_err=studio_err,
                product="MySQL",
                materialize_stamp=mysql_type,
                dest_db="mysql",
                column_types=column_types,
            )
            if add_err:
                additive_refuse = add_err
                return
            altered = False
            for col, typ in zip(target_cols, target_types):
                if col not in existing:
                    cursor.execute(
                        f"ALTER TABLE {table_q} ADD COLUMN {quote_sql_identifier(col, '`')} {typ}"
                    )
                    altered = True
            if altered:
                # Real shape change: any cached physical types describe the
                # table as it was before these columns existed.
                reflection_cache.invalidate_by_identity(_identity, "", table_name)

            # Map≡ALTER: source DDL may propose a wider type, but an explicit
            # Map target_type is a hard ceiling — never MODIFY past it.
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
                candidate_by_col[col] = mysql_type(source_type)

            desired_types, alter_refusals = desired_types_honoring_map_stamps(
                target_cols=target_cols,
                current_target_types=target_types,
                mappings=mappings,
                candidate_by_col=candidate_by_col,
            )
            if alter_refusals:
                logger.info(
                    "mysql Map≡ALTER refusals (stamp ceiling): %s",
                    alter_refusals,
                )

            suppressed_widens: dict[str, str] = {}
            widen_existing_columns_native(
                cursor,
                "mysql",
                database,
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
            reflection_cache.invalidate_by_identity(_identity, "", table_name)

        # Map VARCHAR + live DATE/INT/BOOL/JSON — shared overlay before bind refuse.
        # Empty physical on an existing table → fail closed (never invent NULL).
        from connectors.writer_common import (
            materialize_missing_as_null_for_dense_write,
            overlay_physical_bind_types,
            require_physical_types_for_existing_table,
        )

        physical = _fetch_mysql_column_types(
            cursor, table_name, identity=_identity, required_columns=list(target_cols)
        )
        overlay_err = require_physical_types_for_existing_table(
            table_existed=table_existed,
            physical=physical,
            dialect_label="MySQL",
            target_cols=target_cols,
        )
        if overlay_err:
            raise RuntimeError(overlay_err)
        if physical:
            from connectors.writer_common import rematerialize_live_dest_types

            live_dest_types = rematerialize_live_dest_types(
                physical, list(target_cols or []), product="MySQL"
            )
            if live_dest_types is None:
                raise RuntimeError(
                    "MySQL live DDL incomplete for mapped columns — refuse Map "
                    "VARCHAR rematerialize invent. Re-run destination schema "
                    "introspect and retry."
                )
            carriers_differ = any(
                str(dest_types.get(c) or "").strip().upper()
                != str(live_dest_types.get(c) or "").strip().upper()
                for c in target_cols
            )
            need_remap = carriers_differ or (bool(studio_err) and not mapped_rows)
            if need_remap:
                # Rematerialize from source against live DDL (no Map VARCHAR invent).
                dest_types = live_dest_types
                _batch = _mysql_materialize_mapped_batch(
                    headers=headers,
                    data_rows=data_rows,
                    mappings=mappings,
                    target_cols=target_cols,
                    column_types=column_types,
                    dest_types=dest_types,
                    logical_types=logical_types,
                    policy=policy,
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
                rows_for_checksum = _batch.rows_for_checksum
            else:
                target_types = overlay_physical_bind_types(
                    target_cols, target_types, physical
                )

        bound = bind_sql_mapped_rows_with_quarantine(
            mapped_rows,
            target_cols,
            target_types,
            rejected_details,
            policy,
            engine="mysql",
            dialect_label="MySQL",
            mappings=mappings,
        )
        converted_rows = materialize_missing_as_null_for_dense_write(bound)
        if sparse_rows:
            sparse_rows[:] = bind_sql_mapped_rows_with_quarantine(
                sparse_rows,
                target_cols,
                target_types,
                rejected_details,
                policy,
                engine="mysql",
                dialect_label="MySQL",
                mappings=mappings,
            )
        conn.commit()

    try:
        conn = _open_mysql(
            host=host, port=port, database=database,
            username=username, password=password,
            connection_string=connection_string, ssl=ssl,
            purpose="setup",
        )
        cur = conn.cursor()
        try:
            # Existence before CREATE — create-new may skip overlay require.
            table_existed = False
            try:
                cur.execute(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = DATABASE() AND table_name = %s "
                    "LIMIT 1",
                    (table_name,),
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
                    target_schema=database,
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
                except RuntimeError as setup_exc:
                    # Fail-closed physical overlay — do not reconnect-retry.
                    if "refuse silent Map VARCHAR bind" in str(setup_exc):
                        return WriteResult(
                            ok=False,
                            rows_written=0,
                            table_name=table_name,
                            target_schema=database,
                            checksum="",
                            chunks_completed=0,
                            error=str(setup_exc),
                            rejected_details=rejected_details,
                            warnings=transform_errors,
                        )
                    raise
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
                    _reconnect(purpose="setup")

            if additive_refuse:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=table_name,
                    target_schema=database,
                    checksum="",
                    chunks_completed=0,
                    error=additive_refuse,
                    rejected_details=rejected_details,
                    warnings=transform_errors,
                )

            # Setup used short lock waits; INSERT/UPSERT needs write I/O budget.
            _reconnect(purpose="write")

            # Defensive: if setup skipped conversion somehow, coerce with mapping types.
            if not converted_rows and mapped_rows:
                from connectors.writer_common import materialize_missing_as_null_for_dense_write

                bound = bind_sql_mapped_rows_with_quarantine(
                    mapped_rows,
                    target_cols,
                    target_types,
                    rejected_details,
                    policy,
                    engine="mysql",
                    dialect_label="MySQL",
                    mappings=mappings,
                )
                converted_rows = materialize_missing_as_null_for_dense_write(bound)

            _bind_abort = reject_on_strict_policy(
                policy, rejected_details, "MySQL", transform_errors
            )
            if _bind_abort:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=table_name,
                    target_schema=database,
                    checksum="",
                    chunks_completed=0,
                    error=_bind_abort,
                    rejected_rows=_rejected_row_count(
                        data_rows, converted_rows or mapped_rows, rejected_details, policy,
                        sparse_rows=sparse_rows,
                    ),
                    rejected_details=rejected_details,
                    warnings=transform_errors,
                )

            rows_skipped = 0
            sparse_written = 0
            sparse_checksum_rows: list[tuple] = []
            if sparse_rows and write_mode == "upsert" and conflict_columns:
                # Convert sparse with physical types but keep DF_MISSING intact.
                sparse_converted = [
                    tuple(_to_mysql_value(v, target_types[i]) for i, v in enumerate(row))
                    for row in sparse_rows
                ]
                sparse_written, sparse_skipped, sparse_checksum = (
                    _mysql_apply_sparse_upsert(
                        cur,
                        table_q=table_q,
                        target_cols=target_cols,
                        conflict_columns=conflict_columns,
                        sparse_rows=sparse_converted,
                        rejected_details=rejected_details,
                        policy=policy,
                    )
                )
                conn.commit()
                written += sparse_written
                rows_skipped += sparse_skipped
                sparse_checksum_rows = list(sparse_checksum)

            # Dense empty PK once before chunks — checksum + write + row-retry
            # share the same holdouts (never mass-touch on ON DUPLICATE).
            if write_mode == "upsert" and conflict_columns and converted_rows:
                from connectors.writer_common import partition_dense_upsert_rows

                conflict_cols = [c for c in conflict_columns if c in target_cols]
                if conflict_cols:
                    before = len(converted_rows)
                    converted_rows = partition_dense_upsert_rows(
                        converted_rows,
                        conflict_cols,
                        target_cols=target_cols,
                        rejected_details=rejected_details,
                        policy=policy,
                    )
                    rows_skipped += before - len(converted_rows)

            # Ack checksum must match rows that land (partitioned dense + sparse).
            rows_for_checksum = list(converted_rows) + sparse_checksum_rows
            # Rematerialize under studio_err fills converted_rows after early defer —
            # recompute chunk count so the write loop covers the full batch.
            total = len(converted_rows)
            chunks = max(1, (total + chunk_size - 1) // chunk_size) if total else 0
            rejected_rows = _rejected_row_count(
                data_rows,
                converted_rows or mapped_rows,
                rejected_details,
                policy,
                sparse_rows=sparse_rows,
            )
            coerced_null_rows = _coerced_null_row_count(rejected_details, policy)

            for chunk_idx in range(chunks):
                start = chunk_idx * chunk_size
                batch = converted_rows[start : start + chunk_size]
                if not batch:
                    break

                attempt = 0
                chunk_started = time.monotonic()
                chunk_written = 0
                write_batch = batch
                while True:
                    try:
                        if use_ledger:
                            already = raw_chunk_rows_written(
                                cur,
                                dialect="mysql",
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
                        write_batch = batch
                        if (
                            write_mode == "upsert"
                            and conflict_columns
                            and DF_LSN_COL in target_cols
                        ):
                            conflict_cols = [c for c in conflict_columns if c in target_cols]
                            write_batch, skipped = filter_stale_lsn_rows(
                                cur,
                                table_name,
                                None,
                                conflict_cols,
                                write_batch,
                                target_cols,
                                quote="`",
                                placeholder="%s",
                            )
                            rows_skipped += skipped
                        if write_batch:
                            cur.executemany(insert_sql, write_batch)
                        if use_ledger:
                            mark_raw_chunk_committed(
                                cur,
                                dialect="mysql",
                                job_id=job_id,
                                batch_key=write_batch_key,
                                chunk_idx=chunk_idx,
                                rows_written=len(write_batch),
                                row_start=start,
                                row_end=start + max(len(write_batch) - 1, 0),
                                attempt=1,
                            )
                        conn.commit()
                        chunk_written = len(write_batch)
                        break
                    except Exception as chunk_exc:
                        try:
                            conn.rollback()
                        except Exception as exc:
                            logger.warning("Exception suppressed: %s", exc, exc_info=exc)
                        # Bad cells: write row-by-row and quarantine failures so one
                        # Incorrect datetime cannot abort a 100k-row transfer.
                        # write_batch is empty-PK partitioned (pre-loop); never retry
                        # raw batch conflict keys that would ON DUPLICATE mass-touch.
                        if is_sql_data_error(chunk_exc) and policy in {"quarantine", "coerce_null"}:
                            for row_i, row in enumerate(write_batch):
                                try:
                                    cur.execute(insert_sql, row)
                                    conn.commit()
                                    chunk_written += 1
                                except Exception as row_exc:
                                    try:
                                        conn.rollback()
                                    except Exception as exc:
                                        logger.debug("Cleanup exception suppressed: %s", exc, exc_info=exc)
                                    if is_connection_lost(row_exc):
                                        raise
                                    source_row = start + row_i
                                    col_name = extract_column_from_sql_error(row_exc) or "*"
                                    sample_val = ""
                                    if col_name != "*" and col_name in target_cols:
                                        try:
                                            sample_val = str(row[target_cols.index(col_name)])[:120]
                                        except Exception:
                                            sample_val = ""
                                    from connectors.writer_common import (
                                        append_write_quarantine_detail,
                                    )

                                    append_write_quarantine_detail(
                                        rejected_details,
                                        {
                                            "row": source_row,
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
                            if use_ledger and chunk_written:
                                try:
                                    mark_raw_chunk_committed(
                                        cur,
                                        dialect="mysql",
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

                written += chunk_written
                chunks_completed = chunk_idx + 1
                if on_checkpoint:
                    on_checkpoint(chunks_completed, chunks, written)

            # Document → relational child fan-out (normalize/hybrid) after parent land.
            child_flush = flush_normalized_child_batches(
                headers=headers,
                data_rows=data_rows,
                mappings=mappings,
                dest_db="mysql",
                create_table=create_table,
                cursor=cur,
                quote="`",
                placeholder="%s",
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

        close_quietly(conn)
        if child_flush_error:
            return WriteResult(
                ok=False,
                rows_written=written,
                table_name=table_name,
                target_schema=database,
                checksum="",
                chunks_completed=chunks_completed or chunks,
                error=child_flush_error,
                rejected_rows=max(rejected_rows, len(data_rows) - written - rows_skipped),
                rejected_details=rejected_details,
                coerced_null_rows=coerced_null_rows,
                rows_skipped=rows_skipped,
                warnings=transform_errors,
            )
        _final_abort = reject_on_strict_policy(policy, rejected_details, "MySQL")
        if _final_abort:
            return WriteResult(
                ok=False,
                rows_written=written,
                table_name=table_name,
                target_schema=database,
                checksum="",
                chunks_completed=chunks_completed or chunks,
                error=_final_abort,
                rejected_rows=max(rejected_rows, len(data_rows) - written - rows_skipped),
                rejected_details=rejected_details,
                coerced_null_rows=coerced_null_rows,
                rows_skipped=rows_skipped,
                warnings=transform_errors,
            )
        return WriteResult(
            ok=True, rows_written=written, table_name=table_name, target_schema=database,
            checksum=row_checksum(
                rows_for_checksum if "rows_for_checksum" in locals() else (converted_rows or mapped_rows),
                target_cols,
                dest_db_type="mysql",
                dest_types={c: target_types[i] for i, c in enumerate(target_cols)},
            ),
            chunks_completed=chunks_completed or chunks,
            rejected_rows=max(rejected_rows, len(data_rows) - written - rows_skipped),
            rejected_details=rejected_details,
            coerced_null_rows=coerced_null_rows,
            rows_skipped=rows_skipped,
            warnings=transform_errors,
            meta=(
                {"schema_fidelity": _kwargs["_schema_fidelity_report"]}
                if isinstance(_kwargs.get("_schema_fidelity_report"), dict)
                else {}
            ),
        )
    except Exception as exc:
        close_quietly(conn)
        return WriteResult(
            ok=False,
            rows_written=written,
            table_name=table_name,
            target_schema=database,
            checksum=row_checksum(
                rows_for_checksum if "rows_for_checksum" in locals() else (converted_rows or mapped_rows),
                target_cols,
                dest_db_type="mysql",
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
