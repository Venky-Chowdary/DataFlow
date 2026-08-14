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

from services.decision_kernel import materialize_dest_ddl
from services.value_serializer import json_default

from connectors.sqlite_common import sqlite_file_path
from connectors.write_resilience import (
    ensure_raw_write_ledger,
    mark_raw_chunk_committed,
    raw_chunk_rows_written,
)
from connectors.writer_common import (
    reject_on_strict_policy,
    CHUNK_SIZE,
    _coerced_null_row_count,
    _rejected_row_count,
    apply_write_quarantine_matrix,
    bind_sql_mapped_rows_with_quarantine,
    build_mapped_rows_with_details,
    filter_stale_lsn_rows,
    gate8_writer_meta,
    quote_sql_identifier,
    resolve_conflict_targets,
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


def _sqlite_bind_carrier(map_carrier: str, physical_or_ddl: str = "") -> str:
    """Bind/quarantine carrier — never collapse Map DATETIME→TEXT affinity.

    SQLite stores DATETIME as TEXT/NUMERIC affinity (``sqlite_type`` / PRAGMA).
    Quarantine and temporal refuse (audit §2.7 TZ→NTZ) must see the Map/Studio
    temporal stamp; CREATE DDL still uses ``sqlite_type``.
    """
    from services.decision_kernel import normalize_logical_type

    map_c = (map_carrier or "").strip()
    phys = (physical_or_ddl or "").strip()
    map_logical = normalize_logical_type(map_c) if map_c else ""
    if map_logical in {"datetime", "date", "time"}:
        phys_u = phys.upper()
        # Vague SQLite affinities must not erase NTZ/TZ polarity for quarantine.
        if not phys or phys_u in {
            "TEXT",
            "NUMERIC",
            "INTEGER",
            "REAL",
            "BLOB",
            "ANY",
            "",
        }:
            return map_c
        phys_logical = normalize_logical_type(phys)
        if phys_logical in {"datetime", "date", "time"}:
            return phys
        return map_c
    return phys or map_c


def _to_sqlite_value(value: Any, source_type: str) -> Any:
    from services.value_serializer import is_missing_sentinel, safe_decimal_text

    # Sparse CDC: never coerce DF_MISSING → NULL (would wipe present destination cols).
    if is_missing_sentinel(value):
        return value
    if value is None:
        return None
    # SQLite has no Decimal affinity — always bind exact decimal text (currency /
    # DECIMAL / MONEY carriers and TEXT affinity after semantic currency normalize).
    if isinstance(value, Decimal):
        text = safe_decimal_text(value)
        if text is None:
            raise ValueError(
                f"SQLite refused non-finite Decimal {value!r} "
                "(refuse silent NULL / float invent)"
            )
        return text
    upper = source_type.upper()
    if upper in {
        "DECIMAL",
        "NUMERIC",
        "NUMBER",
        "MONEY",
        "SMALLMONEY",
        "BIGNUMERIC",
        "BIGDECIMAL",
        "CURRENCY",
        "DOUBLE",
        "REAL",
        "FLOAT",
    } or upper.startswith(("DECIMAL(", "NUMERIC(", "NUMBER(", "BIGNUMERIC(")):
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
        from connectors.sql_temporal import (
            coerce_sql_temporal,
            format_wire_value,
            input_has_timezone,
        )

        # NTZ carriers refuse Z/offset — never silent strip (audit §2.7).
        ntz = upper in {"DATETIME", "TIMESTAMP", "TIMESTAMP_NTZ"}
        if ntz and input_has_timezone(value):
            raise ValueError(
                f"SQLite {upper} refuses timezone-aware wire (would strip offset). "
                "Map to TIMESTAMPTZ or provide a naive wall-clock value."
            )

        try:
            coerced = coerce_sql_temporal(
                value, upper if upper != "TIMESTAMP_NTZ" else "TIMESTAMP"
            )
            wire = format_wire_value(
                value, upper if upper != "TIMESTAMP_NTZ" else "TIMESTAMP"
            )
        except ValueError:
            # Fail-closed at row bind — never invent NULL from empty temporal.
            raise
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
        from connectors.sql_bind import coerce_boolean_wire

        coerced = coerce_boolean_wire(value, as_int=True)
        if coerced is not None and coerced not in (0, 1):
            raise ValueError(
                f"SQLite BOOLEAN refused unrecognized value {value!r} "
                "(would invent non-canonical boolean integer)"
            )
        return coerced
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
        resolve_conflict_targets,
        sparse_present_bindings,
    )
    from services.cdc_effectively_once import should_apply_pk_row

    conflict = resolve_conflict_targets(conflict_columns, target_cols, strict=True)
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
    rejected_details: list[dict[str, Any]] | None = None,
    policy: str = "quarantine",
    row_offset: int = 0,
) -> tuple[int, int]:
    """Upsert with optional ``_df_lsn`` monotonic guard (at-least-once CDC).

    Prefetch existing LSNs for the batch, drop stale rows, then either use
    ``INSERT … ON CONFLICT DO UPDATE WHERE`` or delete+insert fallback. Returns
    (rows_written, rows_skipped) for accurate reconciliation accounting.
    """
    from connectors.writer_common import (
        DF_LSN_COL,
        dedupe_rows_by_pk_and_lsn,
        partition_dense_upsert_rows,
        sqlite_lsn_update_guard_sql,
    )

    rows = dedupe_rows_by_pk_and_lsn(batch, conflict_cols, target_cols)
    if not rows:
        return 0, 0

    before_pk = len(rows)
    rows = partition_dense_upsert_rows(
        rows,
        conflict_cols,
        target_cols=target_cols,
        rejected_details=rejected_details,
        policy=policy,
        row_offset=row_offset,
    )
    empty_pk_skipped = before_pk - len(rows)
    if not rows:
        return 0, empty_pk_skipped

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
        return 0, empty_pk_skipped + skipped

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
            return len(rows), empty_pk_skipped + skipped
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
    # Dedup may drop duplicates within the batch — count those as skipped too.
    dedup_skipped = original_count - len(rows) - skipped
    return len(rows), empty_pk_skipped + skipped + max(0, dedup_skipped)


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
        dest_db="sqlite",
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
                target_schema=schema or "main",
                checksum="",
                chunks_completed=0,
                error=str(exc),
            )

    table_name = sanitize_identifier(table_name, preserve_case=True)
    table_quoted = quote_sql_identifier(table_name)
    # Prefer Studio-probed live DDL over Map stamps (BOOLEAN→TEXT invent cliff).
    from connectors.writer_common import resolve_studio_or_map_dest_types

    live_dest = _kwargs.get("destination_column_types")
    dest_types, studio_err = resolve_studio_or_map_dest_types(
        target_cols,
        mappings,
        column_types,
        logical_types=logical_types,
        studio_types=live_dest if isinstance(live_dest, dict) else None,
        product="SQLite",
        dest_db="sqlite",
    )
    # ddl_types → CREATE/ALTER affinity; tgt_types → Map carriers for quarantine/bind
    # (DATETIME must not collapse to TEXT before TZ→NTZ refuse — audit §2.7).
    target_types: list[str] = []
    tgt_types: list[str] = []
    for i, c in enumerate(target_cols):
        carrier = str(dest_types.get(c) or "").strip()
        if not carrier and not studio_err:
            carrier = str(logical_types[i] if i < len(logical_types) else "").strip()
        bind_c = _sqlite_bind_carrier(carrier)
        tgt_types.append(bind_c)
        target_types.append(sqlite_type(bind_c) if bind_c else "")
    policy = transform_error_policy(error_policy)

    mapped_rows: list[tuple] = []
    converted_rows: list[tuple] = []
    chunks = 0
    written = 0
    rows_skipped = 0
    transform_errors: list[str] = []
    rejected_details: list[dict] = []
    rows_for_checksum: list[tuple] = []
    sparse_rows: list[tuple] = []
    conflict_cols = [c for c in (conflict_columns or []) if c in target_cols]

    try:
        # Probe before Map so create-new refuse / rematerialize win over
        # Map-blank invent under partial Studio (generic_sql / PG parity).
        from connectors.writer_common import (
            overlay_physical_bind_types,
            require_physical_types_for_existing_table,
        )

        table_existed = False
        physical: dict[str, str] = {}
        try:
            probe = sqlite3.connect(path, timeout=8)
            try:
                probe_cur = probe.cursor()
                probe_cur.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                    (table_name,),
                )
                table_existed = probe_cur.fetchone() is not None
                probe_cur.execute(f"PRAGMA table_info({table_quoted})")  # nosec B608
                physical = {
                    str(row[1]): str(row[2] or "")
                    for row in probe_cur.fetchall()
                    if row[1]
                }
            finally:
                probe.close()
        except Exception:
            logger.debug("sqlite physical column introspection failed", exc_info=True)
            table_existed = not create_table
            physical = {}

        # Create-new: partial Studio must not soft-bind Map VARCHAR.
        if not table_existed and studio_err:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=table_name,
                target_schema=schema or "main",
                checksum="",
                chunks_completed=0,
                error=studio_err,
                rejected_details=rejected_details,
                warnings=transform_errors,
            )

        if not studio_err:
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
                    dest_kind="sqlite",
                    destination_pk_columns=list(conflict_columns or []) or None,
                    destination_column_nullability=_kwargs.get(
                        "destination_column_nullability"
                    ),
                    empty_cells_as_null=bool(_kwargs.get("empty_cells_as_null")),
                )
            )
            # Shared quarantine matrix — SQLite is PRODUCTION_SKU; never skip fit
            # checks that generic_sql / Postgres / BQ run (silent truncate / invent).
            mapped_rows = apply_write_quarantine_matrix(
                mapped_rows,
                target_cols,
                tgt_types,
                rejected_details,
                policy,
                dialect_label="SQLite",
                mappings=list(mappings or []) or None,
                dest_db="sqlite",
            )
            if write_mode == "upsert" and conflict_cols:
                mapped_rows, sparse_rows = split_dense_sparse_rows(mapped_rows)
            else:
                from connectors.writer_common import (
                    materialize_missing_as_null_for_dense_write,
                )

                mapped_rows = materialize_missing_as_null_for_dense_write(mapped_rows)

        overlay_err = require_physical_types_for_existing_table(
            table_existed=table_existed,
            physical=physical,
            dialect_label="SQLite",
            # With backfill, ADD COLUMN runs later — only require carriers for
            # columns already on the table (PG/MySQL fetch physical post-ALTER).
            target_cols=(
                [
                    c
                    for c in target_cols
                    if c
                    and (
                        c in physical
                        or str(c).lower() in {str(k).lower() for k in physical}
                    )
                ]
                if (table_existed and backfill_new_fields)
                else target_cols
            ),
        )
        if overlay_err:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=table_name,
                target_schema=schema or "main",
                checksum="",
                chunks_completed=0,
                error=overlay_err,
                rejected_details=rejected_details,
                warnings=transform_errors,
            )

        # Partial Studio + ADD: stamp Map target_type into dest_types BEFORE
        # rematerialize/bind so we never coerce against source logical invent
        # then ALTER ADD a different affinity (PG/MySQL order parity).
        if studio_err and backfill_new_fields:
            from connectors.writer_common import gate_additive_types_under_partial_studio

            existing_probe = {str(k) for k in physical.keys() if k}
            stamped_logical, add_err = gate_additive_types_under_partial_studio(
                target_cols=target_cols,
                target_types=[""] * len(target_cols),
                existing=existing_probe,
                mappings=mappings,
                studio_err=studio_err,
                product="SQLite",
                # Identity — dest_types need Map logicals; sqlite_type applied below.
                materialize_stamp=lambda s: str(s or "").strip(),
                col_in_existing=lambda col, ex: (
                    col in ex
                    or str(col).lower() in {str(k).lower() for k in ex}
                ),
                dest_db="sqlite",
                column_types=column_types,
            )
            if add_err:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=table_name,
                    target_schema=schema or "main",
                    checksum="",
                    chunks_completed=0,
                    error=add_err,
                    rejected_details=rejected_details,
                    warnings=transform_errors,
                )
            for i, col in enumerate(target_cols):
                stamp = str(stamped_logical[i] if i < len(stamped_logical) else "").strip()
                if col and stamp:
                    dest_types[col] = stamp

        if physical:
            from connectors.writer_common import rematerialize_live_dest_types

            # Overlay live carriers for existing columns; additive Map cols keep
            # Map stamps until ALTER ADD COLUMN (schema-evolution parity).
            covered_cols: list[str] = []
            covered_physical: dict[str, str] = {}
            for c in target_cols or []:
                if not c:
                    continue
                hit = (
                    physical.get(c)
                    or physical.get(str(c).lower())
                    or physical.get(str(c).upper())
                )
                if hit and str(hit).strip():
                    covered_cols.append(c)
                    covered_physical[c] = str(hit).strip()
            live_partial = (
                rematerialize_live_dest_types(
                    covered_physical, covered_cols, product="SQLite"
                )
                if covered_cols
                else None
            )
            if covered_cols and live_partial is None:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=table_name,
                    target_schema=schema or "main",
                    checksum="",
                    chunks_completed=0,
                    error=(
                        "SQLite live DDL incomplete for existing mapped columns — "
                        "refuse Map VARCHAR rematerialize invent. Re-run "
                        "destination schema introspect and retry."
                    ),
                    rejected_details=rejected_details,
                    warnings=transform_errors,
                )
            live_dest_types = dict(dest_types or {})
            if live_partial:
                live_dest_types.update(live_partial)
            carriers_differ = bool(covered_cols) and any(
                str(dest_types.get(c) or "").strip().upper()
                != str(live_dest_types.get(c) or "").strip().upper()
                for c in covered_cols
            )
            need_remap = carriers_differ or (bool(studio_err) and not mapped_rows)
            if need_remap:
                # Rematerialize from source against live DDL (no Map VARCHAR invent).
                # Keep Map temporal stamps over TEXT/NUMERIC affinity for bind
                # fidelity (PRAGMA cannot express DATETIME vs TIMESTAMPTZ).
                map_dest_before = dict(dest_types or {})
                dest_types = live_dest_types
                target_types = []
                tgt_types = []
                for i, c in enumerate(target_cols):
                    live_c = str(dest_types.get(c) or "").strip()
                    map_c = str(map_dest_before.get(c) or "").strip()
                    if not map_c and not studio_err:
                        map_c = str(
                            logical_types[i] if i < len(logical_types) else ""
                        ).strip()
                    carrier = _sqlite_bind_carrier(map_c, live_c)
                    if not carrier and studio_err:
                        return WriteResult(
                            ok=False,
                            rows_written=0,
                            table_name=table_name,
                            target_schema=schema or "main",
                            checksum="",
                            chunks_completed=0,
                            error=(
                                f"SQLite mapped field {c!r} lacks live/Map carrier "
                                "under partial Studio — refuse logical bind invent."
                            ),
                            rejected_details=rejected_details,
                            warnings=transform_errors,
                        )
                    if carrier:
                        dest_types[c] = carrier
                    tgt_types.append(carrier)
                    target_types.append(sqlite_type(carrier) if carrier else "")
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
                        dest_kind="sqlite",
                        destination_pk_columns=list(conflict_columns or []) or None,
                        destination_column_nullability=_kwargs.get(
                            "destination_column_nullability"
                        ),
                        empty_cells_as_null=bool(_kwargs.get("empty_cells_as_null")),
                    )
                )
                mapped_rows = apply_write_quarantine_matrix(
                    mapped_rows,
                    target_cols,
                    tgt_types,
                    rejected_details,
                    policy,
                    dialect_label="SQLite",
                    mappings=list(mappings or []) or None,
                    dest_db="sqlite",
                )
                sparse_rows = []
                if write_mode == "upsert" and conflict_cols:
                    mapped_rows, sparse_rows = split_dense_sparse_rows(mapped_rows)
                else:
                    from connectors.writer_common import (
                        materialize_missing_as_null_for_dense_write,
                    )

                    mapped_rows = materialize_missing_as_null_for_dense_write(
                        mapped_rows
                    )
            else:
                tgt_types = overlay_physical_bind_types(
                    target_cols, tgt_types, physical
                )

        converted_rows = bind_sql_mapped_rows_with_quarantine(
            mapped_rows,
            target_cols,
            tgt_types,
            rejected_details,
            policy,
            engine="sqlite",
            dialect_label="SQLite",
            mappings=list(mappings or []) or None,
        )
        # Dense ISO-Z / typed polish after shared bind refuse path.
        converted_rows = [
            tuple(_to_sqlite_value(v, tgt_types[i] if i < len(tgt_types) else "") for i, v in enumerate(row))
            for row in converted_rows
        ]
        sparse_converted = bind_sql_mapped_rows_with_quarantine(
            sparse_rows,
            target_cols,
            tgt_types,
            rejected_details,
            policy,
            engine="sqlite",
            dialect_label="SQLite",
            mappings=list(mappings or []) or None,
        )
        sparse_converted = [
            tuple(_to_sqlite_value(v, tgt_types[i] if i < len(tgt_types) else "") for i, v in enumerate(row))
            for row in sparse_converted
        ]
        # Dense rows are fully written — include them in writer-ack checksum.
        rows_for_checksum = list(converted_rows)

        rejected_rows = _rejected_row_count(
            data_rows, mapped_rows, rejected_details, policy, sparse_rows=sparse_rows
        )
        coerced_null_rows = _coerced_null_row_count(rejected_details, policy)
        _map_abort = reject_on_strict_policy(policy, rejected_details, 'SQLite', transform_errors)
        if _map_abort:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=table_name,
                target_schema=schema or "main",
                checksum="",
                chunks_completed=0,
                error=_map_abort or f"Transform errors: {'; '.join(transform_errors[:3])}",
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
                    from services.schema_fidelity import (
                        empty_unsupported_report,
                        render_create_column_defs,
                        resolve_create_fidelity_plan,
                    )

                    try:
                        fidelity_plan = resolve_create_fidelity_plan(
                            source_schema_catalog=_kwargs.get("source_schema_catalog"),
                            mappings=mappings,
                            target_columns=target_cols,
                            target_types=target_types,
                            dest_dialect="sqlite",
                            table_already_exists=bool(table_existed),
                            dest_table=table_name,
                        )
                    except Exception as exc:
                        logger.warning(
                            "SQLite schema fidelity plan failed (types-only CREATE): %s",
                            exc,
                        )
                        fidelity_plan = None
                        _kwargs["_schema_fidelity_report"] = empty_unsupported_report(
                            source_dialect="",
                            dest_dialect="sqlite",
                            reason=(
                                f"Schema fidelity planner raised ({type(exc).__name__}); "
                                "create-new emitted column types only."
                            ),
                        ).to_dict()
                    if fidelity_plan is not None:
                        if fidelity_plan.column_renames and fidelity_plan.dest_columns:
                            target_cols = list(fidelity_plan.dest_columns)
                            # INSERT was built before CREATE; rebuild after collision remaps.
                            placeholders = ", ".join("?" for _ in target_cols)
                            insert = (
                                f"INSERT INTO {table_quoted} ("
                                f"{', '.join(quote_sql_identifier(c) for c in target_cols)}"
                                f") VALUES ({placeholders})"
                            )
                        col_defs = render_create_column_defs(
                            columns=target_cols,
                            types=target_types,
                            plan=(None if table_existed else fidelity_plan),
                            dialect="sqlite",
                        )
                        cur.execute(
                            f"CREATE TABLE IF NOT EXISTS {table_quoted} ({col_defs})"
                        )
                        from services.schema_fidelity import apply_post_create_sql

                        # A refused CREATE INDEX downgrades that index in the
                        # certificate instead of failing the load.
                        apply_post_create_sql(fidelity_plan, cur.execute)
                        # Re-read the destination catalog and settle PK / NOT NULL
                        # / DEFAULT / UNIQUE from what SQLite actually took — an
                        # emitted clause is a claim, not proof. Only for create-new
                        # (we did not touch an existing table's structure).
                        if not table_existed:
                            from services.schema_fidelity import (
                                certify_structure_on_destination,
                            )

                            certify_structure_on_destination(
                                fidelity_plan,
                                dialect="sqlite",
                                schema="",
                                table=table_name,
                                fetchall=lambda sql, params: list(
                                    cur.execute(sql, params).fetchall()
                                ),
                            )
                        _kwargs["_schema_fidelity_report"] = fidelity_plan.report.to_dict()
                    else:
                        col_defs = ", ".join(
                            f"{quote_sql_identifier(c)} {t}"
                            for c, t in zip(target_cols, target_types)
                        )
                        cur.execute(
                            f"CREATE TABLE IF NOT EXISTS {table_quoted} ({col_defs})"
                        )
                        _kwargs.setdefault(
                            "_schema_fidelity_report",
                            empty_unsupported_report(
                                source_dialect="",
                                dest_dialect="sqlite",
                                reason=(
                                    "Schema fidelity plan unavailable; "
                                    "create-new emitted column types only."
                                ),
                            ).to_dict(),
                        )

                if backfill_new_fields:
                    existing = {
                        row[1]
                        for row in cur.execute(f"PRAGMA table_info({table_quoted})")
                    }
                    from connectors.writer_common import (
                        gate_additive_types_under_partial_studio,
                    )

                    target_types, add_err = gate_additive_types_under_partial_studio(
                        target_cols=target_cols,
                        target_types=target_types,
                        existing=existing,
                        mappings=mappings,
                        studio_err=studio_err,
                        product="SQLite",
                        materialize_stamp=sqlite_type,
                        dest_db="sqlite",
                        column_types=column_types,
                    )
                    if add_err:
                        return WriteResult(
                            ok=False,
                            rows_written=0,
                            table_name=table_name,
                            target_schema=schema or "main",
                            checksum="",
                            chunks_completed=0,
                            error=add_err,
                            rejected_details=rejected_details,
                            warnings=transform_errors,
                        )
                    for col, typ in zip(target_cols, target_types):
                        if col not in existing:
                            try:
                                cur.execute(
                                    f"ALTER TABLE {table_quoted} ADD COLUMN {quote_sql_identifier(col)} {typ}"
                                )
                            except sqlite3.OperationalError as exc:
                                # Fail closed — silent skip invents schema drift.
                                return WriteResult(
                                    ok=False,
                                    rows_written=0,
                                    table_name=table_name,
                                    target_schema=schema or "main",
                                    checksum="",
                                    chunks_completed=0,
                                    error=(
                                        f"SQLite ADD COLUMN {col!r} failed: {exc} — "
                                        "refuse silent schema drift."
                                    ),
                                    rejected_details=rejected_details,
                                    warnings=transform_errors,
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
                            cur,
                            table_name,
                            target_cols,
                            batch,
                            conflict_cols,
                            schema=schema or None,
                            rejected_details=rejected_details,
                            policy=policy,
                            row_offset=start,
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
                                row_start=start,
                                row_end=start + len(batch) - 1,
                                attempt=1,
                            )

                if on_checkpoint:
                    on_checkpoint(chunk_idx + 1, max(chunks, 1), written)

            if ledger_chunks_skipped:
                transform_errors.append(
                    f"Skipped {ledger_chunks_skipped} chunk(s) already committed by a "
                    "previous attempt (write ledger prevented duplicate rows)"
                )

            _final_abort = reject_on_strict_policy(policy, rejected_details, "SQLite")
            if _final_abort:
                return WriteResult(
                    ok=False,
                    rows_written=written,
                    table_name=table_name,
                    target_schema=schema or "main",
                    checksum="",
                    chunks_completed=chunks or (1 if sparse_converted else 0),
                    error=_final_abort,
                    rejected_rows=max(
                        rejected_rows, len(data_rows) - written - rows_skipped
                    ),
                    rejected_details=rejected_details,
                    coerced_null_rows=coerced_null_rows,
                    rows_skipped=rows_skipped,
                    warnings=transform_errors,
                )

            meta_out = gate8_writer_meta(
                rows_for_checksum,
                target_cols,
                conflict_columns=conflict_cols or None,
            )
            fid_report = _kwargs.get("_schema_fidelity_report")
            if isinstance(fid_report, dict):
                meta_out = dict(meta_out or {})
                meta_out["schema_fidelity"] = fid_report
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
                meta=meta_out,
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
