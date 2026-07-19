"""Change-data-capture transfer runner for database sources.

Implemented:
  - **Query-based CDC**: polls the source table using a monotonic cursor column.
  - **MongoDB Change Streams**, **MySQL binlog (ROW)**, **PostgreSQL logical decoding**
    (pgoutput when available, else test_decoding), with query CDC fallback.

Apply semantics are **at-least-once upsert** (not exactly-once). Job checkpoints
persist watermark progress alongside sync_cursor watermarks.
"""
from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from bson import json_util

from connectors.mongodb_change_stream import MongodbChangeStreamCdc
from connectors.mysql_change_stream import MySqlChangeStreamCdc
from connectors.postgresql_change_stream import PostgreSqlChangeStreamCdc
from connectors.table_manager import delete_by_primary_keys
from services.cdc_engine import (
    ChangeBatch,
    WatermarkType,
    advance_watermark,
    infer_watermark_type,
    max_watermark,
)
from services.error_handling import RetryBudget, with_retry
from services.value_serializer import cell_to_string
from connectors.writer_common import DF_LSN_COL, extract_cdc_lsn
from services.sync_cursor import (
    build_cursor_key,
    get_watermark,
    map_source_to_target,
    resolve_selected_sync_contracts,
    resolve_sync_contract,
    set_watermark,
)

try:
    from .adapters import resolve_connector_config, resolve_dest_table
    from .connector_capabilities import resolve_driver_type
    from .stream import _read_batch, _unwrap_read, _write_batch
except ImportError:  # pragma: no cover - tests with api root on PYTHONPATH
    from src.transfer.adapters import resolve_connector_config, resolve_dest_table
    from src.transfer.connector_capabilities import resolve_driver_type
    from src.transfer.stream import _read_batch, _unwrap_read, _write_batch


CHUNK_SIZE = 1000


def _cdc_lag_fields(cdc: Any) -> dict[str, Any]:
    """Collect lag / heartbeat / last-DDL fields from a CDC reader."""
    lag_bytes = None
    lag_seconds = None
    last_ddl = None
    heartbeat_at = None
    if hasattr(cdc, "replication_lag_bytes"):
        try:
            lag_bytes = cdc.replication_lag_bytes()
        except Exception:
            lag_bytes = None
    if hasattr(cdc, "replication_lag_seconds"):
        try:
            lag_seconds = cdc.replication_lag_seconds()
        except Exception:
            lag_seconds = None
    last_ddl = getattr(cdc, "last_ddl_at", None)
    hb = getattr(cdc, "_last_heartbeat_at", None) or getattr(cdc, "_last_event_at", None)
    if isinstance(hb, datetime):
        heartbeat_at = hb.astimezone(timezone.utc).isoformat()
    return {
        "replication_lag_bytes": lag_bytes,
        "cdc_lag_seconds": lag_seconds,
        "cdc_last_ddl_at": last_ddl,
        "cdc_heartbeat_at": heartbeat_at,
    }


@dataclass
class CdcState:
    cursor_key: str = ""
    watermark: str | None = None
    running_cursor: str | None = None
    rows_written: int = 0
    inserts: int = 0
    updates: int = 0
    deletes: int = 0
    ddl_log: list[str] = field(default_factory=list)
    last_dest_summary: dict[str, Any] = field(default_factory=dict)
    last_checksum: str = ""


def _records_to_matrix(records: list[dict[str, Any]], headers: list[str]) -> list[list[str]]:
    return [[cell_to_string(r.get(h, "")) for h in headers] for r in records]


def _source_headers(headers: list[str], mappings: list[dict[str, Any]]) -> tuple[list[str], list[str]]:
    """Return source headers as expected by _write_batch and the target column list."""
    return headers, [m.get("target", m.get("source", "")).strip() for m in mappings if m.get("source")]


def _detect_tombstone_column(schema: dict[str, str], columns: list[str]) -> str | None:
    """Return a soft-delete/tombstone column name if one exists."""
    for c in columns:
        lowered = c.lower()
        if lowered in {"deleted_at", "deleted", "is_deleted", "tombstone", "is_active"}:
            return c
        if "delete" in lowered or "tombstone" in lowered:
            return c
    return None


def _is_tombstone_set(record: dict[str, Any], tombstone_column: str) -> bool:
    value = record.get(tombstone_column)
    if value is None:
        return False
    text = str(value).strip().lower()
    if text in {"", "0", "false", "f", "no", "n", "null", "none"}:
        return False
    if text in {"1", "true", "t", "yes", "y"}:
        return True
    # Non-empty timestamp/text means deleted (soft-delete pattern)
    return bool(text)


class CdcEngine:
    """Query-based CDC engine."""

    def __init__(
        self,
        src_cfg: dict[str, Any],
        src_type: str,
        table_name: str,
        cursor_field: str,
        primary_key: str,
        watermark: str | None,
        columns: list[str] | None = None,
        schema: dict[str, str] | None = None,
        batch_size: int = CHUNK_SIZE,
        tombstone_column: str | None = None,
    ) -> None:
        self.src_cfg = src_cfg
        self.src_type = src_type
        self.table_name = table_name
        self.cursor_field = cursor_field
        self.primary_key = primary_key
        self.watermark = watermark
        self.batch_size = batch_size
        self.columns = columns or []
        self.schema = schema or {}
        self.tombstone_column = tombstone_column or _detect_tombstone_column(self.schema, self.columns)

    def _read(self, cursor_after: str | None = None) -> Iterator[tuple[list[str], list[list[str]]]]:
        """Yield (headers, rows) batches from the source table."""
        offset = 0
        cursor_type = None
        if cursor_after:
            samples = [cursor_after]
            inferred = infer_watermark_type(samples)
            cursor_type = inferred.value
        while True:
            result, _ = _unwrap_read(
                _read_batch(
                    self.src_type,
                    self.src_cfg,
                    self.table_name,
                    self.columns or None,
                    offset,
                    self.batch_size,
                    cursor_column=self.cursor_field if cursor_after else "",
                    cursor_after=cursor_after,
                    cursor_type=cursor_type,
                    database=self.src_cfg.get("database", ""),
                )
            )
            if not result or not getattr(result, "rows", None):
                break
            headers = result.headers
            rows = result.rows
            if not rows:
                break
            yield headers, rows
            offset += len(rows)

    def _yield_batches(self, reader: Iterator[tuple[list[str], list[list[str]]]]) -> Iterator[ChangeBatch]:
        """Stream batches from a (headers, rows) reader without materializing all rows."""
        buffer: list[dict[str, Any]] = []
        headers: list[str] = []
        emitted = False
        for h, rows in reader:
            if not headers:
                headers = h
            for row in rows:
                buffer.append({h: row[i] if i < len(row) else "" for i, h in enumerate(headers)})
                if len(buffer) >= self.batch_size:
                    yield self._split_batch(buffer)
                    emitted = True
                    buffer = []
        if buffer:
            yield self._split_batch(buffer)
        elif not emitted:
            yield ChangeBatch()

    def _split_batch(self, records: list[dict[str, Any]]) -> ChangeBatch:
        if not self.tombstone_column:
            return ChangeBatch(inserts=records)
        inserts = [r for r in records if not _is_tombstone_set(r, self.tombstone_column)]
        deletes = [
            str(r.get(self.primary_key, "")) for r in records
            if _is_tombstone_set(r, self.tombstone_column) and r.get(self.primary_key)
        ]
        return ChangeBatch(inserts=inserts, deletes=deletes)

    def snapshot(self) -> Iterator[ChangeBatch]:
        """Yield the full source table as a single INSERT-only change batch."""
        yield from self._yield_batches(self._read())

    def poll(self) -> Iterator[ChangeBatch]:
        """Yield changes since the last watermark."""
        if not self.watermark:
            yield from self.snapshot()
            return
        yield from self._yield_batches(self._read(cursor_after=self.watermark))


def _max_cursor_value(records: list[dict[str, Any]], cursor_field: str, wm_type: WatermarkType) -> str | None:
    values = [str(r.get(cursor_field, "")) for r in records if r.get(cursor_field) is not None]
    return max_watermark(values, wm_type)


def _stamp_cdc_lsn(
    change: ChangeBatch,
    headers: list[str],
    mappings: list[dict[str, Any]],
    column_types: dict[str, str],
) -> tuple[list[str], list[dict[str, Any]], dict[str, str]]:
    """Attach ``_df_lsn`` from the batch resume token for monotonic MERGE at the dest."""
    lsn = extract_cdc_lsn(change.resume_token)
    if not lsn:
        return headers, mappings, column_types
    for record in change.inserts:
        record[DF_LSN_COL] = lsn
    for record in change.updates:
        record[DF_LSN_COL] = lsn
    out_headers = list(headers)
    out_mappings = list(mappings)
    out_types = dict(column_types)
    if DF_LSN_COL not in out_headers:
        out_headers.append(DF_LSN_COL)
    if not any(m.get("source") == DF_LSN_COL for m in out_mappings):
        out_mappings.append(
            {"source": DF_LSN_COL, "target": DF_LSN_COL, "confidence": 1.0}
        )
    out_types.setdefault(DF_LSN_COL, "string")
    return out_headers, out_mappings, out_types


def _apply_change_batch(
    dest_type: str,
    destination: Any,
    dest_cfg: dict[str, Any],
    dest_table: str,
    change: ChangeBatch,
    mappings: list[dict[str, Any]],
    column_types: dict[str, str],
    headers: list[str],
    pk_target_col: str,
    chunk_idx: int,
    total_chunks: int,
) -> tuple[int, str, dict[str, Any], int]:
    """Apply a single ChangeBatch to the destination. Returns rows_written, checksum, summary, deleted_count."""
    headers, mappings, column_types = _stamp_cdc_lsn(
        change, headers, mappings, column_types
    )
    source_headers, target_cols = _source_headers(headers, mappings)
    rows_written = 0
    deleted = 0
    last_checksum = ""
    dest_summary: dict[str, Any] = {}

    if change.inserts:
        data_rows = _records_to_matrix(change.inserts, headers)
        write_op = lambda: _write_batch(
            dest_type,
            destination,
            dest_cfg,
            dest_table,
            source_headers,
            data_rows,
            mappings,
            column_types,
            create_table=(chunk_idx == 0),
            on_checkpoint=None,
            chunk_idx=chunk_idx,
            total_chunks=total_chunks,
            rows_so_far=0,
            write_mode="upsert",
            conflict_columns=[pk_target_col] if pk_target_col else None,
            backfill_new_fields=False,
        )
        rows, last_checksum, dest_summary = with_retry(
            write_op,
            budget=RetryBudget(max_attempts=3, base_delay_seconds=0.5, max_delay_seconds=5.0),
        )
        rows_written += rows

    if change.updates:
        data_rows = _records_to_matrix(change.updates, headers)
        write_op = lambda: _write_batch(
            dest_type,
            destination,
            dest_cfg,
            dest_table,
            source_headers,
            data_rows,
            mappings,
            column_types,
            create_table=False,
            on_checkpoint=None,
            chunk_idx=chunk_idx,
            total_chunks=total_chunks,
            rows_so_far=0,
            write_mode="upsert",
            conflict_columns=[pk_target_col] if pk_target_col else None,
            backfill_new_fields=False,
        )
        rows, last_checksum, dest_summary = with_retry(
            write_op,
            budget=RetryBudget(max_attempts=3, base_delay_seconds=0.5, max_delay_seconds=5.0),
        )
        rows_written += rows

    if change.deletes:
        deleted = delete_by_primary_keys(
            db_type=dest_type,
            cfg=dest_cfg,
            table_name=dest_table,
            primary_key_column=pk_target_col,
            keys=change.deletes,
            schema=dest_cfg.get("schema"),
        )

    return rows_written, last_checksum, dest_summary, deleted


def run_cdc_database_transfer(
    source: Any,
    destination: Any,
    mappings: list[dict],
    schema: dict[str, str],
    on_checkpoint: Any | None = None,
    *,
    sync_mode: str = "cdc",
    stream_contracts: list[dict] | None = None,
    job_id: str = "",
    checkpoint: Any | None = None,
    checkpoint_service: Any | None = None,
    backfill_new_fields: bool = False,
    validation_mode: str = "strict",
    limit: int = 0,
) -> tuple[int, list[str], dict[str, Any], list[str]]:
    """Run a CDC transfer from a database source to a database destination.

    When multiple stream contracts are selected, each stream runs with its own
    cursor key and destination object; job summary includes ``streams[]`` health.
    """
    selected = resolve_selected_sync_contracts(stream_contracts)
    if len(selected) > 1:
        return _run_cdc_multi_stream(
            source,
            destination,
            mappings,
            schema,
            on_checkpoint,
            sync_mode=sync_mode,
            stream_contracts=stream_contracts or [],
            selected=selected,
            job_id=job_id,
            checkpoint=checkpoint,
            checkpoint_service=checkpoint_service,
            backfill_new_fields=backfill_new_fields,
            validation_mode=validation_mode,
            limit=limit,
        )
    return _run_cdc_single_stream(
        source,
        destination,
        mappings,
        schema,
        on_checkpoint,
        sync_mode=sync_mode,
        stream_contracts=stream_contracts,
        job_id=job_id,
        checkpoint=checkpoint,
        checkpoint_service=checkpoint_service,
        backfill_new_fields=backfill_new_fields,
        validation_mode=validation_mode,
        limit=limit,
    )


def _run_cdc_multi_stream(
    source: Any,
    destination: Any,
    mappings: list[dict],
    schema: dict[str, str],
    on_checkpoint: Any | None,
    *,
    sync_mode: str,
    stream_contracts: list[dict],
    selected: list[Any],
    job_id: str,
    checkpoint: Any | None,
    checkpoint_service: Any | None,
    backfill_new_fields: bool,
    validation_mode: str,
    limit: int,
) -> tuple[int, list[str], dict[str, Any], list[str]]:
    """Run CDC for each selected stream with independent watermarks."""
    total_rows = 0
    ddl_log: list[str] = []
    headers: list[str] = list(schema.keys())
    stream_health: list[dict[str, Any]] = []
    worst_lag: float | None = None
    last_summary: dict[str, Any] = {}

    original_table = getattr(source, "table", None)
    original_collection = getattr(source, "collection", None)
    original_dest_table = getattr(destination, "table", None)
    original_dest_collection = getattr(destination, "collection", None)

    try:
        for contract in selected:
            stream_name = (contract.name or "").strip() or "stream"
            # Bind source/dest object to this stream (table/collection name).
            if getattr(source, "format", "") == "mongodb" or original_collection:
                source.collection = stream_name
            else:
                source.table = stream_name
            if original_dest_table is not None or original_dest_collection is not None:
                if getattr(destination, "format", "") == "mongodb" or original_dest_collection:
                    destination.collection = stream_name
                else:
                    destination.table = stream_name

            single_contracts = [
                {
                    **(
                        next(
                            (c for c in stream_contracts if c.get("name") == stream_name),
                            {},
                        )
                    ),
                    "name": stream_name,
                    "selected": True,
                    "sync_mode": contract.sync_mode or sync_mode,
                    "cursor_field": contract.cursor_field,
                    "primary_key": contract.primary_key,
                    "schema_policy": contract.schema_policy,
                    "validation_mode": contract.validation_mode or validation_mode,
                }
            ]
            status = "completed"
            error: str | None = None
            rows = 0
            summary: dict[str, Any] = {}
            try:
                rows, stream_ddl, summary, headers = _run_cdc_single_stream(
                    source,
                    destination,
                    mappings,
                    schema,
                    on_checkpoint,
                    sync_mode=sync_mode,
                    stream_contracts=single_contracts,
                    job_id=job_id,
                    checkpoint=checkpoint,
                    checkpoint_service=checkpoint_service,
                    backfill_new_fields=backfill_new_fields,
                    validation_mode=validation_mode,
                    limit=limit,
                )
                ddl_log.extend(stream_ddl)
                total_rows += rows
                last_summary = summary
                lag = summary.get("cdc_lag_seconds")
                if isinstance(lag, (int, float)):
                    worst_lag = lag if worst_lag is None else max(worst_lag, float(lag))
            except Exception as exc:
                status = "failed"
                error = str(exc)
                stream_health.append(
                    {
                        "name": stream_name,
                        "status": status,
                        "records_processed": rows,
                        "error": error,
                    }
                )
                raise
            cdc_meta = summary.get("cdc") if isinstance(summary.get("cdc"), dict) else {}
            stream_health.append(
                {
                    "name": stream_name,
                    "status": status,
                    "records_processed": rows,
                    "cdc_lag_seconds": summary.get("cdc_lag_seconds"),
                    "replication_lag_bytes": cdc_meta.get("replication_lag_bytes"),
                    "watermark": cdc_meta.get("watermark"),
                    "error": error,
                }
            )
    finally:
        if original_table is not None:
            source.table = original_table
        if original_collection is not None:
            source.collection = original_collection
        if original_dest_table is not None:
            destination.table = original_dest_table
        if original_dest_collection is not None:
            destination.collection = original_dest_collection

    last_summary = dict(last_summary or {})
    last_summary["streams"] = stream_health
    if worst_lag is not None:
        last_summary["cdc_lag_seconds"] = worst_lag
    return total_rows, ddl_log, last_summary, headers


def _run_cdc_single_stream(
    source: Any,
    destination: Any,
    mappings: list[dict],
    schema: dict[str, str],
    on_checkpoint: Any | None = None,
    *,
    sync_mode: str = "cdc",
    stream_contracts: list[dict] | None = None,
    job_id: str = "",
    checkpoint: Any | None = None,
    checkpoint_service: Any | None = None,
    backfill_new_fields: bool = False,
    validation_mode: str = "strict",
    limit: int = 0,
) -> tuple[int, list[str], dict[str, Any], list[str]]:
    """Run a CDC transfer for a single stream contract."""
    src_type = resolve_driver_type(source.format)
    dest_type = resolve_driver_type(destination.format)
    src_cfg = resolve_connector_config(source)
    dest_cfg = resolve_connector_config(destination)
    table_name = source.table or source.collection or ""
    dest_table = resolve_dest_table(dest_type, destination, table_name)

    contract = resolve_sync_contract(stream_contracts)
    primary_key = contract.primary_key if contract else ""
    cursor_field = contract.cursor_field if contract else ""
    if not primary_key:
        raise ValueError("CDC sync requires primary_key in the stream contract")
    if src_type in {"mongodb", "mysql", "postgresql"}:
        cursor_field = cursor_field or primary_key or ("_id" if src_type == "mongodb" else "id")
    elif not cursor_field:
        raise ValueError("CDC sync requires cursor_field in the stream contract")

    pk_target_col = map_source_to_target(primary_key, mappings)
    cursor_key = build_cursor_key(
        source_type=src_type,
        source_database=src_cfg.get("database", ""),
        source_object=table_name,
        dest_type=dest_type,
        dest_database=dest_cfg.get("database", ""),
        dest_object=dest_table,
        stream_name=contract.name if contract else "stream",
    )
    watermark = get_watermark(cursor_key)

    headers = list(schema.keys())
    column_types = {c: schema.get(c, "string") for c in headers}

    if src_type == "mongodb":
        try:
            cdc: CdcEngine | MongodbChangeStreamCdc | MySqlChangeStreamCdc | PostgreSqlChangeStreamCdc = MongodbChangeStreamCdc(
                src_cfg,
                collection=table_name,
                primary_key=primary_key,
                columns=headers,
                resume_token=watermark,
                batch_size=CHUNK_SIZE,
            )
            if not cdc.is_available():
                raise RuntimeError("MongoDB change streams not available; falling back to query CDC")
            ddl_log = [
                f"CDC(change_stream) {src_type}.{table_name} → {dest_type}.{dest_table} "
                f"(pk={primary_key}, resume_token={'set' if watermark else 'initial'})"
            ]
        except Exception:
            cdc = CdcEngine(
                src_cfg,
                src_type,
                table_name,
                cursor_field,
                primary_key,
                watermark,
                columns=headers,
                schema=schema,
            )
            ddl_log = [
                f"CDC(query) {src_type}.{table_name} → {dest_type}.{dest_table} "
                f"(cursor={cursor_field}, pk={primary_key}, watermark={watermark or 'initial'})"
            ]
    elif src_type == "mysql":
        try:
            cdc = MySqlChangeStreamCdc(
                src_cfg,
                table=table_name,
                primary_key=primary_key,
                columns=headers,
                resume_token=watermark,
                batch_size=CHUNK_SIZE,
            )
            if not cdc.is_available():
                raise RuntimeError("MySQL binlog not available; falling back to query CDC")
            ddl_log = [
                f"CDC(binlog) {src_type}.{table_name} → {dest_type}.{dest_table} "
                f"(pk={primary_key}, resume={'set' if watermark else 'initial'})"
            ]
        except Exception:
            cdc = CdcEngine(
                src_cfg,
                src_type,
                table_name,
                cursor_field,
                primary_key,
                watermark,
                columns=headers,
                schema=schema,
            )
            ddl_log = [
                f"CDC(query) {src_type}.{table_name} → {dest_type}.{dest_table} "
                f"(cursor={cursor_field}, pk={primary_key}, watermark={watermark or 'initial'})"
            ]
    elif src_type == "postgresql":
        try:
            cdc = PostgreSqlChangeStreamCdc(
                src_cfg,
                table=table_name,
                primary_key=primary_key,
                cursor_key=cursor_key,
                schema=src_cfg.get("schema") or "public",
                columns=headers,
                resume_token=watermark,
                batch_size=CHUNK_SIZE,
            )
            if not cdc.is_available():
                raise RuntimeError("PostgreSQL logical decoding not available; falling back to query CDC")
            ddl_log = [
                f"CDC(logical_decoding) {src_type}.{table_name} → {dest_type}.{dest_table} "
                f"(pk={primary_key}, resume={'set' if watermark else 'initial+slot+lsn'})"
            ]
        except Exception:
            cdc = CdcEngine(
                src_cfg,
                src_type,
                table_name,
                cursor_field,
                primary_key,
                watermark,
                columns=headers,
                schema=schema,
            )
            ddl_log = [
                f"CDC(query) {src_type}.{table_name} → {dest_type}.{dest_table} "
                f"(cursor={cursor_field}, pk={primary_key}, watermark={watermark or 'initial'})"
            ]
    else:
        cdc = CdcEngine(
            src_cfg,
            src_type,
            table_name,
            cursor_field,
            primary_key,
            watermark,
            columns=headers,
            schema=schema,
        )
        ddl_log = [
            f"CDC {src_type}.{table_name} → {dest_type}.{dest_table} "
            f"(cursor={cursor_field}, pk={primary_key}, watermark={watermark or 'initial'})"
        ]

    state = CdcState(cursor_key=cursor_key, watermark=watermark)
    # Resume from durable job checkpoint watermark when present.
    cp_dict: dict[str, Any] = {}
    if checkpoint is not None:
        if isinstance(checkpoint, dict):
            cp_dict = checkpoint
        elif hasattr(checkpoint, "to_dict"):
            cp_dict = checkpoint.to_dict()  # type: ignore[assignment]
    if cp_dict:
        cp_wm = cp_dict.get("watermark") or (cp_dict.get("cdc") or {}).get("watermark")
        if cp_wm:
            state.running_cursor = str(cp_wm)
            state.watermark = str(cp_wm)
            watermark = str(cp_wm)
    total_chunks = max(1, int(cp_dict.get("chunk_index") or 0) + 1) if cp_dict else 1
    chunk_idx = int(cp_dict.get("chunk_index") or 0) if cp_dict else 0

    for change in cdc.snapshot() if watermark is None else cdc.poll():
        if not change.total_changes and not change.deletes and change.resume_token is None:
            continue

        rows_written, last_checksum, dest_summary, deleted = _apply_change_batch(
            dest_type,
            destination,
            dest_cfg,
            dest_table,
            change,
            mappings,
            column_types,
            headers,
            pk_target_col,
            chunk_idx,
            total_chunks,
        )
        state.rows_written += rows_written
        state.inserts += len(change.inserts)
        state.updates += len(change.updates)
        state.deletes += deleted
        state.last_checksum = last_checksum or state.last_checksum
        if dest_summary:
            state.last_dest_summary = dest_summary

        # Advance watermark / resume token from the rows we just applied.
        if change.resume_token is not None:
            try:
                state.running_cursor = json.dumps(change.resume_token, default=json_util.default)
            except TypeError:
                state.running_cursor = str(change.resume_token)
        elif change.inserts or change.updates:
            values = [r.get(cursor_field) for r in (change.inserts + change.updates) if r.get(cursor_field) is not None]
            if values:
                wm_type = infer_watermark_type([str(v) for v in values])
                batch_max = max_watermark([str(v) for v in values], wm_type)
                if batch_max:
                    new_watermark, advanced = advance_watermark(state.running_cursor, [batch_max], wm_type)
                    if advanced and new_watermark is not None:
                        state.running_cursor = new_watermark

        chunk_idx += 1
        total_chunks = max(total_chunks, chunk_idx)
        lag_fields = _cdc_lag_fields(cdc)
        if state.running_cursor:
            set_watermark(
                cursor_key,
                state.running_cursor,
                metadata={
                    "job_id": job_id,
                    "sync_mode": sync_mode,
                    "chunk": chunk_idx,
                    **lag_fields,
                },
            )
        if on_checkpoint:
            on_checkpoint(
                chunk_idx,
                total_chunks,
                state.rows_written,
                {
                    "chunk_index": chunk_idx,
                    "watermark": state.running_cursor,
                    "rows_written": state.rows_written,
                    "cdc_lag_seconds": lag_fields.get("cdc_lag_seconds"),
                    "replication_lag_bytes": lag_fields.get("replication_lag_bytes"),
                    "cdc_heartbeat_at": lag_fields.get("cdc_heartbeat_at"),
                    "cdc_last_ddl_at": lag_fields.get("cdc_last_ddl_at"),
                    "cdc": {
                        "inserts": state.inserts,
                        "updates": state.updates,
                        "deletes": state.deletes,
                        **lag_fields,
                    },
                },
            )

    final_watermark = state.running_cursor or watermark
    lag_fields = _cdc_lag_fields(cdc)
    if final_watermark:
        set_watermark(
            cursor_key,
            final_watermark,
            metadata={"job_id": job_id, "sync_mode": sync_mode, **lag_fields},
        )

    summary = state.last_dest_summary or {}
    summary["cdc"] = {
        "inserts": state.inserts,
        "updates": state.updates,
        "deletes": state.deletes,
        "watermark": final_watermark,
        **lag_fields,
    }
    summary["cdc_lag_seconds"] = lag_fields.get("cdc_lag_seconds")
    summary["replication_lag_bytes"] = lag_fields.get("replication_lag_bytes")
    summary["cdc_heartbeat_at"] = lag_fields.get("cdc_heartbeat_at")
    summary["cdc_last_ddl_at"] = lag_fields.get("cdc_last_ddl_at")
    summary["checksum"] = state.last_checksum
    return state.rows_written, ddl_log, summary, headers
