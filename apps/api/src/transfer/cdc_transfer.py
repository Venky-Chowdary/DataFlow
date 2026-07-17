"""Change-data-capture transfer runner for database sources.

Implemented:
  - **Query-based CDC**: polls the source table using a monotonic cursor column
    (``updated_at`` / ``created_at`` / incrementing id). Optional soft-delete / tombstone
    columns are honoured so rows flagged as deleted propagate as ``DELETE`` events.
  - **MongoDB log-based CDC**: uses native Change Streams via
    ``connectors.mongodb_change_stream`` when the deployment supports it and falls
    back to query-based CDC otherwise.

Not yet implemented: PostgreSQL ``pgoutput`` logical decoding, MySQL binlog,
SQL Server CDC, Oracle LogMiner.

The runner returns the same ``(rows_written, ddl_log, dest_summary, columns)``
shape as ``stream_database_transfer`` so the engine can use it interchangeably.
"""
from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

from bson import json_util

from connectors.mongodb_change_stream import MongodbChangeStreamCdc
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
from services.sync_cursor import (
    build_cursor_key,
    get_watermark,
    map_source_to_target,
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
    return [[str(r.get(h, "")) for h in headers] for r in records]


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
) -> tuple[int, list[str], dict[str, Any], list[str]]:
    """Run a CDC transfer from a database source to a database destination."""
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
    if src_type in {"mongodb", "postgresql"}:
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
            cdc: CdcEngine | MongodbChangeStreamCdc | PostgreSqlChangeStreamCdc = MongodbChangeStreamCdc(
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
                f"(pk={primary_key}, slot={'set' if watermark else 'initial'})"
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
    total_chunks = 1
    chunk_idx = 0

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

        if on_checkpoint:
            on_checkpoint(chunk_idx + 1, total_chunks, state.rows_written, {})

        chunk_idx += 1

    final_watermark = state.running_cursor or watermark
    if final_watermark:
        set_watermark(cursor_key, final_watermark, metadata={"job_id": job_id, "sync_mode": sync_mode})

    summary = state.last_dest_summary or {}
    summary["cdc"] = {
        "inserts": state.inserts,
        "updates": state.updates,
        "deletes": state.deletes,
        "watermark": final_watermark,
    }
    summary["checksum"] = state.last_checksum
    return state.rows_written, ddl_log, summary, headers
