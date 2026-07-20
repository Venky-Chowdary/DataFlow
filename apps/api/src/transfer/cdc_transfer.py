"""Change-data-capture transfer runner for database sources.

Debezium-class capabilities:
  - MongoDB change streams, MySQL binlog (ROW + GTID), PostgreSQL logical
    decoding (txn-buffered peek/ack, ``test_decoding`` / ``pgoutput``)
  - SQL Server native CDC (``cdc.*``) with Change Tracking fallback
  - Oracle LogMiner with flashback versions fallback
  - Snapshot modes: ``initial|always|never|initial_only|when_needed``
  - Incremental snapshot signals interleaved with stream poll
  - Transaction buffering (BEGIN/COMMIT atomic apply batches)

Apply semantics are **at-least-once upsert** (not exactly-once). Job checkpoints
persist watermark progress alongside sync_cursor watermarks.
"""
from __future__ import annotations

import json
import logging
import os
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from bson import json_util

from connectors.mongodb_change_stream import MongodbChangeStreamCdc
from connectors.mysql_change_stream import MySqlChangeStreamCdc
from connectors.oracle_change_stream import OracleFlashbackCdc
from services.cdc_effectively_once import gate_cdc_destination
from connectors.oracle_logminer import OracleLogMinerCdc
from connectors.postgresql_change_stream import PostgreSqlChangeStreamCdc
from connectors.sqlserver_cdc_native import SqlServerNativeCdc
from connectors.sqlserver_change_stream import SqlServerChangeTrackingCdc
from connectors.table_manager import delete_by_primary_keys
from services.cdc_engine import (
    ChangeBatch,
    WatermarkType,
    advance_watermark,
    infer_watermark_type,
    max_watermark,
)
from services.cdc_snapshot_mode import (
    resolve_snapshot_mode,
    should_run_snapshot,
    should_run_stream,
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

logger = logging.getLogger(__name__)


def _cdc_lag_fields(cdc: Any) -> dict[str, Any]:
    """Collect lag / heartbeat / last-DDL / plugin fields from a CDC reader."""
    lag_bytes = None
    lag_seconds = None
    last_ddl = None
    heartbeat_at = None
    plugin = None
    slot_name = None
    if hasattr(cdc, "cdc_metadata"):
        try:
            meta = cdc.cdc_metadata() or {}
            plugin = meta.get("plugin")
            slot_name = meta.get("slot_name")
            if meta.get("replication_lag_bytes") is not None:
                lag_bytes = meta.get("replication_lag_bytes")
            if meta.get("replication_lag_seconds") is not None:
                lag_seconds = meta.get("replication_lag_seconds")
        except Exception:
            pass
    if hasattr(cdc, "replication_lag_bytes") and lag_bytes is None:
        try:
            lag_bytes = cdc.replication_lag_bytes()
        except Exception:
            lag_bytes = None
    if hasattr(cdc, "replication_lag_seconds") and lag_seconds is None:
        try:
            lag_seconds = cdc.replication_lag_seconds()
        except Exception:
            lag_seconds = None
    if lag_seconds is None and hasattr(cdc, "lag_seconds"):
        try:
            lag_seconds = cdc.lag_seconds()
        except Exception:
            lag_seconds = None
    last_ddl = getattr(cdc, "last_ddl_at", None)
    hb = getattr(cdc, "_last_heartbeat_at", None) or getattr(cdc, "_last_event_at", None)
    if isinstance(hb, datetime):
        heartbeat_at = hb.astimezone(timezone.utc).isoformat()
    if plugin is None:
        plugin = getattr(cdc, "output_plugin", None)
    if slot_name is None:
        slot_name = getattr(cdc, "slot_name", None)
    lease_fields: dict[str, Any] = {}
    lease = getattr(cdc, "_lease", None)
    if lease is not None and hasattr(lease, "theater_fields"):
        try:
            lease_fields = dict(lease.theater_fields() or {})
        except Exception:
            lease_fields = {}
    elif hasattr(cdc, "cdc_metadata"):
        try:
            meta = cdc.cdc_metadata() or {}
            for key in (
                "cdc_lease_holder",
                "cdc_lease_resource",
                "cdc_lease_stale",
                "cdc_lease_heartbeat_age_sec",
                "cdc_lease_backend",
                "cdc_lease_generation",
            ):
                if key in meta:
                    lease_fields[key] = meta[key]
        except Exception:
            pass
    return {
        "replication_lag_bytes": lag_bytes,
        "cdc_lag_seconds": lag_seconds,
        "cdc_last_ddl_at": last_ddl,
        "cdc_heartbeat_at": heartbeat_at,
        "cdc_plugin": plugin,
        "cdc_slot_name": slot_name,
        "cdc_delivery": "at-least-once",
        **lease_fields,
        **_source_ha_lag_fields(cdc),
    }


def _source_ha_lag_fields(cdc: Any) -> dict[str, Any]:
    probe = getattr(cdc, "_source_ha", None)
    if probe is None:
        return {}
    try:
        return dict(probe.job_fields())
    except Exception:
        return {}


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


def _truthy_cfg(cfg: dict[str, Any] | None, *keys: str) -> bool:
    raw = cfg or {}
    for key in keys:
        val = raw.get(key)
        if val is True:
            return True
        if isinstance(val, str) and val.strip().lower() in {"1", "true", "yes", "on"}:
            return True
    return False


def _gate_cdc_sink(
    *,
    dest_type: str,
    dest_cfg: dict[str, Any] | None,
    has_primary_key: bool,
) -> dict[str, Any]:
    """Fail-fast append-only CDC sinks unless operator opts in."""
    return gate_cdc_destination(
        dest_type=dest_type,
        has_primary_key=has_primary_key,
        write_mode="upsert",
        allow_append_only=_truthy_cfg(
            dest_cfg, "allow_append_only", "cdc_allow_append_only"
        ),
        require_effectively_once=_truthy_cfg(
            dest_cfg, "require_effectively_once", "cdc_require_effectively_once"
        ),
    )


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
    *,
    backfill_new_fields: bool = False,
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
            backfill_new_fields=backfill_new_fields,
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
            backfill_new_fields=backfill_new_fields,
        )
        rows, last_checksum, dest_summary = with_retry(
            write_op,
            budget=RetryBudget(max_attempts=3, base_delay_seconds=0.5, max_delay_seconds=5.0),
        )
        rows_written += rows

    if change.deletes:
        if not pk_target_col:
            raise ValueError("CDC deletes require a primary key on the destination")
        deleted = delete_by_primary_keys(
            db_type=dest_type,
            cfg=dest_cfg,
            table_name=dest_table,
            primary_key_column=pk_target_col,
            keys=change.deletes,
            schema=dest_cfg.get("schema"),
        )
        # Fail closed: unsupported destinations used to silently no-op deletes.
        if deleted == 0 and change.deletes:
            from connectors.table_manager import UnsupportedCdcDeleteError

            # Re-check: 0 can mean keys already absent (idempotent). Probe support.
            supported = (dest_type or "").lower() in {
                "postgresql",
                "redshift",
                "mysql",
                "sqlite",
                "generic_sql",
                "mongodb",
                "sqlserver",
                "mssql",
                "oracle",
                "snowflake",
                "bigquery",
            }
            if not supported:
                raise UnsupportedCdcDeleteError(
                    f"CDC deletes are not supported for destination type '{dest_type}'"
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
    """Run CDC for each selected stream.

    Prefer Debezium-class shared log reader (one PG slot / one MySQL server_id)
    when all streams share a postgresql or mysql source. Fall back to sequential
    N independent readers otherwise.
    """
    from services.cdc_multi_table import can_share_log_reader

    src_fmt = str(getattr(source, "format", "") or "").lower()
    if can_share_log_reader(src_fmt, len(selected)):
        try:
            return _run_cdc_shared_multi_table(
                source,
                destination,
                mappings,
                schema,
                on_checkpoint,
                sync_mode=sync_mode,
                stream_contracts=stream_contracts,
                selected=selected,
                job_id=job_id,
                checkpoint=checkpoint,
                checkpoint_service=checkpoint_service,
                backfill_new_fields=backfill_new_fields,
                validation_mode=validation_mode,
                limit=limit,
            )
        except Exception as exc:
            from services.cdc_lease import CdcLeaseConflict

            if isinstance(exc, CdcLeaseConflict):
                raise
            logger.warning(
                "Shared multi-table CDC reader unavailable (%s); "
                "falling back to per-table readers",
                exc,
            )

    return _run_cdc_multi_stream_sequential(
        source,
        destination,
        mappings,
        schema,
        on_checkpoint,
        sync_mode=sync_mode,
        stream_contracts=stream_contracts,
        selected=selected,
        job_id=job_id,
        checkpoint=checkpoint,
        checkpoint_service=checkpoint_service,
        backfill_new_fields=backfill_new_fields,
        validation_mode=validation_mode,
        limit=limit,
    )


def _run_cdc_shared_multi_table(
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
    """One log consumer for N tables (Debezium-class); demux apply per stream.

    Semantics remain **at-least-once upsert**. Shared LSN/GTID advances only after
    the demux barrier batch (``ack_barrier``) is applied.
    """
    from services.cdc_multi_table import shared_route_cursor_key, should_ack_shared_batch
    from services.cdc_resume_tokens import is_durable_log_resume_token, is_side_channel_resume_token

    src_type = resolve_driver_type(getattr(source, "format", "") or "")
    dest_type = resolve_driver_type(getattr(destination, "format", "") or "")
    src_cfg = resolve_connector_config(source)
    dest_cfg = resolve_connector_config(destination)

    tables = [(c.name or "").strip() for c in selected if (c.name or "").strip()]
    if len(tables) < 2:
        raise RuntimeError("shared multi-table CDC requires ≥2 tables")

    primary_keys = {
        (c.name or "").strip(): str(c.primary_key or "id")
        for c in selected
        if (c.name or "").strip()
    }
    _gate_cdc_sink(
        dest_type=dest_type,
        dest_cfg=dest_cfg,
        has_primary_key=all(bool(pk) for pk in primary_keys.values()),
    )
    stream_cfg: dict[str, dict[str, Any]] = {}
    for contract in selected:
        name = (contract.name or "").strip()
        if not name:
            continue
        raw = next((c for c in stream_contracts if c.get("name") == name), {}) or {}
        stream_maps = raw.get("mappings")
        use_maps = stream_maps if isinstance(stream_maps, list) and stream_maps else mappings
        stream_cfg[name] = {
            "primary_key": str(contract.primary_key or primary_keys.get(name) or "id"),
            "cursor_field": str(contract.cursor_field or ""),
            "mappings": use_maps,
            "cursor_key": build_cursor_key(
                source_type=src_type,
                source_database=str(src_cfg.get("database") or ""),
                source_object=name,
                dest_type=dest_type,
                dest_database=str(dest_cfg.get("database") or ""),
                dest_object=name,
                stream_name=name,
            ),
        }

    shared_key = shared_route_cursor_key(
        engine=src_type,
        database=str(src_cfg.get("database") or ""),
        tables=tables,
        job_id=job_id,
    )
    shared_wm = get_watermark(shared_key)

    cdc: Any
    ddl_log: list[str] = [
        f"CDC(shared_reader) {src_type} tables={tables} → {dest_type} "
        f"(one slot/server_id; at-least-once upsert)"
    ]
    if src_type in {"postgresql", "postgres"}:
        from services.dialect_profiles import default_schema_for

        cdc = PostgreSqlChangeStreamCdc(
            {**src_cfg, "job_id": job_id},
            table=tables,
            primary_key=primary_keys.get(tables[0], "id"),
            primary_keys=primary_keys,
            cursor_key=shared_key,
            schema=src_cfg.get("schema") or default_schema_for("postgresql") or "public",
            columns=list(schema.keys()) or None,
            resume_token=shared_wm,
            batch_size=CHUNK_SIZE,
        )
        if not cdc.is_available():
            raise RuntimeError("PostgreSQL shared logical decoding not available")
    elif src_type == "mysql":
        cdc = MySqlChangeStreamCdc(
            {**src_cfg, "job_id": job_id},
            table=tables,
            primary_key=primary_keys.get(tables[0], "id"),
            primary_keys=primary_keys,
            columns=list(schema.keys()) or None,
            resume_token=shared_wm,
            batch_size=CHUNK_SIZE,
            cursor_key=shared_key,
        )
        if not cdc.is_available():
            raise RuntimeError("MySQL shared binlog reader not available")
    elif src_type in {"sqlserver", "mssql"}:
        from services.dialect_profiles import default_schema_for

        cdc = SqlServerNativeCdc(
            {**src_cfg, "job_id": job_id},
            table=tables,
            primary_key=primary_keys.get(tables[0], "id"),
            primary_keys=primary_keys,
            schema=str(src_cfg.get("schema") or default_schema_for("sqlserver") or "dbo"),
            resume_token=shared_wm if isinstance(shared_wm, str) else (
                json.dumps(shared_wm) if shared_wm else None
            ),
            batch_size=CHUNK_SIZE,
            cursor_key=shared_key,
            row_filter=str(src_cfg.get("cdc_row_filter") or src_cfg.get("row_filter") or ""),
        )
        if not cdc.is_available():
            raise RuntimeError(
                "SQL Server shared native CDC not available "
                "(enable CDC on the database and each selected table)"
            )
    elif src_type == "oracle":
        from services.dialect_profiles import default_schema_for

        cdc = OracleLogMinerCdc(
            {**src_cfg, "job_id": job_id},
            table=tables,
            primary_key=primary_keys.get(tables[0], "id"),
            primary_keys=primary_keys,
            schema=str(
                src_cfg.get("schema")
                or src_cfg.get("username")
                or default_schema_for("oracle")
                or ""
            ),
            resume_token=shared_wm if isinstance(shared_wm, str) else (
                json.dumps(shared_wm) if shared_wm else None
            ),
            batch_size=CHUNK_SIZE,
            cursor_key=shared_key,
        )
        if not cdc.is_available():
            raise RuntimeError(
                "Oracle shared LogMiner CDC not available "
                "(need LogMiner privileges + supplemental logging)"
            )
    else:
        raise RuntimeError(f"shared multi-table CDC unsupported for {src_type}")

    try:
        from services.source_ha_probe import attach_source_ha

        ha = attach_source_ha(cdc, src_cfg)
        if ha is not None:
            ddl_log.append(f"source_ha role={ha.role} topology={ha.topology}")
    except Exception:
        pass

    snapshot_mode = resolve_snapshot_mode(
        stream_contracts,
        cfg_snapshot_mode=str(src_cfg.get("snapshot_mode") or ""),
    )
    run_snapshot = should_run_snapshot(snapshot_mode, watermark=shared_wm)
    run_stream = should_run_stream(snapshot_mode)
    ddl_log.append(f"CDC snapshot_mode={snapshot_mode.value} shared_reader=1")

    total_rows = 0
    stream_health: dict[str, dict[str, Any]] = {
        t: {"name": t, "status": "running", "records_processed": 0} for t in tables
    }
    chunk_idx = 0
    headers = list(schema.keys())
    last_summary: dict[str, Any] = {}
    original_dest_table = getattr(destination, "table", None)
    original_dest_collection = getattr(destination, "collection", None)

    def _resolve_stream(change: ChangeBatch) -> str:
        name = (change.table or "").strip()
        if name and name in stream_cfg:
            return name
        # Case-insensitive match for MySQL/PG identifier quirks.
        lower = name.lower()
        for t in tables:
            if t.lower() == lower:
                return t
        return tables[0]

    def _apply_tagged(change: ChangeBatch) -> bool:
        nonlocal total_rows, chunk_idx, headers, last_summary
        stream = _resolve_stream(change)
        cfg = stream_cfg[stream]
        use_maps = cfg["mappings"]
        pk = cfg["primary_key"]
        pk_target = map_source_to_target(pk, use_maps) or pk
        if original_dest_table is not None or original_dest_collection is not None:
            if getattr(destination, "format", "") == "mongodb" or original_dest_collection:
                destination.collection = stream
            else:
                destination.table = stream
        dest_table = resolve_dest_table(dest_type, destination)
        col_types = dict(schema)
        if change.inserts or change.updates:
            sample = (change.inserts or change.updates)[0]
            headers = list(sample.keys())
        rows_written, checksum, dest_summary, deleted = _apply_change_batch(
            dest_type,
            destination,
            dest_cfg,
            dest_table,
            change,
            use_maps,
            col_types,
            headers,
            pk_target,
            chunk_idx,
            max(1, chunk_idx + 1),
            backfill_new_fields=backfill_new_fields,
        )
        chunk_idx += 1
        total_rows += rows_written + deleted
        stream_health[stream]["records_processed"] = (
            int(stream_health[stream].get("records_processed") or 0) + rows_written + deleted
        )
        if dest_summary:
            last_summary = dest_summary

        skip_ack = False
        if change.resume_token is not None:
            if is_side_channel_resume_token(change.resume_token):
                skip_ack = True
            else:
                token_s: str
                try:
                    token_s = json.dumps(change.resume_token, default=json_util.default)
                except TypeError:
                    token_s = str(change.resume_token)
                set_watermark(
                    cfg["cursor_key"],
                    token_s,
                    metadata={"job_id": job_id, "sync_mode": sync_mode, "shared_reader": True},
                )
                if should_ack_shared_batch(change) and not skip_ack:
                    set_watermark(
                        shared_key,
                        token_s,
                        metadata={
                            "job_id": job_id,
                            "sync_mode": sync_mode,
                            "tables": tables,
                            "shared_reader": True,
                        },
                    )
                    if hasattr(cdc, "ack") and (
                        is_durable_log_resume_token(change.resume_token)
                        or isinstance(change.resume_token, str)
                    ):
                        try:
                            cdc.ack(change.resume_token)
                        except Exception as ack_exc:
                            logger.warning(
                                "Shared CDC ack failed (at-least-once redelivery): %s",
                                ack_exc,
                            )
        if on_checkpoint:
            on_checkpoint(
                chunk_idx,
                max(1, chunk_idx),
                total_rows,
                {
                    "chunk_index": chunk_idx,
                    "watermark": shared_wm,
                    "rows_written": total_rows,
                    "streams": list(stream_health.values()),
                    "cdc_delivery": "at-least-once",
                    "cdc_shared_reader": True,
                    **_cdc_lag_fields(cdc),
                },
            )
        return bool(change.total_changes)

    try:
        if run_snapshot:
            for change in cdc.snapshot():
                _apply_tagged(change)
                if limit and total_rows >= limit:
                    break
        if run_stream and not (limit and total_rows >= limit):
            max_idle = max(1, int(os.getenv("DATAFLOW_CDC_MAX_IDLE_POLLS", "3")))
            max_rounds = max(1, int(os.getenv("DATAFLOW_CDC_MAX_POLL_ROUNDS", "50")))
            idle = 0
            for _ in range(max_rounds):
                had = False
                for change in cdc.poll():
                    if _apply_tagged(change):
                        had = True
                    if limit and total_rows >= limit:
                        break
                if limit and total_rows >= limit:
                    break
                if had:
                    idle = 0
                else:
                    idle += 1
                    if idle >= max_idle:
                        break
    finally:
        if original_dest_table is not None:
            destination.table = original_dest_table
        if original_dest_collection is not None:
            destination.collection = original_dest_collection
        if hasattr(cdc, "close"):
            try:
                cdc.close()
            except Exception:
                pass

    for h in stream_health.values():
        h["status"] = "completed"
    lag_fields = _cdc_lag_fields(cdc)
    last_summary = dict(last_summary or {})
    last_summary["streams"] = list(stream_health.values())
    last_summary["cdc"] = {
        "shared_reader": True,
        "tables": tables,
        "watermark": get_watermark(shared_key),
        **lag_fields,
    }
    last_summary["cdc_delivery"] = "at-least-once"
    last_summary["cdc_shared_reader"] = True
    last_summary["snapshot_mode"] = snapshot_mode.value
    for k, v in lag_fields.items():
        last_summary[k] = v
    return total_rows, ddl_log, last_summary, headers


def _run_cdc_multi_stream_sequential(
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
    """Legacy path: N independent CDC readers (N slots / N server_ids)."""
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
            # Prefer per-stream mappings when the operator mapped each stream on Map.
            stream_maps = single_contracts[0].get("mappings")
            use_mappings = stream_maps if isinstance(stream_maps, list) and stream_maps else mappings
            status = "completed"
            error: str | None = None
            rows = 0
            summary: dict[str, Any] = {}
            try:
                rows, stream_ddl, summary, headers = _run_cdc_single_stream(
                    source,
                    destination,
                    use_mappings,
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
    # Driver type is used for generic read/write; CDC source kind uses the
    # catalog format so sqlserver/oracle are not collapsed to generic_sql.
    src_driver = resolve_driver_type(source.format)
    dest_type = resolve_driver_type(destination.format)
    src_format = (source.format or src_driver or "").strip().lower().replace("-", "_")
    if src_format in {"mssql", "sql_server"}:
        src_format = "sqlserver"
    src_type = src_format if src_format in {
        "mongodb",
        "mysql",
        "postgresql",
        "postgres",
        "sqlserver",
        "oracle",
    } else src_driver
    if src_type == "postgres":
        src_type = "postgresql"
    src_cfg = resolve_connector_config(source)
    dest_cfg = resolve_connector_config(destination)
    table_name = source.table or source.collection or ""
    dest_table = resolve_dest_table(dest_type, destination, table_name)

    contract = resolve_sync_contract(stream_contracts)
    primary_key = contract.primary_key if contract else ""
    cursor_field = contract.cursor_field if contract else ""
    if not primary_key:
        raise ValueError("CDC sync requires primary_key in the stream contract")
    _gate_cdc_sink(
        dest_type=dest_type,
        dest_cfg=dest_cfg,
        has_primary_key=True,
    )
    if src_type in {"mongodb", "mysql", "postgresql", "sqlserver", "oracle"}:
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
                src_driver,
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
                {**src_cfg, "job_id": job_id, "lease_holder_id": ""},
                table=table_name,
                primary_key=primary_key,
                columns=headers,
                resume_token=watermark,
                batch_size=CHUNK_SIZE,
                cursor_key=cursor_key,
            )
            if not cdc.is_available():
                raise RuntimeError("MySQL binlog not available; falling back to query CDC")
            ddl_log = [
                f"CDC(binlog) {src_type}.{table_name} → {dest_type}.{dest_table} "
                f"(pk={primary_key}, resume={'set' if watermark else 'initial'})"
            ]
        except Exception as exc:
            from services.cdc_lease import CdcLeaseConflict

            if isinstance(exc, CdcLeaseConflict):
                raise
            cdc = CdcEngine(
                src_cfg,
                src_driver,
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
            from services.dialect_profiles import default_schema_for

            cdc = PostgreSqlChangeStreamCdc(
                {**src_cfg, "job_id": job_id},
                table=table_name,
                primary_key=primary_key,
                cursor_key=cursor_key,
                schema=src_cfg.get("schema") or default_schema_for("postgresql") or "public",
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
        except Exception as exc:
            from services.cdc_lease import CdcLeaseConflict

            if isinstance(exc, CdcLeaseConflict):
                raise
            cdc = CdcEngine(
                src_cfg,
                src_driver,
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
            try:
                from services.ops_metrics import record_cdc_poll

                record_cdc_poll(used_query_fallback=True)
            except Exception:
                pass
    elif src_type in {"sqlserver", "mssql"}:
        from services.dialect_profiles import default_schema_for

        ss_schema = src_cfg.get("schema") or default_schema_for("sqlserver") or "dbo"
        cdc = None
        try:
            native = SqlServerNativeCdc(
                {**src_cfg, "job_id": job_id},
                table=table_name,
                primary_key=primary_key,
                schema=ss_schema,
                resume_token=watermark,
                batch_size=CHUNK_SIZE,
                cursor_key=cursor_key,
            )
            if native.is_available():
                cdc = native
                ddl_log = [
                    f"CDC(sqlserver_native) {src_type}.{table_name} → {dest_type}.{dest_table} "
                    f"(pk={primary_key}, resume={'set' if watermark else 'initial'})"
                ]
        except Exception as exc:
            from services.cdc_lease import CdcLeaseConflict

            if isinstance(exc, CdcLeaseConflict):
                raise
            cdc = None
        if cdc is None:
            try:
                cdc = SqlServerChangeTrackingCdc(
                    {**src_cfg, "job_id": job_id},
                    table=table_name,
                    primary_key=primary_key,
                    schema=ss_schema,
                    resume_token=watermark,
                    batch_size=CHUNK_SIZE,
                    cursor_key=cursor_key,
                )
                if not cdc.is_available():
                    raise RuntimeError("SQL Server CDC/CT not available; falling back to query CDC")
                ddl_log = [
                    f"CDC(change_tracking) {src_type}.{table_name} → {dest_type}.{dest_table} "
                    f"(pk={primary_key}, resume={'set' if watermark else 'initial'})"
                ]
            except Exception as exc:
                from services.cdc_lease import CdcLeaseConflict

                if isinstance(exc, CdcLeaseConflict):
                    raise
                cdc = CdcEngine(
                    src_cfg,
                    src_driver,
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
                try:
                    from services.ops_metrics import record_cdc_poll

                    record_cdc_poll(used_query_fallback=True)
                except Exception:
                    pass
    elif src_type == "oracle":
        from services.dialect_profiles import normalize_schema as _norm_schema

        ora_schema = _norm_schema(
            "oracle", src_cfg.get("schema"), username=src_cfg.get("username")
        ) or ""
        cdc = None
        try:
            logminer = OracleLogMinerCdc(
                {**src_cfg, "job_id": job_id},
                table=table_name,
                primary_key=primary_key,
                schema=ora_schema,
                resume_token=watermark,
                batch_size=CHUNK_SIZE,
                cursor_key=cursor_key,
            )
            if logminer.is_available():
                cdc = logminer
                ddl_log = [
                    f"CDC(logminer) {src_type}.{table_name} → {dest_type}.{dest_table} "
                    f"(pk={primary_key}, resume={'set' if watermark else 'initial'})"
                ]
        except Exception as exc:
            from services.cdc_lease import CdcLeaseConflict

            if isinstance(exc, CdcLeaseConflict):
                raise
            cdc = None
        if cdc is None:
            try:
                cdc = OracleFlashbackCdc(
                    {**src_cfg, "job_id": job_id},
                    table=table_name,
                    primary_key=primary_key,
                    schema=ora_schema,
                    resume_token=watermark,
                    batch_size=CHUNK_SIZE,
                    cursor_key=cursor_key,
                )
                if not cdc.is_available():
                    raise RuntimeError("Oracle LogMiner/flashback not available; falling back to query CDC")
                ddl_log = [
                    f"CDC(flashback) {src_type}.{table_name} → {dest_type}.{dest_table} "
                    f"(pk={primary_key}, resume={'set' if watermark else 'initial'})"
                ]
            except Exception as exc:
                from services.cdc_lease import CdcLeaseConflict

                if isinstance(exc, CdcLeaseConflict):
                    raise
                cdc = CdcEngine(
                    src_cfg,
                    src_driver,
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
                try:
                    from services.ops_metrics import record_cdc_poll

                    record_cdc_poll(used_query_fallback=True)
                except Exception:
                    pass
    else:
        cdc = CdcEngine(
            src_cfg,
            src_driver,
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

    try:
        from services.source_ha_probe import attach_source_ha

        ha = attach_source_ha(cdc, src_cfg)
        if ha is not None:
            ddl_log.append(f"source_ha role={ha.role} topology={ha.topology}")
    except Exception:
        pass

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

    import os

    # Continuous CDC: drain snapshot, then poll until idle or budget exhausted.
    max_idle_polls = max(1, int(os.getenv("DATAFLOW_CDC_MAX_IDLE_POLLS", "3")))
    max_poll_rounds = max(1, int(os.getenv("DATAFLOW_CDC_MAX_POLL_ROUNDS", "50")))
    txn_hold_sleep = float(os.getenv("DATAFLOW_CDC_TXN_HOLD_SLEEP_SEC", "0.25"))

    def _apply_and_checkpoint(change: ChangeBatch) -> bool:
        """Apply one batch, persist watermark, ack source. Returns True if data moved."""
        nonlocal chunk_idx, total_chunks
        from services.cdc_resume_tokens import (
            is_durable_log_resume_token,
            is_side_channel_resume_token,
            is_txn_held_token,
        )

        if not change.total_changes and change.resume_token is None:
            return False

        # Mid-txn hold: no watermark/ack. Treat as non-progress so one open txn
        # cannot busy-spin and starve sibling streams under load.
        if is_txn_held_token(change.resume_token):
            if txn_hold_sleep > 0:
                time.sleep(min(txn_hold_sleep, 2.0))
            return False

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
            backfill_new_fields=backfill_new_fields,
        )
        state.rows_written += rows_written
        state.inserts += len(change.inserts)
        state.updates += len(change.updates)
        state.deletes += deleted
        state.last_checksum = last_checksum or state.last_checksum
        if dest_summary:
            state.last_dest_summary = dest_summary

        # Never overwrite a durable log resume with incremental/side-channel tokens
        # (binlog gaps / wrong PG slots under load). Never ack those tokens either.
        skip_ack = False
        if change.resume_token is not None:
            if is_side_channel_resume_token(change.resume_token):
                skip_ack = True
            elif is_durable_log_resume_token(change.resume_token):
                try:
                    state.running_cursor = json.dumps(
                        change.resume_token, default=json_util.default
                    )
                except TypeError:
                    state.running_cursor = str(change.resume_token)
            else:
                try:
                    state.running_cursor = json.dumps(
                        change.resume_token, default=json_util.default
                    )
                except TypeError:
                    state.running_cursor = str(change.resume_token)
        elif change.inserts or change.updates:
            values = [
                r.get(cursor_field)
                for r in (change.inserts + change.updates)
                if r.get(cursor_field) is not None
            ]
            if values:
                wm_type = infer_watermark_type([str(v) for v in values])
                batch_max = max_watermark([str(v) for v in values], wm_type)
                if batch_max:
                    new_watermark, advanced = advance_watermark(
                        state.running_cursor, [batch_max], wm_type
                    )
                    if advanced and new_watermark is not None:
                        state.running_cursor = new_watermark

        chunk_idx += 1
        total_chunks = max(total_chunks, chunk_idx)
        lag_fields = _cdc_lag_fields(cdc)
        try:
            from services.ops_metrics import record_cdc_poll

            record_cdc_poll(
                lag_seconds=lag_fields.get("cdc_lag_seconds"),
                job_id=str(job_id or ""),
                stream=str(table_name or ""),
            )
        except Exception:
            pass
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
        # Ack source only AFTER durable watermark (peek→apply→ack).
        if hasattr(cdc, "ack") and not skip_ack:
            try:
                cdc.ack(change.resume_token)
            except Exception as ack_exc:
                logger.warning(
                    "CDC ack failed after watermark persist (at-least-once redelivery): %s",
                    ack_exc,
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
                    "cdc_plugin": lag_fields.get("cdc_plugin"),
                    "cdc_slot_name": lag_fields.get("cdc_slot_name"),
                    "cdc_delivery": lag_fields.get("cdc_delivery"),
                    "cdc_lease_holder": lag_fields.get("cdc_lease_holder"),
                    "cdc_lease_resource": lag_fields.get("cdc_lease_resource"),
                    "cdc_lease_stale": lag_fields.get("cdc_lease_stale"),
                    "cdc_lease_backend": lag_fields.get("cdc_lease_backend"),
                    "cdc_lease_generation": lag_fields.get("cdc_lease_generation"),
                    "source_ha_role": lag_fields.get("source_ha_role"),
                    "source_ha_topology": lag_fields.get("source_ha_topology"),
                    "source_ha_enabled": lag_fields.get("source_ha_enabled"),
                    "source_ha_group": lag_fields.get("source_ha_group"),
                    "source_ha_replica": lag_fields.get("source_ha_replica"),
                    "source_ha_message": lag_fields.get("source_ha_message"),
                    "watermark": state.running_cursor,
                    "cdc": {
                        "inserts": state.inserts,
                        "updates": state.updates,
                        "deletes": state.deletes,
                        **lag_fields,
                    },
                },
            )
        return bool(change.total_changes)

    snapshot_mode = resolve_snapshot_mode(
        stream_contracts,
        cfg_snapshot_mode=str(src_cfg.get("snapshot_mode") or ""),
    )
    run_snapshot = should_run_snapshot(snapshot_mode, watermark=watermark)
    run_stream = should_run_stream(snapshot_mode)
    ddl_log.append(f"CDC snapshot_mode={snapshot_mode.value}")

    if run_snapshot:
        for change in cdc.snapshot():
            _apply_and_checkpoint(change)

    # Query CDC (CdcEngine): one incremental pass when resuming. Log CDC adapters
    # continuously poll until idle so a single job drains the slot/binlog/CT stream.
    if run_stream:
        if isinstance(cdc, CdcEngine):
            if watermark is not None or not run_snapshot:
                for change in cdc.poll():
                    _apply_and_checkpoint(change)
        else:
            idle_polls = 0
            for _round in range(max_poll_rounds):
                had_data = False
                for change in cdc.poll():
                    if _apply_and_checkpoint(change):
                        had_data = True
                if had_data:
                    idle_polls = 0
                else:
                    idle_polls += 1
                    if idle_polls >= max_idle_polls:
                        break

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
        "poll_rounds": max_poll_rounds,
        **lag_fields,
    }
    summary["cdc_lag_seconds"] = lag_fields.get("cdc_lag_seconds")
    summary["replication_lag_bytes"] = lag_fields.get("replication_lag_bytes")
    summary["cdc_heartbeat_at"] = lag_fields.get("cdc_heartbeat_at")
    summary["cdc_last_ddl_at"] = lag_fields.get("cdc_last_ddl_at")
    summary["cdc_plugin"] = lag_fields.get("cdc_plugin")
    summary["cdc_slot_name"] = lag_fields.get("cdc_slot_name")
    summary["cdc_delivery"] = lag_fields.get("cdc_delivery")
    summary["cdc_lease_holder"] = lag_fields.get("cdc_lease_holder")
    summary["cdc_lease_resource"] = lag_fields.get("cdc_lease_resource")
    summary["cdc_lease_stale"] = lag_fields.get("cdc_lease_stale")
    summary["cdc_lease_backend"] = lag_fields.get("cdc_lease_backend")
    summary["cdc_lease_generation"] = lag_fields.get("cdc_lease_generation")
    for ha_key in (
        "source_ha_role",
        "source_ha_topology",
        "source_ha_enabled",
        "source_ha_group",
        "source_ha_replica",
        "source_ha_open_mode",
        "source_ha_message",
    ):
        if lag_fields.get(ha_key) is not None:
            summary[ha_key] = lag_fields.get(ha_key)
    summary["snapshot_mode"] = snapshot_mode.value
    summary["watermark"] = final_watermark
    summary["checksum"] = state.last_checksum
    if hasattr(cdc, "close"):
        try:
            cdc.close()
        except Exception:
            pass
    return state.rows_written, ddl_log, summary, headers
