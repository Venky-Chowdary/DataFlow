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
from services.brand_env import getenv_brand
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from bson import json_util
from connectors.mongodb_change_stream import MongodbChangeStreamCdc
from connectors.mysql_change_stream import MySqlChangeStreamCdc
from connectors.oracle_change_stream import OracleFlashbackCdc
from connectors.oracle_logminer import OracleLogMinerCdc
from connectors.postgresql_change_stream import PostgreSqlChangeStreamCdc
from connectors.sqlserver_cdc_native import SqlServerNativeCdc
from connectors.sqlserver_change_stream import SqlServerChangeTrackingCdc
from connectors.table_manager import delete_by_primary_keys
from connectors.writer_common import DF_LSN_COL, extract_cdc_lsn
from services.cdc_effectively_once import gate_cdc_destination
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
from services.replay_safety import classify_replay_safety
from services.sync_cursor import (
    build_cursor_key,
    get_watermark,
    map_source_to_target,
    resolve_selected_sync_contracts,
    resolve_sync_contract,
    set_watermark,
)
from services.value_serializer import cell_to_string

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


def _cdc_span(name: str, **attrs: Any):
    """OTEL span for CDC phases. Fail-open — tracing must never block apply."""
    from contextlib import contextmanager, nullcontext

    @contextmanager
    def _inner():
        try:
            from services.tracing import start_span
        except ImportError:  # pragma: no cover
            yield None
            return
        cleaned = {
            "dataflow.cdc.delivery": "at-least-once",
            **{
                (f"dataflow.{k}" if not str(k).startswith("dataflow.") else str(k)): v
                for k, v in attrs.items()
                if isinstance(v, (str, int, float, bool)) or v is None
            },
        }
        with start_span(name, attributes=cleaned, kind="internal") as span:
            yield span

    try:
        return _inner()
    except Exception:
        return nullcontext()


def _cdc_lag_fields(cdc: Any) -> dict[str, Any]:
    """Collect lag / heartbeat / last-DDL / plugin fields from a CDC reader.

    Lag seconds never invent catch-up from heartbeat age — see
    ``services.cdc_lag_honesty.observe_cdc_lag``.
    """
    from services.cdc_lag_honesty import observe_cdc_lag

    lag_bytes = None
    lag_seconds = None
    lag_basis = None
    heartbeat_age = None
    last_ddl = None
    heartbeat_at = None
    plugin = None
    slot_name = None
    meta: dict[str, Any] = {}
    if hasattr(cdc, "cdc_metadata"):
        try:
            meta = cdc.cdc_metadata() or {}
            plugin = meta.get("plugin")
            slot_name = meta.get("slot_name")
            if meta.get("replication_lag_bytes") is not None:
                lag_bytes = meta.get("replication_lag_bytes")
            if meta.get("replication_lag_seconds") is not None:
                lag_seconds = meta.get("replication_lag_seconds")
            lag_basis = meta.get("cdc_lag_basis")
            heartbeat_age = meta.get("cdc_heartbeat_age_sec")
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
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
    # Recompute via SSOT when commit clock or byte lag is available.
    # Heartbeat-only readers must not keep inventing catch-up seconds.
    commit_at = getattr(cdc, "_last_event_commit_at", None)
    hb_raw = getattr(cdc, "_last_heartbeat_at", None)
    reader_seconds = lag_seconds
    try:
        if isinstance(commit_at, datetime) or lag_bytes is not None:
            obs = observe_cdc_lag(
                last_event_commit_at=commit_at if isinstance(commit_at, datetime) else None,
                last_heartbeat_at=hb_raw if isinstance(hb_raw, datetime) else None,
                replication_lag_bytes=lag_bytes,
            )
            lag_seconds = obs.get("cdc_lag_seconds")
            lag_basis = obs.get("cdc_lag_basis") or lag_basis
            heartbeat_age = obs.get("cdc_heartbeat_age_sec")
            if obs.get("replication_lag_bytes") is not None:
                lag_bytes = obs.get("replication_lag_bytes")
            if obs.get("freshness_severity"):
                meta = {**meta, "freshness_severity": obs.get("freshness_severity")}
            if obs.get("cdc_lag_unknown_reason"):
                meta = {
                    **meta,
                    "cdc_lag_unknown_reason": obs.get("cdc_lag_unknown_reason"),
                }
        else:
            # Legacy dialect lag_seconds only — keep value, never call it catch-up.
            if reader_seconds is not None and not lag_basis:
                lag_basis = "legacy_seconds"
            if isinstance(hb_raw, datetime):
                from services.cdc_lag_honesty import age_seconds

                heartbeat_age = age_seconds(hb_raw)
    except Exception as exc:
        logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
    last_ddl = getattr(cdc, "last_ddl_at", None)
    # Heartbeat clock only — never fall back to last event (that greenwashed lag).
    hb = getattr(cdc, "_last_heartbeat_at", None)
    if isinstance(hb, datetime):
        heartbeat_at = hb.astimezone(timezone.utc).isoformat()
    if plugin is None:
        plugin = getattr(cdc, "output_plugin", None)
    if slot_name is None:
        slot_name = getattr(cdc, "slot_name", None)
    lease_fields: dict[str, Any] = {}
    row_filter = None
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
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
    if hasattr(cdc, "cdc_metadata"):
        try:
            meta = cdc.cdc_metadata() or {}
            if meta.get("cdc_row_filter"):
                row_filter = meta.get("cdc_row_filter")
            elif meta.get("row_filter"):
                row_filter = meta.get("row_filter")
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
    if row_filter is None:
        row_filter = getattr(cdc, "row_filter", None)
    out: dict[str, Any] = {
        "replication_lag_bytes": lag_bytes,
        "cdc_lag_seconds": lag_seconds,
        "cdc_lag_basis": lag_basis,
        "cdc_heartbeat_age_sec": heartbeat_age,
        "cdc_last_ddl_at": last_ddl,
        "cdc_heartbeat_at": heartbeat_at,
        "cdc_plugin": plugin,
        "cdc_slot_name": slot_name,
        "cdc_delivery": "at-least-once",
        **lease_fields,
        **_source_ha_lag_fields(cdc),
        **_cdc_retention_lag_fields(cdc),
    }
    if meta.get("freshness_severity"):
        out["cdc_freshness_severity"] = meta.get("freshness_severity")
    if meta.get("cdc_lag_unknown_reason"):
        out["cdc_lag_unknown_reason"] = meta.get("cdc_lag_unknown_reason")
    # Live pg_replication_slots catalog (PG) beats in-memory consistent_point.
    if meta.get("active") is not None:
        out["cdc_slot_active"] = bool(meta.get("active"))
    if meta.get("slot_exists") is not None:
        out["cdc_slot_exists"] = bool(meta.get("slot_exists"))
    if meta.get("restart_lsn"):
        out["cdc_restart_lsn"] = str(meta.get("restart_lsn"))
    if meta.get("wal_status"):
        out["cdc_wal_status"] = str(meta.get("wal_status"))
        # lost / unreserved is operator-critical — never look "healthy" on lag alone.
        wal = str(meta.get("wal_status") or "").strip().lower()
        if wal in {"lost", "unreserved"} and out.get("cdc_freshness_severity") != "critical":
            out["cdc_freshness_severity"] = "critical"
            plugin_label = str(plugin or meta.get("plugin") or "cdc").strip().lower()
            if "mysql" in plugin_label or "mariadb" in plugin_label or "binlog" in plugin_label:
                reason_prefix = "mysql_binlog"
            elif "sqlserver" in plugin_label or "mssql" in plugin_label:
                reason_prefix = "sqlserver_cdc"
            else:
                reason_prefix = "pg_replication_slots"
            out["cdc_lag_unknown_reason"] = (
                out.get("cdc_lag_unknown_reason")
                or f"{reason_prefix}.wal_status={wal}"
            )
    if meta.get("active") is False and out.get("cdc_freshness_severity") not in {
        "critical",
        "warn",
    }:
        # Inactive slot while job claims streaming — surface warn (lease/other consumer).
        out["cdc_freshness_severity"] = "warn"
    # Retained WAL is the number an on-call engineer needs first: an idle slot
    # that stops advancing fills the primary's disk, and the job looks healthy
    # right up until the database stops accepting writes.
    confirmed = (
        meta.get("confirmed_flush_lsn")
        or getattr(cdc, "consistent_point_lsn", None)
    )
    if confirmed:
        out["cdc_confirmed_flush_lsn"] = str(confirmed)
    # SQL Server native CDC capture window (parity with PG restart / MySQL oldest).
    if meta.get("min_lsn"):
        out["cdc_min_lsn"] = str(meta.get("min_lsn"))
        if not out.get("cdc_restart_lsn"):
            out["cdc_restart_lsn"] = str(meta.get("min_lsn"))
    if meta.get("max_lsn"):
        out["cdc_max_lsn"] = str(meta.get("max_lsn"))
    capture_inst = meta.get("capture_instance") or meta.get("slot_name")
    if capture_inst:
        out["cdc_capture_instance"] = str(capture_inst)
        if not out.get("cdc_slot_name"):
            out["cdc_slot_name"] = str(capture_inst)
    if row_filter:
        out["cdc_row_filter"] = str(row_filter)
    try:
        from services.cdc_mapping_review import open_review_for_source

        source_key = getattr(cdc, "source_key", None) or ""
        table = ""
        if hasattr(cdc, "_qualified_table"):
            try:
                table = str(cdc._qualified_table() or "")
            except Exception:
                table = str(getattr(cdc, "table", "") or "")
        else:
            table = str(getattr(cdc, "table", "") or "")
        review = open_review_for_source(str(source_key), table) if source_key else None
        if review:
            out["mapping_review_required"] = True
            out["mapping_review_id"] = review.get("id")
            out["mapping_review_reason"] = review.get("reason")
            out["mapping_review_honesty"] = review.get("honesty")
    except Exception as exc:
        logging.getLogger(__name__).debug("CDC mapping review lookup skipped: %s", exc)
    return out


def _assert_cdc_lease_before_apply(cdc: Any) -> None:
    """Refuse sink apply when the CDC resource lease was stolen (zombie fence).

    No-op when the connector has no lease guard (unit mocks / non-leased paths).
    Still at-least-once — does not claim platform exactly-once.
    """
    lease = getattr(cdc, "_lease", None)
    if lease is None:
        return
    assert_holder = getattr(lease, "assert_holder", None)
    if callable(assert_holder):
        assert_holder()
        return
    # Older guard shape — renew and raise on fence loss.
    renew = getattr(lease, "renew", None)
    if not callable(renew):
        return
    if renew() is None and getattr(lease, "acquired", True) is False:
        from services.cdc_lease import CdcLeaseConflict

        raise CdcLeaseConflict(
            "CDC lease fenced — refuse zombie apply under at-least-once delivery.",
            holder_id=str(getattr(lease, "holder_id", "") or ""),
            resource=str(getattr(lease, "resource", "") or ""),
            cursor_key=str(getattr(lease, "cursor_key", "") or ""),
        )


def _source_ha_lag_fields(cdc: Any) -> dict[str, Any]:
    probe = getattr(cdc, "_source_ha", None)
    if probe is None:
        return {}
    try:
        return dict(probe.job_fields())
    except Exception:
        return {}


def _cdc_retention_lag_fields(cdc: Any) -> dict[str, Any]:
    try:
        from services.cdc_retention_probe import retention_lag_fields

        return retention_lag_fields(cdc)
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
    # Accumulate quarantine across CDC batches — never keep only the last batch.
    accumulated_rejected_details: list[dict[str, Any]] = field(default_factory=list)
    accumulated_rejected_rows: int = 0
    accumulated_coerced_null_rows: int = 0


def _merge_cdc_dest_summary(
    state: CdcState,
    dest_summary: dict[str, Any] | None,
    *,
    job_id: str = "",
    destination: Any = None,
) -> dict[str, Any]:
    """Merge batch quarantine into CDC state and persist DLQ before watermark.

    Soft-quarantine CDC must still advance (at-least-once + DLQ), but earlier
    batches' rejected_details must not disappear when ``last_dest_summary`` is
    overwritten.
    """
    incoming = dict(dest_summary or {})
    new_details = [
        dict(d) for d in (incoming.get("rejected_details") or []) if isinstance(d, dict)
    ]
    if new_details:
        if not str(job_id or "").strip():
            raise RuntimeError(
                "CDC quarantine rows present but job_id is missing — refuse "
                "watermark advance (cannot durable DLQ; rows cannot disappear)"
            )
        try:
            from services.quarantine_dlq import persist_rejected_rows

            persist_rejected_rows(
                job_id=str(job_id),
                rejected_details=new_details,
                source="cdc_batch",
                connector=str(
                    getattr(destination, "format", None)
                    or getattr(destination, "kind", None)
                    or ""
                ),
            )
            incoming["quarantine_cdc_durable"] = True
            incoming["quarantine_durable"] = True
        except Exception as qexc:
            incoming["quarantine_durable"] = False
            incoming["quarantine_dlq_error"] = str(qexc)[:300]
            raise RuntimeError(
                "CDC quarantine DLQ persist failed — refuse watermark advance "
                f"(rows cannot disappear): {qexc}"
            ) from qexc

    state.accumulated_rejected_details.extend(new_details)
    state.accumulated_rejected_rows += int(incoming.get("rejected_rows") or 0) or len(
        new_details
    )
    state.accumulated_coerced_null_rows += int(incoming.get("coerced_null_rows") or 0)

    merged = {**(state.last_dest_summary or {}), **incoming}
    merged["rejected_details"] = list(state.accumulated_rejected_details)
    merged["rejected_rows"] = int(state.accumulated_rejected_rows)
    merged["coerced_null_rows"] = int(state.accumulated_coerced_null_rows)
    merged["rejected_details_total"] = len(state.accumulated_rejected_details)
    if incoming.get("quarantine_durable"):
        merged["quarantine_dlq_persisted_count"] = len(state.accumulated_rejected_details)
    state.last_dest_summary = merged
    return merged


def _records_to_matrix(records: list[dict[str, Any]], headers: list[str]) -> list[list[str]]:
    """CDC matrix with SQL NULL ≠ empty string; absent keys stay missing sentinels."""
    from services.value_serializer import DF_MISSING_SENTINEL, is_missing_sentinel

    rows: list[list[str]] = []
    for r in records:
        row: list[str] = []
        for h in headers:
            if h not in r:
                row.append(DF_MISSING_SENTINEL)
            else:
                val = r.get(h)
                # Present DF_MISSING must stay omit-from-SET (never cell_to_string → "").
                if is_missing_sentinel(val):
                    row.append(DF_MISSING_SENTINEL)
                else:
                    row.append(cell_to_string(val, preserve_sql_null=True))
        rows.append(row)
    return rows


def _source_headers(headers: list[str], mappings: list[dict[str, Any]]) -> tuple[list[str], list[str]]:
    """Return source headers as expected by _write_batch and the target column list."""
    return headers, [m.get("target", m.get("source", "")).strip() for m in mappings if m.get("source")]


# Exact column names whose truthy value means "this row is deleted". Matching is
# exact, never substring: `deleted_by`, `deleted_reason` and `delete_count` are
# ordinary business columns, and a substring rule turned every row with a
# non-empty `deleted_by` into a destination DELETE.
_TOMBSTONE_COLUMNS = frozenset(
    {
        "deleted",
        "deleted_at",
        "deletedat",
        "deleted_on",
        "deleted_ts",
        "deleted_time",
        "date_deleted",
        "is_deleted",
        "isdeleted",
        "deleted_flag",
        "row_deleted",
        "tombstone",
        "is_tombstone",
        "_deleted",
        "__deleted",
    }
)

#: Columns that look deletion-adjacent but are audit metadata, not tombstones.
#: Listed explicitly so a future rule change cannot quietly re-capture them.
_TOMBSTONE_LOOKALIKES = frozenset(
    {
        "deleted_by",
        "deleted_by_id",
        "deleted_by_user",
        "deleted_reason",
        "delete_count",
        "deletes",
        "deletable",
        "is_deletable",
        "can_delete",
        "soft_delete_enabled",
    }
)

#: Tokens that unambiguously mean "not deleted".
_FALSEY_TOKENS = frozenset({"", "0", "false", "f", "no", "n", "null", "none", "nan"})
#: Tokens that unambiguously mean "deleted".
_TRUTHY_TOKENS = frozenset({"1", "true", "t", "yes", "y"})

#: Timestamp-style tombstones follow the well-known `deleted_at IS NULL` pattern,
#: where *any* value at all means deleted. Boolean-style tombstones do not — an
#: unrecognised token there is ambiguous and must never be read as "delete".
_TIMESTAMP_TOMBSTONES = frozenset(
    {"deleted_at", "deletedat", "deleted_on", "deleted_ts", "deleted_time", "date_deleted"}
)


def _detect_tombstone_column(schema: dict[str, str], columns: list[str]) -> str | None:
    """Return a soft-delete column name if one is unambiguously present.

    Deliberately conservative, because a false positive here converts live
    source rows into destination ``DELETE`` statements. Two rules that used to
    exist have been removed:

    * **Liveness columns are not tombstones.** ``is_active`` was previously
      treated as a deletion marker with no polarity handling, so every row with
      ``is_active = 1`` was deleted at the destination and every inactive row
      was kept — a complete inversion that silently wiped live data. An
      inactive row is also still a row that *exists* in the source, so even the
      corrected polarity would be wrong to delete. An operator who genuinely
      wants that behaviour must configure it explicitly.
    * **Substring matching is gone.** ``"delete" in name`` captured
      ``deleted_by`` and ``delete_count``.
    """
    del schema  # Detection is by name; type is validated at interpretation time.
    for c in columns:
        lowered = (c or "").strip().lower()
        if not lowered or lowered in _TOMBSTONE_LOOKALIKES:
            continue
        if lowered in _TOMBSTONE_COLUMNS:
            return c
    return None


def _is_tombstone_set(record: dict[str, Any], tombstone_column: str) -> bool:
    """Whether ``record`` is marked deleted by its soft-delete column.

    Fails safe: an unrecognised value on a boolean-style column returns
    ``False``. Refusing to delete on ambiguity is recoverable (a stale row that
    a later sync corrects); deleting on ambiguity is not.
    """
    if not tombstone_column:
        return False
    value = record.get(tombstone_column)
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in _FALSEY_TOKENS:
        return False
    if text in _TRUTHY_TOKENS:
        return True
    # Timestamp-style soft deletes: any concrete value means deleted. This is
    # the `deleted_at IS NULL` convention and is safe to read literally.
    if tombstone_column.strip().lower() in _TIMESTAMP_TOMBSTONES:
        return _looks_like_timestamp(text)
    # Boolean-style column carrying something we do not recognise. Do not guess.
    logger.warning(
        "Soft-delete column %r held unrecognised value %r; treating the row as "
        "present rather than deleting it at the destination.",
        tombstone_column,
        text[:64],
    )
    return False


def _looks_like_timestamp(text: str) -> bool:
    """Whether a value is a real instant rather than a zero/sentinel date.

    ``0000-00-00`` and friends are MySQL's "no date" sentinels; reading them as
    a deletion timestamp would delete every row that was never soft-deleted.
    """
    stripped = text.strip()
    if not stripped:
        return False
    if set(stripped) <= {"0", "-", ":", " ", "/", "."}:
        return False
    return True


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
        # Empty-string watermarks are valid (e.g. '' cursor after coalesce) —
        # truthiness checks would re-snapshot forever.
        if cursor_after is not None:
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
                    cursor_column=self.cursor_field if cursor_after is not None else "",
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
        if self.watermark is None:
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
        # CDC transfer always stamps DF_LSN_COL on upsert routes.
        has_lsn_column=True,
        allow_append_only=_truthy_cfg(
            dest_cfg, "allow_append_only", "cdc_allow_append_only"
        ),
        require_effectively_once=_truthy_cfg(
            dest_cfg, "require_effectively_once", "cdc_require_effectively_once"
        ),
    )


def _refuse_cdc_advance_on_abort(
    dest_summary: dict[str, Any] | None,
    validation_mode: str,
) -> None:
    """Raise when CDC must not advance watermark/ack after abort-class rejects.

    At-least-once CDC must redeliver FAIL_JOB / strict-blocked rows. Advancing
    the cursor after quarantine would permanently skip them.
    """
    from connectors.writer_common import (
        reject_on_strict_policy,
        transform_error_policy_for_validation_mode,
    )

    if not isinstance(dest_summary, dict):
        return
    if dest_summary.get("ok") is False:
        raise ValueError(
            str(
                dest_summary.get("error")
                or "CDC destination write blocked — refuse watermark advance"
            )
        )
    abort = reject_on_strict_policy(
        transform_error_policy_for_validation_mode(validation_mode),
        dest_summary.get("rejected_details") or [],
        "CDC",
    )
    if abort:
        raise ValueError(abort)


def _apply_change_batch(
    dest_type: str,
    destination: Any,
    dest_cfg: dict[str, Any],
    dest_table: str,
    change: ChangeBatch,
    mappings: list[dict[str, Any]],
    column_types: dict[str, str],
    headers: list[str],
    pk_target_col: str | list[str],
    chunk_idx: int,
    total_chunks: int,
    *,
    backfill_new_fields: bool = False,
    job_id: str = "",
) -> tuple[int, str, dict[str, Any], int]:
    """Apply a single ChangeBatch to the destination. Returns rows_written, checksum, summary, deleted_count."""
    from services.cdc_snapshot_window import _pk_columns

    # Normalize once so every writer and the delete path see a real column list.
    # A comma-joined string here used to survive into conflict_columns, where
    # every writer then filtered it out as "column not in target".
    pk_target_cols = _pk_columns(pk_target_col) if pk_target_col else []
    headers, mappings, column_types = _stamp_cdc_lsn(
        change, headers, mappings, column_types
    )
    source_headers, target_cols = _source_headers(headers, mappings)
    rows_written = 0
    deleted = 0
    last_checksum = ""
    dest_summary: dict[str, Any] = {}

    # CDC asks for upsert, but without a destination primary key most writers
    # degrade to plain inserts. Replaying such a batch after an ambiguous
    # failure would duplicate change events, so classify before retrying.
    cdc_replay_safety = classify_replay_safety(
        dest_type=dest_type,
        write_mode="upsert",
        conflict_columns=pk_target_cols or None,
        job_id=job_id,
        has_primary_key=bool(pk_target_cols),
    )

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
            create_table=True,
            on_checkpoint=None,
            chunk_idx=chunk_idx,
            total_chunks=total_chunks,
            rows_so_far=0,
            write_mode="upsert",
            conflict_columns=pk_target_cols or None,
            backfill_new_fields=backfill_new_fields,
            job_id=job_id,
            sync_mode="cdc",
        )
        rows, last_checksum, dest_summary = with_retry(
            write_op,
            budget=RetryBudget(max_attempts=3, base_delay_seconds=0.5, max_delay_seconds=5.0),
            replay_safety=cdc_replay_safety,
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
            create_table=True,
            on_checkpoint=None,
            chunk_idx=chunk_idx,
            total_chunks=total_chunks,
            rows_so_far=0,
            write_mode="upsert",
            conflict_columns=pk_target_cols or None,
            backfill_new_fields=backfill_new_fields,
            job_id=job_id,
            sync_mode="cdc",
        )
        rows, last_checksum, dest_summary = with_retry(
            write_op,
            budget=RetryBudget(max_attempts=3, base_delay_seconds=0.5, max_delay_seconds=5.0),
            replay_safety=cdc_replay_safety,
        )
        rows_written += rows

    if change.deletes:
        if not pk_target_cols:
            raise ValueError("CDC deletes require a primary key on the destination")
        deleted = delete_by_primary_keys(
            db_type=dest_type,
            cfg=dest_cfg,
            table_name=dest_table,
            primary_key_column=pk_target_cols,
            keys=change.deletes,
            schema=dest_cfg.get("schema"),
            incoming_lsn=extract_cdc_lsn(change.resume_token),
            lsn_column=DF_LSN_COL,
        )
        # Fail closed: unsupported destinations used to silently no-op deletes.
        if deleted == 0 and change.deletes:
            from connectors.table_manager import UnsupportedCdcDeleteError

            # Re-check: 0 can mean keys already absent (idempotent). Probe support.
            # Keep in sync with connectors.table_manager.delete_by_primary_keys.
            supported = (dest_type or "").lower() in {
                "postgresql",
                "redshift",
                "mysql",
                "sqlite",
                "generic_sql",
                "mongodb",
                "mongo",
                "sqlserver",
                "mssql",
                "oracle",
                "oracle_db",
                "oracle_autonomous_warehouse",
                "snowflake",
                "bigquery",
                "duckdb",
                "databricks",
                "synapse_analytics",
                "azure_sql_database",
                "amazon_rds_sql_server",
                "google_cloud_sql_sql_server",
                "azure_synapse_dedicated",
                "azure_synapse_serverless",
                "iceberg",
                "apache_iceberg",
            }
            if not supported:
                raise UnsupportedCdcDeleteError(
                    f"CDC deletes are not supported for destination type '{dest_type}'"
                )

    # Stash a bounded source sample so Gate-8 reconciliation can compare the
    # rows we just wrote against a read-back of the destination.
    sample_rows = list(change.inserts or []) + list(change.updates or [])
    if dest_summary is None:
        dest_summary = {}
    if sample_rows:
        dest_summary["reconcile_sample"] = sample_rows[:50]
    if change.deletes:
        # PK tombstones for post-write absence proof (not full after-images).
        dest_summary["reconcile_deletes"] = [str(k) for k in change.deletes[:50]]
        dest_summary["reconcile_delete_count"] = len(change.deletes)

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
    from services.cdc_multi_table import (
        shared_route_cursor_key,
        should_ack_shared_batch,
    )
    from services.cdc_resume_tokens import (
        is_durable_log_resume_token,
        is_side_channel_resume_token,
    )

    src_type = resolve_driver_type(getattr(source, "format", "") or "")
    dest_type = resolve_driver_type(getattr(destination, "format", "") or "")
    src_cfg = resolve_connector_config(source)
    dest_cfg = resolve_connector_config(destination)

    tables = [(c.name or "").strip() for c in selected if (c.name or "").strip()]
    if len(tables) < 2:
        raise RuntimeError("shared multi-table CDC requires ≥2 tables")

    from services.cdc_identity import require_cdc_primary_key

    def _cdc_pk_str(raw: Any, table: str) -> str:
        resolved = require_cdc_primary_key(raw, table=table)
        return ",".join(resolved) if isinstance(resolved, list) else resolved

    primary_keys = {
        (c.name or "").strip(): _cdc_pk_str(c.primary_key, (c.name or "").strip())
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
            "primary_key": _cdc_pk_str(
                contract.primary_key or primary_keys.get(name), name
            ),
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
            primary_key=primary_keys[tables[0]],
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
            primary_key=primary_keys[tables[0]],
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
            primary_key=primary_keys[tables[0]],
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
            primary_key=primary_keys[tables[0]],
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
    except Exception as exc:
        logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
    try:
        from services.cdc_retention_probe import attach_cdc_retention

        ret = attach_cdc_retention(cdc, src_cfg, table=tables[0] if tables else "")
        if ret is not None:
            ddl_log.append(f"cdc_retention status={ret.status}")
    except Exception as exc:
        logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)

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
    shared_accum = CdcState()  # quarantine accumulate across shared-reader batches
    original_dest_table = getattr(destination, "table", None)
    original_dest_collection = getattr(destination, "collection", None)

    def _resolve_stream(change: ChangeBatch) -> str:
        """Map a demuxed batch back to the stream that produced it.

        Raises rather than guessing. The previous ``return tables[0]`` fallback
        meant any batch whose ``table`` tag was missing or unrecognised was
        written into the *first* configured table's destination — rows silently
        landing in the wrong table, with no error and nothing quarantined.
        A shared reader that cannot attribute a batch is a bug in the reader,
        and failing the job is the only safe response.
        """
        name = (change.table or "").strip()
        if name and name in stream_cfg:
            return name
        # Case-insensitive match for MySQL/PG identifier quirks.
        lower = name.lower()
        for t in tables:
            if t.lower() == lower:
                return t
        # Bare table name against a schema-qualified config (or vice versa).
        if lower:
            bare = lower.rsplit(".", 1)[-1]
            for t in tables:
                if t.lower().rsplit(".", 1)[-1] == bare:
                    return t
        if len(tables) == 1:
            # Single-table run: attribution is unambiguous regardless of tagging.
            return tables[0]
        if change.total_changes == 0:
            # Position-only batch (phase transition / ack barrier). It carries no
            # rows, so there is nothing to attribute and nothing to mis-route.
            return tables[0]
        raise ValueError(
            f"Shared CDC reader produced a batch of {change.total_changes} change(s) "
            f"for table {name!r}, which is not one of the captured tables "
            f"{sorted(tables)}. Refusing to write it to an arbitrary destination — "
            "rows would land in the wrong table."
        )

    # Per-table cursors staged for the transaction currently being applied. One
    # source transaction is demuxed into one batch per touched table, and only
    # the last carries ``ack_barrier``. Publishing a table's cursor as soon as
    # its own batch lands would mark that table caught up to the commit while
    # sibling tables from the same transaction are still unapplied — and if the
    # run then dies, resuming those tables individually starts *after* changes
    # that never landed, so the tables diverge permanently with nothing
    # recording that anything is missing. Staging here and flushing on the
    # barrier makes the whole transaction advance together or not at all.
    pending_table_watermarks: dict[str, str] = {}

    def _flush_table_watermarks() -> None:
        """Publish staged per-table cursors now that the transaction is fully applied."""
        if not pending_table_watermarks:
            return
        for name, token in pending_table_watermarks.items():
            set_watermark(
                stream_cfg[name]["cursor_key"],
                token,
                metadata={
                    "job_id": job_id,
                    "sync_mode": sync_mode,
                    "shared_reader": True,
                    "txn_consistent": True,
                },
            )
        pending_table_watermarks.clear()

    def _apply_tagged(change: ChangeBatch) -> bool:
        nonlocal total_rows, chunk_idx, headers, last_summary
        stream = _resolve_stream(change)
        cfg = stream_cfg[stream]
        use_maps = cfg["mappings"]
        from services.cdc_snapshot_window import _pk_columns

        pk_source = _pk_columns(cfg["primary_key"])
        pk_target = [map_source_to_target(c, use_maps) or c for c in pk_source]
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
        _assert_cdc_lease_before_apply(cdc)
        with _cdc_span(
            "cdc.apply_batch",
            job_id=str(job_id or ""),
            dest_table=str(dest_table or ""),
            stream=str(stream or ""),
            chunk_idx=int(chunk_idx),
        ):
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
                job_id=str(job_id or ""),
            )
        chunk_idx += 1
        total_rows += rows_written + deleted
        stream_health[stream]["records_processed"] = (
            int(stream_health[stream].get("records_processed") or 0) + rows_written + deleted
        )
        if dest_summary:
            last_summary = _merge_cdc_dest_summary(
                shared_accum,
                dest_summary,
                job_id=str(job_id or ""),
                destination=destination,
            )
        _refuse_cdc_advance_on_abort(dest_summary, validation_mode)

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
                # Stage, do not publish. Only a table that actually received a
                # batch in this transaction advances, so a table untouched by the
                # commit keeps its previous position.
                if change.total_changes:
                    pending_table_watermarks[stream] = token_s
                if should_ack_shared_batch(change) and not skip_ack:
                    # Barrier reached: the whole transaction is applied, so the
                    # per-table cursors and the shared log position may both move.
                    # A position-only barrier (heartbeat, or a commit that touched
                    # no captured table) advances the log but no table cursor.
                    _flush_table_watermarks()
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
                    "rejected_details": list(
                        (last_summary or {}).get("rejected_details") or []
                    ),
                    "rejected_rows": int((last_summary or {}).get("rejected_rows") or 0),
                    **_cdc_lag_fields(cdc),
                },
            )
        return bool(change.total_changes)

    try:
        if run_snapshot:
            with _cdc_span("cdc.snapshot", job_id=str(job_id or ""), shared_reader=True):
                for change in cdc.snapshot():
                    _apply_tagged(change)
                    if limit and total_rows >= limit:
                        break
        if run_stream and not (limit and total_rows >= limit):
            max_idle = max(1, int(getenv_brand("CDC_MAX_IDLE_POLLS", "3")))
            max_rounds = max(1, int(getenv_brand("CDC_MAX_POLL_ROUNDS", "50")))
            idle = 0
            with _cdc_span("cdc.poll", job_id=str(job_id or ""), shared_reader=True):
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
            except Exception as exc:
                logging.getLogger(__name__).debug("Exception suppressed: %s", exc, exc_info=exc)

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
    # Always expand to a column list. A comma-joined composite left as one
    # string made every writer filter the conflict list to empty (no column
    # named "order_id,line_id"), so CDC silently degraded to append-only
    # inserts and every delete vanished.
    pk_source_cols = (
        list(contract.primary_key_columns())
        if contract is not None
        else [c.strip() for c in primary_key.replace(";", ",").split(",") if c.strip()]
    )
    if not pk_source_cols:
        raise ValueError("CDC sync requires primary_key in the stream contract")
    _gate_cdc_sink(
        dest_type=dest_type,
        dest_cfg=dest_cfg,
        has_primary_key=True,
    )
    if src_type in {"mongodb", "mysql", "postgresql", "sqlserver", "oracle"}:
        cursor_field = cursor_field or pk_source_cols[0] or (
            "_id" if src_type == "mongodb" else "id"
        )
    elif not cursor_field:
        raise ValueError("CDC sync requires cursor_field in the stream contract")

    pk_target_cols = [map_source_to_target(c, mappings) for c in pk_source_cols]
    if any(not c for c in pk_target_cols):
        raise ValueError(
            "CDC sync requires every primary-key column to map to a destination "
            f"column; unmapped sources={pk_source_cols!r}"
        )
    # Single-column shorthand kept for call sites that still take a string
    # (readers, cursor defaults). Composite deletes/upserts use the list.
    pk_target_col = pk_target_cols[0] if len(pk_target_cols) == 1 else ",".join(pk_target_cols)
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
                {
                    **src_cfg,
                    "job_id": job_id,
                    "cursor_key": cursor_key,
                    "lease_holder_id": "",
                },
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
            except Exception as exc:
                logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
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
                except Exception as exc:
                    logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
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
                except Exception as exc:
                    logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
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
    except Exception as exc:
        logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
    try:
        from services.cdc_retention_probe import attach_cdc_retention

        ret = attach_cdc_retention(cdc, src_cfg, table=table_name)
        if ret is not None:
            ddl_log.append(f"cdc_retention status={ret.status}")
    except Exception as exc:
        logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)

    state = CdcState(cursor_key=cursor_key, watermark=watermark)
    # Resume from durable job checkpoint watermark when present.
    cp_dict: dict[str, Any] = {}
    if checkpoint is not None:
        if isinstance(checkpoint, dict):
            cp_dict = checkpoint
        elif hasattr(checkpoint, "to_dict"):
            cp_dict = checkpoint.to_dict()  # type: ignore[assignment]
    if cp_dict:
        cp_wm = cp_dict.get("watermark")
        if cp_wm is None and isinstance(cp_dict.get("cdc"), dict):
            cp_wm = cp_dict["cdc"].get("watermark")
        if cp_wm is not None:
            state.running_cursor = str(cp_wm)
            state.watermark = str(cp_wm)
            watermark = str(cp_wm)
    total_chunks = max(1, int(cp_dict.get("chunk_index") or 0) + 1) if cp_dict else 1
    chunk_idx = int(cp_dict.get("chunk_index") or 0) if cp_dict else 0

    import os

    # Continuous CDC: drain snapshot, then poll until idle or budget exhausted.
    max_idle_polls = max(1, int(getenv_brand("CDC_MAX_IDLE_POLLS", "3")))
    max_poll_rounds = max(1, int(getenv_brand("CDC_MAX_POLL_ROUNDS", "50")))
    txn_hold_sleep = float(getenv_brand("CDC_TXN_HOLD_SLEEP_SEC", "0.25"))

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

        # Dual-writer fence: renew lease before sink apply. Zombie after steal
        # must not upsert. Still at-least-once — new holder may redeliver.
        _assert_cdc_lease_before_apply(cdc)

        with _cdc_span(
            "cdc.apply_batch",
            job_id=str(job_id or ""),
            dest_table=str(dest_table or ""),
            chunk_idx=int(chunk_idx),
        ):
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
                job_id=str(job_id or ""),
            )
        state.rows_written += rows_written
        state.inserts += len(change.inserts)
        state.updates += len(change.updates)
        state.deletes += deleted
        state.last_checksum = last_checksum or state.last_checksum
        if dest_summary:
            dest_summary = _merge_cdc_dest_summary(
                state,
                dest_summary,
                job_id=str(job_id or ""),
                destination=destination,
            )

        _refuse_cdc_advance_on_abort(dest_summary, validation_mode)

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
                lag_bytes=lag_fields.get("replication_lag_bytes"),
                lag_basis=lag_fields.get("cdc_lag_basis"),
                job_id=str(job_id or ""),
                stream=str(table_name or ""),
            )
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
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
                    "cdc_row_filter": lag_fields.get("cdc_row_filter"),
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
                    "cdc_retention_status": lag_fields.get("cdc_retention_status"),
                    "cdc_retention_resume": lag_fields.get("cdc_retention_resume"),
                    "cdc_retention_retained": lag_fields.get("cdc_retention_retained"),
                    "cdc_retention_message": lag_fields.get("cdc_retention_message"),
                    "cdc": {
                        "inserts": state.inserts,
                        "updates": state.updates,
                        "deletes": state.deletes,
                        **lag_fields,
                    },
                    "rejected_details": list(
                        (state.last_dest_summary or {}).get("rejected_details") or []
                    ),
                    "rejected_rows": int(
                        (state.last_dest_summary or {}).get("rejected_rows") or 0
                    ),
                    "coerced_null_rows": int(
                        (state.last_dest_summary or {}).get("coerced_null_rows") or 0
                    ),
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
        with _cdc_span("cdc.snapshot", job_id=str(job_id or "")):
            for change in cdc.snapshot():
                _apply_and_checkpoint(change)

    # Query CDC (CdcEngine): one incremental pass when resuming. Log CDC adapters
    # continuously poll until idle so a single job drains the slot/binlog/CT stream.
    if run_stream:
        with _cdc_span("cdc.poll", job_id=str(job_id or "")):
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

    final_watermark = state.running_cursor if state.running_cursor is not None else watermark
    lag_fields = _cdc_lag_fields(cdc)
    if final_watermark is not None:
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
    summary["cdc_row_filter"] = lag_fields.get("cdc_row_filter")
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
        "cdc_row_filter",
        "cdc_retention_status",
        "cdc_retention_resume",
        "cdc_retention_retained",
        "cdc_retention_message",
        "cdc_retention_dialect",
    ):
        if lag_fields.get(ha_key) is not None:
            summary[ha_key] = lag_fields.get(ha_key)
    summary["snapshot_mode"] = snapshot_mode.value
    summary["watermark"] = final_watermark
    summary["checksum"] = state.last_checksum
    if hasattr(cdc, "close"):
        try:
            cdc.close()
        except Exception as exc:
            logging.getLogger(__name__).debug("Exception suppressed: %s", exc, exc_info=exc)
    return state.rows_written, ddl_log, summary, headers
