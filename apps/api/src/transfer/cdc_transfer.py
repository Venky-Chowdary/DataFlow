"""Change-data-capture transfer runner for database sources.

Debezium-class capabilities:
  - MongoDB change streams, MySQL binlog (ROW + GTID), PostgreSQL logical
    decoding (txn-buffered peek/ack, ``test_decoding`` / ``pgoutput``)
  - SQL Server native CDC (``cdc.*``) with Change Tracking fallback
  - Oracle LogMiner with flashback versions fallback
  - Snapshot modes: ``initial|always|never|initial_only|when_needed``
  - Incremental snapshot signals interleaved with stream poll
  - Transaction buffering (BEGIN/COMMIT atomic apply batches)

Apply semantics default to **at-least-once upsert**. Opt-in
``delivery_guarantee=exactly_once`` uses a dest-owned watermark in the same
dest transaction as apply. Shared-log multi-stream applies N tables in one
dest transaction (Estuary multi-binding), then acks the source LSN once.
Job checkpoints persist after dest commit. Platform-wide exactly-once is
not claimed.
"""
from __future__ import annotations

import json
import logging
import os
import sys
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
from connectors.oracle_logminer import (
    OracleLogMinerCdc,
    is_unparsed_sql_redo,
    sql_redo_reject_detail,
)
from connectors.postgresql_change_stream import PostgreSqlChangeStreamCdc
from connectors.sqlserver_cdc_native import SqlServerNativeCdc
from connectors.sqlserver_change_stream import SqlServerChangeTrackingCdc
from connectors.table_manager import delete_by_primary_keys
from connectors.writer_common import DF_LSN_COL, extract_cdc_lsn
from services.cdc_capability import (
    LogCaptureRefusal,
    LogCaptureUnavailable,
    classify_log_capture_failure,
)
from services.cdc_catchup import stamp_capture_identity
from services.cdc_effectively_once import gate_cdc_destination
from services.dest_precount import DestBeforeCensus
from services.tombstone import (
    detect_tombstone_column as _detect_tombstone_column,
    is_tombstone_set as _is_tombstone_set,
)
from services.cdc_engine import (
    ChangeBatch,
    WatermarkType,
    infer_watermark_type,
    max_watermark,
)
from services.cdc_snapshot_mode import (
    KIND_INCREMENTAL,
    adapter_supports_incremental_interleave,
    measure_dest_already_keyed,
    resolve_cdc_snapshot_plan,
    resolve_snapshot_mode,
    snapshot_plan_stamp,
)
from services.error_handling import RetryBudget, with_retry
from services.keyset_pagination import compare_keyset_bookmark, max_keyset_bookmark
from services.replay_safety import classify_replay_safety
from services.sync_cursor import (
    build_cursor_key,
    get_watermark,
    map_source_to_target,
    resolve_selected_sync_contracts,
    resolve_sync_contract,
    resume_watermark,
    set_watermark,
)
from services.value_serializer import cell_to_string
from .stream_row_accounting import stamp_source_row_count

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


def _stamp_cdc_source_image(
    summary: dict[str, Any],
    *,
    src_type: str,
    src_cfg: dict[str, Any],
    schema: str,
    table_name: str,
    events: int,
) -> dict[str, Any]:
    """Independent COUNT(*) of the live source table after catch-up.

    Changelog ``inserts+updates+deletes`` is not dest population. Gate-8 must
    not invent source_rows from writer ack (that fails closed today). This
    stamp is the source image dest COUNT is compared against.
    """
    from services.dest_precount import destination_row_count

    summary["cdc_events_applied"] = max(int(events or 0), 0)
    summary["checksum_mode"] = "cdc_source_image"
    try:
        counted = destination_row_count(
            src_type,
            dict(src_cfg or {}),
            schema=str(schema or ""),
            table_name=str(table_name or ""),
        )
    except Exception as exc:
        logger.warning(
            "CDC source image COUNT failed: %s", exc, extra={"table": table_name}
        )
        counted = None
    if counted is None:
        return summary
    summary["source_row_count"] = int(counted)
    summary["source_row_count_source"] = "cdc_source_image_count"
    return summary


def _log_capture_pending(cdc: Any) -> bool | None:
    """Reader proof that the slot or binlog still has an unread change.

    ``None`` when this reader cannot prove it (query CDC, or the head could
    not be read). Callers must not treat ``None`` as caught up or as behind.
    """
    fn = getattr(cdc, "capture_has_pending", None)
    if not callable(fn):
        return None
    try:
        value = fn()
    except Exception as exc:
        logging.getLogger(__name__).debug("CDC pending check failed: %s", exc)
        return None
    if value is None:
        return None
    return bool(value)


def _drain_log_reader(
    cdc: Any,
    apply_one: Any,
    *,
    max_idle: int,
    max_rounds: int,
    sleep_sec: float,
    stop_early: Any = None,
) -> str:
    """Poll until the log is quiet, the row limit hits, or the round budget ends.

    Returns ``caught_up`` when an empty poll was proven to have nothing
    waiting, ``unknown`` when the reader cannot prove that, ``stopped`` when
    ``stop_early`` fired, and ``behind`` when the budget ended while a change
    was still unread. An empty poll alone is not caught up.
    """
    idle = 0
    last_had = False
    for _ in range(max(1, max_rounds)):
        if stop_early is not None and stop_early():
            return "stopped"
        had = False
        for change in cdc.poll():
            if apply_one(change):
                had = True
            if stop_early is not None and stop_early():
                return "stopped"
        if stop_early is not None and stop_early():
            return "stopped"
        last_had = had
        pending = _log_capture_pending(cdc)
        if had or pending is True:
            idle = 0
            if pending is True and not had and sleep_sec > 0:
                time.sleep(min(float(sleep_sec), 2.0))
            continue
        idle += 1
        if idle >= max(1, max_idle):
            return "unknown" if pending is None else "caught_up"
    # Round budget ended. A reader that cannot prove the log head keeps the
    # previous "stop after N polls" behaviour. A proven unread change does not.
    pending = _log_capture_pending(cdc)
    if pending is True:
        return "behind"
    if pending is False and not last_had:
        return "caught_up"
    return "unknown"


def _raise_if_stream_behind(cdc: Any, outcome: str) -> None:
    if outcome != "behind":
        return
    from services.cdc_catchup import CdcStreamBehind, behind_message

    raise CdcStreamBehind(
        behind_message(cdc),
        slot_name=str(getattr(cdc, "slot_name", "") or ""),
    )


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
        "cdc_publication_name": (
            str(getattr(cdc, "publication_name", "") or "")
            or (str(meta.get("publication_name")) if meta.get("publication_name") else None)
        ),
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
        if wal in {"lost", "unreserved"}:
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
    if meta.get("max_lsn_time"):
        out["cdc_max_lsn_time"] = str(meta.get("max_lsn_time"))
    capture_inst = meta.get("capture_instance") or meta.get("slot_name")
    if capture_inst:
        out["cdc_capture_instance"] = str(capture_inst)
        if not out.get("cdc_slot_name"):
            out["cdc_slot_name"] = str(capture_inst)
    # Capture-stall: reader at frozen max_lsn is not catch-up.
    if meta.get("capture_stall"):
        out["cdc_capture_stall"] = True
        if meta.get("capture_stall_reason"):
            out["cdc_capture_stall_reason"] = str(meta.get("capture_stall_reason"))
        if meta.get("capture_latency_seconds") is not None:
            out["cdc_capture_latency_seconds"] = meta.get("capture_latency_seconds")
        stall_sev = str(meta.get("capture_stall_severity") or "warn").lower()
        if stall_sev == "critical" or out.get("cdc_freshness_severity") not in {
            "critical",
            "warn",
        }:
            out["cdc_freshness_severity"] = (
                "critical" if stall_sev == "critical" else "warn"
            )
        elif stall_sev == "warn" and out.get("cdc_freshness_severity") != "critical":
            out["cdc_freshness_severity"] = "warn"
        # Clear invented 0s catch-up — tip equality under stall is false green.
        if out.get("cdc_lag_seconds") == 0.0:
            out["cdc_lag_seconds"] = None
            out["cdc_lag_basis"] = "capture_scan"
            out["cdc_lag_unknown_reason"] = (
                out.get("cdc_lag_unknown_reason")
                or out.get("cdc_capture_stall_reason")
                or "capture stalled; reader at frozen max_lsn is not catch-up"
            )
        elif not out.get("cdc_lag_basis") or out.get("cdc_lag_basis") == "wal_bytes":
            out["cdc_lag_basis"] = "capture_scan"
            if not out.get("cdc_lag_unknown_reason") and out.get("cdc_capture_stall_reason"):
                out["cdc_lag_unknown_reason"] = out["cdc_capture_stall_reason"]
    elif meta.get("capture_stall_unknown"):
        out["cdc_capture_stall_unknown"] = True
        if meta.get("capture_stall_reason"):
            out["cdc_capture_stall_reason"] = str(meta.get("capture_stall_reason"))
    if meta.get("capture_latency_seconds") is not None and "cdc_capture_latency_seconds" not in out:
        out["cdc_capture_latency_seconds"] = meta.get("capture_latency_seconds")
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


def _refuse_log_capture(cdc: Any, dialect: str) -> LogCaptureUnavailable:
    """Turn ``is_available() is False`` into a classified refusal."""
    reason = getattr(cdc, "unavailable_reason", None)
    if not isinstance(reason, LogCaptureRefusal):
        reason = classify_log_capture_failure(
            dialect, f"{dialect} log-based CDC reader reported unavailable"
        )
    return LogCaptureUnavailable(reason, dialect)


def _query_cdc_downgrade(exc: BaseException, dialect: str) -> dict[str, str | bool]:
    """Fields for a run that must read by cursor instead of the change log.

    Fails closed unless the source server itself does not emit a change log: a
    cursor poll cannot observe a DELETE, so substituting it for log capture on a
    repairable attach failure (slot quota, missing grant) silently diverges the
    destination.
    """
    refusal = (
        exc.refusal
        if isinstance(exc, LogCaptureUnavailable)
        else classify_log_capture_failure(dialect, str(exc))
    )
    if refusal.fail_closed:
        raise RuntimeError(refusal.message(dialect)) from exc
    logging.getLogger(__name__).warning(
        "CDC capture downgraded to cursor polling for %s (%s): %s",
        dialect,
        refusal.cause,
        refusal.detail,
    )
    return refusal.as_fields(dialect)


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
    #: Rows the reader handed to the writer (inserts + updates) across every
    #: batch — the run's source population when no source image COUNT exists.
    source_changes_read: int = 0
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
    #: Per destination table — keys from orders and users must not mix.
    census_accs: dict[str, Any] = field(default_factory=dict)
    #: Dest COUNT(*) before the first write to each table this run.
    dest_before: DestBeforeCensus = field(default_factory=DestBeforeCensus)

    def acc_for(self, table: str) -> Any:
        from services.row_conservation import KeyCensusAccumulator

        key = str(table or "")
        acc = self.census_accs.get(key)
        if acc is None:
            acc = KeyCensusAccumulator()
            self.census_accs[key] = acc
        return acc


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
    # A writer stamps the batch it just wrote as ``source_row_count``; that is
    # one page, not the run's population. The run stamps its own count once.
    incoming.pop("source_row_count", None)
    incoming.pop("source_row_count_source", None)
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
        # Empty-string watermarks are valid (e.g. '' cursor after coalesce) —
        # truthiness checks would re-snapshot forever.
        if cursor_after is None:
            yield from self._read_snapshot_pages()
            return
        yield from self._read_keyset_pages(cursor_after)

    def _read_snapshot_pages(self) -> Iterator[tuple[list[str], list[list[str]]]]:
        """Offset-paged full read (no cursor)."""
        offset = 0
        while True:
            result, _ = _unwrap_read(
                _read_batch(
                    self.src_type,
                    self.src_cfg,
                    self.table_name,
                    self.columns or None,
                    offset,
                    self.batch_size,
                    cursor_column="",
                    cursor_after=None,
                    cursor_type=None,
                    database=self.src_cfg.get("database", ""),
                )
            )
            rows = list(getattr(result, "rows", None) or []) if result else []
            if not rows:
                break
            yield result.headers, rows
            offset += len(rows)

    def _keyset_tiebreak(self) -> str:
        pk = (self.primary_key or "").strip()
        return pk if pk and pk != self.cursor_field else ""

    def _read_keyset_pages(
        self, cursor_after: str
    ) -> Iterator[tuple[list[str], list[list[str]]]]:
        """Keyset-paged read of every row past ``cursor_after``.

        Source readers ignore ``offset`` once a cursor is supplied, so each
        page must seek from the previous page's maximum ``(cursor, pk)``
        bookmark. The first seek is cursor-only (the persisted watermark);
        every later seek is composite when a tie-break primary key exists, so
        a page of rows sharing one cursor value still advances. A page whose
        bookmark does not advance is a spin and fails closed instead of
        re-reading the same rows forever.
        """
        cursor_type = infer_watermark_type([cursor_after]).value
        tiebreak = self._keyset_tiebreak()
        key_columns = [self.cursor_field] + ([tiebreak] if tiebreak else [])
        bookmark = cursor_after
        while True:
            result, _ = _unwrap_read(
                _read_batch(
                    self.src_type,
                    self.src_cfg,
                    self.table_name,
                    self.columns or None,
                    0,
                    self.batch_size,
                    cursor_column=self.cursor_field,
                    cursor_after=bookmark,
                    cursor_type=cursor_type,
                    cursor_primary_key=tiebreak or None,
                    database=self.src_cfg.get("database", ""),
                )
            )
            rows = list(getattr(result, "rows", None) or []) if result else []
            if not rows:
                break
            headers = result.headers
            next_bookmark = max_keyset_bookmark(rows, headers, key_columns)
            if next_bookmark is None:
                raise RuntimeError(
                    f"CDC cursor page for {self.table_name!r} carries no "
                    f"{key_columns!r} value to advance from; refusing to re-read"
                )
            order = compare_keyset_bookmark(next_bookmark, bookmark)
            if order is not None and order <= 0:
                raise RuntimeError(
                    f"CDC cursor for {self.table_name!r} did not advance past "
                    f"{bookmark!r} (page max {next_bookmark!r}); every row on the "
                    "page shares the cursor value and no tie-break key can order "
                    "them — declare a primary key or a unique cursor"
                )
            yield headers, rows
            if len(rows) < self.batch_size:
                break
            bookmark = next_bookmark

    def _yield_batches(self, reader: Iterator[tuple[list[str], list[list[str]]]]) -> Iterator[ChangeBatch]:
        """Stream batches from a (headers, rows) reader without materializing all rows."""
        buffer: list[dict[str, Any]] = []
        headers: list[str] = []
        emitted = False
        for h, rows in reader:
            if not headers:
                headers = h
                # An inherited multi-table stream reads the table (no column
                # list). Soft-delete detection needs those real names; the
                # primary table's schema must not decide them.
                if not self.tombstone_column and not self.columns:
                    self.tombstone_column = _detect_tombstone_column(
                        self.schema, headers
                    )
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


def _split_unparsed_sql_redo(
    records: list[dict[str, Any]] | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Hold unparsed LogMiner SQL_REDO out of dest upsert — quarantine only."""
    keep: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for rec in records or []:
        if is_unparsed_sql_redo(rec):
            rejected.append(sql_redo_reject_detail(rec))
        else:
            keep.append(rec)
    return keep, rejected


def _stamp_unparsed_sql_redo_summary(
    dest_summary: dict[str, Any] | None,
    rejected_details: list[dict[str, Any]],
) -> dict[str, Any]:
    out = dict(dest_summary or {})
    if not rejected_details:
        return out
    out.setdefault("rejected_details", [])
    out["rejected_details"] = list(out["rejected_details"]) + list(rejected_details)
    out["rejected_rows"] = int(out.get("rejected_rows") or 0) + len(rejected_details)
    out["error_policy"] = "quarantine"
    out["cdc_unparsed_sql_redo"] = len(rejected_details)
    return out


def _change_column_names(change: ChangeBatch) -> list[str]:
    """Column names present on this batch's inserts and updates, in first-seen order.

    ``_df_lsn`` is stamped later and is not part of the table's own set.
    """
    ordered: list[str] = []
    seen: set[str] = set()
    for record in list(change.inserts or []) + list(change.updates or []):
        if not isinstance(record, dict):
            continue
        for key in record:
            name = str(key).strip()
            folded = name.casefold()
            if not name or folded in seen or folded == "_df_lsn":
                continue
            seen.add(folded)
            ordered.append(name)
    return ordered


def _note_inherited_map(summary: dict[str, Any] | None, note: str) -> dict[str, Any]:
    """Surface why another table's column map was not applied. Once per note."""
    out = dict(summary or {})
    text = (note or "").strip()
    if not text:
        return out
    warnings = [str(item) for item in (out.get("warnings") or [])]
    if text not in warnings:
        warnings.append(text)
    out["warnings"] = warnings
    return out


def _align_inherited_batch(
    mappings: list[dict[str, Any]],
    headers: list[str],
    change: ChangeBatch,
    pk_target_col: str | list[str],
    pk_source_cols: list[str],
    lock: dict[str, Any] | None,
) -> tuple[list[dict[str, Any]], list[str], str | list[str], str]:
    """Keep this batch's columns when the map was borrowed from another table.

    A later delete-only batch has no row image. ``lock`` remembers the first
    image so that batch does not fall back to the primary table's schema and
    rename the delete key.
    """
    from services.multi_stream_plan import adopt_inherited_mappings
    from services.sync_cursor import map_source_to_target

    imaged = _change_column_names(change)
    note = ""
    if imaged:
        mappings, note = adopt_inherited_mappings(mappings, imaged)
        headers = imaged
        if lock is not None:
            if lock.get("noted"):
                note = ""
            elif note:
                lock["noted"] = True
            lock["mappings"] = list(mappings)
            lock["headers"] = list(headers)
    elif lock and lock.get("mappings"):
        mappings = list(lock["mappings"])
        headers = list(lock.get("headers") or headers)
    elif pk_source_cols:
        # No row image yet, and none remembered. Another table's schema must
        # not become this batch's column list or its delete key.
        mappings = [
            {"source": col, "target": col, "confidence": 0.95}
            for col in pk_source_cols
        ]
        headers = list(pk_source_cols)
    if pk_source_cols:
        resolved = [map_source_to_target(col, mappings) or col for col in pk_source_cols]
        pk_target_col = resolved[0] if len(resolved) == 1 else ",".join(resolved)
    return list(mappings), list(headers), pk_target_col, note


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
    census_acc: Any | None = None,
    delivery_guarantee: str = "at_least_once",
    delivery_pinned: bool = False,
    cursor_key: str = "",
    stream_name: str = "",
    writer_fence: int = 0,
    mappings_inherited: bool = False,
    pk_source_cols: list[str] | None = None,
    align_lock: dict[str, Any] | None = None,
) -> tuple[int, str, dict[str, Any], int]:
    """Apply a single ChangeBatch to the destination. Returns rows_written, checksum, summary, deleted_count."""
    from services.cdc_exactly_once import (
        DELIVERY_SEMANTICS_ALO,
        REASON_NO_LSN,
        delivery_for_batch,
        normalize_delivery_guarantee,
    )
    from services.cdc_snapshot_window import _pk_columns

    clean_inserts, rej_inserts = _split_unparsed_sql_redo(change.inserts)
    clean_updates, rej_updates = _split_unparsed_sql_redo(change.updates)
    rejected_details = [
        dict(d)
        for d in (getattr(change, "rejected", None) or [])
        if isinstance(d, dict)
    ] + rej_inserts + rej_updates
    if rej_inserts or rej_updates or rejected_details:
        change = ChangeBatch(
            inserts=clean_inserts,
            updates=clean_updates,
            deletes=list(change.deletes or []),
            unchanged=change.unchanged,
            resume_token=change.resume_token,
            table=change.table,
            ack_barrier=change.ack_barrier,
            rejected=rejected_details,
        )

    inherit_note = ""
    if mappings_inherited:
        mappings, headers, pk_target_col, inherit_note = _align_inherited_batch(
            mappings,
            headers,
            change,
            pk_target_col,
            list(pk_source_cols or []),
            align_lock,
        )
        column_types = dict(column_types or {})
        for name in headers:
            column_types.setdefault(name, "string")

    effective_delivery = delivery_for_batch(
        delivery_guarantee,
        change.resume_token,
        pinned=delivery_pinned,
    )
    eos_downgraded = (
        normalize_delivery_guarantee(delivery_guarantee) == "exactly_once"
        and effective_delivery != "exactly_once"
    )
    if effective_delivery == "exactly_once":
        from connectors.cdc_eos_sql import apply_change_batch_exactly_once

        rows, checksum, dest_summary, deleted = apply_change_batch_exactly_once(
            dest_type=dest_type,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            change=change,
            mappings=mappings,
            column_types=column_types,
            headers=headers,
            pk_target_cols=_pk_columns(pk_target_col) if pk_target_col else [],
            cursor_key=cursor_key,
            stream_name=stream_name,
            writer_fence=writer_fence,
        )
        return rows, checksum, _note_inherited_map(
            _stamp_unparsed_sql_redo_summary(dest_summary, rejected_details),
            inherit_note,
        ), deleted

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
    census_payload: dict[str, Any] | None = None
    if pk_target_cols:
        from services.row_conservation import census_change_batch, observe_change_batch

        if census_acc is not None:
            observe_change_batch(
                census_acc,
                inserts=change.inserts,
                updates=change.updates,
                deletes=change.deletes,
                key_columns=pk_target_cols,
                db_type=dest_type,
                cfg=dest_cfg,
                schema=str(dest_cfg.get("schema") or ""),
                table_name=dest_table,
                mappings=mappings,
            )
            census = census_acc.to_census()
        else:
            census = census_change_batch(
                inserts=change.inserts,
                updates=change.updates,
                deletes=change.deletes,
                key_columns=pk_target_cols,
                db_type=dest_type,
                cfg=dest_cfg,
                schema=str(dest_cfg.get("schema") or ""),
                table_name=dest_table,
                mappings=mappings,
            )
        if census is not None:
            census_payload = census.to_dict()

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
    if census_payload:
        from services.row_conservation import CENSUS_KEY

        dest_summary[CENSUS_KEY] = census_payload
    dest_summary = _note_inherited_map(
        _stamp_unparsed_sql_redo_summary(dest_summary, rejected_details),
        inherit_note,
    )

    if eos_downgraded and isinstance(dest_summary, dict):
        # The contract said cdc_position, but this batch has no captured
        # LSN/GTID/SCN/resume token. Stay at-least-once. Do not invent one.
        dest_summary["cdc_delivery"] = "at-least-once"
        dest_summary["exactly_once_active"] = False
        dest_summary["exactly_once_downgrade"] = REASON_NO_LSN
        dest_summary["delivery_semantics"] = DELIVERY_SEMANTICS_ALO
    return rows_written, last_checksum, dest_summary, deleted


def _stamp_cdc_poll(
    *,
    job_id: str = "",
    stream: str = "",
    workspace_id: str = "",
    schedule_id: str = "",
    **kwargs: Any,
) -> None:
    from services.ops_metrics import record_cdc_poll

    record_cdc_poll(
        job_id=str(job_id or ""),
        stream=str(stream or ""),
        workspace_id=str(workspace_id or ""),
        schedule_id=str(schedule_id or ""),
        **kwargs,
    )


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
    delivery_guarantee: str = "at_least_once",
    delivery_pinned: bool = False,
    workspace_id: str = "",
    schedule_id: str = "",
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
            delivery_guarantee=delivery_guarantee,
            delivery_pinned=delivery_pinned,
            workspace_id=workspace_id,
            schedule_id=schedule_id,
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
        delivery_guarantee=delivery_guarantee,
        delivery_pinned=delivery_pinned,
        workspace_id=workspace_id,
        schedule_id=schedule_id,
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
    delivery_guarantee: str = "at_least_once",
    delivery_pinned: bool = False,
    workspace_id: str = "",
    schedule_id: str = "",
) -> tuple[int, list[str], dict[str, Any], list[str]]:
    """Run CDC for each selected stream.

    Prefer Debezium-class shared log reader (one PG slot / one MySQL server_id)
    when all streams share a postgresql or mysql source. Fall back to sequential
    N independent readers otherwise.

    Exactly-once on a wired SQL dest uses the shared log reader *and* one
    dest transaction for the demuxed barrier (N tables + one LSN). Unwired
    dests stay sequential and fail-closed at apply.
    """
    from .stream_dest_procedure import (
        CdcDestinationSessionError,
        refuse_cdc_destination_row_apply,
    )

    # Before either reader. A CALL must not fall through to sequential upsert.
    refuse_cdc_destination_row_apply(destination, sync_mode, stream_contracts)
    from services.cdc_exactly_once import EOS_TXN_WIRED_DESTS, normalize_delivery_guarantee
    from services.cdc_multi_table import can_share_log_reader

    src_fmt = str(getattr(source, "format", "") or "").lower()
    dest_fmt = str(getattr(destination, "format", "") or "").lower().replace("-", "_")
    eos = normalize_delivery_guarantee(delivery_guarantee) == "exactly_once"
    dest_wired = dest_fmt in EOS_TXN_WIRED_DESTS
    if (not eos or dest_wired) and can_share_log_reader(src_fmt, len(selected)):
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
                delivery_guarantee=delivery_guarantee,
            )
        except Exception as exc:
            from services.cdc_lease import CdcLeaseConflict

            if isinstance(exc, (CdcLeaseConflict, CdcDestinationSessionError)):
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
        delivery_guarantee=delivery_guarantee,
        delivery_pinned=delivery_pinned,
        workspace_id=workspace_id,
        schedule_id=schedule_id,
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
    delivery_guarantee: str = "at_least_once",
) -> tuple[int, list[str], dict[str, Any], list[str]]:
    """One log consumer for N tables (Debezium-class); demux apply per stream.

    Default semantics are **at-least-once upsert**. Opt-in exactly-once buffers
    demuxed table batches until ``ack_barrier``, then applies them in one dest
    transaction and acks the source LSN once.
    """
    from .stream_dest_procedure import (
        cdc_destination_hooks,
        refuse_cdc_destination_row_apply,
    )

    refuse_cdc_destination_row_apply(destination, sync_mode, stream_contracts)
    from services.cdc_multi_table import (
        shared_route_cursor_key,
        should_ack_shared_batch,
    )
    from services.cdc_resume_tokens import (
        is_durable_log_resume_token,
        is_side_channel_resume_token,
    )
    from services.cdc_snapshot_mode import snapshot_dump_open

    from services.cdc_exactly_once import PROTOCOL, normalize_delivery_guarantee

    src_type = resolve_driver_type(getattr(source, "format", "") or "")
    dest_type = resolve_driver_type(getattr(destination, "format", "") or "")
    src_cfg = resolve_connector_config(source)
    dest_cfg = resolve_connector_config(destination)
    eos_active = normalize_delivery_guarantee(delivery_guarantee) == "exactly_once"

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
        declared = isinstance(stream_maps, list) and bool(stream_maps)
        use_maps = stream_maps if declared else mappings
        stream_cfg[name] = {
            "primary_key": _cdc_pk_str(
                contract.primary_key or primary_keys.get(name), name
            ),
            "cursor_field": str(contract.cursor_field or ""),
            "mappings": use_maps,
            "mappings_inherited": not declared,
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
        dest_type=dest_type,
        dest_database=str(dest_cfg.get("database") or ""),
    )
    shared_wm = get_watermark(shared_key)
    if eos_active:
        from connectors.cdc_eos_sql import open_eos_session

        opened = open_eos_session(
            dest_type=dest_type,
            dest_cfg=dest_cfg,
            stream_key=shared_key,
            incoming_fence=0,
            job_resume=shared_wm,
        )
        shared_wm = opened.resume
        from services.cdc_exactly_once import persist_dest_keyset_on_signal

        persist_dest_keyset_on_signal(opened.resume)

    # One route cursor. The shared store wins. An empty store may resume
    # from the job checkpoint when that record is the shared log position.
    # A token that names one table is that table's keyset, not this route.
    shared_wm = resume_watermark(shared_wm, checkpoint, shared=True)

    cdc: Any
    ddl_log: list[str] = [
        f"CDC(shared_reader) {src_type} tables={tables} → {dest_type} "
        + (
            f"(one slot/server_id; exactly_once dest-owned bundle {PROTOCOL})"
            if eos_active
            else "(one slot/server_id; at-least-once upsert)"
        )
    ]
    if eos_active:
        ddl_log.append(
            f"CDC EOS Open fence={opened.fence_epoch} dest_lsn={opened.dest_lsn} "
            f"raised={opened.fence_raised}"
        )
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
            raise _refuse_log_capture(cdc, "postgresql")
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
            raise _refuse_log_capture(cdc, "mysql")
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
    ret = None
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
    from services.cdc_slot_resume import prepare_resume_for_missing_slot

    shared_wm, slot_note = prepare_resume_for_missing_slot(
        shared_wm, ret, cdc, mode=snapshot_mode
    )
    if slot_note:
        ddl_log.append(slot_note)
    from services.cdc_snapshot_window import _pk_columns

    snapshot_plan = resolve_cdc_snapshot_plan(
        snapshot_mode,
        watermark=shared_wm,
        retention=ret,
        dest_already_keyed=measure_dest_already_keyed(
            dest_type,
            dest_cfg,
            [
                (name, _pk_columns(cfg["primary_key"]))
                for name, cfg in stream_cfg.items()
            ],
            schema=str(dest_cfg.get("schema") or ""),
        ),
        incremental_capable=adapter_supports_incremental_interleave(cdc),
    )
    run_snapshot = bool(snapshot_plan["run_snapshot"])
    run_stream = bool(snapshot_plan["run_stream"])
    ddl_log.append(
        f"CDC snapshot_mode={snapshot_mode.value} snapshot_plan={snapshot_plan['kind']}"
        f"{' lost_window=1' if snapshot_plan.get('lost_window') else ''} shared_reader=1"
    )
    if snapshot_plan.get("kind") == KIND_INCREMENTAL:
        from services.cdc_incremental_snapshot import enqueue_gap_recovery_snapshots

        signals = enqueue_gap_recovery_snapshots(
            str(getattr(cdc, "source_key", "") or ""),
            [
                (name, cfg["primary_key"])
                for name, cfg in stream_cfg.items()
            ],
        )
        ddl_log.append(
            f"CDC incremental gap recovery signals={len(signals)} "
            f"(DDD-3 stream-wins; not blocking dump)"
        )

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
    pending_eos_bundle: list[Any] = []
    # Tables whose latest stored token is still an open dump. The streaming
    # handoff moves them to that log position. Leaving phase=snapshot would
    # make a later per-table run reopen the dump.
    open_snapshot_tables: set[str] = set()
    # Cursor this run has made durable. A phase=snapshot token is an open
    # dump (table + last primary key), not "snapshot finished".
    published_shared = shared_wm

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
        nonlocal total_rows, chunk_idx, headers, last_summary, published_shared
        stream = _resolve_stream(change)
        cfg = stream_cfg[stream]
        use_maps = cfg["mappings"]
        inherit_note = ""
        from services.cdc_snapshot_window import _pk_columns

        pk_source = _pk_columns(cfg["primary_key"])
        if cfg.get("mappings_inherited"):
            use_maps, headers, pk_joined, inherit_note = _align_inherited_batch(
                use_maps,
                headers,
                change,
                cfg["primary_key"],
                pk_source,
                cfg.setdefault("align_lock", {}),
            )
            pk_target = _pk_columns(pk_joined)
        else:
            pk_target = [map_source_to_target(c, use_maps) or c for c in pk_source]
        if original_dest_table is not None or original_dest_collection is not None:
            if getattr(destination, "format", "") == "mongodb" or original_dest_collection:
                destination.collection = stream
            else:
                destination.table = stream
        dest_table = resolve_dest_table(dest_type, destination)
        shared_accum.dest_before.capture(
            destination,
            table_name=str(dest_table or stream or ""),
            aliases=(stream,),
        )
        col_types = dict(schema)
        if change.inserts or change.updates and not cfg.get("mappings_inherited"):
            sample = (change.inserts or change.updates)[0]
            headers = list(sample.keys())
        for name in headers:
            col_types.setdefault(name, "string")
        _assert_cdc_lease_before_apply(cdc)
        rows_written = 0
        deleted = 0
        dest_summary: dict[str, Any] = {}
        if eos_active:
            from services.cdc_exactly_once import EosBundleStream, batch_lsn

            if change.total_changes:
                pending_eos_bundle.append(
                    EosBundleStream(
                        dest_table=str(dest_table or stream),
                        change=change,
                        mappings=use_maps,
                        column_types=col_types,
                        pk_target_cols=pk_target,
                        stream_key=str(cfg["cursor_key"]),
                        headers=list(headers),
                    )
                )
            if should_ack_shared_batch(change):
                from connectors.cdc_eos_sql import apply_eos_bundle

                incoming = batch_lsn(change.resume_token) or ""
                if pending_eos_bundle or incoming:
                    with _cdc_span(
                        "cdc.apply_bundle",
                        job_id=str(job_id or ""),
                        dest_table=str(dest_table or ""),
                        stream=str(stream or ""),
                        chunk_idx=int(chunk_idx),
                    ):
                        bundle = apply_eos_bundle(
                            dest_type=dest_type,
                            dest_cfg=dest_cfg,
                            streams=list(pending_eos_bundle),
                            incoming_lsn=incoming,
                            bundle_key=shared_key,
                            writer_fence=int(
                                getattr(
                                    getattr(cdc, "_lease", None), "generation", 0
                                )
                                or 0
                            ),
                        )
                    pending_eos_bundle.clear()
                    rows_written = bundle.rows_written
                    deleted = bundle.deleted
                    dest_summary = bundle.to_dest_summary()
                chunk_idx += 1
        else:
            with _cdc_span(
                "cdc.apply_batch",
                job_id=str(job_id or ""),
                dest_table=str(dest_table or ""),
                stream=str(stream or ""),
                chunk_idx=int(chunk_idx),
            ):
                rows_written, _checksum, dest_summary, deleted = _apply_change_batch(
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
                    census_acc=shared_accum.acc_for(str(dest_table or stream or "")),
                )
            chunk_idx += 1
        total_rows += rows_written + deleted
        stream_health[stream]["records_processed"] = (
            int(stream_health[stream].get("records_processed") or 0) + rows_written + deleted
        )
        dest_summary = _note_inherited_map(dest_summary, inherit_note)
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
                from services.cdc_resume_tokens import serialize_resume_token

                try:
                    token_s = serialize_resume_token(
                        change.resume_token, default=json_util.default
                    )
                except TypeError:
                    token_s = str(change.resume_token)
                # Stage, do not publish. Only a table that actually received a
                # batch in this transaction advances, so a table untouched by the
                # commit keeps its previous position.
                if change.total_changes:
                    pending_table_watermarks[stream] = token_s
                    if snapshot_dump_open(token_s):
                        open_snapshot_tables.add(stream)
                if should_ack_shared_batch(change) and not skip_ack:
                    # Barrier reached: the whole transaction is applied, so the
                    # per-table cursors and the shared log position may both move.
                    # A position-only barrier (heartbeat, or a commit that touched
                    # no captured table) advances the log but no table cursor.
                    # A streaming handoff closes every table still marked as an
                    # open dump. Their snapshot token is not a finished cursor.
                    if not snapshot_dump_open(token_s):
                        for name in open_snapshot_tables:
                            if name in stream_cfg:
                                pending_table_watermarks[name] = token_s
                        open_snapshot_tables.clear()
                    _flush_table_watermarks()
                    set_watermark(
                        shared_key,
                        token_s,
                        metadata={
                            "job_id": job_id,
                            "sync_mode": sync_mode,
                            "tables": tables,
                            "shared_reader": True,
                            "snapshot_dump_open": snapshot_dump_open(token_s),
                        },
                    )
                    published_shared = token_s
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
                elif (
                    snapshot_dump_open(token_s)
                    and not skip_ack
                    and not eos_active
                ):
                    # SQL Server and Oracle snapshot pages are not a log
                    # barrier. The page is still the keyset resume (table +
                    # last primary key). Store it without acking: a crash
                    # continues the dump, and the streaming handoff remains
                    # the only ack. Exactly-once waits for that barrier so
                    # the cursor cannot move before the destination commit.
                    if change.total_changes and stream in stream_cfg:
                        set_watermark(
                            stream_cfg[stream]["cursor_key"],
                            token_s,
                            metadata={
                                "job_id": job_id,
                                "sync_mode": sync_mode,
                                "shared_reader": True,
                                "snapshot_dump_open": True,
                            },
                        )
                    set_watermark(
                        shared_key,
                        token_s,
                        metadata={
                            "job_id": job_id,
                            "sync_mode": sync_mode,
                            "tables": tables,
                            "shared_reader": True,
                            "snapshot_dump_open": True,
                        },
                    )
                    published_shared = token_s
        if on_checkpoint:
            on_checkpoint(
                chunk_idx,
                max(1, chunk_idx),
                total_rows,
                {
                    "chunk_index": chunk_idx,
                    "watermark": published_shared,
                    "rows_written": total_rows,
                    "streams": list(stream_health.values()),
                    "cdc_delivery": "exactly_once" if eos_active else "at-least-once",
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
        with cdc_destination_hooks(destination, ddl_log):
            if run_snapshot:
                with _cdc_span("cdc.snapshot", job_id=str(job_id or ""), shared_reader=True):
                    for change in cdc.snapshot():
                        _apply_tagged(change)
                        if limit and total_rows >= limit:
                            break
            if run_stream and not (limit and total_rows >= limit):
                max_idle = max(1, int(getenv_brand("CDC_MAX_IDLE_POLLS", "3")))
                max_rounds = max(1, int(getenv_brand("CDC_MAX_POLL_ROUNDS", "50")))
                sleep_sec = float(getenv_brand("CDC_TXN_HOLD_SLEEP_SEC", "0.25"))
                with _cdc_span("cdc.poll", job_id=str(job_id or ""), shared_reader=True):
                    outcome = _drain_log_reader(
                        cdc,
                        _apply_tagged,
                        max_idle=max_idle,
                        max_rounds=max_rounds,
                        sleep_sec=sleep_sec,
                        stop_early=lambda: bool(limit and total_rows >= limit),
                    )
                    _raise_if_stream_behind(cdc, outcome)
    finally:
        stamp_capture_identity(sys.exc_info()[1], cdc)
        if original_dest_table is not None:
            destination.table = original_dest_table
        if original_dest_collection is not None:
            destination.collection = original_dest_collection
        if hasattr(cdc, "close"):
            try:
                cdc.close()
            except Exception as exc:
                logging.getLogger(__name__).debug("Exception suppressed: %s", exc, exc_info=exc)

    from services.row_conservation import record_stream_health

    for name, h in stream_health.items():
        acc = shared_accum.census_accs.get(name)
        census = acc.to_census() if acc is not None else None
        payload: dict[str, Any] = {}
        shared_accum.dest_before.stamp(payload, name)
        record_stream_health(
            stream_health,
            name=name,
            status="completed",
            records_processed=int(h.get("records_processed") or 0),
            summary=payload,
            extra={
                "cdc_lag_seconds": h.get("cdc_lag_seconds"),
                "watermark": h.get("watermark"),
            },
            sync_mode=sync_mode,
            census=census,
            destination=destination,
            dest_table=name,
            count_source=False,
        )
    lag_fields = _cdc_lag_fields(cdc)
    last_summary = dict(last_summary or {})
    last_summary["streams"] = list(stream_health.values())
    last_summary["cursor_key"] = shared_key
    last_summary["cdc"] = {
        "shared_reader": True,
        "tables": tables,
        "watermark": get_watermark(shared_key),
        "cursor_key": shared_key,
        **lag_fields,
    }
    if eos_active:
        last_summary["cdc_delivery"] = "exactly_once"
        last_summary["delivery_semantics"] = "exactly_once_dest_owned_watermark_txn"
        last_summary["exactly_once_algorithm"] = "dest_owned_watermark_txn"
        last_summary["exactly_once_protocol"] = PROTOCOL
        last_summary["exactly_once_active"] = True
        last_summary["exactly_once_claimed_platform"] = False
        last_summary["eos_dest_authoritative"] = True
        last_summary["eos_bundle"] = True
    else:
        last_summary["cdc_delivery"] = "at-least-once"
    from services.cdc_named_eos import stamp_named_eos_on_summary

    last_summary = stamp_named_eos_on_summary(
        last_summary,
        source_type=src_type,
        dest_type=dest_type,
        sync_mode=sync_mode or "cdc",
        eos_operator_requested=eos_active,
    )
    last_summary["cdc_shared_reader"] = True
    last_summary["snapshot_mode"] = snapshot_mode.value
    stamp = snapshot_plan_stamp(snapshot_plan)
    if stamp:
        last_summary["snapshot_plan"] = stamp
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
    delivery_guarantee: str = "at_least_once",
    delivery_pinned: bool = False,
    workspace_id: str = "",
    schedule_id: str = "",
) -> tuple[int, list[str], dict[str, Any], list[str]]:
    """Legacy path: N independent CDC readers (N slots / N server_ids)."""
    from .stream_dest_procedure import (
        cdc_destination_hooks,
        hide_session_hooks,
        refuse_cdc_destination_row_apply,
    )

    # Direct callers fail here, before any per-table reader opens.
    refuse_cdc_destination_row_apply(destination, sync_mode, stream_contracts)
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
        # One Advanced before/after pair is one session. Clearing the live
        # extra stops each table from replaying a shared TRUNCATE.
        with cdc_destination_hooks(destination, ddl_log), hide_session_hooks(destination):
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
                declared_maps = isinstance(stream_maps, list) and bool(stream_maps)
                use_mappings = stream_maps if declared_maps else mappings
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
                        delivery_guarantee=delivery_guarantee,
                        delivery_pinned=delivery_pinned,
                        workspace_id=workspace_id,
                        schedule_id=schedule_id,
                        mappings_inherited=not declared_maps,
                        checkpoint_bound_to_stream=True,
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
                    from services.row_conservation import record_stream_health

                    record_stream_health(
                        stream_health,
                        name=stream_name,
                        status=status,
                        records_processed=rows,
                        summary=summary,
                        extra={"error": error},
                        sync_mode=sync_mode,
                        destination=destination,
                        count_source=False,
                    )
                    raise
                cdc_meta = summary.get("cdc") if isinstance(summary.get("cdc"), dict) else {}
                from services.row_conservation import record_stream_health

                record_stream_health(
                    stream_health,
                    name=stream_name,
                    status=status,
                    records_processed=rows,
                    summary=summary,
                    extra={
                        "cdc_lag_seconds": summary.get("cdc_lag_seconds"),
                        "replication_lag_bytes": cdc_meta.get("replication_lag_bytes"),
                        "watermark": cdc_meta.get("watermark"),
                        "error": error,
                    },
                    sync_mode=sync_mode,
                    destination=destination,
                    dest_table=stream_name,
                    count_source=False,
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


def _catalog_cdc_primary_key(
    src_type: str,
    src_cfg: dict[str, Any],
    table_name: str,
    mappings: list[dict],
) -> str:
    """Source-catalog primary key, mapped through the write mapping.

    Empty when the catalog has no key or any key column is unmapped. Does not
    invent ``id``. Introspect failure is empty too — the caller still refuses
    rather than upserting on a guessed column.
    """
    if not (table_name or "").strip():
        return ""
    try:
        from .adapters import _introspect_table_schema_rich

        _schema, _nulls, keys = _introspect_table_schema_rich(
            src_type, src_cfg, table_name, []
        )
    except (OSError, RuntimeError, ValueError, TypeError):
        return ""
    cols = list((keys or {}).get("primary_key_columns") or [])
    if mappings:
        from services.primary_key import mapped_catalog_upsert_key

        sources, _targets = mapped_catalog_upsert_key(cols, mappings)
        return ",".join(sources)
    return ",".join(str(col).strip() for col in cols if str(col or "").strip())


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
    delivery_guarantee: str = "at_least_once",
    delivery_pinned: bool = False,
    workspace_id: str = "",
    schedule_id: str = "",
    mappings_inherited: bool = False,
    checkpoint_bound_to_stream: bool = False,
) -> tuple[int, list[str], dict[str, Any], list[str]]:
    """Run a CDC transfer for a single stream contract."""
    from .stream_dest_procedure import (
        cdc_destination_hooks,
        refuse_cdc_destination_row_apply,
    )

    # A destination CALL must not become a table upsert, and must not open a slot.
    refuse_cdc_destination_row_apply(destination, sync_mode, stream_contracts)
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
        # The catalog key is the same identity upsert uses. A table that has
        # a primary key must not fail only because the stream contract left
        # the field blank. A table with no key still refuses below.
        primary_key = _catalog_cdc_primary_key(
            src_type, src_cfg, table_name, mappings
        )
        if contract is not None and primary_key:
            contract.primary_key = primary_key
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
    from services.cdc_exactly_once import (
        DELIVERY_SEMANTICS_ALO,
        DELIVERY_SEMANTICS_EOS,
        PROTOCOL,
        assert_requested_cdc_delivery,
        dest_allow_append_only,
    )
    from services.procedure_source import is_callable_source

    eos_guarantee = assert_requested_cdc_delivery(
        delivery_guarantee,
        sync_mode=sync_mode or "cdc",
        dest_type=dest_type,
        source_type=src_type,
        has_primary_key=True,
        write_mode="upsert",
        allow_append_only=dest_allow_append_only(destination)
        or _truthy_cfg(dest_cfg, "allow_append_only", "cdc_allow_append_only"),
        callable_source=is_callable_source(source),
    )
    eos_active = eos_guarantee == "exactly_once"
    if src_type in {"mongodb", "mysql", "postgresql", "sqlserver", "oracle"}:
        # Query-CDC fallback polls a column. The log position stays the cursor
        # when the contract says cdc_position. The primary key is the only
        # column that may stand in for that poll. A column named id is not
        # invented when the key is something else.
        if not cursor_field:
            cursor_field = pk_source_cols[0]
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
    if eos_active:
        from connectors.cdc_eos_sql import open_eos_session

        opened = open_eos_session(
            dest_type=dest_type,
            dest_cfg=dest_cfg,
            stream_key=cursor_key,
            incoming_fence=0,
            job_resume=watermark,
        )
        watermark = opened.resume
        from services.cdc_exactly_once import persist_dest_keyset_on_signal

        persist_dest_keyset_on_signal(opened.resume)

    # The reader and the snapshot plan must share this cursor. A checkpoint
    # fills a missing store only. It must not rewind a cursor the store
    # already advanced. A multi-table run adopts that checkpoint only when
    # it names this stream. An unnamed job cursor is one table's position
    # and must not seek the others.
    watermark = resume_watermark(
        watermark,
        checkpoint,
        stream=table_name,
        allow_unnamed=not checkpoint_bound_to_stream,
    )

    from services.multi_stream_plan import reader_columns_for_stream

    headers = reader_columns_for_stream(
        list(schema.keys()), inherited=mappings_inherited
    )
    column_types = {c: schema.get(c, "string") for c in headers}
    align_lock: dict[str, Any] = {}
    # Non-empty only when log capture was refused and cursor polling took over.
    capture_downgrade: dict[str, str | bool] = {}

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
        except Exception as exc:
            # No oplog to tail (standalone deployment or change streams denied):
            # a server-side condition DataFlow cannot repair mid-run. Declare the
            # capture actually used instead of implying change-stream fidelity.
            capture_downgrade = classify_log_capture_failure(
                "mongodb", str(exc), server_log_enabled=False
            ).as_fields("mongodb")
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
                f"(cursor={cursor_field}, pk={primary_key}, watermark={watermark or 'initial'})",
                "CDC capture downgraded: change stream → cursor poll — deletes are not "
                "captured. Run MongoDB as a replica set to capture deletes.",
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
                raise _refuse_log_capture(cdc, "mysql")
            ddl_log = [
                f"CDC(binlog) {src_type}.{table_name} → {dest_type}.{dest_table} "
                f"(pk={primary_key}, resume={'set' if watermark else 'initial'})"
            ]
        except Exception as exc:
            from services.cdc_lease import CdcLeaseConflict

            if isinstance(exc, CdcLeaseConflict):
                raise
            capture_downgrade = _query_cdc_downgrade(exc, "mysql")
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
                f"(cursor={cursor_field}, pk={primary_key}, watermark={watermark or 'initial'})",
                f"CDC capture downgraded: binlog → cursor poll "
                f"({capture_downgrade['cdc_capture_downgrade_cause']}) — DELETEs are not "
                f"captured. {capture_downgrade['cdc_capture_downgrade_remedy']}",
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
                raise _refuse_log_capture(cdc, "postgresql")
            ddl_log = [
                f"CDC(logical_decoding) {src_type}.{table_name} → {dest_type}.{dest_table} "
                f"(pk={primary_key}, resume={'set' if watermark else 'initial+slot+lsn'})"
            ]
        except Exception as exc:
            from services.cdc_lease import CdcLeaseConflict

            if isinstance(exc, CdcLeaseConflict):
                raise
            capture_downgrade = _query_cdc_downgrade(exc, "postgresql")
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
                f"(cursor={cursor_field}, pk={primary_key}, watermark={watermark or 'initial'})",
                f"CDC capture downgraded: logical decoding → cursor poll "
                f"({capture_downgrade['cdc_capture_downgrade_cause']}) — DELETEs are not "
                f"captured. {capture_downgrade['cdc_capture_downgrade_remedy']}",
            ]
            try:
                _stamp_cdc_poll(
                    used_query_fallback=True,
                    job_id=job_id,
                    stream=table_name,
                    workspace_id=workspace_id,
                    schedule_id=schedule_id,
                )
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
                    _stamp_cdc_poll(
                    used_query_fallback=True,
                    job_id=job_id,
                    stream=table_name,
                    workspace_id=workspace_id,
                    schedule_id=schedule_id,
                )
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
                    _stamp_cdc_poll(
                    used_query_fallback=True,
                    job_id=job_id,
                    stream=table_name,
                    workspace_id=workspace_id,
                    schedule_id=schedule_id,
                )
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
    ret = None
    try:
        from services.cdc_retention_probe import attach_cdc_retention

        ret = attach_cdc_retention(cdc, src_cfg, table=table_name)
        if ret is not None:
            ddl_log.append(f"cdc_retention status={ret.status}")
    except Exception as exc:
        logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)

    if capture_downgrade:
        # Query CDC has no slot LSN, binlog GTID, or change-stream resume token.
        # Auto had already selected exactly-once from the contract's cdc_position.
        # That declaration is not a captured position. Stay at-least-once.
        eos_guarantee = "at_least_once"
        eos_active = False
        ddl_log.append(
            "CDC delivery stays at-least-once upsert — no durable "
            "LSN/GTID/resume token was captured "
            "(exactly_once_requires_durable_lsn). None was invented."
        )
    state = CdcState(cursor_key=cursor_key, watermark=watermark)
    # Chunk progress only. The cursor itself was resolved before the reader
    # was opened, and a checkpoint must not replace it here.
    cp_dict: dict[str, Any] = {}
    if checkpoint is not None:
        if isinstance(checkpoint, dict):
            cp_dict = checkpoint
        elif hasattr(checkpoint, "to_dict"):
            cp_dict = checkpoint.to_dict()  # type: ignore[assignment]
    total_chunks = max(1, int(cp_dict.get("chunk_index") or 0) + 1) if cp_dict else 1
    chunk_idx = int(cp_dict.get("chunk_index") or 0) if cp_dict else 0

    import os

    # Continuous CDC: drain snapshot, then poll until idle or budget exhausted.
    max_idle_polls = max(1, int(getenv_brand("CDC_MAX_IDLE_POLLS", "3")))
    max_poll_rounds = max(1, int(getenv_brand("CDC_MAX_POLL_ROUNDS", "50")))
    txn_hold_sleep = float(getenv_brand("CDC_TXN_HOLD_SLEEP_SEC", "0.25"))

    def _apply_and_checkpoint(change: ChangeBatch, *, publish_resume: bool = True) -> bool:
        """Apply one batch. A finished page may become the resume cursor.

        An offset snapshot page is not a resume position: rows still unread
        can sort before the page's cursor. ``publish_resume`` stays false
        until that dump has read the table. A crash then runs the snapshot
        again and upserts what already landed.

        A log snapshot token (``phase=snapshot``, table + last primary key)
        is a resume position. It is stored immediately and not acked. The
        next run continues ``snapshot()`` from that key. The slot or LSN is
        acked when the dump finishes.
        """
        nonlocal chunk_idx, total_chunks, eos_active
        from services.cdc_resume_tokens import (
            is_durable_log_resume_token,
            is_side_channel_resume_token,
            is_txn_held_token,
            serialize_resume_token,
        )

        if (
            not change.total_changes
            and not getattr(change, "rejected", None)
            and change.resume_token is None
        ):
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
            state.dest_before.capture(destination, table_name=str(dest_table or ""))
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
                census_acc=state.acc_for(str(dest_table or "")),
                delivery_guarantee=eos_guarantee,
                delivery_pinned=delivery_pinned,
                cursor_key=cursor_key,
                stream_name=str(table_name or dest_table or ""),
                mappings_inherited=mappings_inherited,
                pk_source_cols=pk_source_cols,
                align_lock=align_lock,
                writer_fence=int(
                    getattr(getattr(cdc, "_lease", None), "generation", 0) or 0
                ),
            )
        state.rows_written += rows_written
        state.source_changes_read += len(change.inserts) + len(change.updates)
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
        if isinstance(dest_summary, dict) and dest_summary.get("exactly_once_downgrade"):
            eos_active = False

        _refuse_cdc_advance_on_abort(dest_summary, validation_mode)

        # Never overwrite a durable log resume with incremental/side-channel tokens
        # (binlog gaps / wrong PG slots under load). Never ack those tokens either.
        skip_ack = False
        if change.resume_token is not None:
            if is_side_channel_resume_token(change.resume_token):
                skip_ack = True
            elif is_durable_log_resume_token(change.resume_token):
                try:
                    state.running_cursor = serialize_resume_token(
                        change.resume_token, default=json_util.default
                    )
                except TypeError:
                    state.running_cursor = str(change.resume_token)
            else:
                try:
                    state.running_cursor = serialize_resume_token(
                        change.resume_token, default=json_util.default
                    )
                except TypeError:
                    state.running_cursor = str(change.resume_token)
        elif change.inserts or change.updates:
            # Query CDC has no log token. Store (cursor, pk) when the cursor
            # is not unique so a restart seeks past the last applied peer
            # instead of dropping every row that shares that cursor value.
            from services.sync_cursor import query_cdc_resume_watermark

            tiebreak = cdc._keyset_tiebreak() if isinstance(cdc, CdcEngine) else ""
            resumed = query_cdc_resume_watermark(
                [r for r in (change.inserts + change.updates) if isinstance(r, dict)],
                cursor_field,
                tiebreak,
                state.running_cursor,
            )
            if resumed and resumed != state.running_cursor:
                state.running_cursor = resumed

        chunk_idx += 1
        total_chunks = max(total_chunks, chunk_idx)
        lag_fields = _cdc_lag_fields(cdc)
        try:
            _stamp_cdc_poll(
                lag_seconds=lag_fields.get("cdc_lag_seconds"),
                lag_bytes=lag_fields.get("replication_lag_bytes"),
                lag_basis=lag_fields.get("cdc_lag_basis"),
                job_id=job_id,
                stream=table_name,
                workspace_id=workspace_id,
                schedule_id=schedule_id,
            )
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
        from services.cdc_snapshot_mode import snapshot_dump_open

        open_dump = bool(
            state.running_cursor and snapshot_dump_open(state.running_cursor)
        )
        if state.running_cursor and (publish_resume or open_dump):
            set_watermark(
                cursor_key,
                state.running_cursor,
                metadata={
                    "job_id": job_id,
                    "sync_mode": sync_mode,
                    "chunk": chunk_idx,
                    "snapshot_dump_open": bool(open_dump and not publish_resume),
                    **lag_fields,
                },
            )
        # Ack source only AFTER a durable resume cursor (peek→apply→ack).
        # An open snapshot token is stored above so the next run can continue
        # the dump. The slot stays until that dump finishes.
        if publish_resume and hasattr(cdc, "ack") and not skip_ack:
            try:
                cdc.ack(change.resume_token)
            except Exception as ack_exc:
                logger.warning(
                    "CDC ack failed after watermark persist (at-least-once redelivery): %s",
                    ack_exc,
                )
        if on_checkpoint:
            # An offset page max is not a resume token, so the checkpoint keeps
            # the cursor this run started with. A phase=snapshot log token is
            # the resume position and is recorded with the cursor store.
            if publish_resume or open_dump:
                checkpoint_watermark = state.running_cursor
            else:
                checkpoint_watermark = watermark
            on_checkpoint(
                chunk_idx,
                total_chunks,
                state.rows_written,
                {
                    "chunk_index": chunk_idx,
                    "watermark": checkpoint_watermark,
                    "stream": table_name,
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
    from services.cdc_slot_resume import prepare_resume_for_missing_slot

    watermark, slot_note = prepare_resume_for_missing_slot(
        watermark, ret, cdc, mode=snapshot_mode
    )
    if slot_note:
        ddl_log.append(slot_note)
    snapshot_plan = resolve_cdc_snapshot_plan(
        snapshot_mode,
        watermark=watermark,
        retention=ret,
        dest_already_keyed=measure_dest_already_keyed(
            dest_type,
            dest_cfg,
            [(dest_table, pk_target_cols)],
            schema=str(dest_cfg.get("schema") or ""),
        ),
        incremental_capable=adapter_supports_incremental_interleave(cdc),
    )
    run_snapshot = bool(snapshot_plan["run_snapshot"])
    run_stream = bool(snapshot_plan["run_stream"])
    ddl_log.append(
        f"CDC snapshot_mode={snapshot_mode.value} snapshot_plan={snapshot_plan['kind']}"
        f"{' lost_window=1' if snapshot_plan.get('lost_window') else ''}"
    )
    if snapshot_plan.get("kind") == KIND_INCREMENTAL:
        from services.cdc_incremental_snapshot import enqueue_gap_recovery_snapshots

        signals = enqueue_gap_recovery_snapshots(
            str(getattr(cdc, "source_key", "") or ""),
            [(table_name, pk_source_cols)],
        )
        ddl_log.append(
            f"CDC incremental gap recovery signals={len(signals)} "
            f"(DDD-3 stream-wins; not blocking dump)"
        )

    try:
        with cdc_destination_hooks(destination, ddl_log):
            snapshot_resume = None
            if run_snapshot:
                with _cdc_span("cdc.snapshot", job_id=str(job_id or "")):
                    for change in cdc.snapshot():
                        if change.resume_token is not None:
                            snapshot_resume = change.resume_token
                        _apply_and_checkpoint(change, publish_resume=False)
                # The dump finished. This cursor is the handoff: every snapshot
                # row was applied, so a later poll may seek past it.
                if state.running_cursor:
                    from services.cdc_snapshot_mode import snapshot_dump_open

                    still_open = snapshot_dump_open(state.running_cursor)
                    set_watermark(
                        cursor_key,
                        state.running_cursor,
                        metadata={
                            "job_id": job_id,
                            "sync_mode": sync_mode,
                            "snapshot_complete": not still_open,
                            "snapshot_dump_open": still_open,
                        },
                    )
                if snapshot_resume is not None and hasattr(cdc, "ack"):
                    try:
                        cdc.ack(snapshot_resume)
                    except Exception as ack_exc:
                        logger.warning(
                            "CDC snapshot ack failed (at-least-once redelivery): %s",
                            ack_exc,
                        )

            # Query CDC (CdcEngine): one incremental pass when resuming. Log CDC adapters
            # continuously poll until idle so a single job drains the slot/binlog/CT stream.
            if run_stream:
                with _cdc_span("cdc.poll", job_id=str(job_id or "")):
                    if isinstance(cdc, CdcEngine):
                        if watermark is not None or not run_snapshot:
                            for change in cdc.poll():
                                _apply_and_checkpoint(change)
                    else:
                        outcome = _drain_log_reader(
                            cdc,
                            lambda change: _apply_and_checkpoint(change),
                            max_idle=max_idle_polls,
                            max_rounds=max_poll_rounds,
                            sleep_sec=txn_hold_sleep,
                        )
                        _raise_if_stream_behind(cdc, outcome)
                        # The source COUNT below can take long enough for another
                        # commit to land in the slot. Drain that change before the
                        # count is treated as the catch-up image.
                        if _log_capture_pending(cdc) is True:
                            outcome = _drain_log_reader(
                                cdc,
                                lambda change: _apply_and_checkpoint(change),
                                max_idle=max_idle_polls,
                                max_rounds=max_poll_rounds,
                                sleep_sec=txn_hold_sleep,
                            )
                            _raise_if_stream_behind(cdc, outcome)

        # A resume that already stepped past a row update completes with 0
        # events while the source cell differs. Upsert the current source
        # image for those keys. The binlog cursor is not moved.
        from services.cdc_resume_tokens import (
            is_durable_log_resume_token,
            unwrap_resume_token,
        )
        from services.cdc_value_digest import (
            CdcValueScanIncomplete,
            identity_rows_missing_on_dest,
            rows_absent_by_primary_key,
        )

        cursor_now = (
            state.running_cursor if state.running_cursor is not None else watermark
        )
        if (
            is_durable_log_resume_token(cursor_now)
            and getattr(source, "kind", "") == "database"
        ):
            try:
                missing_image = identity_rows_missing_on_dest(
                    source_type=src_type,
                    source_cfg=src_cfg,
                    source_table=str(table_name or ""),
                    dest_type=dest_type,
                    dest_cfg=dest_cfg,
                    dest_table=str(dest_table or ""),
                    mappings=list(mappings or []),
                    dest_types=column_types if isinstance(column_types, dict) else None,
                )
            except CdcValueScanIncomplete as exc:
                logger.warning("CDC image repair skipped: %s", exc)
                missing_image = None
            if not missing_image:
                # A transformed column (timestamptz → datetime) is not an
                # identity fingerprint, so the scan above declines. Keys the
                # destination does not have are still unapplied inserts. The
                # slot may already sit past them; this upsert does not move it.
                pk = str(primary_key or "")
                if pk:
                    try:
                        missing_image = rows_absent_by_primary_key(
                            source_type=src_type,
                            source_cfg=src_cfg,
                            source_table=str(table_name or ""),
                            dest_type=dest_type,
                            dest_cfg=dest_cfg,
                            dest_table=str(dest_table or ""),
                            mappings=list(mappings or []),
                            primary_key=pk,
                        )
                    except CdcValueScanIncomplete as exc:
                        logger.warning("CDC key image repair skipped: %s", exc)
                        missing_image = None
            if missing_image:
                token = unwrap_resume_token(cursor_now)
                for start in range(0, len(missing_image), 500):
                    _apply_and_checkpoint(
                        ChangeBatch(
                            updates=missing_image[start : start + 500],
                            resume_token=token,
                        )
                    )
                ddl_log.append(
                    f"CDC image repair upserted {len(missing_image)} source row(s) "
                    "whose cells were not on the destination. At-least-once. "
                    "Not a replay of the missed binlog event."
                )

        final_watermark = state.running_cursor if state.running_cursor is not None else watermark
        lag_fields = _cdc_lag_fields(cdc)
        if final_watermark is not None:
            set_watermark(
                cursor_key,
                final_watermark,
                metadata={"job_id": job_id, "sync_mode": sync_mode, **lag_fields},
            )

        summary = state.last_dest_summary or {}
        state.dest_before.stamp(summary, str(dest_table or table_name or ""))
        summary["cursor_key"] = cursor_key
        summary["cdc"] = {
            "inserts": state.inserts,
            "updates": state.updates,
            "deletes": state.deletes,
            "watermark": final_watermark,
            "cursor_key": cursor_key,
            "poll_rounds": max_poll_rounds,
            **lag_fields,
        }
        summary["cdc_lag_seconds"] = lag_fields.get("cdc_lag_seconds")
        summary["replication_lag_bytes"] = lag_fields.get("replication_lag_bytes")
        summary["cdc_heartbeat_at"] = lag_fields.get("cdc_heartbeat_at")
        summary["cdc_last_ddl_at"] = lag_fields.get("cdc_last_ddl_at")
        summary["cdc_plugin"] = lag_fields.get("cdc_plugin")
        summary["cdc_slot_name"] = lag_fields.get("cdc_slot_name")
        summary["cdc_publication_name"] = lag_fields.get("cdc_publication_name")
        if eos_active:
            summary["cdc_delivery"] = "exactly_once"
            summary["delivery_semantics"] = DELIVERY_SEMANTICS_EOS
            summary["exactly_once_algorithm"] = "dest_owned_watermark_txn"
            summary["exactly_once_protocol"] = PROTOCOL
            summary["exactly_once_active"] = True
            summary["exactly_once_claimed_platform"] = False
            summary["eos_dest_authoritative"] = True
        else:
            summary["cdc_delivery"] = lag_fields.get("cdc_delivery") or "at-least-once"
            summary.setdefault("delivery_semantics", DELIVERY_SEMANTICS_ALO)
        from services.cdc_named_eos import stamp_named_eos_on_summary

        summary = stamp_named_eos_on_summary(
            summary,
            source_type=src_type,
            dest_type=dest_type,
            sync_mode=sync_mode or "cdc",
            eos_operator_requested=eos_active,
        )
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
        stamp = snapshot_plan_stamp(snapshot_plan)
        if stamp:
            summary["snapshot_plan"] = stamp
        summary["watermark"] = final_watermark
        summary["checksum"] = state.last_checksum
        _stamp_cdc_source_image(
            summary,
            src_type=src_type,
            src_cfg=src_cfg,
            schema=str(src_cfg.get("schema") or getattr(source, "schema", "") or ""),
            table_name=table_name,
            events=int(state.inserts or 0) + int(state.updates or 0) + int(state.deletes or 0),
        )
        if summary.get("source_row_count_source") != "cdc_source_image_count":
            # No live source-table image to count (log/stream source or COUNT
            # failed): the reader's own change population is the measured count.
            stamp_source_row_count(
                summary,
                reader_count=int(state.source_changes_read or 0),
                rows_written=int(state.rows_written or 0),
                source="cdc_reader_changes",
            )
        # COUNT(*) is slow enough for a commit to land after the drain proved
        # the slot was empty. Read that change before this job may complete.
        if run_stream and not isinstance(cdc, CdcEngine) and _log_capture_pending(cdc) is True:
            outcome = _drain_log_reader(
                cdc,
                lambda change: _apply_and_checkpoint(change),
                max_idle=max_idle_polls,
                max_rounds=max_poll_rounds,
                sleep_sec=txn_hold_sleep,
            )
            _raise_if_stream_behind(cdc, outcome)
            _stamp_cdc_source_image(
                summary,
                src_type=src_type,
                src_cfg=src_cfg,
                schema=str(src_cfg.get("schema") or getattr(source, "schema", "") or ""),
                table_name=table_name,
                events=int(state.inserts or 0) + int(state.updates or 0) + int(state.deletes or 0),
            )
            if _log_capture_pending(cdc) is True:
                _raise_if_stream_behind(cdc, "behind")
        if capture_downgrade:
            summary.update(capture_downgrade)
        return state.rows_written, ddl_log, summary, headers
    finally:
        stamp_capture_identity(sys.exc_info()[1], cdc)
        if hasattr(cdc, "close"):
            try:
                cdc.close()
            except Exception as exc:
                logging.getLogger(__name__).debug(
                    "Exception suppressed: %s", exc, exc_info=exc
                )
