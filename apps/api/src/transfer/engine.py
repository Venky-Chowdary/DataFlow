"""Universal transfer orchestrator — routes any source to any destination."""

from __future__ import annotations

import logging
import os
import resource
import sys
import time
from pathlib import Path
from typing import Any, Optional

# Ensure the API root (parent of the `src` package) is first on sys.path so the
# `services` intelligence package resolves to `apps/api/services`, not an
# accidentally-shadowing `apps/api/src/services` that may be on PYTHONPATH.
_api_root = Path(__file__).resolve().parents[2]
if str(_api_root) not in sys.path:
    sys.path.insert(0, str(_api_root))

try:
    from services import lineage_telemetry as lineage
    from services.data_quality_history import validate_batch_against_history
    from services.error_handling import (
        RetryBudget,
        TransferCancelled,
        classify_error,
        with_retry,
    )
    from services.mirror_engine import apply_inferred_soft_deletes
    from services.mongodb_service import get_mongodb_service
    from services.pipeline_explanation import build_pipeline_explanation
    from services.value_serializer import cell_to_string
    from services.preflight_service import (
        apply_policy_gates,
        confidence_threshold_for_mode,
        probe_destination,
        run_file_preflight,
        run_transfer_policy_gates,
    )
    from services.row_filter import apply_row_filter
    from services.scd2_engine import apply_scd2
    from services.sync_cursor import (
        map_source_to_target,
        requires_upsert,
        resolve_sync_contract,
    )
except ImportError:  # pragma: no cover - compatibility for tests with api root on PYTHONPATH
    from src.services import lineage_telemetry as lineage
    from src.services.data_quality_history import validate_batch_against_history
    from src.services.error_handling import (
        RetryBudget,
        TransferCancelled,
        classify_error,
        with_retry,
    )
    from src.services.mirror_engine import apply_inferred_soft_deletes
    from src.services.mongodb_service import get_mongodb_service
    from src.services.pipeline_explanation import build_pipeline_explanation
    from src.services.value_serializer import cell_to_string
    from src.services.preflight_service import (
        apply_policy_gates,
        confidence_threshold_for_mode,
        probe_destination,
        run_file_preflight,
        run_transfer_policy_gates,
    )
    from src.services.row_filter import apply_row_filter
    from src.services.scd2_engine import apply_scd2
    from src.services.sync_cursor import (
        map_source_to_target,
        requires_upsert,
        resolve_sync_contract,
    )
from .adapters import (
    parse_file_content,
    read_source_database,
    resolve_connector_config,
    write_destination_database,
    write_destination_file,
)
from .cdc_transfer import run_cdc_database_transfer
from .file_stream import (
    peek_file_source,
    prepare_stream_content,
    should_stream_file,
    stream_file_to_database,
)
from .models import (
    EndpointConfig,
    TransferRequest,
    TransferResult,
    transfer_request_to_dict,
)
from .reconcile_step import run_reconciliation
from .registry import validate_transfer
from .stream import (
    peek_stream_source,
    stream_database_transfer,
    stream_scd2_mirror_transfer,
    supports_streaming,
)
from .type_mapper import default_mappings

try:
    from services.data_contract import ContractViolation

    from .contract_engine import enforce_or_create_contract, finalize_contract
except ImportError:  # pragma: no cover - compatibility for tests
    from src.services.data_contract import ContractViolation
    from src.transfer.contract_engine import (
        enforce_or_create_contract,
        finalize_contract,
    )
try:
    from ai.training.training_scheduler import schedule_training_on_transfer
except ImportError:  # pragma: no cover - compatibility for tests with api root on PYTHONPATH
    from src.ai.training.training_scheduler import schedule_training_on_transfer

from connectors.writer_common import CHUNK_SIZE
from services.batch_progress import ThrottledCheckpoint

try:
    from services import schema_registry
except ImportError:  # pragma: no cover - compatibility for tests with api root on PYTHONPATH
    from src.services import schema_registry
from services.checkpoint_service import (
    Checkpoint,
    CheckpointService,
    resume_or_create_checkpoint,
)

logger = logging.getLogger("dataflow.transfer")


def _compare_and_publish_load_history(
    mongo: Any,
    job_id: str,
    rows: list[dict],
    request: TransferRequest,
    schema: dict[str, str] | None,
    *,
    validation_mode: str,
    row_count_hint: int | None = None,
) -> dict[str, Any]:
    """Compare current sample/batch to last-N route history; publish on the job.

    Streaming paths pass a bounded ``rows`` sample plus ``row_count_hint`` for
    volume drift. Never raises — history must not abort writes.
    """
    load_history_report: dict[str, Any] = {}
    try:
        from services.data_quality_history import compare_route_to_history

        route = transfer_request_to_dict(request)
        load_history_report = compare_route_to_history(
            rows,
            route["source"],
            route["destination"],
            schema=schema,
            current_row_count=row_count_hint,
        )
        quality_anomalies = list(load_history_report.get("anomalies") or [])
        if quality_anomalies:
            mongo.update_job_status(
                job_id, "running", phase="quality_check", progress_pct=20,
                message="; ".join(quality_anomalies[:5]),
                load_history_report=load_history_report,
            )
            if validation_mode == "strict":
                load_history_report = {
                    **load_history_report,
                    "passed": False,
                    "strict_blocked": True,
                }
        else:
            mongo.update_job_status(
                job_id, "running",
                load_history_report=load_history_report,
            )
    except Exception as hist_exc:
        load_history_report = {
            "passed": True,
            "anomalies": [],
            "warning": f"Load-history compare unavailable: {hist_exc!s}"[:240],
            "prior_load_count": 0,
        }
        try:
            mongo.update_job_status(
                job_id, "running",
                load_history_report=load_history_report,
                message=load_history_report["warning"],
            )
        except Exception:
            pass
    return load_history_report


def _persist_load_history_profile(
    request: TransferRequest,
    rows: list[dict],
    schema: dict[str, str] | None,
    *,
    job_id: str,
    dest_summary: dict[str, Any],
    row_count: int,
) -> None:
    """Append this load to the route ring buffer (streaming-safe)."""
    try:
        from services.data_quality_history import profile_batch, save_profile

        route = transfer_request_to_dict(request)
        save_profile(
            route["source"],
            route["destination"],
            profile_batch(rows, schema),
            job_id=job_id,
            rejected_details=dest_summary.get("rejected_details") or [],
            rejected_rows=int(dest_summary.get("rejected_rows", 0) or 0),
            row_count=row_count,
        )
    except Exception:
        logger.debug("load-history save_profile skipped", exc_info=True)


def _fail_job_preflight(mongo, job_id: str, pf: dict, *, lineage) -> tuple[str, dict]:
    """Mark job failed at preflight and persist inspectable quarantine rows."""
    from services.quarantine_from_preflight import quarantine_rows_from_preflight

    decision = (pf.get("proof_bundle") or {}).get("transfer_decision", {}) or {}
    blocker_reasons = [b.get("message") for b in pf.get("blockers", []) if isinstance(b, dict)]
    qrows = quarantine_rows_from_preflight(pf)
    row_ids = {d.get("row") for d in qrows if d.get("row") is not None}
    rejected_rows = len(row_ids) if row_ids else len(qrows)
    error_details = {
        "reason": "Preflight blocked transfer",
        "blockers": blocker_reasons,
        "guidance": [
            {
                "gate": b.get("id"),
                "message": b.get("message"),
                "why": (b.get("guidance") or {}).get("why", ""),
                "fix": (b.get("guidance") or {}).get("fix", ""),
            }
            for b in pf.get("blockers", [])
            if isinstance(b, dict) and b.get("guidance")
        ],
        "proof_bundle": {
            "decision": decision.get("decision"),
            "reason": decision.get("reason"),
            "semantic_mapping_score": pf.get("proof_bundle", {}).get("semantic_mapping_score"),
            "min_confidence": pf.get("proof_bundle", {}).get("min_confidence"),
            "quality_score": pf.get("proof_bundle", {}).get("quality_score"),
            "compliance_risk": (pf.get("proof_bundle", {}).get("compliance") or {}).get("risk_score"),
        },
        "readiness_score": pf.get("readiness_score"),
        "validation_plan": pf.get("validation_plan"),
        "payload_shape": pf.get("payload_shape"),
        "quarantine_issue_count": len(qrows),
        "quarantine_row_count": rejected_rows,
    }
    error_message = decision.get("reason") or "; ".join(str(x) for x in blocker_reasons if x) or "Preflight blocked transfer"
    lineage.emit_preflight_completed(
        run_id=job_id, passed=False,
        readiness_score=pf.get("readiness_score", 0),
        blockers=pf.get("blockers", []),
        validation_plan=pf.get("validation_plan"),
    )
    lineage.emit_run_failed(
        run_id=job_id, job_id=job_id, error=error_message,
        error_details=error_details,
    )
    mongo.update_job_status(
        job_id, "failed",
        error=error_message,
        phase="failed",
        progress_pct=0,
        error_details=error_details,
        preflight=pf,
        rejected_details=qrows,
        rejected_rows=rejected_rows,
    )
    return error_message, error_details


def _coalesce_sort_value(value: Any) -> Any:
    """Return a tuple that sorts None/empty values last regardless of direction."""
    if value is None or value == "":
        return (1, "")
    if isinstance(value, (int, float)):
        return (0, value)
    try:
        return (0, float(value))
    except (TypeError, ValueError):
        return (0, str(value).lower())


def _apply_priority_and_limit(
    records: list[dict[str, Any]],
    priority_column: str,
    direction: str,
    limit: int,
) -> list[dict[str, Any]]:
    """Sort source rows by a priority column and optionally cap the row count."""
    if not priority_column or not records:
        if limit > 0:
            return records[:limit]
        return records

    reverse = direction != "asc"
    sorted_records = sorted(
        records,
        key=lambda r: _coalesce_sort_value(r.get(priority_column)),
        reverse=reverse,
    )
    if limit > 0:
        return sorted_records[:limit]
    return sorted_records


def _redacted_endpoint(ep: EndpointConfig) -> dict[str, Any]:
    """Return endpoint metadata without credentials for lineage and logging."""
    d = {
        "kind": ep.kind,
        "format": ep.format,
        "connector_id": ep.connector_id,
        "host": ep.host,
        "port": ep.port,
        "database": ep.database,
        "schema": ep.schema,
        "table": ep.table,
        "collection": ep.collection,
        "warehouse": ep.warehouse,
    }
    return {k: v for k, v in d.items() if v}


def _build_explanation(
    request: TransferRequest,
    columns: list[str],
    schema: dict[str, str] | None,
    mappings: list[dict[str, Any]],
    recon: dict[str, Any],
    dest_summary: dict[str, Any],
    pf: dict[str, Any] | None,
    rows_written: int,
) -> str:
    rejected = int(dest_summary.get("rejected_rows", 0) or 0)
    return build_pipeline_explanation(
        request=request,
        columns=columns,
        source_schema=schema,
        mappings=mappings,
        reconciliation=recon,
        destination_summary=dest_summary,
        validation_plan=pf.get("validation_plan") if pf else None,
        rows_written=rows_written,
        rejected_rows=rejected,
    )


def _destination_schema_types(destination: EndpointConfig, sync_mode: str = "") -> dict[str, str]:
    """Introspect destination column types for schema-aware preflight and transforms.

    For full-refresh overwrite sync modes the destination table will be dropped
    and recreated, so any existing schema is irrelevant and should not influence
    mapping or preflight decisions.
    """
    if destination.kind != "database":
        return {}
    if (sync_mode or "full_refresh_overwrite").lower() in {"full_refresh_overwrite", "overwrite"}:
        return {}
    try:
        from .endpoint_intelligence import introspect_endpoint

        info = introspect_endpoint(destination)
        return dict(info.get("schema") or {})
    except Exception:
        return {}


def _infer_primary_key(columns: list[str], mappings: list[dict[str, Any]]) -> str:
    """Infer the primary key target column for mirror/upsert transfers.

    Prefers a mapping whose source column is named ``id`` or ends with ``_id``,
    then any source column that looks like a unique identifier.  Returns the
    target column name (or the source name if the mapping has no explicit target).
    """
    if not columns:
        return ""
    mapping_dict = {m.get("source", ""): m.get("target", m.get("source", "")) for m in mappings}
    candidates = [c for c in columns if c.lower() == "id" or c.lower().endswith("_id")]
    if not candidates:
        # Fall back to the first short, non-nullable-looking column name.
        candidates = [columns[0]]
    for src in candidates:
        tgt = mapping_dict.get(src, src)
        if tgt:
            return tgt
    return mapping_dict.get(columns[0], columns[0])


def _checkpoint_has_progress(checkpoint: Any) -> bool:
    """True when the checkpoint has committed rows from a previous run."""
    if not checkpoint:
        return False
    return bool(
        getattr(checkpoint, "chunk_index", 0)
        or getattr(checkpoint, "offset", 0)
        or getattr(checkpoint, "rows_processed", 0)
    )


def _persist_job_quarantine(job_id: str, dest_summary: dict[str, Any], request: Any = None) -> None:
    """Best-effort durable DLQ write for rejected rows; never hide transfer success."""
    details = dest_summary.get("rejected_details") or []
    if not details:
        return
    try:
        from services.quarantine_dlq import persist_rejected_rows

        persist_rejected_rows(
            job_id=job_id,
            rejected_details=details,
            workspace_id=str(getattr(request, "workspace_id", "") or "") if request else "",
            source="universal_engine",
        )
    except Exception as exc:
        dest_summary["quarantine_dlq_error"] = str(exc)[:300]
        dest_summary["quarantine_durable"] = False
    else:
        dest_summary["quarantine_durable"] = True


_CDC_JOB_FIELDS = (
    "cdc_lag_seconds",
    "replication_lag_bytes",
    "cdc_heartbeat_at",
    "cdc_last_ddl_at",
    "cdc_plugin",
    "cdc_slot_name",
    "cdc_delivery",
    "cdc_lease_holder",
    "cdc_lease_resource",
    "cdc_lease_stale",
    "cdc_lease_heartbeat_age_sec",
    "cdc_lease_backend",
    "cdc_lease_generation",
    "watermark",
)


def _promote_cdc_job_fields(checkpoint: dict[str, Any], update: dict[str, Any]) -> None:
    """Copy CDC lag/health fields onto the job document for SSE + UI tiles."""
    if not isinstance(checkpoint, dict):
        return
    for key in _CDC_JOB_FIELDS:
        if key in checkpoint and key not in update:
            update[key] = checkpoint.get(key)
    cdc_meta = checkpoint.get("cdc") or {}
    if isinstance(cdc_meta, dict):
        for key in _CDC_JOB_FIELDS:
            if key in cdc_meta and key not in update:
                update[key] = cdc_meta.get(key)
    streams = checkpoint.get("streams")
    if isinstance(streams, list) and streams:
        update["streams"] = streams
    summary_streams = (checkpoint.get("destination_summary") or {}).get("streams")
    if isinstance(summary_streams, list) and summary_streams and "streams" not in update:
        update["streams"] = summary_streams


def _job_failure_fields(exc: Exception) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build error_details + top-level job fields for a failed transfer."""
    classification = classify_error(exc)
    details: dict[str, Any] = {
        "retriable": classification.get("retriable"),
        "evidence": classification.get("evidence"),
    }
    extras: dict[str, Any] = {}
    try:
        from services.cdc_lease import CdcLeaseConflict, LeaseStoreError

        if isinstance(exc, CdcLeaseConflict):
            details.update(exc.to_dict())
            details["retriable"] = False
            extras = {
                "cdc_lease_conflict": True,
                "cdc_lease_holder": exc.holder_id or None,
                "cdc_lease_resource": exc.resource or None,
            }
        elif isinstance(exc, LeaseStoreError):
            details["code"] = "cdc_lease_store_unavailable"
            details["retriable"] = True  # Redis blip — safe to retry once store is back
            extras = {"cdc_lease_backend": "unavailable"}
    except Exception:
        pass
    return details, extras


def _cdc_fields_from_summary(dest_summary: dict[str, Any] | None) -> dict[str, Any]:
    """Top-level job fields from a CDC destination summary."""
    if not isinstance(dest_summary, dict):
        return {}
    out: dict[str, Any] = {}
    for key in _CDC_JOB_FIELDS:
        if key in dest_summary:
            out[key] = dest_summary.get(key)
    cdc_meta = dest_summary.get("cdc") or {}
    if isinstance(cdc_meta, dict):
        for key in _CDC_JOB_FIELDS:
            if key in cdc_meta and key not in out:
                out[key] = cdc_meta.get(key)
    streams = dest_summary.get("streams")
    if isinstance(streams, list) and streams:
        out["streams"] = streams
    return out


def _drop_destination_table(destination: EndpointConfig) -> bool:
    """Drop the destination object for full-refresh overwrite sync modes."""
    if destination.kind != "database":
        return False
    try:
        from connectors.table_manager import drop_table

        from .adapters import resolve_connector_config, resolve_dest_table
        from .connector_capabilities import resolve_driver_type

        db_type = resolve_driver_type(destination.format)
        cfg = resolve_connector_config(destination)
        table_name = resolve_dest_table(db_type, destination)
        schema = cfg.get("schema")
        return drop_table(db_type, cfg, table_name, schema)
    except Exception:
        return False


def _schema_for_endpoint(destination: EndpointConfig) -> str | None:
    """Return the SQL schema name implied by a database endpoint config."""
    try:
        from connectors.generic_sql import get_sql_schema

        from .adapters import resolve_connector_config

        cfg = resolve_connector_config(destination)
        return get_sql_schema(cfg)
    except Exception:
        return None


def _enrich_mappings_with_types(
    mappings: list[dict],
    dest_types: dict[str, str] | None = None,
    column_types: dict[str, str] | None = None,
) -> list[dict]:
    if not mappings:
        return mappings
    try:
        from services.transform_resolver import attach_transforms_to_mappings

        return attach_transforms_to_mappings(
            mappings,
            column_types=column_types or {},
            dest_types=dest_types or {},
        )
    except Exception:
        pass
    out = []
    for m in mappings:
        enriched = dict(m)
        tgt = m.get("target")
        if tgt and dest_types and tgt in dest_types:
            enriched["target_type"] = dest_types[tgt]
        out.append(enriched)
    return out


def _auto_map(
    request: TransferRequest,
    columns: list[str],
    schema: dict[str, str],
    sample_rows: list[dict] | None = None,
    job_id: str = "",
) -> list[dict]:
    """Generate destination-aware mappings when no mapping contract was supplied.

    For append/upsert/merge into an existing target, the destination schema is
    introspected and the semantic mapper aligns source columns to target columns.
    For full-refresh/overwrites into a new target, identity mappings are used so
    the destination can be created from the source shape.
    """
    mappings: list[dict] | None = None

    if request.mappings:
        mappings = request.mappings
    elif request.destination.kind != "database":
        mappings = default_mappings(columns)
    else:
        sync_mode = (request.sync_mode or "full_refresh_overwrite").lower()
        if sync_mode in {"full_refresh_overwrite", "overwrite"}:
            mappings = default_mappings(columns)
        else:
            target_schema = _destination_schema_types(request.destination, sync_mode=request.sync_mode)
            if not target_schema:
                mappings = default_mappings(columns)
            else:
                # For MongoDB append/upsert, do not let the semantic mapper overwrite _id
                # unless the source literally contains an _id column or the user supplied a mapping.
                if (
                    request.destination.format == "mongodb"
                    and sync_mode not in {"full_refresh_overwrite", "overwrite"}
                    and "_id" not in columns
                ):
                    target_schema = {k: v for k, v in target_schema.items() if k != "_id"}

                try:
                    from services.mapping_pipeline import run_mapping_pipeline

                    source_schemas = [
                        {
                            "name": c,
                            "inferred_type": schema.get(c, "string"),
                            "samples": [cell_to_string(r.get(c, "")) for r in (sample_rows or [])[:8]],
                        }
                        for c in columns
                    ]
                    target_columns = list(target_schema.keys())
                    target_schemas = [
                        {"name": c, "inferred_type": target_schema.get(c, "string"), "samples": []}
                        for c in target_columns
                    ]
                    source_samples = {
                        c: [cell_to_string(r.get(c, "")) for r in (sample_rows or [])[:8]]
                        for c in columns
                    }
                    result = run_mapping_pipeline(
                        source_columns=columns,
                        target_columns=target_columns,
                        source_schemas=source_schemas,
                        target_schemas=target_schemas,
                        file_format=request.source.format,
                        confidence_threshold=confidence_threshold_for_mode(request.validation_mode),
                        source_samples=source_samples,
                        validation_mode=request.validation_mode,
                        use_llm=False,
                        schema_policy=request.schema_policy,
                    )
                    auto = result.get("mappings")
                    if auto and isinstance(auto, list) and any(m.get("source") for m in auto):
                        mapped_sources = {str(m.get("source")) for m in auto}
                        if request.backfill_new_fields:
                            for c in columns:
                                if c not in mapped_sources:
                                    auto.append({"source": c, "target": c, "confidence": 0.95})
                        mappings = auto
                except Exception as exc:
                    logger.warning("Auto-mapping failed: %s; falling back to identity mappings", exc)

    if mappings is None:
        mappings = default_mappings(columns)

    _record_schema_and_lineage(request, mappings, schema, job_id)
    return mappings


def _record_schema_and_lineage(
    request: TransferRequest,
    mappings: list[dict[str, Any]],
    schema: dict[str, str],
    job_id: str,
) -> None:
    """Register source/target schemas and column-level lineage for this job."""
    src_id = (
        request.source.connector_id
        or request.source.host
        or (request.source_filename if request.source.kind == "file" else "")
        or f"{request.source.kind}:{request.source.format}"
    )
    src_object = (
        request.source.table
        or request.source.collection
        or (request.source_filename if request.source.kind == "file" else "")
        or "source"
    )
    dst_id = (
        request.destination.connector_id
        or request.destination.host
        or f"{request.destination.kind}:{request.destination.format}"
    )
    dst_object = (
        request.destination.table
        or request.destination.collection
        or f"{request.destination.format}_export"
    )

    source_columns = [
        {"name": name, "type": (schema.get(name) or "string"), "primary_key": False}
        for name in schema.keys()
    ]
    schema_registry.register_schema(
        columns=source_columns,
        connector_type=request.source.format,
        connector_id=src_id,
        object_name=src_object,
        job_id=job_id,
        source_of_truth=True,
    )

    # Build a best-effort target schema from the mappings.
    target_columns = []
    for m in mappings:
        src = str(m.get("source", "")).strip()
        tgt = str(m.get("target", "")).strip()
        if not src or not tgt:
            continue
        target_columns.append({
            "name": tgt,
            "type": schema.get(src, "string"),
            "primary_key": False,
        })
    if target_columns:
        schema_registry.register_schema(
            columns=target_columns,
            connector_type=request.destination.format,
            connector_id=dst_id,
            object_name=dst_object,
            job_id=job_id,
        )
        schema_registry.record_lineage(
            source={"connector_type": request.source.format, "connector_id": src_id, "object_name": src_object},
            target={"connector_type": request.destination.format, "connector_id": dst_id, "object_name": dst_object},
            mappings=mappings,
            job_id=job_id,
        )


class UniversalTransferEngine:
    """
    Orchestrates universal data movement:
    - File → Database (MongoDB, PostgreSQL, Snowflake)
    - File → File (CSV, JSON, JSONL)
    - Database → Database
    - Database → File
    Auto-creates tables, collections, and schemas as needed.
    """

    def execute(self, request: TransferRequest) -> TransferResult:
        """Synchronous transfer — creates job record on completion."""
        self._resolve_saved_connectors(request)
        job_id = self._create_pending_job(request)
        return self.execute_tracked(request, job_id)

    @staticmethod
    def _peak_memory_bytes() -> int:
        """Return the maximum resident set size (bytes) for this process so far."""
        try:
            return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
        except Exception:
            return 0

    def _resolve_saved_connectors(self, request: TransferRequest) -> None:
        """Expand connector_id references into full host/port/credentials before execution."""
        try:
            from .adapters import resolve_endpoint
        except Exception:
            return
        request.source = resolve_endpoint(request.source, workspace_id=request.workspace_id)
        request.destination = resolve_endpoint(request.destination, workspace_id=request.workspace_id)

    def execute_tracked(self, request: TransferRequest, job_id: str, resume: bool = False) -> TransferResult:
        """Timed wrapper around the core transfer engine."""
        self._resolve_saved_connectors(request)
        start = time.monotonic()
        start_mem = self._peak_memory_bytes()
        result = self._execute_tracked_core(request, job_id, resume=resume)
        elapsed = time.monotonic() - start
        result.elapsed_seconds = round(elapsed, 3)
        result.records_per_second = round(result.records_transferred / elapsed, 3) if elapsed > 0 else 0.0
        result.peak_memory_bytes = max(self._peak_memory_bytes() - start_mem, 0)
        # Surface SLA metrics in the destination summary for the UI / API consumers.
        result.destination_summary["elapsed_seconds"] = result.elapsed_seconds
        result.destination_summary["records_per_second"] = result.records_per_second
        result.destination_summary["peak_memory_bytes"] = result.peak_memory_bytes
        self._notify_job_status(request, result)
        return result

    def _notify_job_status(self, request: TransferRequest, result: TransferResult) -> None:
        """Fire workspace notifications for failed or partially-quarantined jobs."""
        rejected = result.destination_summary.get("rejected_rows", 0) or 0
        coerced = result.destination_summary.get("coerced_null_rows", 0) or 0
        if result.success and not rejected and not coerced:
            return
        try:
            from services.notification_service import (
                build_job_payload,
                log_job_notifications,
                notify_workspace,
            )
            from services.platform_config import public_url, web_url

            status = "failed"
            if result.success and (rejected or coerced):
                # Successful terminal run that altered/dropped data — consistent
                # with the persisted job status.
                status = "completed_with_quarantine"
            elif result.success:
                status = "completed"
            payload = build_job_payload(
                job_id=result.job_id,
                status=status,
                source=request.source.kind or "unknown",
                destination=request.destination.kind or "unknown",
                records_transferred=result.records_transferred or 0,
                rejected_rows=int(rejected),
                error=result.error or "",
                retry_url=f"/api/v1/connectors/jobs/{result.job_id}/resume",
                workspace_id=request.workspace_id or "",
                base_url=public_url(),
                web_url=web_url(),
            )
            results = notify_workspace(request.workspace_id or "", payload)
            log_job_notifications(result.job_id, results)
        except Exception:
            # Notifications must never fail a transfer.
            pass

    def _execute_tracked_core(self, request: TransferRequest, job_id: str, resume: bool = False) -> TransferResult:
        mongo = get_mongodb_service()
        checkpoint_service = CheckpointService(mongo)
        checkpoint = None
        if resume:
            try:
                checkpoint = resume_or_create_checkpoint(job_id, checkpoint_service)
            except Exception:
                pass
        if not resume:
            checkpoint = Checkpoint(job_id=job_id)
        lineage.emit_run_started(
            run_id=job_id,
            job_id=job_id,
            source=_redacted_endpoint(request.source),
            destination=_redacted_endpoint(request.destination),
            validation_mode=request.validation_mode,
            write_semantics=request.sync_mode,
        )
        src_fmt = request.source.format or "csv"
        dst_fmt = request.destination.format or "mongodb"
        ok, msg = validate_transfer(
            request.source.kind, src_fmt,
            request.destination.kind, dst_fmt,
        )
        if not ok:
            from .connector_capabilities import transfer_live_driver_types
            live = ", ".join(transfer_live_driver_types())
            msg = f"{msg}. Transfer-live drivers: {live}."
            mongo.update_job_status(job_id, "failed", error=msg, phase="failed", progress_pct=0)
            lineage.emit_run_failed(
                run_id=job_id, job_id=job_id, error=msg,
                error_details={"reason": "Unsupported route", "supported": live},
            )
            return TransferResult(success=False, error=msg, operation=request.operation, job_id=job_id)

        if (
            supports_streaming(request.source, request.destination)
            and not request.priority_column
        ):
            try:
                return self._execute_streaming(
                    request, job_id, mongo, src_fmt,
                    resume=resume,
                    checkpoint=checkpoint,
                    checkpoint_service=checkpoint_service,
                )
            except NotImplementedError:
                # Streaming transfer is not implemented for this sync-mode/destination
                # combination (e.g. SCD2/mirror to a non-SQL destination). Fall through
                # to the buffered path which supports all destinations.
                pass

        non_streaming_mode = request.sync_mode.lower() in ("full_refresh_mirror", "mirror", "scd2")
        if (
            request.source.kind == "file"
            and request.destination.kind == "database"
            and (request.source_content or request.source_path)
            and not non_streaming_mode
            and not request.priority_column
            and request.limit == 0
            and should_stream_file(
                request.source_path or request.source_content,
                request.source_filename or "upload.csv",
                request.destination,
            )
        ):
            return self._execute_file_streaming(
                request, job_id, mongo, src_fmt,
                resume=resume,
                checkpoint=checkpoint,
                checkpoint_service=checkpoint_service,
            )

        pf = None
        contract_id = ""
        try:
            mongo.update_job_status(
                job_id, "running", phase="reading", progress_pct=5,
                message="Reading source data…",
            )
            records, columns, schema = with_retry(
                lambda: self._read_source(request),
                budget=RetryBudget(max_attempts=3, base_delay_seconds=0.5, max_delay_seconds=5.0),
            )
            if request.source_filter:
                records = apply_row_filter(records, request.source_filter)
            records = _apply_priority_and_limit(
                records,
                request.priority_column,
                request.priority_direction,
                request.limit,
            )
            if not records and request.source.kind != "database":
                mongo.update_job_status(job_id, "failed", error="No records to transfer", phase="failed")
                return TransferResult(success=False, error="No records to transfer", operation=request.operation, job_id=job_id)

            total_rows = len(records)
            mongo.update_job_status(job_id, "running", total_rows=total_rows, records_processed=0)

            dest_schema_types = _destination_schema_types(request.destination, sync_mode=request.sync_mode)
            mappings = _enrich_mappings_with_types(
                _auto_map(request, columns, schema, sample_rows=records[:100], job_id=job_id),
                column_types=schema,
                dest_types=dest_schema_types,
            )
            # Resolve upsert mode for non-streaming database writes.
            contract = resolve_sync_contract(request.stream_contracts)
            effective_sync = contract.sync_mode if contract else request.sync_mode
            effective_sync_lower = (effective_sync or "").lower()
            write_mode = "insert"
            conflict_columns: list[str] = []
            if contract and contract.primary_key:
                conflict_columns = [
                    map_source_to_target(col, mappings) for col in contract.primary_key_columns()
                ]
            if not conflict_columns and effective_sync_lower in ("full_refresh_mirror", "mirror", "scd2"):
                inferred_pk = _infer_primary_key(columns, mappings)
                if inferred_pk:
                    conflict_columns = [inferred_pk]
            if requires_upsert(effective_sync) and conflict_columns:
                write_mode = "upsert"

            activation_notes: list[str] = []
            if effective_sync_lower == "reverse_etl":
                from services.reverse_etl import plan_activation

                plan = plan_activation(
                    destination_kind=request.destination.format or "",
                    object_name=request.destination.table
                    or request.destination.collection
                    or "",
                    primary_key=conflict_columns or ["id"],
                    field_map={
                        str(m.get("source") or ""): str(m.get("target") or m.get("source") or "")
                        for m in (mappings or [])
                        if m.get("source")
                    },
                    mode="upsert",
                )
                write_mode = plan.mode or "upsert"
                activation_notes = list(plan.notes or [])
                if not conflict_columns:
                    conflict_columns = list(plan.primary_key)
                # Apply planner object name so SaaS writers hit the intended CRM object.
                if plan.object_name:
                    request.destination.table = plan.object_name
                if plan.batch_size:
                    request.destination.extra = dict(request.destination.extra or {})
                    request.destination.extra["activation_batch_size"] = plan.batch_size
                activation_notes.append(
                    f"Activation apply: mode={write_mode} pk={','.join(conflict_columns)} "
                    f"object={plan.object_name} batch={plan.batch_size}"
                )

            mongo.update_job_status(
                job_id, "running", phase="preflight", progress_pct=15,
                message="Validating mapping and schema…",
            )
            if not request.skip_preflight and request.destination.kind == "database":
                dest_ok, dest_msg = probe_destination(request.destination)
                pf = run_file_preflight(
                    columns=columns,
                    column_types=schema,
                    row_count=len(records),
                    mappings=mappings,
                    destination_connected=dest_ok,
                    destination_error=None if dest_ok else dest_msg,
                    source_kind=request.source.kind,
                    source_format=request.source.format,
                    sync_mode=request.sync_mode,
                    sample_rows=records[:100],
                    confidence_threshold=confidence_threshold_for_mode(request.validation_mode),
                    destination_column_types=dest_schema_types,
                    destination_table_exists=bool(dest_schema_types),
                    destination_can_create=dest_ok,
                    destination_db_type=dst_fmt.lower(),
                    validation_mode=request.validation_mode,
                    source_table=(
                        request.source.table
                        or request.source.collection
                        or request.source_filename
                        or ""
                    ),
                    destination_table=(
                        request.destination.table or request.destination.collection or ""
                    ),
                    source_filename=request.source_filename or "",
                )
                pf = apply_policy_gates(
                    pf,
                    run_transfer_policy_gates(
                        sync_mode=request.sync_mode,
                        schema_policy=request.schema_policy,
                        validation_mode=request.validation_mode,
                        stream_contracts=request.stream_contracts,
                        backfill_new_fields=request.backfill_new_fields,
                    ),
                    validation_mode=request.validation_mode,
                    destination_db_type=dst_fmt.lower(),
                )
                if not dest_ok:
                    mongo.update_job_status(
                        job_id, "failed",
                        error=f"Destination unreachable: {dest_msg}",
                        phase="failed", progress_pct=0,
                    )
                    line_msg = f"Destination unreachable: {dest_msg}"
                    lineage.emit_run_failed(
                        run_id=job_id, job_id=job_id, error=line_msg,
                        error_details={"reason": "Destination unreachable"},
                    )
                    return TransferResult(
                        success=False,
                        error=line_msg,
                        operation=request.operation,
                        job_id=job_id,
                    )
                if not pf["passed"]:
                    error_message, error_details = _fail_job_preflight(mongo, job_id, pf, lineage=lineage)
                    return TransferResult(
                        success=False,
                        error=error_message,
                        error_details=error_details,
                        validation_plan=pf.get("validation_plan") or {},
                        payload_shape=pf.get("payload_shape") or {},
                        operation=request.operation,
                        job_id=job_id,
                    )

            if pf:
                mongo.update_job_status(job_id, "running", phase="preflight", progress_pct=15, preflight=pf)

            # Data contract / circuit breaker enforcement.
            try:
                contract_id = enforce_or_create_contract(request, schema, mappings, pf)
            except ContractViolation as cv:
                msg = cv.message
                mongo.update_job_status(job_id, "failed", error=msg, phase="failed", progress_pct=0)
                return TransferResult(
                    success=False,
                    error=msg,
                    error_details={"violations": cv.violations},
                    operation=request.operation,
                    job_id=job_id,
                )

            # History-aware data quality: compare this load to the last N runs
            # for the same source→destination route (null-rate, volume, mean MAD).
            load_history_report = _compare_and_publish_load_history(
                mongo, job_id, records, request, schema,
                validation_mode=request.validation_mode,
                row_count_hint=len(records),
            )
            if load_history_report.get("strict_blocked"):
                anomalies = list(load_history_report.get("anomalies") or [])
                msg = "Data quality anomaly: " + "; ".join(anomalies[:8])
                mongo.update_job_status(
                    job_id, "failed", error=msg, phase="failed", progress_pct=0,
                    load_history_report=load_history_report,
                )
                return TransferResult(
                    success=False,
                    error=msg,
                    operation=request.operation,
                    job_id=job_id,
                    destination_summary={"load_history_report": load_history_report},
                    error_details={"load_history_report": load_history_report},
                )

            ddl_log: list[str] = []
            dest_summary: dict = {}
            rows_written = 0

            mongo.update_job_status(
                job_id, "running", phase="writing", progress_pct=25,
                message=f"Writing {total_rows:,} rows…",
            )

            def _check_cancelled() -> None:
                try:
                    job = mongo.get_job(job_id)
                    if job and job.get("status") == "cancelled":
                        raise TransferCancelled("Transfer cancelled by user")
                except TransferCancelled:
                    raise
                except Exception:
                    pass

            def on_checkpoint(chunk: int, chunks: int, rows: int, checkpoint: dict | None = None) -> None:
                _check_cancelled()
                pct = 25 + int((chunk / max(chunks, 1)) * 65)
                update = dict(
                    records_processed=rows,
                    progress_pct=min(pct, 90),
                    chunk_current=chunk,
                    chunk_total=chunks,
                    message=f"Writing batch {chunk}/{chunks} ({rows:,} rows)…",
                )
                if checkpoint:
                    update["checkpoint"] = checkpoint
                    update["destination_summary"] = {
                        "checksum": checkpoint.get("checksum", ""),
                        "rejected_rows": checkpoint.get("rejected_rows", 0),
                        "rejected_details": (checkpoint.get("rejected_details") or [])[:50],
                    }
                    _promote_cdc_job_fields(checkpoint, update)
                mongo.update_job_status(job_id, "running", **update)

            throttled_checkpoint = ThrottledCheckpoint(on_checkpoint)

            if request.destination.kind == "database":
                # Buffered path still reloads all rows; only skip the destructive
                # full-refresh DROP when resuming with a durable checkpoint that
                # already wrote progress (avoids wiping destination on resume).
                checkpoint_has_progress = _checkpoint_has_progress(checkpoint)
                should_drop_full_refresh = (
                    request.sync_mode.lower() in ("full_refresh_overwrite", "overwrite")
                    and not (resume and checkpoint_has_progress)
                )
                if resume and checkpoint_has_progress and write_mode == "insert":
                    # Non-idempotent resume would duplicate; force upsert when PK known.
                    if conflict_columns:
                        write_mode = "upsert"
                    else:
                        raise ValueError(
                            "Cannot safely resume a buffered insert without primary key; "
                            "use upsert sync mode or restart with full_refresh_overwrite"
                        )

                def _write_destination_with_drop():
                    # Drop inside the retry boundary so a failed full-refresh write
                    # retries from an empty table and cannot duplicate already-loaded rows.
                    if should_drop_full_refresh:
                        _drop_destination_table(request.destination)
                    return write_destination_database(
                        request.destination, records, columns, schema, mappings,
                        on_checkpoint=throttled_checkpoint,
                        validation_mode=request.validation_mode,
                        backfill_new_fields=request.backfill_new_fields,
                        write_mode=write_mode,
                        conflict_columns=conflict_columns,
                        job_id=job_id,
                    )

                if effective_sync_lower == "scd2" and conflict_columns:
                    scd2_summary = with_retry(
                        lambda: apply_scd2(
                            request.destination,
                            records,
                            columns,
                            schema,
                            mappings,
                            conflict_columns,
                        ),
                        budget=RetryBudget(max_attempts=3, base_delay_seconds=0.5, max_delay_seconds=5.0),
                    )
                    dest_summary = {
                        "table": request.destination.table or request.destination.collection,
                        "schema": _schema_for_endpoint(request.destination),
                        "checksum": scd2_summary.get("active_checksum", ""),
                        "scd2": scd2_summary,
                    }
                    rows_written = scd2_summary.get("rows_written", 0)
                    ddl_log.append(
                        f"SCD2 merge: {scd2_summary.get('active_rows', 0)} active, "
                        f"{scd2_summary.get('updated_rows', 0)} expired"
                    )
                else:
                    rows_written, ddl_log, dest_summary = with_retry(
                        _write_destination_with_drop,
                        budget=RetryBudget(max_attempts=3, base_delay_seconds=0.5, max_delay_seconds=5.0),
                    )
                    if activation_notes:
                        ddl_log = list(ddl_log or []) + [
                            f"reverse-ETL: {n}" for n in activation_notes
                        ]
                        dest_summary["reverse_etl"] = {"notes": activation_notes}
                    if effective_sync_lower in ("full_refresh_mirror", "mirror") and conflict_columns:
                        mirror_summary = apply_inferred_soft_deletes(
                            request.destination,
                            records,
                            columns,
                            schema,
                            mappings,
                            conflict_columns,
                        )
                        dest_summary["mirror"] = mirror_summary
                        rows_written = mirror_summary.get("active_rows", rows_written)
                if total_rows <= CHUNK_SIZE:
                    mongo.update_job_status(job_id, "running", records_processed=rows_written, progress_pct=90)
            elif request.destination.kind == "file_export":
                export_bytes, export_name, dest_summary = with_retry(
                    lambda: write_destination_file(
                        request.destination,
                        records,
                        columns,
                        source_format=src_fmt,
                        mappings=mappings,
                        column_types=request.column_types or schema,
                    ),
                    budget=RetryBudget(max_attempts=3, base_delay_seconds=0.5, max_delay_seconds=5.0),
                )
                rows_written = len(records)
                ext = os.path.splitext(export_name)[1].lstrip(".") or (request.destination.format or "json")
                unique_name = f"export_{job_id}.{ext}"

                output_path = request.destination.output_path.strip() if request.destination.output_path else ""
                if output_path:
                    export_path = os.path.abspath(output_path)
                    if not export_path.startswith(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))):
                        mongo.update_job_status(job_id, "failed", error="File export path must be inside the application workspace", phase="failed")
                        return TransferResult(success=False, error="File export path must be inside the application workspace", job_id=job_id)
                    os.makedirs(os.path.dirname(export_path) or ".", exist_ok=True)
                    with open(export_path, "wb") as f:
                        f.write(export_bytes)
                    dest_summary["filename"] = os.path.basename(export_path)
                    dest_summary["path"] = export_path
                    dest_summary["download_url"] = f"/api/v1/transfer/download/{os.path.basename(export_path)}"
                else:
                    export_dir = os.path.join(os.path.dirname(__file__), "..", "..", "exports")
                    os.makedirs(export_dir, exist_ok=True)
                    export_path = os.path.join(export_dir, unique_name)
                    with open(export_path, "wb") as f:
                        f.write(export_bytes)
                    dest_summary["filename"] = unique_name
                    dest_summary["path"] = export_path
                    dest_summary["download_url"] = f"/api/v1/transfer/download/{unique_name}"
                ddl_log.append(f"Exported {rows_written} rows to {dest_summary['filename']}")
            else:
                mongo.update_job_status(job_id, "failed", error=f"Unknown destination: {request.destination.kind}", phase="failed")
                return TransferResult(success=False, error=f"Unknown destination kind: {request.destination.kind}", job_id=job_id)

            mongo.update_job_status(
                job_id, "running", phase="reconcile", progress_pct=95,
                message="Running reconciliation…",
            )

            recon = run_reconciliation(
                endpoint=request.destination,
                records=records,
                columns=columns,
                rows_written=rows_written,
                writer_checksum=dest_summary.get("checksum", ""),
                dest_summary=dest_summary,
                mappings=mappings,
                source_schema=schema,
                validation_mode=request.validation_mode,
            )
            if not recon.get("passed"):
                mongo.update_job_status(
                    job_id, "failed",
                    error=recon.get("message", "Reconciliation failed"),
                    phase="failed",
                    progress_pct=95,
                    message=recon.get("message"),
                    reconciliation=recon,
                    destination_summary=dest_summary,
                    rejected_rows=int(dest_summary.get("rejected_rows", 0) or 0),
                    coerced_null_rows=int(dest_summary.get("coerced_null_rows", 0) or 0),
                )
                return TransferResult(
                    success=False,
                    error=recon.get("message", "Reconciliation failed"),
                    operation=request.operation,
                    job_id=job_id,
                    records_transferred=rows_written,
                    destination_summary=dest_summary,
                    reconciliation=recon,
                )

            explanation = _build_explanation(
                request, columns, schema, mappings, recon, dest_summary, pf, rows_written
            )
            from services.job_status import terminal_status_for

            terminal_status = terminal_status_for(
                dest_summary.get("rejected_rows", 0), dest_summary.get("coerced_null_rows", 0)
            )
            if load_history_report:
                dest_summary["load_history_report"] = load_history_report
            mongo.update_job_status(
                job_id, terminal_status,
                records_processed=rows_written,
                progress_pct=100,
                phase="completed",
                message=recon.get("message", f"Transferred {rows_written:,} rows successfully"),
                destination_database=dest_summary.get("database", request.destination.database or ""),
                destination_collection=dest_summary.get("collection") or dest_summary.get("table", ""),
                rejected_rows=int(dest_summary.get("rejected_rows", 0) or 0),
                coerced_null_rows=int(dest_summary.get("coerced_null_rows", 0) or 0),
                rejected_details=(dest_summary.get("rejected_details") or [])[:200],
                destination_summary=dest_summary,
                explanation=explanation,
                reconciliation=recon,
                load_history_report=dest_summary.get("load_history_report") or {},
            )
            _persist_job_quarantine(job_id, dest_summary, request)
            try:
                from services.usage_metering import record_transfer_usage

                record_transfer_usage(
                    job_id=job_id,
                    workspace_id=str(getattr(request, "workspace_id", "") or ""),
                    rows_written=rows_written,
                    source_type=request.source.format,
                    dest_type=request.destination.format,
                )
            except Exception:
                pass
            if os.environ.get("DATAFLOW_POST_TRANSFER_TRAINING", "").lower() in {"1", "true", "on"}:
                try:
                    samples = {c: [cell_to_string(r.get(c, "")) for r in records[:5] if r.get(c) is not None] for c in columns}
                    schedule_training_on_transfer(
                        request.source_filename or dest_summary.get("table", "transfer"),
                        columns, len(records), samples,
                    )
                except Exception:
                    pass

            lineage.emit_preflight_completed(
                run_id=job_id, passed=True,
                readiness_score=pf.get("readiness_score", 100) if pf else 100,
                validation_plan=pf.get("validation_plan") if pf else {},
            )
            lineage.emit_lineage(
                run_id=job_id,
                source_dataset=f"{request.source.kind}/{src_fmt}/{request.source.table or request.source.collection}",
                target_dataset=f"{request.destination.kind}/{dst_fmt}/{request.destination.table or request.destination.collection}",
                mappings=[{"source": m.get("source"), "target": m.get("target"), "confidence": m.get("confidence")} for m in mappings],
            )
            lineage.emit_run_completed(
                run_id=job_id, job_id=job_id,
                records_transferred=rows_written,
                source_summary={"kind": request.source.kind, "format": src_fmt, "columns": len(columns), "rows": len(records)},
                destination_summary=dest_summary,
            )
            # Append this load to the route ring buffer (last-N multi-load intelligence).
            if load_history_report:
                dest_summary["load_history_report"] = load_history_report
            _persist_load_history_profile(
                request, records, schema,
                job_id=job_id, dest_summary=dest_summary, row_count=len(records),
            )
            finalize_contract(contract_id, success=True)
            return TransferResult(
                success=True,
                job_id=job_id,
                records_transferred=rows_written,
                operation=request.operation,
                source_summary={
                    "kind": request.source.kind,
                    "format": src_fmt,
                    "columns": len(columns),
                    "rows": len(records),
                },
                destination_summary=dest_summary,
                ddl_executed=ddl_log,
                columns=columns,
                reconciliation=recon,
                validation_plan=pf.get("validation_plan") if pf else {},
                payload_shape=pf.get("payload_shape") if pf else {},
                contract_id=contract_id,
                explanation=explanation,
            )
        except Exception as e:
            finalize_contract(contract_id, success=False)
            cancelled = isinstance(e, TransferCancelled)
            status = "cancelled" if cancelled else "failed"
            error_details, lease_extras = _job_failure_fields(e)
            mongo.update_job_status(
                job_id, status,
                error=str(e), phase=status, progress_pct=0, message=str(e),
                error_details=error_details,
                **lease_extras,
            )
            lineage.emit_run_failed(
                run_id=job_id, job_id=job_id, error=str(e),
                error_details=error_details,
                retriable=bool(error_details.get("retriable", False)),
            )
            return TransferResult(
                success=False,
                job_id=job_id,
                error=str(e),
                error_details=error_details,
                operation=request.operation,
                contract_id=contract_id,
            )

    def _execute_streaming(
        self,
        request: TransferRequest,
        job_id: str,
        mongo,
        src_fmt: str,
        resume: bool = False,
        checkpoint: Any = None,
        checkpoint_service: Any = None,
    ) -> TransferResult:
        """Batched DB→DB path — never loads full table into memory."""
        dst_fmt = request.destination.format or "mongodb"
        pf: dict | None = None
        contract_id = ""
        load_history_report: dict[str, Any] = {}
        try:
            mongo.update_job_status(
                job_id, "running", phase="reading", progress_pct=5,
                message="Analyzing source table…",
            )
            columns, schema, total_rows, sample_rows = peek_stream_source(request.source)
            if request.limit > 0:
                total_rows = min(total_rows, request.limit)
            if total_rows == 0:
                mongo.update_job_status(job_id, "failed", error="Source table is empty", phase="failed")
                return TransferResult(
                    success=False, error="Source table is empty",
                    operation=request.operation, job_id=job_id,
                )

            if request.source_filter:
                sample_rows = apply_row_filter(sample_rows, request.source_filter)

            mongo.update_job_status(job_id, "running", total_rows=total_rows, records_processed=0)
            dest_schema_types = _destination_schema_types(request.destination, sync_mode=request.sync_mode)
            mappings = _enrich_mappings_with_types(
                _auto_map(request, columns, schema, sample_rows=sample_rows, job_id=job_id),
                column_types=schema,
                dest_types=dest_schema_types,
            )
            mongo.update_job_status(
                job_id, "running", phase="preflight", progress_pct=15,
                message="Validating mapping and schema…",
            )
            if not request.skip_preflight:
                dest_ok, dest_msg = probe_destination(request.destination)
                pf = run_file_preflight(
                    columns=columns,
                    column_types=schema,
                    row_count=total_rows,
                    mappings=mappings,
                    destination_connected=dest_ok,
                    destination_error=None if dest_ok else dest_msg,
                    source_kind=request.source.kind,
                    source_format=request.source.format,
                    sync_mode=request.sync_mode,
                    sample_rows=sample_rows,
                    confidence_threshold=confidence_threshold_for_mode(request.validation_mode),
                    destination_column_types=dest_schema_types,
                    destination_table_exists=bool(dest_schema_types),
                    destination_can_create=dest_ok,
                    destination_db_type=dst_fmt.lower(),
                    validation_mode=request.validation_mode,
                    source_table=(
                        request.source.table
                        or request.source.collection
                        or request.source_filename
                        or ""
                    ),
                    destination_table=(
                        request.destination.table or request.destination.collection or ""
                    ),
                    source_filename=request.source_filename or "",
                )
                pf = apply_policy_gates(
                    pf,
                    run_transfer_policy_gates(
                        sync_mode=request.sync_mode,
                        schema_policy=request.schema_policy,
                        validation_mode=request.validation_mode,
                        stream_contracts=request.stream_contracts,
                        backfill_new_fields=request.backfill_new_fields,
                    ),
                    validation_mode=request.validation_mode,
                    destination_db_type=dst_fmt.lower(),
                )
                if not dest_ok:
                    mongo.update_job_status(
                        job_id, "failed",
                        error=f"Destination unreachable: {dest_msg}",
                        phase="failed", progress_pct=0,
                    )
                    line_msg = f"Destination unreachable: {dest_msg}"
                    lineage.emit_run_failed(
                        run_id=job_id, job_id=job_id, error=line_msg,
                        error_details={"reason": "Destination unreachable"},
                    )
                    return TransferResult(
                        success=False,
                        error=line_msg,
                        operation=request.operation,
                        job_id=job_id,
                    )
                if not pf["passed"]:
                    error_message, error_details = _fail_job_preflight(mongo, job_id, pf, lineage=lineage)
                    return TransferResult(
                        success=False,
                        error=error_message,
                        error_details=error_details,
                        validation_plan=pf.get("validation_plan") or {},
                        payload_shape=pf.get("payload_shape") or {},
                        operation=request.operation,
                        job_id=job_id,
                    )

            if pf:
                mongo.update_job_status(job_id, "running", phase="preflight", progress_pct=15, preflight=pf)

            # Data contract / circuit breaker enforcement.
            try:
                contract_id = enforce_or_create_contract(request, schema, mappings, pf)
            except ContractViolation as cv:
                msg = cv.message
                mongo.update_job_status(job_id, "failed", error=msg, phase="failed", progress_pct=0)
                return TransferResult(
                    success=False,
                    error=msg,
                    error_details={"violations": cv.violations},
                    operation=request.operation,
                    job_id=job_id,
                )

            # Sample-based history compare (full table never loaded in streaming path).
            load_history_report = _compare_and_publish_load_history(
                mongo, job_id, sample_rows or [], request, schema,
                validation_mode=request.validation_mode,
                row_count_hint=total_rows,
            )
            if load_history_report.get("strict_blocked"):
                anomalies = list(load_history_report.get("anomalies") or [])
                msg = "Data quality anomaly: " + "; ".join(anomalies[:8])
                mongo.update_job_status(
                    job_id, "failed", error=msg, phase="failed", progress_pct=0,
                    load_history_report=load_history_report,
                )
                return TransferResult(
                    success=False,
                    error=msg,
                    operation=request.operation,
                    job_id=job_id,
                    destination_summary={"load_history_report": load_history_report},
                    error_details={"load_history_report": load_history_report},
                )

            def _check_cancelled() -> None:
                try:
                    job = mongo.get_job(job_id)
                    if job and job.get("status") == "cancelled":
                        raise TransferCancelled("Transfer cancelled by user")
                except TransferCancelled:
                    raise
                except Exception:
                    pass

            def on_checkpoint(chunk: int, chunks: int, rows: int, checkpoint: dict | None = None) -> None:
                _check_cancelled()
                pct = 25 + int((chunk / max(chunks, 1)) * 65)
                update = dict(
                    records_processed=rows,
                    progress_pct=min(pct, 90),
                    chunk_current=chunk,
                    chunk_total=chunks,
                    message=f"Writing batch {chunk}/{chunks} ({rows:,} rows)…",
                )
                if checkpoint:
                    update["checkpoint"] = checkpoint
                    update["destination_summary"] = {
                        "checksum": checkpoint.get("checksum", ""),
                        "rejected_rows": checkpoint.get("rejected_rows", 0),
                        "rejected_details": (checkpoint.get("rejected_details") or [])[:50],
                    }
                    _promote_cdc_job_fields(checkpoint, update)
                mongo.update_job_status(job_id, "running", **update)

            throttled_checkpoint = ThrottledCheckpoint(on_checkpoint)

            mongo.update_job_status(
                job_id, "running", phase="writing", progress_pct=25,
                message=f"Streaming {total_rows:,} rows in batches…",
            )

            is_streaming = True
            if request.sync_mode.lower() in ("full_refresh_overwrite", "overwrite"):
                if not resume or not is_streaming or not _checkpoint_has_progress(checkpoint):
                    _drop_destination_table(request.destination)

            stream_contract = resolve_sync_contract(request.stream_contracts)
            effective_sync = (stream_contract.sync_mode if stream_contract else request.sync_mode).lower()
            if effective_sync in ("full_refresh_mirror", "mirror", "scd2"):
                rows_written, ddl_log, dest_summary, _ = stream_scd2_mirror_transfer(
                    request.source,
                    request.destination,
                    mappings,
                    schema,
                    on_checkpoint=throttled_checkpoint,
                    sync_mode=request.sync_mode,
                    stream_contracts=request.stream_contracts,
                    job_id=job_id,
                    checkpoint=checkpoint,
                    checkpoint_service=checkpoint_service,
                    backfill_new_fields=request.backfill_new_fields,
                    validation_mode=request.validation_mode,
                    limit=request.limit,
                )
            elif effective_sync == "cdc":
                rows_written, ddl_log, dest_summary, _ = run_cdc_database_transfer(
                    request.source,
                    request.destination,
                    mappings,
                    schema,
                    on_checkpoint=throttled_checkpoint,
                    sync_mode=request.sync_mode,
                    stream_contracts=request.stream_contracts,
                    job_id=job_id,
                    checkpoint=checkpoint,
                    checkpoint_service=checkpoint_service,
                    backfill_new_fields=request.backfill_new_fields,
                    validation_mode=request.validation_mode,
                    limit=request.limit,
                )
            else:
                rows_written, ddl_log, dest_summary, _ = stream_database_transfer(
                    request.source,
                    request.destination,
                    mappings,
                    schema,
                    on_checkpoint=throttled_checkpoint,
                    sync_mode=request.sync_mode,
                    stream_contracts=request.stream_contracts,
                    job_id=job_id,
                    checkpoint=checkpoint,
                    checkpoint_service=checkpoint_service,
                    backfill_new_fields=request.backfill_new_fields,
                    validation_mode=request.validation_mode,
                    source_filter=request.source_filter,
                    limit=request.limit,
                )

            mongo.update_job_status(
                job_id, "running", phase="reconcile", progress_pct=95,
                message="Running reconciliation…",
            )

            recon = run_reconciliation(
                endpoint=request.destination,
                records=[],
                columns=columns,
                rows_written=rows_written,
                writer_checksum=dest_summary.get("checksum", ""),
                dest_summary=dest_summary,
                mappings=mappings,
                source_schema=schema,
                validation_mode=request.validation_mode,
            )
            if not recon.get("passed"):
                mongo.update_job_status(
                    job_id, "failed",
                    error=recon.get("message", "Reconciliation failed"),
                    phase="failed",
                    progress_pct=95,
                    message=recon.get("message"),
                    reconciliation=recon,
                    destination_summary=dest_summary,
                    rejected_rows=int(dest_summary.get("rejected_rows", 0) or 0),
                    coerced_null_rows=int(dest_summary.get("coerced_null_rows", 0) or 0),
                )
                return TransferResult(
                    success=False,
                    error=recon.get("message", "Reconciliation failed"),
                    operation=request.operation,
                    job_id=job_id,
                    records_transferred=rows_written,
                    destination_summary=dest_summary,
                    reconciliation=recon,
                )

            explanation = _build_explanation(
                request, columns, schema, mappings, recon, dest_summary, pf, rows_written
            )
            from services.job_status import terminal_status_for

            terminal_status = terminal_status_for(
                dest_summary.get("rejected_rows", 0), dest_summary.get("coerced_null_rows", 0)
            )
            if load_history_report:
                dest_summary["load_history_report"] = load_history_report
            _persist_load_history_profile(
                request, sample_rows or [], schema,
                job_id=job_id, dest_summary=dest_summary, row_count=rows_written or total_rows,
            )
            mongo.update_job_status(
                job_id, terminal_status,
                records_processed=rows_written,
                progress_pct=100,
                phase="completed",
                message=recon.get("message", f"Transferred {rows_written:,} rows successfully"),
                destination_database=dest_summary.get("database", request.destination.database or ""),
                destination_collection=dest_summary.get("collection") or dest_summary.get("table", ""),
                rejected_rows=int(dest_summary.get("rejected_rows", 0) or 0),
                coerced_null_rows=int(dest_summary.get("coerced_null_rows", 0) or 0),
                rejected_details=(dest_summary.get("rejected_details") or [])[:200],
                destination_summary=dest_summary,
                explanation=explanation,
                reconciliation=recon,
                load_history_report=load_history_report or {},
                **_cdc_fields_from_summary(dest_summary),
            )
            _persist_job_quarantine(job_id, dest_summary, request)

            lineage.emit_preflight_completed(
                run_id=job_id, passed=True,
                readiness_score=pf.get("readiness_score", 100) if pf else 100,
                validation_plan=pf.get("validation_plan") if pf else {},
            )
            lineage.emit_lineage(
                run_id=job_id,
                source_dataset=f"{request.source.kind}/{src_fmt}/{request.source.table or request.source.collection}",
                target_dataset=f"{request.destination.kind}/{dst_fmt}/{request.destination.table or request.destination.collection}",
                mappings=[{"source": m.get("source"), "target": m.get("target"), "confidence": m.get("confidence")} for m in mappings],
            )
            lineage.emit_run_completed(
                run_id=job_id, job_id=job_id,
                records_transferred=rows_written,
                source_summary={"kind": request.source.kind, "format": src_fmt, "columns": len(columns), "rows": total_rows, "streaming": True},
                destination_summary=dest_summary,
            )
            finalize_contract(contract_id, success=True)
            return TransferResult(
                success=True,
                job_id=job_id,
                records_transferred=rows_written,
                operation=request.operation,
                source_summary={
                    "kind": request.source.kind,
                    "format": src_fmt,
                    "columns": len(columns),
                    "rows": total_rows,
                    "streaming": True,
                },
                destination_summary=dest_summary,
                ddl_executed=ddl_log,
                columns=columns,
                reconciliation=recon,
                validation_plan=pf.get("validation_plan") if pf else {},
                payload_shape=pf.get("payload_shape") if pf else {},
                contract_id=contract_id,
                explanation=explanation,
            )
        except Exception as e:
            finalize_contract(contract_id, success=False)
            cancelled = isinstance(e, TransferCancelled)
            status = "cancelled" if cancelled else "failed"
            error_details, lease_extras = _job_failure_fields(e)
            mongo.update_job_status(
                job_id, status,
                error=str(e), phase=status, progress_pct=0, message=str(e),
                error_details=error_details,
                **lease_extras,
            )
            lineage.emit_run_failed(
                run_id=job_id, job_id=job_id, error=str(e),
                error_details=error_details,
                retriable=bool(error_details.get("retriable", False)),
            )
            return TransferResult(
                success=False,
                job_id=job_id,
                error=str(e),
                error_details=error_details,
                operation=request.operation,
                contract_id=contract_id,
            )

    def _execute_file_streaming(
        self,
        request: TransferRequest,
        job_id: str,
        mongo,
        src_fmt: str,
        resume: bool = False,
        checkpoint: Any = None,
        checkpoint_service: Any = None,
    ) -> TransferResult:
        """Batched file → database path for large CSV/TSV/JSONL uploads."""
        dst_fmt = request.destination.format or "mongodb"
        pf: dict | None = None
        contract_id = ""
        load_history_report: dict[str, Any] = {}
        try:
            filename = request.source_filename or "upload.csv"
            content = prepare_stream_content(
                content=request.source_content or b"",
                filename=filename,
                source_path=request.source_path or "",
            )

            mongo.update_job_status(
                job_id, "running", phase="reading", progress_pct=5,
                message="Analyzing uploaded file…",
            )
            columns, schema, total_rows, sample_rows = peek_file_source(content, filename)
            if total_rows == 0:
                mongo.update_job_status(job_id, "failed", error="File contains no records", phase="failed")
                return TransferResult(
                    success=False, error="File contains no records",
                    operation=request.operation, job_id=job_id,
                )

            if request.source_filter:
                sample_rows = apply_row_filter(sample_rows, request.source_filter)

            mongo.update_job_status(job_id, "running", total_rows=total_rows, records_processed=0)
            dest_schema_types = _destination_schema_types(request.destination, sync_mode=request.sync_mode)
            mappings = _enrich_mappings_with_types(
                _auto_map(request, columns, schema, sample_rows=sample_rows, job_id=job_id),
                column_types=schema,
                dest_types=dest_schema_types,
            )
            mongo.update_job_status(
                job_id, "running", phase="preflight", progress_pct=15,
                message="Validating mapping and schema…",
            )
            if not request.skip_preflight:
                dest_ok, dest_msg = probe_destination(request.destination)
                pf = run_file_preflight(
                    columns=columns,
                    column_types=schema,
                    row_count=total_rows,
                    mappings=mappings,
                    destination_connected=dest_ok,
                    destination_error=None if dest_ok else dest_msg,
                    source_kind=request.source.kind,
                    source_format=request.source.format,
                    sync_mode=request.sync_mode,
                    sample_rows=sample_rows,
                    confidence_threshold=confidence_threshold_for_mode(request.validation_mode),
                    destination_column_types=dest_schema_types,
                    destination_table_exists=bool(dest_schema_types),
                    destination_can_create=dest_ok,
                    destination_db_type=dst_fmt.lower(),
                    validation_mode=request.validation_mode,
                    source_table=(
                        request.source.table
                        or request.source.collection
                        or request.source_filename
                        or ""
                    ),
                    destination_table=(
                        request.destination.table or request.destination.collection or ""
                    ),
                    source_filename=request.source_filename or "",
                )
                pf = apply_policy_gates(
                    pf,
                    run_transfer_policy_gates(
                        sync_mode=request.sync_mode,
                        schema_policy=request.schema_policy,
                        validation_mode=request.validation_mode,
                        stream_contracts=request.stream_contracts,
                        backfill_new_fields=request.backfill_new_fields,
                    ),
                    validation_mode=request.validation_mode,
                    destination_db_type=dst_fmt.lower(),
                )
                if not dest_ok:
                    mongo.update_job_status(
                        job_id, "failed",
                        error=f"Destination unreachable: {dest_msg}",
                        phase="failed", progress_pct=0,
                    )
                    line_msg = f"Destination unreachable: {dest_msg}"
                    lineage.emit_run_failed(
                        run_id=job_id, job_id=job_id, error=line_msg,
                        error_details={"reason": "Destination unreachable"},
                    )
                    return TransferResult(
                        success=False,
                        error=line_msg,
                        operation=request.operation,
                        job_id=job_id,
                    )
                if not pf["passed"]:
                    error_message, error_details = _fail_job_preflight(mongo, job_id, pf, lineage=lineage)
                    return TransferResult(
                        success=False,
                        error=error_message,
                        error_details=error_details,
                        validation_plan=pf.get("validation_plan") or {},
                        payload_shape=pf.get("payload_shape") or {},
                        operation=request.operation,
                        job_id=job_id,
                    )

            if pf:
                mongo.update_job_status(job_id, "running", phase="preflight", progress_pct=15, preflight=pf)

            # Data contract / circuit breaker enforcement.
            try:
                contract_id = enforce_or_create_contract(request, schema, mappings, pf)
            except ContractViolation as cv:
                msg = cv.message
                mongo.update_job_status(job_id, "failed", error=msg, phase="failed", progress_pct=0)
                return TransferResult(
                    success=False,
                    error=msg,
                    error_details={"violations": cv.violations},
                    operation=request.operation,
                    job_id=job_id,
                )

            # Sample-based history compare (file is streamed; full table never loaded).
            load_history_report = _compare_and_publish_load_history(
                mongo, job_id, sample_rows or [], request, schema,
                validation_mode=request.validation_mode,
                row_count_hint=total_rows,
            )
            if load_history_report.get("strict_blocked"):
                anomalies = list(load_history_report.get("anomalies") or [])
                msg = "Data quality anomaly: " + "; ".join(anomalies[:8])
                mongo.update_job_status(
                    job_id, "failed", error=msg, phase="failed", progress_pct=0,
                    load_history_report=load_history_report,
                )
                return TransferResult(
                    success=False,
                    error=msg,
                    operation=request.operation,
                    job_id=job_id,
                    destination_summary={"load_history_report": load_history_report},
                    error_details={"load_history_report": load_history_report},
                )

            def _check_cancelled() -> None:
                try:
                    job = mongo.get_job(job_id)
                    if job and job.get("status") == "cancelled":
                        raise TransferCancelled("Transfer cancelled by user")
                except TransferCancelled:
                    raise
                except Exception:
                    pass

            def on_checkpoint(chunk: int, chunks: int, rows: int, checkpoint: dict | None = None) -> None:
                _check_cancelled()
                pct = 25 + int((chunk / max(chunks, 1)) * 65)
                update = dict(
                    records_processed=rows,
                    progress_pct=min(pct, 90),
                    chunk_current=chunk,
                    chunk_total=chunks,
                    message=f"Writing batch {chunk}/{chunks} ({rows:,} rows)…",
                )
                if checkpoint:
                    update["checkpoint"] = checkpoint
                    update["destination_summary"] = {
                        "checksum": checkpoint.get("checksum", ""),
                        "rejected_rows": checkpoint.get("rejected_rows", 0),
                        "rejected_details": (checkpoint.get("rejected_details") or [])[:50],
                    }
                    _promote_cdc_job_fields(checkpoint, update)
                mongo.update_job_status(job_id, "running", **update)

            throttled_checkpoint = ThrottledCheckpoint(on_checkpoint)

            mongo.update_job_status(
                job_id, "running", phase="writing", progress_pct=25,
                message=f"Streaming {total_rows:,} rows in batches…",
            )

            is_streaming = True
            if request.sync_mode.lower() in ("full_refresh_overwrite", "overwrite"):
                if not resume or not is_streaming or not _checkpoint_has_progress(checkpoint):
                    _drop_destination_table(request.destination)

            rows_written, ddl_log, dest_summary, _ = stream_file_to_database(
                content,
                filename,
                request.destination,
                mappings,
                schema,
                on_checkpoint=throttled_checkpoint,
                sync_mode=request.sync_mode,
                stream_contracts=request.stream_contracts,
                job_id=job_id,
                checkpoint=checkpoint,
                checkpoint_service=checkpoint_service,
                backfill_new_fields=request.backfill_new_fields,
                validation_mode=request.validation_mode,
                source_filter=request.source_filter,
            )

            mongo.update_job_status(
                job_id, "running", phase="reconcile", progress_pct=95,
                message="Running reconciliation…",
            )

            recon = run_reconciliation(
                endpoint=request.destination,
                records=[],
                columns=columns,
                rows_written=rows_written,
                writer_checksum=dest_summary.get("checksum", ""),
                dest_summary=dest_summary,
                mappings=mappings,
                source_schema=schema,
                validation_mode=request.validation_mode,
            )
            if not recon.get("passed"):
                mongo.update_job_status(
                    job_id, "failed",
                    error=recon.get("message", "Reconciliation failed"),
                    phase="failed",
                    progress_pct=95,
                    message=recon.get("message"),
                    reconciliation=recon,
                    destination_summary=dest_summary,
                    rejected_rows=int(dest_summary.get("rejected_rows", 0) or 0),
                    coerced_null_rows=int(dest_summary.get("coerced_null_rows", 0) or 0),
                )
                return TransferResult(
                    success=False,
                    error=recon.get("message", "Reconciliation failed"),
                    operation=request.operation,
                    job_id=job_id,
                    records_transferred=rows_written,
                    destination_summary=dest_summary,
                    reconciliation=recon,
                )

            explanation = _build_explanation(
                request, columns, schema, mappings, recon, dest_summary, pf, rows_written
            )
            from services.job_status import terminal_status_for

            terminal_status = terminal_status_for(
                dest_summary.get("rejected_rows", 0), dest_summary.get("coerced_null_rows", 0)
            )
            if load_history_report:
                dest_summary["load_history_report"] = load_history_report
            _persist_load_history_profile(
                request, sample_rows or [], schema,
                job_id=job_id, dest_summary=dest_summary, row_count=rows_written or total_rows,
            )
            mongo.update_job_status(
                job_id, terminal_status,
                records_processed=rows_written,
                progress_pct=100,
                phase="completed",
                message=recon.get("message", f"Transferred {rows_written:,} rows successfully"),
                destination_database=dest_summary.get("database", request.destination.database or ""),
                destination_collection=dest_summary.get("collection") or dest_summary.get("table", ""),
                rejected_rows=int(dest_summary.get("rejected_rows", 0) or 0),
                coerced_null_rows=int(dest_summary.get("coerced_null_rows", 0) or 0),
                rejected_details=(dest_summary.get("rejected_details") or [])[:200],
                destination_summary=dest_summary,
                explanation=explanation,
                reconciliation=recon,
                load_history_report=load_history_report or {},
            )
            _persist_job_quarantine(job_id, dest_summary, request)

            lineage.emit_preflight_completed(
                run_id=job_id, passed=True,
                readiness_score=pf.get("readiness_score", 100) if pf else 100,
                validation_plan=pf.get("validation_plan") if pf else {},
            )
            lineage.emit_lineage(
                run_id=job_id,
                source_dataset=f"{request.source.kind}/{src_fmt}/{request.source_filename}",
                target_dataset=f"{request.destination.kind}/{dst_fmt}/{request.destination.table or request.destination.collection}",
                mappings=[{"source": m.get("source"), "target": m.get("target"), "confidence": m.get("confidence")} for m in mappings],
            )
            lineage.emit_run_completed(
                run_id=job_id, job_id=job_id,
                records_transferred=rows_written,
                source_summary={"kind": "file", "format": src_fmt, "columns": len(columns), "rows": total_rows, "streaming": True},
                destination_summary=dest_summary,
            )
            finalize_contract(contract_id, success=True)
            return TransferResult(
                success=True,
                job_id=job_id,
                records_transferred=rows_written,
                operation=request.operation,
                source_summary={
                    "kind": "file",
                    "format": src_fmt,
                    "columns": len(columns),
                    "rows": total_rows,
                    "streaming": True,
                },
                destination_summary=dest_summary,
                ddl_executed=ddl_log,
                columns=columns,
                reconciliation=recon,
                validation_plan=pf.get("validation_plan") if pf else {},
                payload_shape=pf.get("payload_shape") if pf else {},
                contract_id=contract_id,
                explanation=explanation,
            )
        except Exception as e:
            finalize_contract(contract_id, success=False)
            cancelled = isinstance(e, TransferCancelled)
            status = "cancelled" if cancelled else "failed"
            error_details, lease_extras = _job_failure_fields(e)
            mongo.update_job_status(
                job_id, status,
                error=str(e), phase=status, progress_pct=0, message=str(e),
                error_details=error_details,
                **lease_extras,
            )
            lineage.emit_run_failed(
                run_id=job_id, job_id=job_id, error=str(e),
                error_details=error_details,
                retriable=bool(error_details.get("retriable", False)),
            )
            return TransferResult(
                success=False,
                job_id=job_id,
                error=str(e),
                error_details=error_details,
                operation=request.operation,
                contract_id=contract_id,
            )

    def _create_pending_job(self, request: TransferRequest) -> str:
        self._resolve_saved_connectors(request)
        mongo = get_mongodb_service()
        source_name = (
            request.source_filename
            or request.source.table
            or request.source.collection
            or "database"
        )
        dest_label = (
            request.destination.collection
            or request.destination.table
            or request.destination.database
            or request.destination.format
            or "destination"
        )
        return mongo.create_transfer_job({
            "source_type": request.source.kind,
            "source_name": source_name,
            "name": f"{source_name} → {dest_label}",
            "name_key": f"{source_name} → {dest_label}".strip().casefold(),
            "source_format": request.source.format,
            "destination_type": request.destination.format,
            "destination_kind": request.destination.kind,
            "destination_database": request.destination.database or "",
            "destination_collection": request.destination.collection or request.destination.table or "",
            "operation": request.operation,
            "records_processed": 0,
            "total_rows": 0,
            "progress_pct": 0,
            "phase": "queued",
            "message": "Transfer queued",
            "workspace_id": request.workspace_id or "",
            "data_region": request.data_region or "",
            "transfer_request": transfer_request_to_dict(request),
            "retry_of": None,
        })

    def _read_source(self, request: TransferRequest) -> tuple[list, list[str], dict[str, str]]:
        if request.source.kind == "file":
            if not request.source_content:
                raise ValueError("File content required for file source")
            return parse_file_content(request.source_content, request.source_filename or "upload.csv")
        if request.source.kind == "database":
            return read_source_database(request.source)
        raise ValueError(f"Unsupported source kind: {request.source.kind}")

    def analyze_compatibility(
        self,
        source: EndpointConfig,
        destination: EndpointConfig,
        sample_content: bytes | None = None,
        filename: str = "",
        source_columns: list[str] | None = None,
        source_schema: dict[str, str] | None = None,
    ) -> dict:
        """Understand source + destination and recommend auto-creation plan."""
        from services.universal_router import analyze_route

        from .endpoint_intelligence import build_transfer_plan, introspect_endpoint

        source_info = introspect_endpoint(source, sample_content, filename)
        if source_columns and not source_info.get("columns"):
            source_info["columns"] = source_columns
            source_info["schema"] = source_schema or {}
            source_info["connected"] = True
            source_info["message"] = f"Schema ready — {len(source_columns)} columns"
        elif source_columns:
            source_info["columns"] = source_columns
            if source_schema:
                source_info["schema"] = source_schema
        plan = build_transfer_plan(source, destination, source_info)
        if source_info.get("columns"):
            plan["source_columns"] = source_info["columns"]
            plan["source_schema"] = source_info["schema"]
        src_fmt = self._resolved_format(source)
        dst_fmt = self._resolved_format(destination)
        plan["route_analysis"] = analyze_route(source.kind, src_fmt, destination.kind, dst_fmt)
        return plan

    def _resolved_format(self, endpoint: EndpointConfig) -> str:
        """Return the canonical driver format, preferring saved connector type."""
        if endpoint.connector_id:
            try:
                cfg = resolve_connector_config(endpoint)
                return (cfg.get("type") or endpoint.format or "").lower()
            except Exception:
                pass
        if endpoint.kind == "file":
            return (endpoint.format or "csv").lower()
        if endpoint.kind == "file_export":
            return (endpoint.format or "json").lower()
        return (endpoint.format or "").lower()


_engine: Optional[UniversalTransferEngine] = None


def get_transfer_engine() -> UniversalTransferEngine:
    global _engine
    if _engine is None:
        _engine = UniversalTransferEngine()
    return _engine
