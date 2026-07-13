"""Universal transfer orchestrator — routes any source to any destination."""

from __future__ import annotations
import logging
import os
import sys
from pathlib import Path
from typing import Any, Optional

# Ensure the API root (parent of the `src` package) is first on sys.path so the
# `services` intelligence package resolves to `apps/api/services`, not an
# accidentally-shadowing `apps/api/src/services` that may be on PYTHONPATH.
_api_root = Path(__file__).resolve().parents[2]
if str(_api_root) not in sys.path:
    sys.path.insert(0, str(_api_root))

try:
    from services.mongodb_service import get_mongodb_service
    from services.preflight_service import (
        apply_policy_gates,
        confidence_threshold_for_mode,
        probe_destination,
        run_file_preflight,
        run_transfer_policy_gates,
    )
    from services import lineage_telemetry as lineage
    from services.error_handling import classify_error, RetryBudget, TransferCancelled, with_retry
    from services.sync_cursor import map_source_to_target, requires_upsert, resolve_sync_contract
except ImportError:  # pragma: no cover - compatibility for tests with api root on PYTHONPATH
    from src.services.mongodb_service import get_mongodb_service
    from src.services.preflight_service import (
        apply_policy_gates,
        confidence_threshold_for_mode,
        probe_destination,
        run_file_preflight,
        run_transfer_policy_gates,
    )
    from src.services import lineage_telemetry as lineage
    from src.services.error_handling import classify_error, RetryBudget, TransferCancelled, with_retry
    from src.services.sync_cursor import map_source_to_target, requires_upsert, resolve_sync_contract
from .adapters import (
    parse_file_content,
    read_source_database,
    resolve_connector_config,
    write_destination_database,
    write_destination_file,
)
from .models import EndpointConfig, TransferRequest, TransferResult, transfer_request_to_dict
from .reconcile_step import run_reconciliation
from .registry import validate_transfer
from .file_stream import peek_file_source, should_stream_file, stream_file_to_database
from .stream import peek_stream_source, stream_database_transfer, supports_streaming
from .type_mapper import build_column_types, default_mappings
try:
    from ai.training.training_scheduler import schedule_training_on_transfer
except ImportError:  # pragma: no cover - compatibility for tests with api root on PYTHONPATH
    from src.ai.training.training_scheduler import schedule_training_on_transfer

from connectors.writer_common import CHUNK_SIZE
from services.batch_progress import ThrottledCheckpoint
from services.checkpoint_service import Checkpoint, CheckpointService, resume_or_create_checkpoint

logger = logging.getLogger("dataflow.transfer")


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


def _checkpoint_has_progress(checkpoint: Any) -> bool:
    """True when the checkpoint has committed rows from a previous run."""
    if not checkpoint:
        return False
    return bool(
        getattr(checkpoint, "chunk_index", 0)
        or getattr(checkpoint, "offset", 0)
        or getattr(checkpoint, "rows_processed", 0)
    )


def _drop_destination_table(destination: EndpointConfig) -> bool:
    """Drop the destination object for full-refresh overwrite sync modes."""
    if destination.kind != "database":
        return False
    try:
        from .connector_capabilities import resolve_driver_type
        from .adapters import resolve_connector_config, resolve_dest_table
        from connectors.table_manager import drop_table

        db_type = resolve_driver_type(destination.format)
        cfg = resolve_connector_config(destination)
        table_name = resolve_dest_table(db_type, destination)
        schema = cfg.get("schema")
        return drop_table(db_type, cfg, table_name, schema)
    except Exception:
        return False


def _enrich_mappings_with_types(mappings: list[dict], dest_types: dict[str, str]) -> list[dict]:
    if not mappings:
        return mappings
    try:
        from services.transform_resolver import attach_transforms_to_mappings

        return attach_transforms_to_mappings(mappings, dest_types=dest_types)
    except Exception:
        pass
    out = []
    for m in mappings:
        enriched = dict(m)
        tgt = m.get("target")
        if tgt and tgt in dest_types:
            enriched["target_type"] = dest_types[tgt]
        out.append(enriched)
    return out


def _auto_map(
    request: TransferRequest,
    columns: list[str],
    schema: dict[str, str],
    sample_rows: list[dict] | None = None,
) -> list[dict]:
    """Generate destination-aware mappings when no mapping contract was supplied.

    For append/upsert/merge into an existing target, the destination schema is
    introspected and the semantic mapper aligns source columns to target columns.
    For full-refresh/overwrites into a new target, identity mappings are used so
    the destination can be created from the source shape.
    """
    if request.mappings:
        return request.mappings
    if request.destination.kind != "database":
        return default_mappings(columns)

    sync_mode = (request.sync_mode or "full_refresh_overwrite").lower()
    if sync_mode in {"full_refresh_overwrite", "overwrite"}:
        return default_mappings(columns)

    target_schema = _destination_schema_types(request.destination, sync_mode=request.sync_mode)
    if not target_schema:
        return default_mappings(columns)

    try:
        from services.mapping_pipeline import run_mapping_pipeline

        source_schemas = [
            {
                "name": c,
                "inferred_type": schema.get(c, "string"),
                "samples": [str(r.get(c, "")) for r in (sample_rows or [])[:8]],
            }
            for c in columns
        ]
        target_columns = list(target_schema.keys())
        target_schemas = [
            {"name": c, "inferred_type": target_schema.get(c, "string"), "samples": []}
            for c in target_columns
        ]
        source_samples = {
            c: [str(r.get(c, "")) for r in (sample_rows or [])[:8]]
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
        )
        mappings = result.get("mappings")
        if mappings and isinstance(mappings, list) and any(m.get("source") for m in mappings):
            return mappings
    except Exception as exc:
        logger.warning("Auto-mapping failed: %s; falling back to identity mappings", exc)

    return default_mappings(columns)


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
        job_id = self._create_pending_job(request)
        return self.execute_tracked(request, job_id)

    def execute_tracked(self, request: TransferRequest, job_id: str, resume: bool = False) -> TransferResult:
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

        if supports_streaming(request.source, request.destination):
            return self._execute_streaming(
                request, job_id, mongo, src_fmt,
                resume=resume,
                checkpoint=checkpoint,
                checkpoint_service=checkpoint_service,
            )

        if (
            request.source.kind == "file"
            and request.destination.kind == "database"
            and request.source_content
            and should_stream_file(
                request.source_content,
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
        try:
            mongo.update_job_status(
                job_id, "running", phase="reading", progress_pct=5,
                message="Reading source data…",
            )
            records, columns, schema = with_retry(
                lambda: self._read_source(request),
                budget=RetryBudget(max_attempts=3, base_delay_seconds=0.5, max_delay_seconds=5.0),
            )
            if not records and request.source.kind != "database":
                mongo.update_job_status(job_id, "failed", error="No records to transfer", phase="failed")
                return TransferResult(success=False, error="No records to transfer", operation=request.operation, job_id=job_id)

            total_rows = len(records)
            mongo.update_job_status(job_id, "running", total_rows=total_rows, records_processed=0)

            dest_schema_types = _destination_schema_types(request.destination, sync_mode=request.sync_mode)
            mappings = _enrich_mappings_with_types(
                _auto_map(request, columns, schema, sample_rows=records[:100]),
                dest_schema_types,
            )
            column_types = request.column_types or build_column_types(columns, schema)

            # Resolve upsert mode for non-streaming database writes.
            contract = resolve_sync_contract(request.stream_contracts)
            effective_sync = contract.sync_mode if contract else request.sync_mode
            write_mode = "insert"
            conflict_columns: list[str] = []
            if contract and contract.primary_key:
                conflict_columns = [map_source_to_target(contract.primary_key, mappings)]
            if requires_upsert(effective_sync) and conflict_columns:
                write_mode = "upsert"

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
                    decision = pf.get("proof_bundle", {}).get("transfer_decision", {}) or {}
                    blocker_reasons = [b.get("message") for b in pf.get("blockers", [])]
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
                            if b.get("guidance")
                        ],
                        "proof_bundle": {
                            "decision": decision.get("decision"),
                            "reason": decision.get("reason"),
                            "semantic_mapping_score": pf.get("proof_bundle", {}).get("semantic_mapping_score"),
                            "min_confidence": pf.get("proof_bundle", {}).get("min_confidence"),
                            "quality_score": pf.get("proof_bundle", {}).get("quality_score"),
                            "compliance_risk": pf.get("proof_bundle", {}).get("compliance", {}).get("risk_score"),
                        },
                        "readiness_score": pf.get("readiness_score"),
                        "validation_plan": pf.get("validation_plan"),
                        "payload_shape": pf.get("payload_shape"),
                    }
                    error_message = decision.get("reason") or "; ".join(blocker_reasons) or "Preflight blocked transfer"
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
                        job_id, "failed", error=error_message,
                        phase="failed", progress_pct=0,
                        error_details=error_details,
                        preflight=pf,
                    )
                    return TransferResult(
                        success=False,
                        error=error_message,
                        error_details=error_details,
                        validation_plan=pf.get("validation_plan") or {},
                        payload_shape=pf.get("payload_shape") or {},
                        operation=request.operation,
                        job_id=job_id,
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
                mongo.update_job_status(job_id, "running", **update)

            throttled_checkpoint = ThrottledCheckpoint(on_checkpoint)

            if request.destination.kind == "database":
                is_streaming = False
                if request.sync_mode.lower() in ("full_refresh_overwrite", "overwrite"):
                    if not resume or not is_streaming or not _checkpoint_has_progress(checkpoint):
                        _drop_destination_table(request.destination)
                rows_written, ddl_log, dest_summary = with_retry(
                    lambda: write_destination_database(
                        request.destination, records, columns, schema, mappings,
                        on_checkpoint=throttled_checkpoint,
                        validation_mode=request.validation_mode,
                        backfill_new_fields=request.backfill_new_fields,
                        write_mode=write_mode,
                        conflict_columns=conflict_columns,
                    ),
                    budget=RetryBudget(max_attempts=3, base_delay_seconds=0.5, max_delay_seconds=5.0),
                )
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
                export_dir = os.path.join(os.path.dirname(__file__), "..", "..", "exports")
                os.makedirs(export_dir, exist_ok=True)
                ext = os.path.splitext(export_name)[1].lstrip(".") or (request.destination.format or "json")
                unique_name = f"export_{job_id}.{ext}"
                export_path = os.path.join(export_dir, unique_name)
                with open(export_path, "wb") as f:
                    f.write(export_bytes)
                dest_summary["filename"] = unique_name
                dest_summary["path"] = export_path
                dest_summary["download_url"] = f"/api/v1/transfer/download/{unique_name}"
                ddl_log.append(f"Exported {rows_written} rows to {unique_name}")
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
            )
            if not recon.get("passed"):
                mongo.update_job_status(
                    job_id, "failed",
                    error=recon.get("message", "Reconciliation failed"),
                    phase="failed",
                    progress_pct=95,
                    message=recon.get("message"),
                )
                return TransferResult(
                    success=False,
                    error=recon.get("message", "Reconciliation failed"),
                    operation=request.operation,
                    job_id=job_id,
                    records_transferred=rows_written,
                    destination_summary=dest_summary,
                )

            mongo.update_job_status(
                job_id, "completed",
                records_processed=rows_written,
                progress_pct=100,
                phase="completed",
                message=recon.get("message", f"Transferred {rows_written:,} rows successfully"),
                destination_database=dest_summary.get("database", request.destination.database or ""),
                destination_collection=dest_summary.get("collection") or dest_summary.get("table", ""),
                rejected_rows=int(dest_summary.get("rejected_rows", 0) or 0),
                rejected_details=(dest_summary.get("rejected_details") or [])[:200],
                destination_summary=dest_summary,
            )
            try:
                samples = {c: [str(r.get(c, "")) for r in records[:5] if r.get(c) is not None] for c in columns}
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
            )
        except Exception as e:
            error_classification = classify_error(e)
            cancelled = isinstance(e, TransferCancelled)
            status = "cancelled" if cancelled else "failed"
            mongo.update_job_status(
                job_id, status,
                error=str(e), phase=status, progress_pct=0, message=str(e),
                error_details={"retriable": error_classification.get("retriable"), "evidence": error_classification.get("evidence")},
            )
            lineage.emit_run_failed(
                run_id=job_id, job_id=job_id, error=str(e),
                error_details={"retriable": error_classification.get("retriable"), "evidence": error_classification.get("evidence")},
                retriable=error_classification.get("retriable", False),
            )
            return TransferResult(
                success=False,
                job_id=job_id,
                error=str(e),
                error_details={"retriable": error_classification.get("retriable"), "evidence": error_classification.get("evidence")},
                operation=request.operation,
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
        try:
            mongo.update_job_status(
                job_id, "running", phase="reading", progress_pct=5,
                message="Analyzing source table…",
            )
            columns, schema, total_rows, sample_rows = peek_stream_source(request.source)
            if total_rows == 0:
                mongo.update_job_status(job_id, "failed", error="Source table is empty", phase="failed")
                return TransferResult(
                    success=False, error="Source table is empty",
                    operation=request.operation, job_id=job_id,
                )

            mongo.update_job_status(job_id, "running", total_rows=total_rows, records_processed=0)
            dest_schema_types = _destination_schema_types(request.destination, sync_mode=request.sync_mode)
            mappings = _enrich_mappings_with_types(
                _auto_map(request, columns, schema, sample_rows=sample_rows),
                dest_schema_types,
            )
            column_types = request.column_types or build_column_types(columns, schema)

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
                    decision = pf.get("proof_bundle", {}).get("transfer_decision", {}) or {}
                    blocker_reasons = [b.get("message") for b in pf.get("blockers", [])]
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
                            if b.get("guidance")
                        ],
                        "proof_bundle": {
                            "decision": decision.get("decision"),
                            "reason": decision.get("reason"),
                            "semantic_mapping_score": pf.get("proof_bundle", {}).get("semantic_mapping_score"),
                            "min_confidence": pf.get("proof_bundle", {}).get("min_confidence"),
                            "quality_score": pf.get("proof_bundle", {}).get("quality_score"),
                            "compliance_risk": pf.get("proof_bundle", {}).get("compliance", {}).get("risk_score"),
                        },
                        "readiness_score": pf.get("readiness_score"),
                        "validation_plan": pf.get("validation_plan"),
                        "payload_shape": pf.get("payload_shape"),
                    }
                    error_message = decision.get("reason") or "; ".join(blocker_reasons) or "Preflight blocked transfer"
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
                        job_id, "failed", error=error_message,
                        phase="failed", progress_pct=0,
                        error_details=error_details,
                        preflight=pf,
                    )
                    return TransferResult(
                        success=False,
                        error=error_message,
                        error_details=error_details,
                        validation_plan=pf.get("validation_plan") or {},
                        payload_shape=pf.get("payload_shape") or {},
                        operation=request.operation,
                        job_id=job_id,
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
            )
            if not recon.get("passed"):
                mongo.update_job_status(
                    job_id, "failed",
                    error=recon.get("message", "Reconciliation failed"),
                    phase="failed",
                    progress_pct=95,
                    message=recon.get("message"),
                )
                return TransferResult(
                    success=False,
                    error=recon.get("message", "Reconciliation failed"),
                    operation=request.operation,
                    job_id=job_id,
                    records_transferred=rows_written,
                    destination_summary=dest_summary,
                )

            mongo.update_job_status(
                job_id, "completed",
                records_processed=rows_written,
                progress_pct=100,
                phase="completed",
                message=recon.get("message", f"Transferred {rows_written:,} rows successfully"),
                destination_database=dest_summary.get("database", request.destination.database or ""),
                destination_collection=dest_summary.get("collection") or dest_summary.get("table", ""),
                rejected_rows=int(dest_summary.get("rejected_rows", 0) or 0),
                rejected_details=(dest_summary.get("rejected_details") or [])[:200],
                destination_summary=dest_summary,
            )

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
            )
        except Exception as e:
            error_classification = classify_error(e)
            cancelled = isinstance(e, TransferCancelled)
            status = "cancelled" if cancelled else "failed"
            mongo.update_job_status(
                job_id, status,
                error=str(e), phase=status, progress_pct=0, message=str(e),
                error_details={"retriable": error_classification.get("retriable"), "evidence": error_classification.get("evidence")},
            )
            lineage.emit_run_failed(
                run_id=job_id, job_id=job_id, error=str(e),
                error_details={"retriable": error_classification.get("retriable"), "evidence": error_classification.get("evidence")},
                retriable=error_classification.get("retriable", False),
            )
            return TransferResult(
                success=False,
                job_id=job_id,
                error=str(e),
                error_details={"retriable": error_classification.get("retriable"), "evidence": error_classification.get("evidence")},
                operation=request.operation,
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
        try:
            content = request.source_content or b""
            filename = request.source_filename or "upload.csv"

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

            mongo.update_job_status(job_id, "running", total_rows=total_rows, records_processed=0)
            dest_schema_types = _destination_schema_types(request.destination, sync_mode=request.sync_mode)
            mappings = _enrich_mappings_with_types(
                _auto_map(request, columns, schema, sample_rows=sample_rows),
                dest_schema_types,
            )
            column_types = request.column_types or build_column_types(columns, schema)

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
                    decision = pf.get("proof_bundle", {}).get("transfer_decision", {}) or {}
                    blocker_reasons = [b.get("message") for b in pf.get("blockers", [])]
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
                            if b.get("guidance")
                        ],
                        "proof_bundle": {
                            "decision": decision.get("decision"),
                            "reason": decision.get("reason"),
                            "semantic_mapping_score": pf.get("proof_bundle", {}).get("semantic_mapping_score"),
                            "min_confidence": pf.get("proof_bundle", {}).get("min_confidence"),
                            "quality_score": pf.get("proof_bundle", {}).get("quality_score"),
                            "compliance_risk": pf.get("proof_bundle", {}).get("compliance", {}).get("risk_score"),
                        },
                        "readiness_score": pf.get("readiness_score"),
                        "validation_plan": pf.get("validation_plan"),
                        "payload_shape": pf.get("payload_shape"),
                    }
                    error_message = decision.get("reason") or "; ".join(blocker_reasons) or "Preflight blocked transfer"
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
                        job_id, "failed", error=error_message,
                        phase="failed", progress_pct=0,
                        error_details=error_details,
                        preflight=pf,
                    )
                    return TransferResult(
                        success=False,
                        error=error_message,
                        error_details=error_details,
                        validation_plan=pf.get("validation_plan") or {},
                        payload_shape=pf.get("payload_shape") or {},
                        operation=request.operation,
                        job_id=job_id,
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
            )
            if not recon.get("passed"):
                mongo.update_job_status(
                    job_id, "failed",
                    error=recon.get("message", "Reconciliation failed"),
                    phase="failed",
                    progress_pct=95,
                    message=recon.get("message"),
                )
                return TransferResult(
                    success=False,
                    error=recon.get("message", "Reconciliation failed"),
                    operation=request.operation,
                    job_id=job_id,
                    records_transferred=rows_written,
                    destination_summary=dest_summary,
                )

            mongo.update_job_status(
                job_id, "completed",
                records_processed=rows_written,
                progress_pct=100,
                phase="completed",
                message=recon.get("message", f"Transferred {rows_written:,} rows successfully"),
                destination_database=dest_summary.get("database", request.destination.database or ""),
                destination_collection=dest_summary.get("collection") or dest_summary.get("table", ""),
                rejected_rows=int(dest_summary.get("rejected_rows", 0) or 0),
                rejected_details=(dest_summary.get("rejected_details") or [])[:200],
                destination_summary=dest_summary,
            )

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
            )
        except Exception as e:
            error_classification = classify_error(e)
            cancelled = isinstance(e, TransferCancelled)
            status = "cancelled" if cancelled else "failed"
            mongo.update_job_status(
                job_id, status,
                error=str(e), phase=status, progress_pct=0, message=str(e),
                error_details={"retriable": error_classification.get("retriable"), "evidence": error_classification.get("evidence")},
            )
            lineage.emit_run_failed(
                run_id=job_id, job_id=job_id, error=str(e),
                error_details={"retriable": error_classification.get("retriable"), "evidence": error_classification.get("evidence")},
                retriable=error_classification.get("retriable", False),
            )
            return TransferResult(
                success=False,
                job_id=job_id,
                error=str(e),
                error_details={"retriable": error_classification.get("retriable"), "evidence": error_classification.get("evidence")},
                operation=request.operation,
            )

    def _create_pending_job(self, request: TransferRequest) -> str:
        mongo = get_mongodb_service()
        return mongo.create_transfer_job({
            "source_type": request.source.kind,
            "source_name": request.source_filename or request.source.table or request.source.collection or "database",
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
        from .endpoint_intelligence import build_transfer_plan, introspect_endpoint
        from services.universal_router import analyze_route

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
        src_fmt = source.format or ("csv" if source.kind == "file" else source.format or "")
        dst_fmt = destination.format or ("mongodb" if destination.kind == "database" else "json")
        plan["route_analysis"] = analyze_route(source.kind, src_fmt, destination.kind, dst_fmt)
        return plan


_engine: Optional[UniversalTransferEngine] = None


def get_transfer_engine() -> UniversalTransferEngine:
    global _engine
    if _engine is None:
        _engine = UniversalTransferEngine()
    return _engine
