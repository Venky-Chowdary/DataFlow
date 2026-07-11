"""Universal transfer orchestrator — routes any source to any destination."""

from __future__ import annotations
import os
from typing import Optional

try:
    from services.mongodb_service import get_mongodb_service
    from services.preflight_service import (
        apply_policy_gates,
        confidence_threshold_for_mode,
        probe_destination,
        run_file_preflight,
        run_transfer_policy_gates,
    )
except ImportError:  # pragma: no cover - compatibility for tests with api root on PYTHONPATH
    from src.services.mongodb_service import get_mongodb_service
    from src.services.preflight_service import (
        apply_policy_gates,
        confidence_threshold_for_mode,
        probe_destination,
        run_file_preflight,
        run_transfer_policy_gates,
    )
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

import sys
from pathlib import Path
_api_root = Path(__file__).resolve().parents[2]
if str(_api_root) not in sys.path:
    sys.path.insert(0, str(_api_root))
from connectors.writer_common import CHUNK_SIZE  # noqa: E402
from services.batch_progress import ThrottledCheckpoint  # noqa: E402


def _destination_schema_types(destination: EndpointConfig) -> dict[str, str]:
    """Introspect destination column types for schema-aware preflight and transforms."""
    if destination.kind != "database":
        return {}
    try:
        from .endpoint_intelligence import introspect_endpoint

        info = introspect_endpoint(destination)
        return dict(info.get("schema") or {})
    except Exception:
        return {}


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

    def execute_tracked(self, request: TransferRequest, job_id: str) -> TransferResult:
        mongo = get_mongodb_service()
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
            return TransferResult(success=False, error=msg, operation=request.operation, job_id=job_id)

        if supports_streaming(request.source, request.destination):
            return self._execute_streaming(request, job_id, mongo, src_fmt)

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
            return self._execute_file_streaming(request, job_id, mongo, src_fmt)

        try:
            mongo.update_job_status(
                job_id, "running", phase="reading", progress_pct=5,
                message="Reading source data…",
            )
            records, columns, schema = self._read_source(request)
            if not records and request.source.kind != "database":
                mongo.update_job_status(job_id, "failed", error="No records to transfer", phase="failed")
                return TransferResult(success=False, error="No records to transfer", operation=request.operation, job_id=job_id)

            total_rows = len(records)
            mongo.update_job_status(job_id, "running", total_rows=total_rows, records_processed=0)

            mappings = _enrich_mappings_with_types(
                request.mappings or default_mappings(columns),
                _destination_schema_types(request.destination),
            )
            column_types = request.column_types or build_column_types(columns, schema)

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
                    sample_rows=records[:100],
                    confidence_threshold=confidence_threshold_for_mode(request.validation_mode),
                    destination_column_types=_destination_schema_types(request.destination),
                    destination_db_type=(request.destination.format or "postgresql").lower(),
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
                )
                if not dest_ok:
                    mongo.update_job_status(
                        job_id, "failed",
                        error=f"Destination unreachable: {dest_msg}",
                        phase="failed", progress_pct=0,
                    )
                    return TransferResult(
                        success=False,
                        error=f"Destination unreachable: {dest_msg}",
                        operation=request.operation,
                        job_id=job_id,
                    )
                if not pf["passed"]:
                    mongo.update_job_status(
                        job_id, "failed", error="Preflight blocked transfer",
                        phase="failed", progress_pct=0,
                    )
                    return TransferResult(
                        success=False,
                        error="Preflight blocked transfer",
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

            def on_checkpoint(chunk: int, chunks: int, rows: int) -> None:
                pct = 25 + int((chunk / max(chunks, 1)) * 65)
                mongo.update_job_status(
                    job_id, "running",
                    records_processed=rows,
                    progress_pct=min(pct, 90),
                    chunk_current=chunk,
                    chunk_total=chunks,
                    message=f"Writing batch {chunk}/{chunks} ({rows:,} rows)…",
                )

            throttled_checkpoint = ThrottledCheckpoint(on_checkpoint)

            if request.destination.kind == "database":
                rows_written, ddl_log, dest_summary = write_destination_database(
                    request.destination, records, columns, schema, mappings,
                    on_checkpoint=throttled_checkpoint,
                    validation_mode=request.validation_mode,
                )
                if total_rows <= CHUNK_SIZE:
                    mongo.update_job_status(job_id, "running", records_processed=rows_written, progress_pct=90)
            elif request.destination.kind == "file_export":
                export_bytes, export_name, dest_summary = write_destination_file(
                    request.destination,
                    records,
                    columns,
                    source_format=src_fmt,
                    mappings=mappings,
                    column_types=request.column_types,
                )
                rows_written = len(records)
                export_dir = os.path.join(os.path.dirname(__file__), "..", "..", "exports")
                os.makedirs(export_dir, exist_ok=True)
                export_path = os.path.join(export_dir, export_name)
                with open(export_path, "wb") as f:
                    f.write(export_bytes)
                dest_summary["path"] = export_path
                ddl_log.append(f"Exported {rows_written} rows to {export_name}")
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
            )
        except Exception as e:
            mongo.update_job_status(
                job_id, "failed",
                error=str(e), phase="failed", progress_pct=0, message=str(e),
            )
            return TransferResult(
                success=False,
                job_id=job_id,
                error=str(e),
                operation=request.operation,
            )

    def _execute_streaming(
        self,
        request: TransferRequest,
        job_id: str,
        mongo,
        src_fmt: str,
    ) -> TransferResult:
        """Batched DB→DB path — never loads full table into memory."""
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
            mappings = _enrich_mappings_with_types(
                request.mappings or default_mappings(columns),
                _destination_schema_types(request.destination),
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
                    sample_rows=sample_rows,
                    confidence_threshold=confidence_threshold_for_mode(request.validation_mode),
                    destination_column_types=_destination_schema_types(request.destination),
                    destination_db_type=(request.destination.format or "postgresql").lower(),
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
                )
                if not dest_ok:
                    mongo.update_job_status(
                        job_id, "failed",
                        error=f"Destination unreachable: {dest_msg}",
                        phase="failed", progress_pct=0,
                    )
                    return TransferResult(
                        success=False,
                        error=f"Destination unreachable: {dest_msg}",
                        operation=request.operation,
                        job_id=job_id,
                    )
                if not pf["passed"]:
                    mongo.update_job_status(
                        job_id, "failed", error="Preflight blocked transfer",
                        phase="failed", progress_pct=0,
                    )
                    return TransferResult(
                        success=False,
                        error="Preflight blocked transfer",
                        operation=request.operation,
                        job_id=job_id,
                    )

            def on_checkpoint(chunk: int, chunks: int, rows: int) -> None:
                pct = 25 + int((chunk / max(chunks, 1)) * 65)
                mongo.update_job_status(
                    job_id, "running",
                    records_processed=rows,
                    progress_pct=min(pct, 90),
                    chunk_current=chunk,
                    chunk_total=chunks,
                    message=f"Writing batch {chunk}/{chunks} ({rows:,} rows)…",
                )

            throttled_checkpoint = ThrottledCheckpoint(on_checkpoint)

            mongo.update_job_status(
                job_id, "running", phase="writing", progress_pct=25,
                message=f"Streaming {total_rows:,} rows in batches…",
            )

            rows_written, ddl_log, dest_summary, _ = stream_database_transfer(
                request.source,
                request.destination,
                mappings,
                schema,
                on_checkpoint=throttled_checkpoint,
                sync_mode=request.sync_mode,
                stream_contracts=request.stream_contracts,
                job_id=job_id,
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
            )
        except Exception as e:
            mongo.update_job_status(
                job_id, "failed",
                error=str(e), phase="failed", progress_pct=0, message=str(e),
            )
            return TransferResult(
                success=False,
                job_id=job_id,
                error=str(e),
                operation=request.operation,
            )

    def _execute_file_streaming(
        self,
        request: TransferRequest,
        job_id: str,
        mongo,
        src_fmt: str,
    ) -> TransferResult:
        """Batched file → database path for large CSV/TSV/JSONL uploads."""
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
            mappings = _enrich_mappings_with_types(
                request.mappings or default_mappings(columns),
                _destination_schema_types(request.destination),
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
                    sample_rows=sample_rows,
                    confidence_threshold=confidence_threshold_for_mode(request.validation_mode),
                    destination_column_types=_destination_schema_types(request.destination),
                    destination_db_type=(request.destination.format or "postgresql").lower(),
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
                )
                if not dest_ok:
                    mongo.update_job_status(
                        job_id, "failed",
                        error=f"Destination unreachable: {dest_msg}",
                        phase="failed", progress_pct=0,
                    )
                    return TransferResult(
                        success=False,
                        error=f"Destination unreachable: {dest_msg}",
                        operation=request.operation,
                        job_id=job_id,
                    )
                if not pf["passed"]:
                    mongo.update_job_status(
                        job_id, "failed", error="Preflight blocked transfer",
                        phase="failed", progress_pct=0,
                    )
                    return TransferResult(
                        success=False,
                        error="Preflight blocked transfer",
                        operation=request.operation,
                        job_id=job_id,
                    )

            def on_checkpoint(chunk: int, chunks: int, rows: int) -> None:
                pct = 25 + int((chunk / max(chunks, 1)) * 65)
                mongo.update_job_status(
                    job_id, "running",
                    records_processed=rows,
                    progress_pct=min(pct, 90),
                    chunk_current=chunk,
                    chunk_total=chunks,
                    message=f"Writing batch {chunk}/{chunks} ({rows:,} rows)…",
                )

            throttled_checkpoint = ThrottledCheckpoint(on_checkpoint)

            mongo.update_job_status(
                job_id, "running", phase="writing", progress_pct=25,
                message=f"Streaming {total_rows:,} rows in batches…",
            )

            rows_written, ddl_log, dest_summary, _ = stream_file_to_database(
                content,
                filename,
                request.destination,
                mappings,
                schema,
                on_checkpoint=throttled_checkpoint,
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
            )
        except Exception as e:
            mongo.update_job_status(
                job_id, "failed",
                error=str(e), phase="failed", progress_pct=0, message=str(e),
            )
            return TransferResult(
                success=False,
                job_id=job_id,
                error=str(e),
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
