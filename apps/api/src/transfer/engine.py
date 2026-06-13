"""Universal transfer orchestrator — routes any source to any destination."""

from __future__ import annotations
import os
from typing import Optional

from ..services.mongodb_service import get_mongodb_service
from ..services.preflight_service import run_file_preflight
from .adapters import (
    parse_file_content,
    read_source_database,
    resolve_connector_config,
    write_destination_database,
    write_destination_file,
)
from .models import EndpointConfig, TransferRequest, TransferResult
from .registry import validate_transfer
from .type_mapper import build_column_types, default_mappings


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
        src_fmt = request.source.format or "csv"
        dst_fmt = request.destination.format or "mongodb"
        ok, msg = validate_transfer(
            request.source.kind, src_fmt,
            request.destination.kind, dst_fmt,
        )
        if not ok:
            return TransferResult(success=False, error=msg, operation=request.operation)

        try:
            records, columns, schema = self._read_source(request)
            if not records and request.source.kind != "database":
                return TransferResult(success=False, error="No records to transfer", operation=request.operation)

            mappings = request.mappings or default_mappings(columns)
            column_types = request.column_types or build_column_types(columns, schema)

            if not request.skip_preflight and request.destination.kind == "database":
                pf = run_file_preflight(
                    columns=columns,
                    column_types=schema,
                    row_count=len(records),
                    mappings=mappings,
                    destination_connected=True,
                    sample_rows=records[:100],
                )
                if not pf["passed"]:
                    return TransferResult(
                        success=False,
                        error="Preflight blocked transfer",
                        operation=request.operation,
                    )

            ddl_log: list[str] = []
            dest_summary: dict = {}
            rows_written = 0
            export_bytes: bytes | None = None
            export_name = ""

            if request.destination.kind == "database":
                rows_written, ddl_log, dest_summary = write_destination_database(
                    request.destination, records, columns, schema, mappings,
                )
            elif request.destination.kind == "file_export":
                export_bytes, export_name, dest_summary = write_destination_file(
                    request.destination, records, columns,
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
                return TransferResult(success=False, error=f"Unknown destination kind: {request.destination.kind}")

            job_id = self._create_job(request, rows_written, dest_summary)

            try:
                from ..ai.training.training_scheduler import schedule_training_on_transfer
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
            )
        except Exception as e:
            job_id = self._create_job(request, 0, {}, status="failed")
            return TransferResult(
                success=False,
                job_id=job_id,
                error=str(e),
                operation=request.operation,
            )

    def _read_source(self, request: TransferRequest) -> tuple[list, list[str], dict[str, str]]:
        if request.source.kind == "file":
            if not request.source_content:
                raise ValueError("File content required for file source")
            return parse_file_content(request.source_content, request.source_filename or "upload.csv")
        if request.source.kind == "database":
            return read_source_database(request.source)
        raise ValueError(f"Unsupported source kind: {request.source.kind}")

    def _create_job(
        self,
        request: TransferRequest,
        records: int,
        dest_summary: dict,
        status: str = "completed",
    ) -> str:
        mongo = get_mongodb_service()
        job_id = mongo.create_transfer_job({
            "source_type": request.source.kind,
            "source_name": request.source_filename or request.source.table or request.source.collection or "database",
            "source_format": request.source.format,
            "destination_type": request.destination.format,
            "destination_kind": request.destination.kind,
            "destination_database": request.destination.database or dest_summary.get("database", ""),
            "destination_collection": request.destination.collection or dest_summary.get("collection") or dest_summary.get("table", ""),
            "operation": request.operation,
            "records_processed": records,
        })
        if status == "completed":
            mongo.update_job_status(job_id, "completed", records_processed=records)
        else:
            mongo.update_job_status(job_id, "failed", records_processed=0)
        return job_id

    def analyze_compatibility(
        self,
        source: EndpointConfig,
        destination: EndpointConfig,
        sample_content: bytes | None = None,
        filename: str = "",
    ) -> dict:
        """Understand source + destination and recommend auto-creation plan."""
        from .endpoint_intelligence import build_transfer_plan, introspect_endpoint

        source_info = introspect_endpoint(source, sample_content, filename)
        plan = build_transfer_plan(source, destination, source_info)
        if source_info.get("columns"):
            plan["source_columns"] = source_info["columns"]
            plan["source_schema"] = source_info["schema"]
        return plan


_engine: Optional[UniversalTransferEngine] = None


def get_transfer_engine() -> UniversalTransferEngine:
    global _engine
    if _engine is None:
        _engine = UniversalTransferEngine()
    return _engine
