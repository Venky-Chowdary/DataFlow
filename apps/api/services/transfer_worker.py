"""Dispatch async transfer jobs."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from services.jobs import job_store
from services.reconciliation import checksum_rows, reconcile, verify_target
from services.workflow import WorkflowPhase, run_in_background, set_phase, simulate_chunk_delay


def _wrap_checkpoint(job_id: str, inner):
    def callback(chunk: int, total: int, rows: int) -> None:
        job_store.update_progress(job_id, current_chunk=chunk, total_chunks=total, rows_processed=rows)
        job_store.add_checkpoint(job_id, chunk=chunk, total=total, rows=rows)
        simulate_chunk_delay(chunk, total)
        inner(chunk, total, rows)

    return callback


def dispatch_file_to_database(
    *,
    job_id: str,
    file_id: str,
    mappings: list[dict],
    dest: dict[str, Any],
    db_type: str,
) -> None:
    def _run() -> None:
        set_phase(job_id, WorkflowPhase.TRANSFER, f"Writing batches ({db_type})")
        job_store.set_workflow_phase(job_id, "transfer")
        try:
            import re
            from services.file_parser import get_file, get_file_chunks
            from connectors.writer_common import row_checksum
            
            record = get_file(file_id)
            if not record:
                raise FileNotFoundError("Source file not found")
                
            column_types = {c["name"]: c["inferred_type"] for c in record["columns"]}
            total_rows = record["row_count"]

            base = re.sub(r"[^a-zA-Z0-9_]", "_", Path(record["filename"]).stem.lower())[:40]
            table_name = f"df_{base}_{file_id[:8]}"

            job_store.set_running(job_id, total_rows=total_rows, table_name=table_name)

            common = {
                "host": dest.get("host", ""),
                "port": dest.get("port", 5432),
                "database": dest.get("database", ""),
                "username": dest.get("username", ""),
                "password": dest.get("password", ""),
                "schema": dest.get("schema", "public"),
                "connection_string": dest.get("connection_string", ""),
                "ssl": dest.get("ssl", True),
                "table_name": table_name,
                "mappings": mappings,
                "column_types": column_types,
            }
            
            if db_type == "snowflake":
                from connectors.snowflake_writer import write_mapped_rows
                common["schema"] = dest.get("schema", "PUBLIC")
                common["warehouse"] = dest.get("warehouse", "")
                verify_schema = common["schema"]
            elif db_type == "mongodb":
                from connectors.mongodb_writer import write_mapped_rows
                verify_schema = dest.get("database", dest.get("schema", "db"))
            else:
                from connectors.postgresql_writer import write_mapped_rows
                verify_schema = common["schema"]

            chunks_generator = get_file_chunks(file_id, chunk_size=10000)
            rows_written = 0
            rejected_rows = 0
            final_checksum_list = []
            
            target_schema_out = ""
            table_name_out = table_name
            driver_out = ""

            chunk_idx = 0
            # Rough estimation of total chunks, assuming chunk_size is 10000
            estimated_chunks = max(1, (total_rows + 9999) // 10000)
            
            for headers, data_rows in chunks_generator:
                is_first = (chunk_idx == 0)
                common["headers"] = headers
                common["data_rows"] = data_rows
                common["create_table"] = is_first
                common["on_checkpoint"] = _wrap_checkpoint(job_id, lambda c, t, r: None)
                
                result = write_mapped_rows(**common)
                if not result.ok:
                    raise RuntimeError(result.error or "Batch write failed")
                    
                rows_written += result.rows_written
                rejected_rows += int(getattr(result, "rejected_rows", 0) or 0)
                final_checksum_list.append(result.checksum)
                target_schema_out = result.target_schema
                table_name_out = result.table_name
                driver_out = result.driver
                
                chunk_idx += 1

            combined_checksum = row_checksum([[c] for c in final_checksum_list])

            set_phase(job_id, WorkflowPhase.RECONCILE, "Verifying row fidelity")
            target_rows, target_checksum = verify_target(
                db_type,
                dest,
                schema=verify_schema,
                table_name=table_name_out,
                fallback_rows=rows_written,
                fallback_checksum=combined_checksum,
            )
            recon = reconcile(
                source_rows=total_rows,
                target_rows=target_rows,
                source_checksum="N/A (Streamed)",
                target_checksum=target_checksum if target_checksum else combined_checksum,
                rejected_rows=rejected_rows,
            )

            job_store.complete(
                job_id,
                rows_written,
                reconciliation=recon.to_dict(),
                table_name=f"{target_schema_out}.{table_name_out}",
                driver=driver_out,
            )
            set_phase(job_id, WorkflowPhase.COMPLETED, recon.message)
            job_store.set_workflow_phase(job_id, "completed")
        except Exception as exc:
            job_store.fail(job_id, str(exc))
            set_phase(job_id, WorkflowPhase.FAILED, str(exc))
            job_store.set_workflow_phase(job_id, "failed")

    run_in_background(_run)


def dispatch_file_to_postgres(
    *,
    job_id: str,
    file_id: str,
    mappings: list[dict],
    dest: dict[str, Any],
) -> None:
    dispatch_file_to_database(
        job_id=job_id,
        file_id=file_id,
        mappings=mappings,
        dest=dest,
        db_type="postgresql",
    )


def dispatch_simulated(job_id: str, total_rows: int) -> None:
    def _run() -> None:
        job_store.fail(
            job_id,
            "Operation not yet supported for live execution — configure file → PostgreSQL or Snowflake",
        )
        set_phase(job_id, WorkflowPhase.FAILED, "Unsupported operation path")
        job_store.set_workflow_phase(job_id, "failed")

    run_in_background(_run)
