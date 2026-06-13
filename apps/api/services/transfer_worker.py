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


def _load_file_rows(file_id: str) -> tuple[dict, list[str], list[list[str]]]:
    from services.csv_profiler import parse_csv_full
    from services.file_parser import get_file

    record = get_file(file_id)
    if not record:
        raise FileNotFoundError("Source file not found")
    path = Path(record["path"])
    headers, data_rows, _enc, _delim = parse_csv_full(path.read_bytes())
    return record, headers, data_rows


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

            record, headers, data_rows = _load_file_rows(file_id)
            column_types = {c["name"]: c["inferred_type"] for c in record["columns"]}
            total_rows = len(data_rows)
            source_checksum = checksum_rows(data_rows)

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
                "headers": headers,
                "data_rows": data_rows,
                "mappings": mappings,
                "column_types": column_types,
                "on_checkpoint": _wrap_checkpoint(job_id, lambda c, t, r: None),
            }

            if db_type == "snowflake":
                from connectors.snowflake_writer import write_mapped_rows

                common["schema"] = dest.get("schema", "PUBLIC")
                common["warehouse"] = dest.get("warehouse", "")
                result = write_mapped_rows(**common)
                verify_schema = common["schema"]
            else:
                from connectors.postgresql_writer import write_mapped_rows

                result = write_mapped_rows(**common)
                verify_schema = common["schema"]

            if not result.ok:
                job_store.fail(job_id, result.error or "Write failed")
                set_phase(job_id, WorkflowPhase.FAILED, result.error or "Failed")
                return

            set_phase(job_id, WorkflowPhase.RECONCILE, "Verifying row fidelity")
            target_rows, target_checksum = verify_target(
                db_type,
                dest,
                schema=verify_schema,
                table_name=result.table_name,
                fallback_rows=result.rows_written,
                fallback_checksum=result.checksum,
            )
            recon = reconcile(
                source_rows=total_rows,
                target_rows=target_rows,
                source_checksum=source_checksum,
                target_checksum=target_checksum if target_checksum else result.checksum,
            )

            job_store.complete(
                job_id,
                result.rows_written,
                reconciliation=recon.to_dict(),
                table_name=f"{result.target_schema}.{result.table_name}",
                driver=result.driver,
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
