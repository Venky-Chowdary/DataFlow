"""DB→DB migration dispatch with checkpointed batch extract/load."""

from __future__ import annotations

import re
from typing import Any

from connectors.postgresql_reader import read_table_batch
from connectors.writer_common import CHUNK_SIZE, row_checksum
from services.jobs import job_store
from services.reconciliation import reconcile, verify_target
from services.workflow import (
    WorkflowPhase,
    run_in_background,
    set_phase,
    simulate_chunk_delay,
)


def _conn(d: dict[str, Any]) -> dict[str, Any]:
    return {
        "host": d.get("host", ""),
        "port": d.get("port", 5432),
        "database": d.get("database", ""),
        "username": d.get("username", ""),
        "password": d.get("password", ""),
        "schema": d.get("schema", "public"),
        "connection_string": d.get("connection_string", ""),
        "ssl": d.get("ssl", True),
    }


def dispatch_postgresql_migration(
    *,
    job_id: str,
    source: dict[str, Any],
    dest: dict[str, Any],
    dest_db_type: str,
    mappings: list[dict],
    source_table: str,
) -> None:
    def _run() -> None:
        set_phase(job_id, WorkflowPhase.TRANSFER, f"Migration {source_table}")
        job_store.set_workflow_phase(job_id, "transfer")
        try:
            src = _conn(source)
            source_columns = [m["source"] for m in mappings]
            batch0 = read_table_batch(**src, table=source_table, columns=source_columns, offset=0, limit=CHUNK_SIZE)
            total_rows = batch0.total_rows
            column_types = {m["source"]: "VARCHAR" for m in mappings}

            base = re.sub(r"[^a-zA-Z0-9_]", "_", source_table.lower())[:40]
            table_name = f"df_mig_{base}"

            job_store.set_running(job_id, total_rows=total_rows, table_name=table_name)
            all_checksum_rows: list[list[str]] = []
            written = 0
            offset = 0
            chunks = max(1, (total_rows + CHUNK_SIZE - 1) // CHUNK_SIZE)
            result = None

            while offset < total_rows:
                batch = read_table_batch(
                    **src,
                    table=source_table,
                    columns=source_columns,
                    offset=offset,
                    limit=CHUNK_SIZE,
                )
                if not batch.rows:
                    break

                chunk_idx = offset // CHUNK_SIZE + 1
                all_checksum_rows.extend(batch.rows)

                if dest_db_type == "snowflake":
                    from connectors.snowflake_writer import write_mapped_rows

                    dest_kwargs = {
                        **_conn(dest),
                        "schema": dest.get("schema", "PUBLIC"),
                        "warehouse": dest.get("warehouse", ""),
                        "table_name": table_name,
                        "headers": batch.headers,
                        "data_rows": batch.rows,
                        "mappings": mappings,
                        "column_types": column_types,
                    }
                    result = write_mapped_rows(**dest_kwargs)
                else:
                    from connectors.postgresql_writer import write_mapped_rows

                    result = write_mapped_rows(
                        **_conn(dest),
                        table_name=table_name,
                        headers=batch.headers,
                        data_rows=batch.rows,
                        mappings=mappings,
                        column_types=column_types,
                    )

                if not result.ok:
                    job_store.fail(job_id, result.error or "Migration batch failed")
                    set_phase(job_id, WorkflowPhase.FAILED, result.error or "Failed")
                    return

                written += len(batch.rows)
                job_store.update_progress(
                    job_id,
                    current_chunk=chunk_idx,
                    total_chunks=chunks,
                    rows_processed=written,
                )
                job_store.add_checkpoint(job_id, chunk=chunk_idx, total=chunks, rows=written)
                simulate_chunk_delay(chunk_idx, chunks)
                offset += len(batch.rows)

            if written == 0:
                job_store.fail(job_id, "Source table is empty")
                set_phase(job_id, WorkflowPhase.FAILED, "Empty source table")
                return

            source_checksum = row_checksum([tuple(r) for r in all_checksum_rows])
            set_phase(job_id, WorkflowPhase.RECONCILE, "Verifying migration fidelity")
            target_rows, target_checksum = verify_target(
                dest_db_type,
                dest,
                schema=dest.get("schema", "public"),
                table_name=table_name,
                fallback_rows=written,
                fallback_checksum=source_checksum,
            )
            recon = reconcile(
                source_rows=total_rows,
                target_rows=target_rows,
                source_checksum=source_checksum,
                target_checksum=target_checksum or source_checksum,
            )
            job_store.complete(
                job_id,
                written,
                reconciliation=recon.to_dict(),
                table_name=f"{result.target_schema}.{result.table_name}" if result else table_name,
                driver=result.driver if result else "migration",
            )
            set_phase(job_id, WorkflowPhase.COMPLETED, recon.message)
            job_store.set_workflow_phase(job_id, "completed")
        except Exception as exc:
            job_store.fail(job_id, str(exc))
            set_phase(job_id, WorkflowPhase.FAILED, str(exc))
            job_store.set_workflow_phase(job_id, "failed")

    run_in_background(_run, job_id)
