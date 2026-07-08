"""Execute file → database transfers with checkpoint progress."""

from __future__ import annotations

import re
from pathlib import Path

from connectors.postgresql_writer import sanitize_identifier, write_mapped_rows
from services.csv_profiler import parse_csv
from services.file_parser import get_file
from services.jobs import job_store
from services.reconciliation import checksum_rows, reconcile


def _table_name_from_file(filename: str, file_id: str) -> str:
    base = Path(filename).stem
    clean = re.sub(r"[^a-zA-Z0-9_]", "_", base.lower())[:40]
    return sanitize_identifier(f"df_{clean}_{file_id[:8]}")


def execute_file_to_postgres(
    *,
    job_id: str,
    file_id: str,
    mappings: list[dict],
    dest: dict,
) -> dict:
    record = get_file(file_id)
    if not record:
        job_store.fail(job_id, "Source file not found")
        return {"ok": False, "error": "file_not_found"}

    path = Path(record["path"])
    content = path.read_bytes()
    headers, data_rows, _enc, _delim = parse_csv(content)
    column_types = {c["name"]: c["inferred_type"] for c in record["columns"]}

    source_checksum = checksum_rows(data_rows)
    total_rows = len(data_rows)
    table_name = _table_name_from_file(record["filename"], file_id)

    job_store.set_running(job_id, total_rows=total_rows, table_name=table_name)

    def on_checkpoint(chunk: int, total_chunks: int, rows_done: int) -> None:
        job_store.update_progress(job_id, current_chunk=chunk, total_chunks=total_chunks, rows_processed=rows_done)

    result = write_mapped_rows(
        host=dest.get("host", ""),
        port=dest.get("port", 5432),
        database=dest.get("database", ""),
        username=dest.get("username", ""),
        password=dest.get("password", ""),
        schema=dest.get("schema", "public"),
        connection_string=dest.get("connection_string", ""),
        ssl=dest.get("ssl", True),
        table_name=table_name,
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        column_types=column_types,
        on_checkpoint=on_checkpoint,
    )

    if not result.ok:
        job_store.fail(job_id, result.error or "Write failed")
        return {"ok": False, "error": result.error}

    recon = reconcile(
        source_rows=total_rows,
        target_rows=result.rows_written,
        source_checksum=result.checksum,
        target_checksum=result.checksum,
        rejected_rows=int(getattr(result, "rejected_rows", 0) or 0),
    )

    job_store.complete(
        job_id,
        result.rows_written,
        reconciliation=recon.to_dict(),
        table_name=f"{result.target_schema}.{result.table_name}",
        driver=result.driver,
    )

    return {
        "ok": True,
        "rows_written": result.rows_written,
        "table": f"{result.target_schema}.{result.table_name}",
        "reconciliation": recon.to_dict(),
        "driver": result.driver,
    }
