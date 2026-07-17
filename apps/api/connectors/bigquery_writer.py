"""BigQuery bulk writer — load rows via insert_rows_json with checkpoints."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from connectors.driver_guard import stub_writes_allowed
from connectors.stub_writer import simulate_stub_write
from connectors.writer_common import (
    WriteResult as _WriteResult,
    CHUNK_SIZE,
    build_mapped_rows,
    resolve_target_columns,
    row_checksum,
    sanitize_identifier,
    transform_error_policy,
)
from services.type_system import ddl_type


@dataclass
class WriteResult(_WriteResult):
    driver: str = "google-cloud-bigquery"


def bq_type(inferred: str) -> str:
    return ddl_type("bigquery", inferred)


def write_mapped_rows(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    schema: str,
    connection_string: str,
    ssl: bool,
    warehouse: str,
    table_name: str,
    headers: list[str],
    data_rows: list[list[str]],
    mappings: list[dict],
    column_types: dict[str, str],
    on_checkpoint: Callable[[int, int, int], None] | None = None,
    error_policy: str | None = None,
    backfill_new_fields: bool = False,
    service_account: str = "",
    **_kwargs: Any,
) -> WriteResult:
    del username, password, ssl, warehouse, _kwargs
    project_id = database or host
    dataset_id = schema or "dataflow"
    table_name = sanitize_identifier(table_name)
    policy = transform_error_policy(error_policy)

    if stub_writes_allowed():
        rows, checksum, chunks = simulate_stub_write(
            data_rows=data_rows, table_name=table_name, target_schema=dataset_id,
            on_checkpoint=on_checkpoint,
        )
        return WriteResult(
            ok=True, rows_written=rows, table_name=table_name, target_schema=dataset_id,
            checksum=checksum, chunks_completed=chunks, driver="stub",
        )

    try:
        from google.cloud import bigquery  # noqa: F401
    except ImportError:
        from connectors.driver_guard import require_driver

        if not stub_writes_allowed():
            return WriteResult(
                ok=False, rows_written=0, table_name=table_name, target_schema=dataset_id,
                checksum="", chunks_completed=0,
                error=require_driver("google.cloud.bigquery", "google-cloud-bigquery"),
                driver="none",
            )
        rows, checksum, chunks = simulate_stub_write(
            data_rows=data_rows, table_name=table_name, target_schema=dataset_id,
            on_checkpoint=on_checkpoint,
        )
        return WriteResult(
            ok=True, rows_written=rows, table_name=table_name, target_schema=dataset_id,
            checksum=checksum, chunks_completed=chunks, driver="stub",
        )

    target_cols, logical_types = resolve_target_columns(mappings, column_types)
    if not target_cols:
        return WriteResult(
            ok=False, rows_written=0, table_name=table_name, target_schema=dataset_id,
            checksum="", chunks_completed=0, error="No column mappings",
        )
    dest_types = {target_cols[i]: logical_types[i] for i in range(len(target_cols))}

    try:
        from google.cloud import bigquery
        from connectors.bigquery_conn import get_client

        client = get_client(
            project_id=project_id,
            credentials_path=connection_string,
            service_account=service_account,
            host=host,
            port=port,
            connection_string=connection_string,
        )
        table_id = f"{project_id}.{dataset_id}.{table_name}"

        schema_fields = [
            bigquery.SchemaField(col, bq_type(t)) for col, t in zip(target_cols, logical_types)
        ]
        dataset_ref = f"{project_id}.{dataset_id}"
        existing_datasets = {ds.dataset_id for ds in client.list_datasets()}
        if dataset_id not in existing_datasets:
            client.create_dataset(bigquery.Dataset(dataset_ref))
        table = bigquery.Table(table_id, schema=schema_fields)
        client.create_table(table, exists_ok=True)

        if backfill_new_fields:
            table = client.get_table(table_id)
            existing = {f.name for f in table.schema}
            new_fields = [bigquery.SchemaField(col, bq_type(t)) for col, t in zip(target_cols, logical_types) if col not in existing]
            if new_fields:
                table.schema = list(table.schema) + new_fields
                client.update_table(table, ["schema"])

        mapped_rows, transform_errors = build_mapped_rows(
            headers=headers,
            data_rows=data_rows,
            mappings=mappings,
            target_cols=target_cols,
            column_types=column_types,
            dest_types=dest_types,
            error_policy=policy,
        )
        rejected_rows = len(data_rows) - len(mapped_rows)
        if transform_errors and policy == "fail":
            return WriteResult(
                ok=False, rows_written=0, table_name=table_name, target_schema=dataset_id,
                checksum="", chunks_completed=0,
                error=f"Transform errors: {'; '.join(transform_errors[:3])}",
                rejected_rows=rejected_rows,
                warnings=transform_errors,
            )

        total = len(mapped_rows)
        chunks = max(1, (total + CHUNK_SIZE - 1) // CHUNK_SIZE)
        written = 0

        for chunk_idx in range(chunks):
            start = chunk_idx * CHUNK_SIZE
            batch = mapped_rows[start : start + CHUNK_SIZE]
            if not batch:
                break
            records = [dict(zip(target_cols, row)) for row in batch]
            errors = client.insert_rows_json(table_id, records)
            if errors:
                return WriteResult(
                    ok=False, rows_written=written, table_name=table_name, target_schema=dataset_id,
                    checksum="", chunks_completed=chunk_idx,
                    error=str(errors[:2]),
                )
            written += len(batch)
            if on_checkpoint:
                on_checkpoint(chunk_idx + 1, chunks, written)

        return WriteResult(
            ok=True, rows_written=written, table_name=table_name, target_schema=dataset_id,
            checksum=row_checksum(mapped_rows, target_cols), chunks_completed=chunks,
            rejected_rows=len(data_rows) - written,
            warnings=transform_errors,
        )
    except Exception as exc:
        return WriteResult(
            ok=False, rows_written=0, table_name=table_name, target_schema=dataset_id,
            checksum="", chunks_completed=0, error=str(exc),
        )
