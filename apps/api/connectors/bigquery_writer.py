"""BigQuery bulk writer — insert_rows_json + optional MERGE upsert with ``_df_lsn``."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any, Callable

from connectors.driver_guard import stub_writes_allowed
from connectors.stub_writer import simulate_stub_write
from connectors.writer_common import (
    CHUNK_SIZE,
    DF_LSN_COL,
    _coerced_null_row_count,
    _rejected_row_count,
    build_mapped_rows_with_details,
    dedupe_rows,
    dedupe_rows_by_pk_and_lsn,
    resolve_target_columns,
    row_checksum,
    sanitize_identifier,
    transform_error_policy,
)
from connectors.writer_common import (
    WriteResult as _WriteResult,
)
from services.type_system import ddl_type


@dataclass
class WriteResult(_WriteResult):
    driver: str = "google-cloud-bigquery"


def bq_type(inferred: str) -> str:
    return ddl_type("bigquery", inferred)


def build_bigquery_merge_sql(
    target_table: str,
    staging_table: str,
    target_cols: list[str],
    conflict_columns: list[str],
    *,
    lsn_column: str | None = None,
) -> str:
    """Build a BigQuery MERGE for PK upsert with optional monotonic LSN guard."""
    conflict = [c for c in conflict_columns if c in target_cols]
    if not conflict:
        raise ValueError("BigQuery MERGE requires conflict_columns present in target_cols")
    on_clause = " AND ".join(f"T.`{c}` = S.`{c}`" for c in conflict)
    update_cols = [c for c in target_cols if c not in conflict]
    set_clause = ", ".join(f"T.`{c}` = S.`{c}`" for c in update_cols) or "T.`{0}` = S.`{0}`".format(
        conflict[0]
    )
    matched = "WHEN MATCHED"
    if lsn_column and lsn_column in target_cols:
        matched += (
            f" AND S.`{lsn_column}` > COALESCE(T.`{lsn_column}`, '')"
        )
    matched += f" THEN UPDATE SET {set_clause}"
    insert_cols = ", ".join(f"`{c}`" for c in target_cols)
    insert_vals = ", ".join(f"S.`{c}`" for c in target_cols)
    return (
        f"MERGE `{target_table}` T\n"
        f"USING `{staging_table}` S\n"
        f"ON {on_clause}\n"
        f"{matched}\n"
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
    )


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
    write_mode: str = "insert",
    conflict_columns: list[str] | None = None,
    **_kwargs: Any,
) -> WriteResult:
    del username, password, ssl, warehouse, _kwargs
    project_id = database or host
    dataset_id = schema or "dataflow"
    table_name = sanitize_identifier(table_name)
    policy = transform_error_policy(error_policy)
    conflict_columns = list(conflict_columns or [])

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

    from connectors.writer_common import sample_values_by_source_from_batch

    batch_samples = sample_values_by_source_from_batch(headers, data_rows, mappings)
    target_cols, logical_types = resolve_target_columns(
        mappings,
        column_types,
        sample_values_by_source=batch_samples,
        table_exists=False,
    )
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

        mapped_rows, transform_errors, rejected_details = build_mapped_rows_with_details(
            headers=headers,
            data_rows=data_rows,
            mappings=mappings,
            target_cols=target_cols,
            column_types=column_types,
            dest_types=dest_types,
            error_policy=policy,
        )
        if write_mode == "upsert" and conflict_columns:
            if DF_LSN_COL in target_cols:
                mapped_rows = dedupe_rows_by_pk_and_lsn(
                    mapped_rows, conflict_columns, target_cols
                )
            else:
                mapped_rows = dedupe_rows(mapped_rows, conflict_columns, target_cols)
        rejected_rows = _rejected_row_count(data_rows, mapped_rows, rejected_details, policy)
        coerced_null_rows = _coerced_null_row_count(rejected_details, policy)
        if transform_errors and policy == "fail":
            return WriteResult(
                ok=False, rows_written=0, table_name=table_name, target_schema=dataset_id,
                checksum="", chunks_completed=0,
                error=f"Transform errors: {'; '.join(transform_errors[:3])}",
                rejected_rows=rejected_rows,
                rejected_details=rejected_details,
                warnings=transform_errors,
            )

        from connectors.warehouse_temporal import (
            quarantine_from_bigquery_errors,
            records_for_bigquery,
        )

        bq_types = [bq_type(t) for t in logical_types]
        total = len(mapped_rows)
        chunks = max(1, (total + CHUNK_SIZE - 1) // CHUNK_SIZE) if total else 0
        written = 0
        chunks_completed = 0
        use_merge = write_mode == "upsert" and any(c in target_cols for c in conflict_columns)

        if use_merge:
            staging_name = sanitize_identifier(f"{table_name}_stg_{uuid.uuid4().hex[:8]}")
            staging_id = f"{project_id}.{dataset_id}.{staging_name}"
            staging = bigquery.Table(staging_id, schema=schema_fields)
            client.create_table(staging, exists_ok=True)
            try:
                # Load jobs (not streaming inserts) so staging is immediately MERGE-readable.
                load_config = bigquery.LoadJobConfig(
                    schema=schema_fields,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                )
                for chunk_idx in range(chunks):
                    start = chunk_idx * CHUNK_SIZE
                    batch = mapped_rows[start : start + CHUNK_SIZE]
                    if not batch:
                        break
                    records = records_for_bigquery(batch, target_cols, bq_types)
                    load_job = client.load_table_from_json(
                        records, staging_id, job_config=load_config
                    )
                    load_job.result()
                    merge_sql = build_bigquery_merge_sql(
                        table_id,
                        staging_id,
                        target_cols,
                        conflict_columns,
                        lsn_column=DF_LSN_COL if DF_LSN_COL in target_cols else None,
                    )
                    client.query(merge_sql).result()
                    written += len(batch)
                    chunks_completed = chunk_idx + 1
                    if on_checkpoint:
                        on_checkpoint(chunks_completed, chunks, written)
            finally:
                client.delete_table(staging_id, not_found_ok=True)
        else:
            for chunk_idx in range(chunks):
                start = chunk_idx * CHUNK_SIZE
                batch = mapped_rows[start : start + CHUNK_SIZE]
                if not batch:
                    break
                records = records_for_bigquery(batch, target_cols, bq_types)
                errors = client.insert_rows_json(table_id, records)
                if errors:
                    details, bad = quarantine_from_bigquery_errors(
                        errors, batch, target_cols, row_offset=start, policy=policy,
                    )
                    if policy in {"quarantine", "coerce_null"} and bad:
                        rejected_details.extend(details)
                        transform_errors.extend(d["reason"] for d in details[:5])
                        # Streaming insert commits good rows; count only those.
                        written += len(batch) - len(bad)
                    elif policy == "fail":
                        return WriteResult(
                            ok=False,
                            rows_written=written,
                            table_name=table_name,
                            target_schema=dataset_id,
                            checksum="",
                            chunks_completed=chunk_idx,
                            error=str(errors[:2]),
                            rejected_rows=rejected_rows + len(bad),
                            rejected_details=rejected_details + details,
                            warnings=transform_errors,
                        )
                    else:
                        # Unknown policy: fail closed with details, no silent drop.
                        return WriteResult(
                            ok=False,
                            rows_written=written,
                            table_name=table_name,
                            target_schema=dataset_id,
                            checksum="",
                            chunks_completed=chunk_idx,
                            error=str(errors[:2]),
                            rejected_details=rejected_details + details,
                            warnings=transform_errors,
                        )
                else:
                    written += len(batch)
                chunks_completed = chunk_idx + 1
                if on_checkpoint:
                    on_checkpoint(chunks_completed, chunks, written)

        return WriteResult(
            ok=True, rows_written=written, table_name=table_name, target_schema=dataset_id,
            checksum=row_checksum(mapped_rows, target_cols), chunks_completed=chunks_completed or chunks,
            rejected_rows=max(rejected_rows, len(data_rows) - written),
            rejected_details=rejected_details,
            coerced_null_rows=coerced_null_rows,
            warnings=transform_errors,
        )
    except Exception as exc:
        return WriteResult(
            ok=False, rows_written=0, table_name=table_name, target_schema=dataset_id,
            checksum="", chunks_completed=0, error=str(exc),
            rejected_details=rejected_details if "rejected_details" in locals() else [],
        )
