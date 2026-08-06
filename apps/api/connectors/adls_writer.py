"""Azure Blob Storage / ADLS Gen2 object writer."""

from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass
from typing import Any, Callable

from services.value_serializer import cell_to_string, json_default

from connectors.adls_common import blob_service_client
from connectors.object_store_common import (
    purge_object_store_parts,
    resolve_object_write_layout,
)
from connectors.writer_common import (
    WriteResult as _WriteResult,
)
from connectors.writer_common import (
    _coerced_null_row_count,
    _rejected_row_count,
    apply_write_quarantine_matrix,
    build_mapped_rows_with_details,
    mapped_rows_to_json_records,
    resolve_target_columns,
    row_checksum,
    transform_error_policy,
)


@dataclass
class WriteResult(_WriteResult):
    driver: str = "azure-storage-blob"


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
    warehouse: str = "",
    table_name: str,
    headers: list[str],
    data_rows: list[list[str]],
    mappings: list[dict],
    column_types: dict[str, str],
    on_checkpoint: Callable[[int, int, int], None] | None = None,
    error_policy: str | None = None,
    backfill_new_fields: bool = False,
    create_table: bool = True,
    service_account: str = "",
    **_kwargs: Any,
) -> WriteResult:
    del warehouse, backfill_new_fields
    sync_mode = str(_kwargs.pop("sync_mode", "") or "")
    file_batch_idx = int(_kwargs.pop("file_batch_idx", 0) or 0)
    total_chunks = int(_kwargs.pop("total_chunks", 1) or 1)
    container = database
    if not container:
        return WriteResult(
            ok=False, rows_written=0, table_name=table_name, target_schema="",
            checksum="", chunks_completed=0,
            error="Azure container is required (set the Database field).",
        )

    try:
        layout = resolve_object_write_layout(
            table_name=table_name,
            schema=schema,
            sync_mode=sync_mode,
            file_batch_idx=file_batch_idx,
            total_chunks=total_chunks,
            job_id=str(_kwargs.pop("job_id", "") or ""),
        )
    except ValueError as exc:
        return WriteResult(
            ok=False, rows_written=0, table_name=table_name, target_schema=container,
            checksum="", chunks_completed=0, error=str(exc),
        )
    key = layout.write_key

    cfg = {
        "host": host,
        "port": port,
        "username": username,
        "password": password,
        "connection_string": connection_string,
        "database": container,
        "service_account": service_account,
    }

    target_cols, logical_types = resolve_target_columns(mappings, column_types, preserve_case=True)
    dest_types = {target_cols[i]: logical_types[i] for i in range(len(target_cols))}
    policy = transform_error_policy(error_policy)
    mapped_rows, errors, rejected_details = build_mapped_rows_with_details(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=target_cols,
        column_types=column_types,
        dest_types=dest_types,
        error_policy=policy,
        preserve_case=True,
        dest_kind="adls",
        destination_pk_columns=None,
    )
    tgt_types = [str(dest_types.get(c, "") or "") for c in target_cols]
    mapped_rows = apply_write_quarantine_matrix(
        mapped_rows, target_cols, tgt_types, rejected_details, policy, dialect_label="ADLS",
        mappings=mappings,
    )
    from connectors.writer_common import reject_on_strict_policy

    _map_abort = reject_on_strict_policy(policy, rejected_details, "ADLS")
    if _map_abort or (errors and policy == "fail"):
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=key,
            target_schema=container,
            checksum="",
            chunks_completed=0,
            error=_map_abort or f"Transform errors: {'; '.join(errors[:3])}",
            warnings=errors[:10],
            rejected_rows=len({d.get("row") for d in rejected_details if d.get("row") is not None}),
            rejected_details=list(rejected_details),
        )

    records = mapped_rows_to_json_records(mapped_rows, target_cols, dest_types)

    if key.endswith(".csv"):
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=target_cols, extrasaction="ignore")
        writer.writeheader()
        for record in records:
            writer.writerow({k: cell_to_string(v) for k, v in record.items()})
        body = buf.getvalue().encode("utf-8")
        content_type = "text/csv"
    elif key.endswith(".jsonl"):
        body = "\n".join(json.dumps(r, default=json_default, ensure_ascii=False, allow_nan=False) for r in records).encode("utf-8")
        content_type = "application/x-ndjson"
    else:
        body = json.dumps(records, indent=2, default=json_default, ensure_ascii=False, allow_nan=False).encode("utf-8")
        content_type = "application/json"

    try:
        client = blob_service_client(cfg)
        container_client = client.get_container_client(container)
        if not container_client.exists():
            if not create_table:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=key,
                    target_schema=container,
                    checksum="",
                    chunks_completed=0,
                    error=f"ADLS container {container!r} is missing and create_table is disabled",
                )
            container_client.create_container()
        if layout.should_purge:
            from connectors.adls_reader import list_objects

            def _delete_adls(k: str) -> None:
                client.get_blob_client(container, k).delete_blob()

            purge_object_store_parts(
                list_keys=lambda prefix: list_objects(cfg, container, prefix),
                delete_key=_delete_adls,
                parts_prefix=layout.purge_prefix,
                legacy_base_key=layout.purge_legacy_key,
            )
        blob = client.get_blob_client(container, key)
        blob.upload_blob(body, overwrite=True, content_type=content_type)
        checksum = row_checksum(mapped_rows, target_cols, dest_db_type="adls")
        if on_checkpoint:
            on_checkpoint(1, 1, len(records))
        _final_abort = reject_on_strict_policy(policy, rejected_details, "ADLS")
        if _final_abort:
            return WriteResult(
                ok=False,
                rows_written=len(records),
                table_name=key,
                target_schema=container,
                checksum=checksum,
                chunks_completed=1,
                error=_final_abort,
                warnings=errors[:10],
                rejected_rows=max(
                    _rejected_row_count(data_rows, mapped_rows, rejected_details, policy),
                    len(data_rows) - len(mapped_rows),
                ),
                rejected_details=rejected_details,
            )
        return WriteResult(
            ok=True,
            rows_written=len(records),
            table_name=key,
            target_schema=container,
            checksum=checksum,
            chunks_completed=1,
            warnings=errors[:10],
            rejected_rows=max(
                _rejected_row_count(data_rows, mapped_rows, rejected_details, policy),
                len(data_rows) - len(mapped_rows),
            ),
            rejected_details=rejected_details,
            coerced_null_rows=_coerced_null_row_count(rejected_details, policy),
        )
    except Exception as exc:
        return WriteResult(
            ok=False, rows_written=0, table_name=key, target_schema=container,
            checksum="", chunks_completed=0, error=str(exc),
            rejected_details=rejected_details if "rejected_details" in locals() else [],
        )
