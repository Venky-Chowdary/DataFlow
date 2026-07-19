"""HubSpot reverse-ETL writer — CRM batch upsert by idProperty.

Uses ``/crm/v3/objects/{object}/batch/upsert`` for idempotent activation.
Failed records are quarantined via rejected_details.
"""

from __future__ import annotations

import hashlib
from typing import Any, Callable

from connectors.saas_common import (
    base_url,
    humanize_http_error,
    object_name,
    request,
    token,
)
from connectors.writer_common import (
    WriteResult,
    build_mapped_rows_with_details,
    resolve_target_columns,
    transform_error_policy,
)

DEFAULT_HOST = "api.hubapi.com"
_CHUNK = 100


def write_mapped_rows(
    *,
    host: str = "",
    port: int = 0,
    database: str = "",
    username: str = "",
    password: str = "",
    schema: str = "",
    connection_string: str = "",
    ssl: bool = True,
    table_name: str = "",
    headers: list[str] | None = None,
    data_rows: list[list[str]] | None = None,
    mappings: list[dict] | None = None,
    column_types: dict[str, str] | None = None,
    on_checkpoint: Callable[..., None] | None = None,
    create_table: bool = False,
    error_policy: str | None = None,
    write_mode: str = "upsert",
    conflict_columns: list[str] | None = None,
    api_key: str = "",
    batch_size: int | None = None,
    **_kwargs: Any,
) -> WriteResult:
    headers = headers or []
    data_rows = data_rows or []
    mappings = mappings or []
    column_types = column_types or {}
    chunk = max(1, min(int(batch_size or _kwargs.get("activation_batch_size") or _CHUNK), _CHUNK))
    obj = (table_name or object_name({"table": table_name, "database": database}, "contacts")).strip()
    access = token(api_key, connection_string, username, password)
    if not access:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=obj,
            target_schema="",
            checksum="",
            chunks_completed=0,
            error="HubSpot private app token is required for reverse-ETL writes",
            driver="hubspot",
        )

    target_cols, _ = resolve_target_columns(mappings, column_types, preserve_case=True)
    policy = transform_error_policy(error_policy)
    mapped_rows, transform_errors, rejected_details = build_mapped_rows_with_details(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=target_cols,
        column_types=column_types,
        error_policy=policy,
        dest_types={c: "string" for c in target_cols},
        preserve_case=True,
    )
    if transform_errors and policy == "fail":
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=obj,
            target_schema="",
            checksum="",
            chunks_completed=0,
            error=f"Transform errors: {'; '.join(transform_errors[:3])}",
            rejected_details=rejected_details,
            driver="hubspot",
        )

    id_property = "email"
    if conflict_columns:
        id_property = str(conflict_columns[0]).strip() or "email"
    elif "hs_object_id" in target_cols or "id" in target_cols:
        id_property = "hs_object_id" if "hs_object_id" in target_cols else "id"

    url = f"{base_url(host, DEFAULT_HOST)}/crm/v3/objects/{obj}/batch/upsert"
    written = 0
    chunks = 0
    digest = hashlib.sha256()

    try:
        for i in range(0, len(mapped_rows), chunk):
            batch = mapped_rows[i : i + chunk]
            inputs = []
            for row in batch:
                if isinstance(row, dict):
                    pairs = row.items()
                else:
                    pairs = zip(target_cols, row)
                props = {k: str(v) for k, v in pairs if v is not None and str(v) != ""}
                id_val = props.pop(id_property, None) or props.get("email") or props.get("id")
                if not id_val:
                    rejected_details.append({
                        "row_index": i + len(inputs),
                        "reason": f"Missing idProperty '{id_property}' for HubSpot upsert",
                        "values": props,
                    })
                    continue
                inputs.append({
                    "idProperty": id_property,
                    "id": str(id_val),
                    "properties": props,
                })
            if not inputs:
                continue

            # HubSpot batch upsert; create path for insert-only mode
            endpoint = url if write_mode != "insert" else (
                f"{base_url(host, DEFAULT_HOST)}/crm/v3/objects/{obj}/batch/create"
            )
            body = {"inputs": inputs} if write_mode != "insert" else {
                "inputs": [{"properties": inp["properties"]} for inp in inputs]
            }
            resp = request(method="POST", url=endpoint, token=access, data=body, timeout=120)
            data = resp.json() if resp.content else {}
            results = data.get("results") or []
            errors = data.get("errors") or []
            written += len(results)
            for r in results:
                digest.update(str(r.get("id", "")).encode())
            for err in errors:
                rejected_details.append({
                    "row_index": i,
                    "reason": str(err.get("message") or err),
                    "values": err.get("context") or {},
                })
            chunks += 1
            if on_checkpoint:
                on_checkpoint(written, len(mapped_rows), chunks)
    except Exception as exc:
        return WriteResult(
            ok=False,
            rows_written=written,
            table_name=obj,
            target_schema="",
            checksum=digest.hexdigest()[:16],
            chunks_completed=chunks,
            error=humanize_http_error(exc, "hubspot"),
            rejected_details=rejected_details,
            driver="hubspot",
        )

    return WriteResult(
        ok=True,
        rows_written=written,
        table_name=obj,
        target_schema="",
        checksum=digest.hexdigest()[:16] if written else "",
        chunks_completed=chunks,
        rejected_details=rejected_details,
        rejected_rows=len(rejected_details),
        driver="hubspot",
    )
