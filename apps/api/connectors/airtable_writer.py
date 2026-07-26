"""Airtable reverse-ETL writer — batch create/upsert records.

Uses the Airtable REST API (`POST /v0/{base}/{table}` for create,
`PATCH /v0/{base}/{table}` for upsert by record id) with per-row
idempotency keys. Auth failures fail closed; row-level API errors
are quarantined.
"""

from __future__ import annotations

import hashlib
from typing import Any, Callable

from connectors.saas_common import (
    humanize_http_error,
    is_auth_error,
    request,
    token,
)
from connectors.writer_common import (
    WriteResult,
    build_mapped_rows_with_details,
    resolve_target_columns,
    transform_error_policy,
)

DEFAULT_HOST = "api.airtable.com"
_BATCH = 10


def _batch_payload(
    rows: list[dict[str, Any]],
    *,
    table_name: str,
    base_id: str,
    update: bool,
    merge_field: str | None,
) -> tuple[str, str, dict[str, Any]]:
    """Return (url, method, payload) for an Airtable batch request."""
    url = f"https://{DEFAULT_HOST}/v0/{base_id}/{table_name}"
    if update and merge_field:
        return (
            url,
            "PATCH",
            {
                "records": [{"fields": dict(r)} for r in rows],
                "performUpsert": {"fieldsToMergeOn": [merge_field]},
            },
        )
    if update:
        records = [
            {"id": rid, "fields": {k: v for k, v in r.items() if k.lower() != "id"}}
            for r in rows
            for rid in [r.get("id") or r.get("Id")]
            if rid
        ]
        if records:
            return url, "PATCH", {"records": records}
    return url, "POST", {"records": [{"fields": dict(r)} for r in rows]}


def _id_from_response(body: Any) -> str | None:
    if isinstance(body, dict):
        return body.get("id") or body.get("Id")
    return None


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
    **_kwargs: Any,
) -> WriteResult:
    headers = headers or []
    data_rows = data_rows or []
    mappings = mappings or []
    column_types = column_types or {}

    base_id = (database or "").strip() or (connection_string or "").strip()
    table = (table_name or "").strip()
    if not base_id or not table:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table,
            target_schema=base_id,
            checksum="",
            chunks_completed=0,
            error="Airtable write requires a base id (database) and table name (table_name).",
            driver="airtable",
        )

    access_token = token(api_key, connection_string, username, password)
    if not access_token:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table,
            target_schema=base_id,
            checksum="",
            chunks_completed=0,
            error="Airtable personal access token is required.",
            driver="airtable",
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
            table_name=table,
            target_schema=base_id,
            checksum="",
            chunks_completed=0,
            error=f"Transform errors: {'; '.join(transform_errors[:3])}",
            rejected_details=rejected_details,
            driver="airtable",
        )

    mode = (write_mode or "upsert").lower()
    upsert_modes = {"upsert", "merge", "update", "overwrite"}
    update = mode in upsert_modes
    merge_field = None
    if conflict_columns:
        merge_field = str(conflict_columns[0]).strip() or None

    written = 0
    chunks = 0
    digest = hashlib.sha256()
    all_rejected = list(rejected_details)
    warnings: list[str] = []

    for i in range(0, len(mapped_rows), _BATCH):
        batch = mapped_rows[i : i + _BATCH]
        batch_dicts = [dict(zip(target_cols, row)) if not isinstance(row, dict) else dict(row) for row in batch]

        url, method, payload = _batch_payload(
            batch_dicts,
            table_name=table,
            base_id=base_id,
            update=update,
            merge_field=merge_field,
        )
        if not payload["records"]:
            for j, row in enumerate(batch_dicts):
                all_rejected.append({
                    "row": i + j + 1,
                    "column": merge_field or "id",
                    "target": table,
                    "value": "",
                    "reason": (
                        "Airtable batch produced zero records — update mode "
                        "requires an id (or merge field); refuse silent skip"
                    ),
                    "policy": "write_fail" if policy == "fail" else "write_quarantine",
                })
            if policy == "fail":
                return WriteResult(
                    ok=False,
                    rows_written=written,
                    table_name=table,
                    target_schema=base_id,
                    checksum=digest.hexdigest()[:32] if written else "",
                    chunks_completed=chunks,
                    error="Airtable write blocked: empty batch (missing record id)",
                    rejected_details=all_rejected,
                    rejected_rows=len(all_rejected),
                    driver="airtable",
                )
            continue

        key = f"dataflow-{base_id}-{table}-{i}-{hashlib.sha256(str(payload).encode()).hexdigest()[:16]}"
        try:
            resp = request(
                method=method,
                url=url,
                token=access_token,
                headers={"Idempotency-Key": key},
                data=payload,
                timeout=30,
            )
            resp.raise_for_status()
            body = resp.json()
            records = body.get("records", [body]) if isinstance(body, dict) else []
            for rec in records:
                rec_id = rec.get("id") if isinstance(rec, dict) else None
                if rec_id:
                    written += 1
                    digest.update(str(rec_id).encode())
        except Exception as exc:
            if is_auth_error(exc):
                return WriteResult(
                    ok=False,
                    rows_written=written,
                    table_name=table,
                    target_schema=base_id,
                    checksum=digest.hexdigest()[:32],
                    chunks_completed=chunks,
                    error=humanize_http_error(exc, "airtable"),
                    rejected_details=all_rejected,
                    rejected_rows=len(all_rejected),
                    warnings=warnings,
                    driver="airtable",
                )
            detail = {
                "row": i,
                "column": "",
                "target": table,
                "value": str(batch),
                "reason": humanize_http_error(exc, "airtable"),
                "policy": policy,
                "values": payload,
            }
            all_rejected.append(detail)
            if policy == "fail":
                return WriteResult(
                    ok=False,
                    rows_written=written,
                    table_name=table,
                    target_schema=base_id,
                    checksum=digest.hexdigest()[:32],
                    chunks_completed=chunks,
                    error=f"Airtable write failed for batch starting at row {i}: {detail['reason']}",
                    rejected_details=all_rejected,
                    rejected_rows=len(all_rejected),
                    warnings=warnings,
                    driver="airtable",
                )
            warnings.append(f"batch {i}: {detail['reason']}")

        chunks += 1
        if on_checkpoint:
            on_checkpoint(i + len(batch), written, 1)

    return WriteResult(
        ok=True,
        rows_written=written,
        table_name=table,
        target_schema=base_id,
        checksum=digest.hexdigest()[:32],
        chunks_completed=chunks or 1,
        rejected_details=all_rejected,
        rejected_rows=len(all_rejected),
        warnings=warnings[:20],
        driver="airtable",
    )
