"""Salesforce reverse-ETL writer — Composite/sObject Collections upsert.

Uses the REST Collections API for idempotent upserts keyed by External Id
or Id. Bad rows are quarantined (rejected_details) rather than dropped.
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

DEFAULT_HOST = "login.salesforce.com"
API_VERSION = "v58.0"
_CHUNK = 200


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
    sobject = (table_name or object_name({"table": table_name, "database": database}, "Account")).strip()
    access = token(api_key, connection_string, username, password)
    if not access:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=sobject,
            target_schema="",
            checksum="",
            chunks_completed=0,
            error="Salesforce access token is required for reverse-ETL writes",
            driver="salesforce",
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
            table_name=sobject,
            target_schema="",
            checksum="",
            chunks_completed=0,
            error=f"Transform errors: {'; '.join(transform_errors[:3])}",
            rejected_details=rejected_details,
            driver="salesforce",
        )

    ext_field = ""
    if conflict_columns:
        ext_field = str(conflict_columns[0]).strip()
    elif "Id" in target_cols:
        ext_field = "Id"
    elif "ExternalId" in target_cols:
        ext_field = "ExternalId"

    url_base = base_url(host, DEFAULT_HOST)
    written = 0
    chunks = 0
    digest = hashlib.sha256()

    try:
        for i in range(0, len(mapped_rows), chunk):
            batch = mapped_rows[i : i + chunk]
            records = []
            for row in batch:
                if isinstance(row, dict):
                    pairs = row.items()
                else:
                    pairs = zip(target_cols, row)
                rec = {k: v for k, v in pairs if v is not None and str(v) != ""}
                rec.pop("attributes", None)
                records.append(rec)
            if not records:
                continue

            if write_mode in {"upsert", "update"} and ext_field and ext_field != "Id":
                endpoint = (
                    f"{url_base}/services/data/{API_VERSION}/composite/sobjects/"
                    f"{sobject}/{ext_field}"
                )
                body = {"allOrNone": False, "records": [
                    {"attributes": {"type": sobject}, **r} for r in records
                ]}
                method = "PATCH"
            elif write_mode == "update" and ext_field == "Id":
                endpoint = f"{url_base}/services/data/{API_VERSION}/composite/sobjects"
                body = {"allOrNone": False, "records": [
                    {"attributes": {"type": sobject}, **r} for r in records
                ]}
                method = "PATCH"
            else:
                endpoint = f"{url_base}/services/data/{API_VERSION}/composite/sobjects"
                body = {"allOrNone": False, "records": [
                    {"attributes": {"type": sobject}, **r} for r in records
                ]}
                method = "POST"

            resp = request(method=method, url=endpoint, token=access, data=body, timeout=120)
            results = resp.json() if resp.content else []
            if not isinstance(results, list):
                raise RuntimeError(
                    "Salesforce returned no per-record result list — refusing to claim rows written"
                )
            if len(results) != len(records):
                raise RuntimeError(
                    f"Salesforce acknowledged {len(results)} of {len(records)} submitted records"
                )
            for idx, item in enumerate(results):
                if not isinstance(item, dict):
                    raise RuntimeError(
                        f"Salesforce returned invalid result for row {i + idx}"
                    )
                if item.get("success"):
                    written += 1
                    digest.update(str(item.get("id", idx)).encode())
                else:
                    errs = item.get("errors") or [{"message": "unknown Salesforce error"}]
                    msg = errs[0].get("message", str(errs[0])) if isinstance(errs[0], dict) else str(errs[0])
                    rejected_details.append({
                        "row_index": i + idx,
                        "reason": msg,
                        "values": records[idx] if idx < len(records) else {},
                    })

            batch_failures = sum(
                1 for item in results
                if isinstance(item, dict) and not item.get("success")
            )
            if batch_failures and policy == "fail":
                raise RuntimeError(
                    f"Salesforce rejected {batch_failures} record(s); "
                    "strict error policy blocks partial activation"
                )

            chunks += 1
            if on_checkpoint:
                on_checkpoint(written, len(mapped_rows), chunks)
    except Exception as exc:
        return WriteResult(
            ok=False,
            rows_written=written,
            table_name=sobject,
            target_schema="",
            checksum=digest.hexdigest()[:16],
            chunks_completed=chunks,
            error=humanize_http_error(exc, "salesforce"),
            rejected_details=rejected_details,
            driver="salesforce",
        )

    if rejected_details and policy == "fail":
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=sobject,
            target_schema="",
            checksum="",
            chunks_completed=0,
            error=(
                f"Salesforce rejected {len(rejected_details)} record(s); "
                "strict error policy blocks partial activation"
            ),
            rejected_details=rejected_details,
            rejected_rows=len(rejected_details),
            driver="salesforce",
        )

    return WriteResult(
        ok=True,
        rows_written=written,
        table_name=sobject,
        target_schema="",
        checksum=digest.hexdigest()[:16] if written else "",
        chunks_completed=chunks,
        rejected_details=rejected_details,
        rejected_rows=len(rejected_details),
        driver="salesforce",
    )
