"""Zendesk reverse-ETL writer — create/update tickets, users, organizations, etc.

Uses the Zendesk Support API with Basic auth (email/token:api_token) or Bearer
(OAuth).  Updates require the Zendesk object id in the row; creates are ``POST``
to the resource collection.  Row-level validation errors are quarantined;
auth/scope errors fail closed.
"""

from __future__ import annotations

import base64
import hashlib
from typing import Any, Callable

from connectors.saas_common import (
    base_url,
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

DEFAULT_HOST = "zendesk.com"


def _singular(table: str) -> str:
    t = (table or "").strip().lower()
    if t.endswith("ies"):
        return t[:-3] + "y"
    if t.endswith("s") and len(t) > 1:
        return t[:-1]
    return t


def _make_url(host: str, table: str, record_id: str | None = None) -> str:
    root = base_url(host, DEFAULT_HOST).rstrip("/")
    base = f"{root}/api/v2/{table}"
    if record_id:
        return f"{base}/{record_id}.json"
    return f"{base}.json"


def _make_auth_token(  # nosec B107
    api_key: str = "",
    connection_string: str = "",
    username: str = "",
    password: str = "",
) -> tuple[str, str]:
    """Return (scheme, token_or_credentials) for request()."""
    cred = token(api_key, connection_string, username, password)
    if not cred:
        return ("", "")
    if ":" in cred:
        return ("Basic", base64.b64encode(cred.encode("utf-8")).decode("ascii"))
    return ("Bearer", cred)


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

    obj = (table_name or database or "").strip().lower()
    if not obj:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=obj,
            target_schema="",
            checksum="",
            chunks_completed=0,
            error="Zendesk object/table name is required (e.g. tickets, users).",
            driver="zendesk",
        )

    scheme, access_token = _make_auth_token(api_key, connection_string, username, password)
    if not access_token:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=obj,
            target_schema="",
            checksum="",
            chunks_completed=0,
            error="Zendesk credentials are required (email:token for Basic auth, or OAuth bearer token).",
            driver="zendesk",
        )

    shop_host = host or _kwargs.get("subdomain") or ""
    if not shop_host or ".zendesk.com" not in shop_host.replace("://", ""):
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=obj,
            target_schema="",
            checksum="",
            chunks_completed=0,
            error="Zendesk subdomain host is required (e.g. https://mycompany.zendesk.com).",
            driver="zendesk",
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
            target_schema=shop_host,
            checksum="",
            chunks_completed=0,
            error=f"Transform errors: {'; '.join(transform_errors[:3])}",
            rejected_details=rejected_details,
            driver="zendesk",
        )

    mode = (write_mode or "upsert").lower()
    upsert_modes = {"upsert", "merge", "update", "overwrite", "replace"}
    singular = _singular(obj)

    written = 0
    chunks = 0
    digest = hashlib.sha256()
    all_rejected = list(rejected_details)
    warnings: list[str] = []

    for i, row in enumerate(mapped_rows):
        if isinstance(row, dict):
            row_dict = dict(row)
        else:
            row_dict = dict(zip(target_cols, row))

        record_id = None
        if mode in upsert_modes:
            candidates = list(conflict_columns or ["id"])
            for c in candidates:
                val = row_dict.get(c)
                if val:
                    record_id = str(val).strip() or None
                    break
            # Zendesk updates require a numeric object id; otherwise create.
            if record_id and not record_id.isdigit():
                warnings.append(f"row {i}: Zendesk update id must be numeric; creating instead.")
                record_id = None

        update = mode in upsert_modes and bool(record_id)
        payload = {
            singular: {k: v for k, v in row_dict.items() if k.lower() != "id"}
        }
        url = _make_url(shop_host, obj, record_id if update else None)
        method = "PUT" if update else "POST"

        idem_key = f"dataflow-{shop_host}-{obj}-{i}-{hashlib.sha256(str(payload).encode()).hexdigest()[:16]}"
        try:
            resp = request(
                method=method,
                url=url,
                token=access_token,
                auth_scheme=scheme,
                headers={"Idempotency-Key": idem_key},
                data=payload,
                timeout=30,
            )
            resp.raise_for_status()
            body = resp.json()
            rec = body.get(singular) if isinstance(body, dict) else body
            rec_id = rec.get("id") if isinstance(rec, dict) else None
            if rec_id:
                written += 1
                digest.update(str(rec_id).encode())
        except Exception as exc:
            if is_auth_error(exc):
                return WriteResult(
                    ok=False,
                    rows_written=written,
                    table_name=obj,
                    target_schema=shop_host,
                    checksum=digest.hexdigest()[:32],
                    chunks_completed=chunks,
                    error=humanize_http_error(exc, "zendesk"),
                    rejected_details=all_rejected,
                    rejected_rows=len(all_rejected),
                    warnings=warnings,
                    driver="zendesk",
                )
            detail = {
                "row": i,
                "column": "",
                "target": obj,
                "value": str(record_id or row_dict),
                "reason": humanize_http_error(exc, "zendesk"),
                "policy": policy,
                "values": payload,
            }
            all_rejected.append(detail)
            if policy == "fail":
                return WriteResult(
                    ok=False,
                    rows_written=written,
                    table_name=obj,
                    target_schema=shop_host,
                    checksum=digest.hexdigest()[:32],
                    chunks_completed=chunks,
                    error=f"Zendesk write failed for row {i}: {detail['reason']}",
                    rejected_details=all_rejected,
                    rejected_rows=len(all_rejected),
                    warnings=warnings,
                    driver="zendesk",
                )
            warnings.append(f"row {i}: {detail['reason']}")

        if on_checkpoint and (i + 1) % 100 == 0:
            on_checkpoint(i + 1, written, 1)
            chunks += 1

    if on_checkpoint:
        on_checkpoint(len(mapped_rows), written, 1)

    return WriteResult(
        ok=True,
        rows_written=written,
        table_name=obj,
        target_schema=shop_host,
        checksum=digest.hexdigest()[:32],
        chunks_completed=chunks or 1,
        rejected_details=all_rejected,
        rejected_rows=len(all_rejected),
        warnings=warnings[:20],
        driver="zendesk",
    )
