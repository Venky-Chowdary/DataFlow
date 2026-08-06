"""Shopify reverse-ETL writer — create/update customers, products, orders, etc.

Uses the Shopify Admin REST API with ``X-Shopify-Access-Token`` auth.
Updates require the Shopify object id in the row; creates are ``POST`` to the
resource collection. Row-level validation errors are quarantined; auth/scope
errors fail closed.
"""

from __future__ import annotations

import hashlib
from typing import Any, Callable

from connectors.saas_common import (
    base_url,
    humanize_http_error,
    is_auth_error,
    request,
    token,
)
from connectors.saas_write_carriers import shopify_live_types_for_columns
from connectors.writer_common import (
    reject_on_strict_policy,
    WriteResult,
    apply_write_quarantine_matrix,
    build_mapped_rows_with_details,
    gate8_writer_meta,
    resolve_mapping_dest_types,
    resolve_target_columns,
    transform_error_policy,
)

DEFAULT_HOST = "myshopify.com"
API_VERSION = "2024-04"


def resolve_shopify_dest_types(
    target_cols: list[str],
    mappings: list[dict],
    column_types: dict[str, str],
    *,
    logical_types: list[str] | None = None,
    object_type: str = "customers",
    metafield_defs: list[dict] | None = None,
) -> dict[str, str]:
    """Prefer Admin core + live metafield definitions; else Map/source carriers."""
    live = shopify_live_types_for_columns(
        object_type,
        target_cols,
        metafield_defs=metafield_defs,
    )
    return resolve_mapping_dest_types(
        target_cols,
        mappings,
        column_types,
        logical_types=logical_types,
        live_types=live,
        default="VARCHAR",
    )


def _singular(table: str) -> str:
    t = (table or "").strip().lower()
    if t.endswith("ies"):
        return t[:-3] + "y"
    if t.endswith("s") and len(t) > 1:
        return t[:-1]
    return t


def _make_url(host: str, table: str, record_id: str | None = None) -> str:
    shop = base_url(host, DEFAULT_HOST).rstrip("/")
    if "/" in shop.replace("://", ""):
        # base_url already gave https://host; normalize path.
        shop = shop.rstrip("/")
    base = f"{shop}/admin/api/{API_VERSION}"
    if record_id:
        return f"{base}/{table}/{record_id}.json"
    return f"{base}/{table}.json"


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

    obj = (table_name or database or "").strip()
    if not obj:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=obj,
            target_schema="",
            checksum="",
            chunks_completed=0,
            error="Shopify object/table name is required (e.g. customers, products).",
            driver="shopify",
        )

    access_token = token(api_key, connection_string, username, password)
    if not access_token:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=obj,
            target_schema="",
            checksum="",
            chunks_completed=0,
            error="Shopify access token is required.",
            driver="shopify",
        )

    shop_host = host or _kwargs.get("shop") or ""
    if not shop_host or DEFAULT_HOST not in shop_host.replace("://", ""):
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=obj,
            target_schema="",
            checksum="",
            chunks_completed=0,
            error="Shopify shop hostname is required (e.g. my-shop.myshopify.com).",
            driver="shopify",
        )

    target_cols, logical_types = resolve_target_columns(
        mappings, column_types, preserve_case=True
    )
    policy = transform_error_policy(error_policy)
    metafield_defs: list[dict] | None = None
    try:
        from connectors.shopify import describe_metafield_definitions

        metafield_defs = describe_metafield_definitions(
            {
                "host": shop_host,
                "username": username,
                "password": password,
                "connection_string": connection_string,
                "api_key": api_key,
                "database": database,
                "table": obj,
                "shop": shop_host,
            },
            obj,
        )
    except Exception:
        metafield_defs = None
    dest_types = resolve_shopify_dest_types(
        target_cols,
        mappings,
        column_types,
        logical_types=logical_types,
        object_type=obj,
        metafield_defs=metafield_defs,
    )
    mapped_rows, transform_errors, rejected_details = build_mapped_rows_with_details(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=target_cols,
        column_types=column_types,
        error_policy=policy,
        dest_types=dest_types,
        preserve_case=True,
        dest_kind="shopify",
        destination_pk_columns=list(conflict_columns or []) or None,
        destination_column_nullability=_kwargs.get("destination_column_nullability"),
    )
    tgt_types = [str(dest_types.get(c, "VARCHAR") or "VARCHAR") for c in target_cols]
    mapped_rows = apply_write_quarantine_matrix(
        mapped_rows,
        target_cols,
        tgt_types,
        rejected_details,
        policy,
        dialect_label="Shopify",
        mappings=mappings,
    )
    _map_abort = reject_on_strict_policy(policy, rejected_details, 'Shopify')
    if _map_abort or (transform_errors and policy == "fail"):
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=obj,
            target_schema=shop_host,
            checksum="",
            chunks_completed=0,
            error=_map_abort or f"Transform errors: {'; '.join(transform_errors[:3])}",
            rejected_details=rejected_details,
            driver="shopify",
        )

    mode = (write_mode or "upsert").lower()
    upsert_modes = {"upsert", "merge", "update", "overwrite", "replace"}
    singular = _singular(obj)

    written = 0
    chunks = 0
    digest = hashlib.sha256()
    written_ids: list[str] = []
    all_rejected = list(rejected_details)
    warnings: list[str] = []

    for i, row in enumerate(mapped_rows):
        if isinstance(row, dict):
            row_dict = dict(row)
        else:
            row_dict = dict(zip(target_cols, row))

        record_id = None
        if mode in upsert_modes:
            from services.value_serializer import is_missing_sentinel

            candidates = [c for c in (conflict_columns or []) if c]
            if not candidates:
                detail = {
                    "row": i + 1,
                    "column": "",
                    "target": obj,
                    "value": "",
                    "reason": (
                        "Shopify upsert requires conflict_columns/primary_key — "
                        "refuse inventing default 'id'"
                    ),
                    "policy": "write_fail" if policy == "fail" else "write_quarantine",
                }
                all_rejected.append(detail)
                warnings.append(detail["reason"])
                if policy == "fail":
                    return WriteResult(
                        ok=False,
                        rows_written=written,
                        table_name=obj,
                        target_schema=shop_host,
                        checksum=digest.hexdigest()[:32] if written else "",
                        chunks_completed=chunks,
                        error=detail["reason"],
                        rejected_details=all_rejected,
                        rejected_rows=len(all_rejected),
                        warnings=warnings,
                        driver="shopify",
                    )
                continue
            # Shopify Admin REST only updates by resource ``id`` — never take a
            # secondary conflict column (sku/handle/tenant) as the URL identity
            # when ``id`` is empty (would PUT against the wrong resource).
            id_cols = [c for c in candidates if (c or "").lower() == "id"]
            if not id_cols:
                detail = {
                    "row": i + 1,
                    "column": str(candidates[0]),
                    "target": obj,
                    "value": "",
                    "reason": (
                        "Shopify upsert requires conflict column 'id' — "
                        "refuse create invent from non-id keys "
                        "(would duplicate under at-least-once retry)"
                    ),
                    "policy": "write_fail" if policy == "fail" else "write_quarantine",
                }
                all_rejected.append(detail)
                warnings.append(detail["reason"])
                if policy == "fail":
                    return WriteResult(
                        ok=False,
                        rows_written=written,
                        table_name=obj,
                        target_schema=shop_host,
                        checksum=digest.hexdigest()[:32] if written else "",
                        chunks_completed=chunks,
                        error=detail["reason"],
                        rejected_details=all_rejected,
                        rejected_rows=len(all_rejected),
                        warnings=warnings,
                        driver="shopify",
                    )
                continue
            for c in id_cols:
                val = row_dict.get(c)
                if val is None or is_missing_sentinel(val):
                    continue
                if val:
                    record_id = str(val).strip() or None
                    break
            if not record_id:
                detail = {
                    "row": i + 1,
                    "column": str(id_cols[0]),
                    "target": obj,
                    "value": "",
                    "reason": (
                        "Shopify upsert missing id value — refuse create invent "
                        "(would duplicate under at-least-once retry)"
                    ),
                    "policy": "write_fail" if policy == "fail" else "write_quarantine",
                }
                all_rejected.append(detail)
                warnings.append(detail["reason"])
                if policy == "fail":
                    return WriteResult(
                        ok=False,
                        rows_written=written,
                        table_name=obj,
                        target_schema=shop_host,
                        checksum=digest.hexdigest()[:32] if written else "",
                        chunks_completed=chunks,
                        error=detail["reason"],
                        rejected_details=all_rejected,
                        rejected_rows=len(all_rejected),
                        warnings=warnings,
                        driver="shopify",
                    )
                continue

        update = mode in upsert_modes and bool(record_id)
        from connectors.writer_common import omit_missing_fields

        # STOP_COLUMN / coerce_null → DF_MISSING must omit, never leak the
        # sentinel string into Shopify Admin REST payloads.
        body = omit_missing_fields(
            ((k, v) for k, v in row_dict.items() if k.lower() != "id"),
            drop_empty=False,
        )
        payload = {singular: body}
        url = _make_url(shop_host, obj, record_id if update else None)
        method = "PUT" if update else "POST"

        idem_key = f"dataflow-{shop_host}-{obj}-{i}-{hashlib.sha256(str(payload).encode()).hexdigest()[:16]}"
        try:
            resp = request(
                method=method,
                url=url,
                token=access_token,
                auth_header="",
                headers={
                    "X-Shopify-Access-Token": access_token,
                    "Idempotency-Key": idem_key,
                },
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
                written_ids.append(str(rec_id))
        except Exception as exc:
            if is_auth_error(exc):
                return WriteResult(
                    ok=False,
                    rows_written=written,
                    table_name=obj,
                    target_schema=shop_host,
                    checksum=digest.hexdigest()[:32],
                    chunks_completed=chunks,
                    error=humanize_http_error(exc, "shopify"),
                    rejected_details=all_rejected,
                    rejected_rows=len(all_rejected),
                    warnings=warnings,
                    driver="shopify",
                )
            detail = {
                "row": i,
                "column": "",
                "target": obj,
                "value": str(record_id or row_dict),
                "reason": humanize_http_error(exc, "shopify"),
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
                    error=f"Shopify write failed for row {i}: {detail['reason']}",
                    rejected_details=all_rejected,
                    rejected_rows=len(all_rejected),
                    warnings=warnings,
                    driver="shopify",
                )
            warnings.append(f"row {i}: {detail['reason']}")

        if on_checkpoint and (i + 1) % 100 == 0:
            on_checkpoint(i + 1, written, 1)
            chunks += 1

    if on_checkpoint:
        on_checkpoint(len(mapped_rows), written, 1)

    _final_abort = reject_on_strict_policy(policy, all_rejected, "Shopify")
    if _final_abort:
        return WriteResult(
            ok=False,
            rows_written=written,
            table_name=obj,
            target_schema=shop_host,
            checksum=digest.hexdigest()[:32],
            chunks_completed=chunks or 1,
            error=_final_abort,
            rejected_details=all_rejected,
            rejected_rows=len(all_rejected),
            warnings=warnings[:20],
            driver="shopify",
        )

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
        driver="shopify",
        meta=gate8_writer_meta(mapped_rows, target_cols, written_ids),
    )
