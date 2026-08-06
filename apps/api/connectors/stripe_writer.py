"""Stripe reverse-ETL writer — create or update objects via the Stripe API.

Supports idempotent creates/updates using per-request ``Idempotency-Key`` headers.
Bad rows are quarantined (rejected_details) rather than dropped; auth/permission
failures fail closed.
"""

from __future__ import annotations

import hashlib
from typing import Any, Callable

from connectors.saas_common import (
    base_url,
    humanize_http_error,
    is_auth_error,
    object_name,
    request,
    token,
)
from connectors.saas_write_carriers import stripe_live_types_for_columns
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

DEFAULT_HOST = "api.stripe.com"


def resolve_stripe_dest_types(
    target_cols: list[str],
    mappings: list[dict],
    column_types: dict[str, str],
    *,
    logical_types: list[str] | None = None,
    object_type: str = "customers",
) -> dict[str, str]:
    """Prefer Stripe API-documented field limits; else Map/source carriers."""
    live = stripe_live_types_for_columns(object_type, target_cols)
    return resolve_mapping_dest_types(
        target_cols,
        mappings,
        column_types,
        logical_types=logical_types,
        live_types=live,
        default="VARCHAR",
    )


def _row_id(row: dict[str, Any], conflict_columns: list[str] | None) -> str | None:
    """Return the Stripe object id if the row provides one.

    Empty conflict_columns must not invent a default ``id`` lookup — callers
    quarantine when this returns None under upsert.
    """
    from services.value_serializer import is_missing_sentinel

    candidates = [c for c in (conflict_columns or []) if c]
    if not candidates:
        return None
    # Stripe Admin REST updates by resource ``id`` only — never take a
    # secondary conflict column (email/customer) as the URL identity.
    id_cols = [c for c in candidates if (c or "").lower() == "id"]
    if not id_cols:
        return None
    for c in id_cols:
        val = row.get(c)
        if val is None or is_missing_sentinel(val):
            continue
        if val:
            return str(val).strip() or None
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

    obj = (table_name or object_name({"table": table_name, "database": database}, "customers")).strip()
    secret_key = token(api_key, connection_string, username, password)
    if not secret_key:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=obj,
            target_schema="",
            checksum="",
            chunks_completed=0,
            error="Stripe secret key is required. Paste it in the API key field or connection string.",
            driver="stripe",
        )

    target_cols, logical_types = resolve_target_columns(
        mappings, column_types, preserve_case=True
    )
    policy = transform_error_policy(error_policy)
    # Stripe has no Describe API — use documented OpenAPI field maxima so
    # email/phone/metadata overflow quarantines before inventing bad customers.
    dest_types = resolve_stripe_dest_types(
        target_cols,
        mappings,
        column_types,
        logical_types=logical_types,
        object_type=obj,
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
        dest_kind="stripe",
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
        dialect_label="Stripe",
        mappings=mappings,
    )
    _map_abort = reject_on_strict_policy(policy, rejected_details, 'Stripe', transform_errors)
    if _map_abort:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=obj,
            target_schema="",
            checksum="",
            chunks_completed=0,
            error=_map_abort or f"Transform errors: {'; '.join(transform_errors[:3])}",
            rejected_details=rejected_details,
            driver="stripe",
        )

    base = base_url(host, DEFAULT_HOST)
    mode = (write_mode or "upsert").lower()
    upsert_modes = {"upsert", "merge", "update", "overwrite", "replace"}

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

        record_id = _row_id(row_dict, conflict_columns) if mode in upsert_modes else None
        from connectors.writer_common import omit_missing_fields

        # STOP_COLUMN / coerce_null → DF_MISSING must omit, never leak the
        # sentinel string into Stripe API payloads.
        payload = omit_missing_fields(
            ((k, v) for k, v in row_dict.items() if k not in ("id", "Id"))
        )

        if mode in upsert_modes and not record_id:
            detail = {
                "row": i + 1,
                "column": "id",
                "reason": (
                    "Stripe upsert requires conflict column 'id' with a value — "
                    "refuse inventing default id / create invent "
                    "(would duplicate customers/objects on retry)"
                ),
                "values": {k: str(v)[:80] for k, v in list(payload.items())[:8]},
                "policy": "write_fail" if policy == "fail" else "write_quarantine",
            }
            all_rejected.append(detail)
            if policy == "fail":
                return WriteResult(
                    ok=False,
                    rows_written=written,
                    table_name=obj,
                    target_schema="",
                    checksum="",
                    chunks_completed=chunks,
                    error=detail["reason"],
                    rejected_details=all_rejected,
                    warnings=warnings,
                    driver="stripe",
                )
            continue

        if record_id and mode in upsert_modes:
            url = f"{base}/v1/{obj}/{record_id}"
        else:
            url = f"{base}/v1/{obj}"

        key = f"dataflow-{obj}-{i}-{hashlib.sha256(str(payload).encode()).hexdigest()[:16]}"
        try:
            resp = request(
                method="POST",
                url=url,
                token=secret_key,
                headers={"Idempotency-Key": key},
                data=payload,
                timeout=30,
            )
            resp.raise_for_status()
            body = resp.json()
            written += 1
            rid = str(body.get("id", "") or record_id or i)
            digest.update(rid.encode())
            if rid:
                written_ids.append(rid)
            if isinstance(row, dict):
                row["id"] = body.get("id", "")
        except Exception as exc:
            if is_auth_error(exc):
                return WriteResult(
                    ok=False,
                    rows_written=written,
                    table_name=obj,
                    target_schema="",
                    checksum=digest.hexdigest()[:32],
                    chunks_completed=chunks,
                    error=humanize_http_error(exc, "stripe"),
                    rejected_details=all_rejected,
                    rejected_rows=len(all_rejected),
                    warnings=warnings,
                    driver="stripe",
                )
            detail = {
                "row": i,
                "column": "",
                "target": obj,
                "value": str(record_id or row_dict),
                "reason": humanize_http_error(exc, "stripe"),
                "policy": policy,
                "values": row_dict,
            }
            all_rejected.append(detail)
            if policy == "fail":
                return WriteResult(
                    ok=False,
                    rows_written=written,
                    table_name=obj,
                    target_schema="",
                    checksum=digest.hexdigest()[:32],
                    chunks_completed=chunks,
                    error=f"Stripe write failed for row {i}: {detail['reason']}",
                    rejected_details=all_rejected,
                    rejected_rows=len(all_rejected),
                    warnings=warnings,
                    driver="stripe",
                )
            warnings.append(f"row {i}: {detail['reason']}")

        if on_checkpoint and (i + 1) % 100 == 0:
            on_checkpoint(i + 1, written, 1)
            chunks += 1

    if on_checkpoint:
        on_checkpoint(len(mapped_rows), written, 1)

    _final_abort = reject_on_strict_policy(policy, all_rejected, "Stripe")
    if _final_abort:
        return WriteResult(
            ok=False,
            rows_written=written,
            table_name=obj,
            target_schema=base,
            checksum=digest.hexdigest()[:32],
            chunks_completed=chunks or 1,
            error=_final_abort,
            rejected_details=all_rejected,
            rejected_rows=len(all_rejected),
            warnings=warnings,
            driver="stripe",
        )

    return WriteResult(
        ok=True,
        rows_written=written,
        table_name=obj,
        target_schema=base,
        checksum=digest.hexdigest()[:32],
        chunks_completed=chunks or 1,
        rejected_details=all_rejected,
        rejected_rows=len(all_rejected),
        warnings=warnings[:20],
        driver="stripe",
        meta=gate8_writer_meta(mapped_rows, target_cols, written_ids),
    )
