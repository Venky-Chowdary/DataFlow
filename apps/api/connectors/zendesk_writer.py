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
    reject_on_strict_policy,
    WriteResult,
    apply_write_quarantine_matrix,
    build_mapped_rows_with_details,
    gate8_writer_meta,
    resolve_mapping_dest_types,
    resolve_target_columns,
    transform_error_policy,
)

DEFAULT_HOST = "zendesk.com"
# Zendesk Help Center: text/textarea ≤ 65_536; subject ≤ 255; comments ≤ 65_535.
_ZENDESK_TEXT_CHARS = 65_536
_ZENDESK_SUBJECT_CHARS = 255
_ZENDESK_COMMENT_CHARS = 65_535

# System ticket dropdowns — closed domains (Zendesk Tickets API).
_ZENDESK_STATUS_VALUES = ("new", "open", "pending", "hold", "solved", "closed")
_ZENDESK_PRIORITY_VALUES = ("urgent", "high", "normal", "low")
_ZENDESK_TYPE_VALUES = ("problem", "incident", "question", "task")


def _zendesk_custom_field_option_values(field: dict[str, Any]) -> list[str]:
    """Ordered tag *values* from ``custom_field_options`` (not display names).

    Zendesk tagger/multiselect wire is the option ``value`` tag — Census/Hightouch
    class reverse-ETL must not invent from ``name`` labels.
    """
    raw = field.get("custom_field_options") or field.get("customFieldOptions") or []
    if not isinstance(raw, list):
        return []
    values: list[str] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, dict):
            continue
        val = str(item.get("value") or "").strip()
        if not val or val in seen:
            continue
        seen.add(val)
        values.append(val)
        if len(values) >= 256:
            break
    return values


def zendesk_field_to_carrier(field: dict[str, Any]) -> str:
    """Map Zendesk ticket/user/org field type → quarantine carrier."""
    from services.type_system import (
        format_enum_domain_carrier,
        format_set_domain_carrier,
    )

    ftype = str(field.get("type") or "").strip().lower()
    # System ticket field ``type`` values double as JSON property names.
    if ftype == "subject":
        return f"VARCHAR({_ZENDESK_SUBJECT_CHARS})"
    if ftype in {"description", "comment"}:
        return f"VARCHAR({_ZENDESK_COMMENT_CHARS})"
    if ftype == "status":
        return format_enum_domain_carrier(_ZENDESK_STATUS_VALUES)
    if ftype == "priority":
        return format_enum_domain_carrier(_ZENDESK_PRIORITY_VALUES)
    if ftype in {"type", "tickettype"}:
        return format_enum_domain_carrier(_ZENDESK_TYPE_VALUES)
    if ftype in {"via", "assignee", "group"}:
        return "VARCHAR(64)"
    if ftype in {"text", "textarea", "regexp"}:
        return f"VARCHAR({_ZENDESK_TEXT_CHARS})"
    if ftype == "checkbox":
        return "BOOLEAN"
    if ftype == "date":
        return "DATE"
    if ftype == "integer":
        return "INTEGER"
    if ftype == "decimal":
        return "DECIMAL(38,10)"
    if ftype in {"tagger", "dropdown"}:
        # Single-select dropdown — closed ENUM from option tags.
        labels = _zendesk_custom_field_option_values(field)
        if labels:
            return format_enum_domain_carrier(labels)
        return "VARCHAR(255)"
    if ftype == "multiselect":
        labels = _zendesk_custom_field_option_values(field)
        if labels:
            return format_set_domain_carrier(labels)
        return "VARCHAR(255)"
    if ftype == "partialcreditcard":
        return "VARCHAR(19)"
    if ftype == "lookup":
        return "VARCHAR(64)"
    return f"VARCHAR({_ZENDESK_TEXT_CHARS})"


def resolve_zendesk_dest_types(
    target_cols: list[str],
    mappings: list[dict],
    column_types: dict[str, str],
    *,
    logical_types: list[str] | None = None,
    describe_fields: list[dict[str, Any]] | None = None,
) -> dict[str, str]:
    """Prefer live Zendesk field schema; else Map/source carriers."""
    live: dict[str, str] = {}
    for f in describe_fields or []:
        if not isinstance(f, dict):
            continue
        carrier = zendesk_field_to_carrier(f)
        for key in (f.get("name"), f.get("title"), f.get("id")):
            if key is None or key == "":
                continue
            live[str(key)] = carrier
    # Well-known system columns when Describe is partial / scoped out.
    known = {str(k).lower() for k in live}
    from services.type_system import format_enum_domain_carrier

    for col in target_cols:
        low = str(col).lower()
        if low in known:
            continue
        if low == "subject":
            live[col] = f"VARCHAR({_ZENDESK_SUBJECT_CHARS})"
        elif low in {"description", "comment", "body"}:
            live[col] = f"VARCHAR({_ZENDESK_COMMENT_CHARS})"
        elif low == "status":
            live[col] = format_enum_domain_carrier(_ZENDESK_STATUS_VALUES)
        elif low == "priority":
            live[col] = format_enum_domain_carrier(_ZENDESK_PRIORITY_VALUES)
        elif low in {"type", "ticket_type"}:
            live[col] = format_enum_domain_carrier(_ZENDESK_TYPE_VALUES)
        elif low in {"email", "name"}:
            live[col] = "VARCHAR(255)"
        known.add(low)
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

    target_cols, logical_types = resolve_target_columns(
        mappings, column_types, preserve_case=True
    )
    policy = transform_error_policy(error_policy)
    describe_fields: list[dict[str, Any]] | None = None
    try:
        from connectors.zendesk import describe_fields as zd_describe_fields

        describe_fields = zd_describe_fields(
            {
                "host": shop_host,
                "username": username,
                "password": password,
                "connection_string": connection_string,
                "api_key": api_key,
                "database": database,
                "table": obj,
            },
            obj,
        )
    except Exception:
        describe_fields = None
    dest_types = resolve_zendesk_dest_types(
        target_cols,
        mappings,
        column_types,
        logical_types=logical_types,
        describe_fields=describe_fields,
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
        dest_kind="zendesk",
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
        dialect_label="Zendesk",
        mappings=mappings,
    )
    _map_abort = reject_on_strict_policy(policy, rejected_details, 'Zendesk')
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
            driver="zendesk",
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
                        "Zendesk upsert requires conflict_columns/primary_key — "
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
                        driver="zendesk",
                    )
                continue
            # Zendesk REST updates by numeric resource id only — never take a
            # secondary conflict column when ``id`` is empty.
            id_cols = [c for c in candidates if (c or "").lower() == "id"]
            if not id_cols:
                detail = {
                    "row": i + 1,
                    "column": str(candidates[0]),
                    "target": obj,
                    "value": "",
                    "reason": (
                        "Zendesk upsert requires conflict column 'id' — "
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
                        driver="zendesk",
                    )
                continue
            for c in id_cols:
                val = row_dict.get(c)
                if val is None or is_missing_sentinel(val):
                    continue
                if val:
                    record_id = str(val).strip() or None
                    break
            # Zendesk updates require a numeric object id — never invent create.
            if not record_id or not record_id.isdigit():
                detail = {
                    "row": i + 1,
                    "column": str(id_cols[0]),
                    "target": obj,
                    "value": str(record_id or ""),
                    "reason": (
                        "Zendesk upsert missing numeric id — refuse create invent "
                        "(would duplicate tickets/users under at-least-once retry)"
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
                        driver="zendesk",
                    )
                continue

        update = mode in upsert_modes and bool(record_id)
        from connectors.writer_common import omit_missing_fields

        # STOP_COLUMN / coerce_null → DF_MISSING must omit, never leak the
        # sentinel string into Zendesk API payloads.
        body = omit_missing_fields(
            ((k, v) for k, v in row_dict.items() if k.lower() != "id"),
            drop_empty=False,
        )
        payload = {singular: body}
        url = _make_url(shop_host, obj, record_id if update else None)
        method = "PUT" if update else "POST"

        try:
            resp = request(
                method=method,
                url=url,
                token=access_token,
                auth_scheme=scheme,
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

    _final_abort = reject_on_strict_policy(policy, all_rejected, "Zendesk")
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
            warnings=warnings,
            driver="zendesk",
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
        driver="zendesk",
        meta=gate8_writer_meta(mapped_rows, target_cols, written_ids),
    )
