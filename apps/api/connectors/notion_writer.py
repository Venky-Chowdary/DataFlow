"""Notion reverse-ETL writer — create/update pages in a Notion database.

The writer introspects the target database to discover exact property types
(title, rich_text, number, select, multi_select, status, date, checkbox, url,
email, phone_number, relation) and maps each incoming column to the correct
Notion property payload.  Read-only or unsupported property types are skipped
with a warning.  Updates require the Notion page id in the row.
"""

from __future__ import annotations

import hashlib
import re
from typing import Any, Callable

from connectors.saas_common import (
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

DEFAULT_HOST = "api.notion.com"
NOTION_VERSION = "2022-06-28"
# Notion request-limits: text.content ≤ 2000 chars per rich_text element;
# arrays ≤ 100 elements → 200_000 chars total before we must quarantine.
_NOTION_RICH_TEXT_CHUNK = 2000
_NOTION_RICH_TEXT_MAX_CHUNKS = 100
_NOTION_RICH_TEXT_TOTAL = _NOTION_RICH_TEXT_CHUNK * _NOTION_RICH_TEXT_MAX_CHUNKS


def _notion_option_names(prop: dict[str, Any]) -> list[str]:
    """Ordered select/status/multi_select option *names* from Notion schema.

    Notion page writes reference options by ``name`` (or id). Census/Hightouch
    class reverse-ETL must quarantine unknown names — never invent a new option
    unless the integration has schema write access (we refuse invent).
    """
    if not isinstance(prop, dict):
        return []
    typ = str(prop.get("type") or "").strip().lower()
    cfg = prop.get(typ) if isinstance(prop.get(typ), dict) else {}
    raw = (cfg or {}).get("options") or []
    if not isinstance(raw, list):
        return []
    names: list[str] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        if not name or name in seen:
            continue
        # Notion forbids commas in select option names.
        if "," in name:
            continue
        seen.add(name)
        names.append(name)
        if len(names) >= 256:
            break
    return names


def notion_property_to_carrier(
    notion_type: str,
    *,
    option_names: list[str] | None = None,
) -> str:
    """Map Notion property type → quarantine carrier (official request limits)."""
    from services.type_system import (
        format_enum_domain_carrier,
        format_set_domain_carrier,
    )

    t = (notion_type or "").strip().lower()
    if t in {"title", "rich_text"}:
        return f"VARCHAR({_NOTION_RICH_TEXT_TOTAL})"
    if t == "number":
        # Notion number properties are IEEE doubles — quarantine non-numeric;
        # DECIMAL→FLOAT collapse is surfaced at Map/Validate (Gate 3).
        return "FLOAT"
    if t == "checkbox":
        return "BOOLEAN"
    if t == "date":
        # Notion date may include time — TIMESTAMPTZ preserves the instant.
        return "TIMESTAMPTZ"
    if t == "url":
        return "VARCHAR(2000)"
    if t == "email":
        return "VARCHAR(200)"
    if t == "phone_number":
        return "VARCHAR(200)"
    if t in {"select", "status"}:
        if option_names:
            return format_enum_domain_carrier(option_names)
        return "VARCHAR(100)"
    if t == "multi_select":
        if option_names:
            return format_set_domain_carrier(option_names)
        return "VARCHAR(10000)"
    if t == "relation":
        return "VARCHAR(4000)"
    return "VARCHAR"


def resolve_notion_dest_types(
    target_cols: list[str],
    mappings: list[dict],
    column_types: dict[str, str],
    *,
    logical_types: list[str] | None = None,
    properties: dict[str, str] | None = None,
    property_options: dict[str, list[str]] | None = None,
) -> dict[str, str]:
    """Prefer live Notion database property types; else Map/source carriers."""
    opts = property_options or {}
    live = {
        name: notion_property_to_carrier(
            typ, option_names=opts.get(str(name).lower()) or opts.get(name)
        )
        for name, typ in (properties or {}).items()
        if name and typ
    }
    return resolve_mapping_dest_types(
        target_cols,
        mappings,
        column_types,
        logical_types=logical_types,
        live_types=live,
        default="VARCHAR",
    )


def _rich_text_chunks(text: str) -> list[dict[str, Any]]:
    """Split into ≤2000-char rich_text elements (Notion request-limits)."""
    if not text:
        return []
    chunks: list[dict[str, Any]] = []
    for i in range(0, len(text), _NOTION_RICH_TEXT_CHUNK):
        if len(chunks) >= _NOTION_RICH_TEXT_MAX_CHUNKS:
            break
        piece = text[i : i + _NOTION_RICH_TEXT_CHUNK]
        chunks.append({"type": "text", "text": {"content": piece}})
    return chunks


def _database_id(table_or_db: str) -> str:
    """Normalize a Notion database id from URL, UUID, or bare id."""
    s = (table_or_db or "").strip()
    if not s:
        return s
    # Strip query params and fragment.
    s = s.split("?")[0].split("#")[0].rstrip("/")
    # Extract last path segment if URL.
    if "/" in s:
        s = s.rsplit("/", 1)[-1]
    # Remove dashes from UUID form.
    return s.replace("-", "")


def _page_id(raw: str) -> str:
    """Normalize Notion page id; hyphenated UUIDs are accepted."""
    raw = (raw or "").strip()
    if not raw:
        return raw
    if "/" in raw:
        raw = raw.rsplit("/", 1)[-1]
    # Notion page IDs are UUIDs; hyphenated form is canonical.
    if len(raw) == 32 and re.fullmatch(r"[0-9a-fA-F]{32}", raw):
        return f"{raw[:8]}-{raw[8:12]}-{raw[12:16]}-{raw[16:20]}-{raw[20:]}"
    return raw


def _fetch_database_properties(
    database_id: str, access_token: str
) -> tuple[dict[str, str], dict[str, list[str]]]:
    """Return ``({name_lower: type}, {name_lower: option_names})`` for a database."""
    url = f"https://{DEFAULT_HOST}/v1/databases/{database_id}"
    resp = request(
        method="GET",
        url=url,
        token=access_token,
        headers={"Notion-Version": NOTION_VERSION},
        timeout=20,
    )
    resp.raise_for_status()
    body = resp.json()
    props = body.get("properties", {})
    types: dict[str, str] = {}
    options: dict[str, list[str]] = {}
    if isinstance(props, dict):
        for k, v in props.items():
            if not isinstance(v, dict):
                continue
            key = str(k).lower()
            types[key] = str(v.get("type") or "")
            names = _notion_option_names(v)
            if names:
                options[key] = names
    return types, options


def _title_property(properties: dict[str, str]) -> str | None:
    for name, typ in properties.items():
        if typ == "title":
            return name
    return None


def _as_property_value(
    value: Any,
    notion_type: str,
    property_name: str,
    warnings: list[str],
    row_idx: int,
) -> Any:
    """Map a single cell value into a Notion property object value."""
    from services.value_serializer import is_missing_sentinel

    # STOP_COLUMN / coerce_null → DF_MISSING: omit property (never write sentinel).
    if value is None or is_missing_sentinel(value):
        return None

    text = ""
    if value is not None:
        text = str(value)

    if notion_type == "title":
        chunks = _rich_text_chunks(text) or [{"type": "text", "text": {"content": ""}}]
        return {"title": chunks}
    if notion_type == "rich_text":
        return {"rich_text": _rich_text_chunks(text)}
    if notion_type == "number":
        if text == "":
            return {"number": None}
        try:
            return {"number": float(text)}
        except ValueError:
            warnings.append(f"row {row_idx}: cannot coerce '{property_name}' to number")
            return {"number": None}
    if notion_type == "url":
        return {"url": text or None}
    if notion_type == "email":
        return {"email": text or None}
    if notion_type == "phone_number":
        return {"phone_number": text or None}
    if notion_type == "checkbox":
        return {"checkbox": bool(value) and text.lower() not in {"false", "0", "", "no"}}
    if notion_type == "select":
        return {"select": {"name": text} if text else None}
    if notion_type == "status":
        return {"status": {"name": text} if text else None}
    if notion_type == "multi_select":
        # Accept CSV or semicolon (warehouse SET / HubSpot-class multi-select).
        import re as _re

        names = [
            {"name": v.strip()}
            for v in _re.split(r"[,;]", text)
            if v.strip()
        ]
        return {"multi_select": names}
    if notion_type == "date":
        if text:
            return {"date": {"start": text}}
        return {"date": None}
    if notion_type == "relation":
        ids = [_page_id(v) for v in text.split(",") if v.strip()]
        return {"relation": [{"id": i} for i in ids if i]}

    # Read-only or unsupported (formula, rollup, created_*, last_edited_*, files).
    warnings.append(f"row {row_idx}: Notion property '{property_name}' type '{notion_type}' is not writable; skipped.")
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

    database_id = _database_id(table_name or database or connection_string or "")
    if not database_id:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema="",
            checksum="",
            chunks_completed=0,
            error="Notion database id is required (table_name/database/connection_string).",
            driver="notion",
        )

    access_token = token(api_key, connection_string, username, password)
    if not access_token:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema=database_id,
            checksum="",
            chunks_completed=0,
            error="Notion integration token is required.",
            driver="notion",
        )

    try:
        properties, property_options = _fetch_database_properties(database_id, access_token)
    except Exception as exc:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema=database_id,
            checksum="",
            chunks_completed=0,
            error=f"Unable to read Notion database schema: {humanize_http_error(exc, 'notion')}",
            driver="notion",
        )

    title_name = _title_property(properties)
    target_cols, logical_types = resolve_target_columns(
        mappings, column_types, preserve_case=True
    )
    policy = transform_error_policy(error_policy)
    dest_types = resolve_notion_dest_types(
        target_cols,
        mappings,
        column_types,
        logical_types=logical_types,
        properties=properties,
        property_options=property_options,
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
        dest_kind="notion",
        destination_pk_columns=list(conflict_columns or []) or None,
    )
    tgt_types = [str(dest_types.get(c, "VARCHAR") or "VARCHAR") for c in target_cols]
    mapped_rows = apply_write_quarantine_matrix(
        mapped_rows,
        target_cols,
        tgt_types,
        rejected_details,
        policy,
        dialect_label="Notion",
        mappings=mappings,
    )
    _map_abort = reject_on_strict_policy(policy, rejected_details, 'Notion')
    if _map_abort or (transform_errors and policy == "fail"):
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema=database_id,
            checksum="",
            chunks_completed=0,
            error=_map_abort or f"Transform errors: {'; '.join(transform_errors[:3])}",
            rejected_details=rejected_details,
            driver="notion",
        )

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

        record_id = None
        if mode in upsert_modes:
            candidates = list(conflict_columns or ["id"])
            for c in candidates:
                val = row_dict.get(c)
                if val:
                    record_id = _page_id(str(val))
                    break

        notion_properties: dict[str, Any] = {}
        has_title = False
        from services.value_serializer import is_missing_sentinel

        for col, val in row_dict.items():
            prop_type = properties.get(col.lower())
            if not prop_type:
                warnings.append(f"row {i}: '{col}' does not exist in Notion database; skipped.")
                continue
            if prop_type in {"formula", "rollup", "created_by", "created_time", "last_edited_by", "last_edited_time", "files"}:
                warnings.append(f"row {i}: '{col}' is read-only in Notion; skipped.")
                continue
            prop_value = _as_property_value(val, prop_type, col, warnings, i)
            if prop_value is not None:
                notion_properties[col] = prop_value
                if prop_type == "title" and val is not None and not is_missing_sentinel(val) and str(val):
                    has_title = True

        if title_name and not has_title:
            # Notion requires a title on create; fall back to the first non-empty value.
            fallback_value = ""
            fallback_col = ""
            for col, val in row_dict.items():
                if val is None or is_missing_sentinel(val):
                    continue
                if str(val).strip():
                    fallback_value = str(val).strip()
                    fallback_col = col
                    break
            if fallback_value:
                notion_properties[title_name] = _as_property_value(fallback_value, "title", title_name, warnings, i)
                warnings.append(
                    f"row {i}: missing Notion title; used '{fallback_col}' value as fallback title."
                )
                has_title = True
            else:
                msg = f"row {i}: Notion requires a title property; no value available."
                detail = {
                    "row": i,
                    "column": title_name,
                    "target": title_name,
                    "value": "",
                    "reason": msg,
                    "policy": policy,
                    "values": dict(row_dict),
                }
                all_rejected.append(detail)
                warnings.append(msg)
                if policy == "fail":
                    return WriteResult(
                        ok=False,
                        rows_written=written,
                        table_name=table_name,
                        target_schema=database_id,
                        checksum=digest.hexdigest()[:32],
                        chunks_completed=chunks,
                        error=msg,
                        rejected_details=all_rejected,
                        rejected_rows=len(all_rejected),
                        warnings=warnings[:20],
                        driver="notion",
                    )
                continue

        if record_id:
            url = f"https://{DEFAULT_HOST}/v1/pages/{record_id}"
            method = "PATCH"
            payload: dict[str, Any] = {"properties": notion_properties}
        else:
            url = f"https://{DEFAULT_HOST}/v1/pages"
            method = "POST"
            payload = {
                "parent": {"database_id": database_id},
                "properties": notion_properties,
            }

        try:
            resp = request(
                method=method,
                url=url,
                token=access_token,
                headers={"Notion-Version": NOTION_VERSION},
                data=payload,
                timeout=30,
            )
            resp.raise_for_status()
            body = resp.json()
            rec_id = body.get("id") if isinstance(body, dict) else None
            if rec_id:
                written += 1
                digest.update(str(rec_id).encode())
                written_ids.append(str(rec_id))
        except Exception as exc:
            if is_auth_error(exc):
                return WriteResult(
                    ok=False,
                    rows_written=written,
                    table_name=table_name,
                    target_schema=database_id,
                    checksum=digest.hexdigest()[:32],
                    chunks_completed=chunks,
                    error=humanize_http_error(exc, "notion"),
                    rejected_details=all_rejected,
                    rejected_rows=len(all_rejected),
                    warnings=warnings,
                    driver="notion",
                )
            detail = {
                "row": i,
                "column": "",
                "target": table_name,
                "value": str(record_id or row_dict),
                "reason": humanize_http_error(exc, "notion"),
                "policy": policy,
                "values": payload,
            }
            all_rejected.append(detail)
            if policy == "fail":
                return WriteResult(
                    ok=False,
                    rows_written=written,
                    table_name=table_name,
                    target_schema=database_id,
                    checksum=digest.hexdigest()[:32],
                    chunks_completed=chunks,
                    error=f"Notion write failed for row {i}: {detail['reason']}",
                    rejected_details=all_rejected,
                    rejected_rows=len(all_rejected),
                    warnings=warnings,
                    driver="notion",
                )
            warnings.append(f"row {i}: {detail['reason']}")

        if on_checkpoint and (i + 1) % 100 == 0:
            on_checkpoint(i + 1, written, 1)
            chunks += 1

    if on_checkpoint:
        on_checkpoint(len(mapped_rows), written, 1)

    _final_abort = reject_on_strict_policy(policy, all_rejected, "Notion")
    if _final_abort:
        return WriteResult(
            ok=False,
            rows_written=written,
            table_name=table_name,
            target_schema=database_id,
            checksum=digest.hexdigest()[:32],
            chunks_completed=chunks or 1,
            error=_final_abort,
            rejected_details=all_rejected,
            rejected_rows=len(all_rejected),
            warnings=warnings[:20],
            driver="notion",
        )

    return WriteResult(
        ok=True,
        rows_written=written,
        table_name=table_name,
        target_schema=database_id,
        checksum=digest.hexdigest()[:32],
        chunks_completed=chunks or 1,
        rejected_details=all_rejected,
        rejected_rows=len(all_rejected),
        warnings=warnings[:20],
        driver="notion",
        meta=gate8_writer_meta(mapped_rows, target_cols, written_ids),
    )
