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
    reject_on_strict_policy,
    WriteResult,
    apply_write_quarantine_matrix,
    build_mapped_rows_with_details,
    resolve_mapping_dest_types,
    resolve_target_columns,
    transform_error_policy,
)

DEFAULT_HOST = "api.airtable.com"
_BATCH = 10
# Airtable cell values are capped at 100_000 characters (platform docs /
# community-confirmed). Bound VARCHAR carriers so quarantine catches overflow
# before the REST API returns a late 422.
_AIRTABLE_CELL_CHARS = 100_000


def airtable_field_to_carrier(field: dict[str, Any]) -> str:
    """Map Airtable Meta field schema → Datawrap quarantine carrier.

    Number/percent precision is 0–8; currency 0–7 (Airtable field-model).
    We emit DECIMAL(38,s) so excess scale / non-numeric values quarantine
    instead of round-tripping as silent IEEE drift.

    singleSelect / multipleSelects keep closed ENUM/SET domains from
    ``options.choices[].name`` (Census/Hightouch class — never invent open text).
    """
    from services.type_system import (
        format_enum_domain_carrier,
        format_set_domain_carrier,
    )

    ftype = str(field.get("type") or "").strip()
    options = field.get("options") if isinstance(field.get("options"), dict) else {}
    if ftype == "singleLineText":
        return f"VARCHAR({_AIRTABLE_CELL_CHARS})"
    if ftype in {"multilineText", "richText", "aiText"}:
        return f"VARCHAR({_AIRTABLE_CELL_CHARS})"
    if ftype == "email":
        return "VARCHAR(254)"
    if ftype == "url":
        return "VARCHAR(2048)"
    if ftype == "phoneNumber":
        return "VARCHAR(64)"
    if ftype in {"number", "percent"}:
        prec = options.get("precision")
        if prec is not None:
            try:
                return f"DECIMAL(38,{max(0, min(8, int(prec)))})"
            except (TypeError, ValueError):
                pass
        return "FLOAT"
    if ftype == "currency":
        prec = options.get("precision", 2)
        try:
            return f"DECIMAL(38,{max(0, min(7, int(prec)))})"
        except (TypeError, ValueError):
            return "DECIMAL(38,2)"
    if ftype == "checkbox":
        return "BOOLEAN"
    if ftype == "date":
        return "DATE"
    if ftype == "dateTime":
        # Airtable dateTime includes timezone option — UTC instant polarity.
        return "TIMESTAMPTZ"
    if ftype in {"createdTime", "lastModifiedTime"}:
        # Airbyte maps these to timestamp_with_timezone (RFC3339 Z).
        return "TIMESTAMPTZ"
    if ftype in {"duration", "rating", "autoNumber", "count"}:
        return "INTEGER"
    if ftype in {"singleSelect", "externalSyncSource"}:
        labels = _airtable_choice_names(options)
        if labels:
            return format_enum_domain_carrier(labels)
        return "VARCHAR(256)"
    if ftype == "multipleSelects":
        labels = _airtable_choice_names(options)
        if labels:
            return format_set_domain_carrier(labels)
        return f"VARCHAR({_AIRTABLE_CELL_CHARS})"
    if ftype == "barcode":
        return "VARCHAR(512)"
    # Attachments / collaborators / links / formula / rollup / lookup — wire
    # as unbounded text; writer still sends JSON cell shapes when present.
    return "VARCHAR"


def _airtable_choice_names(options: dict[str, Any]) -> list[str]:
    raw = options.get("choices") or []
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
        seen.add(name)
        names.append(name)
        if len(names) >= 256:
            break
    return names


def _fetch_table_fields(
    base_id: str,
    table_name: str,
    access_token: str,
) -> list[dict[str, Any]] | None:
    """Live Meta schema when ``schema.bases:read`` is granted; else None."""
    url = f"https://{DEFAULT_HOST}/v0/meta/bases/{base_id}/tables"
    try:
        resp = request(method="GET", url=url, token=access_token, timeout=20)
        resp.raise_for_status()
        body = resp.json()
    except Exception:
        return None
    tables = body.get("tables") if isinstance(body, dict) else None
    if not isinstance(tables, list):
        return None
    want = (table_name or "").strip().lower()
    for t in tables:
        if not isinstance(t, dict):
            continue
        name = str(t.get("name") or "").strip()
        tid = str(t.get("id") or "").strip()
        if name.lower() == want or tid.lower() == want:
            fields = t.get("fields")
            return list(fields) if isinstance(fields, list) else None
    return None


def resolve_airtable_dest_types(
    target_cols: list[str],
    mappings: list[dict],
    column_types: dict[str, str],
    *,
    logical_types: list[str] | None = None,
    meta_fields: list[dict[str, Any]] | None = None,
) -> dict[str, str]:
    """Prefer live Meta field types; else Map/source carriers."""
    live: dict[str, str] = {}
    for f in meta_fields or []:
        if not isinstance(f, dict):
            continue
        name = str(f.get("name") or "").strip()
        if name:
            live[name] = airtable_field_to_carrier(f)
    return resolve_mapping_dest_types(
        target_cols,
        mappings,
        column_types,
        logical_types=logical_types,
        live_types=live,
        default="VARCHAR",
    )


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
        # PATCH only rows with Airtable record id — never fall through to POST
        # create invent for the whole batch when some/all ids are missing.
        records = [
            {"id": rid, "fields": {k: v for k, v in r.items() if k.lower() != "id"}}
            for r in rows
            for rid in [r.get("id") or r.get("Id")]
            if rid
        ]
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

    target_cols, logical_types = resolve_target_columns(
        mappings, column_types, preserve_case=True
    )
    policy = transform_error_policy(error_policy)
    # Live Meta when PAT has schema.bases:read — typed DECIMAL/VARCHAR(n)
    # before batch create invents bad cells (Airbyte/Fivetran class honesty).
    meta_fields = _fetch_table_fields(base_id, table, access_token)
    dest_types = resolve_airtable_dest_types(
        target_cols,
        mappings,
        column_types,
        logical_types=logical_types,
        meta_fields=meta_fields,
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
        dest_kind="airtable",
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
        dialect_label="Airtable",
        mappings=mappings,
    )
    _map_abort = reject_on_strict_policy(policy, rejected_details, 'Airtable', transform_errors)
    if _map_abort:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table,
            target_schema=base_id,
            checksum="",
            chunks_completed=0,
            error=_map_abort or f"Transform errors: {'; '.join(transform_errors[:3])}",
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
    written_ids: list[str] = []
    produced_sample: list[dict[str, Any]] = []
    from services.value_serializer import is_missing_sentinel

    for i in range(0, len(mapped_rows), _BATCH):
        batch = mapped_rows[i : i + _BATCH]
        batch_dicts = []
        for row in batch:
            if isinstance(row, dict):
                batch_dicts.append(
                    {k: v for k, v in row.items() if not is_missing_sentinel(v)}
                )
            else:
                batch_dicts.append(
                    {
                        c: row[j]
                        for j, c in enumerate(target_cols)
                        if j < len(row) and not is_missing_sentinel(row[j])
                    }
                )
        for d in batch_dicts:
            if len(produced_sample) < 50:
                produced_sample.append(dict(d))

        if update and merge_field:
            # performUpsert with empty merge value invents creates — quarantine.
            kept: list[dict[str, Any]] = []
            dropped = 0
            for j, row in enumerate(batch_dicts):
                merge_val = row.get(merge_field)
                if merge_val is None or str(merge_val).strip() == "":
                    dropped += 1
                    all_rejected.append({
                        "row": i + j + 1,
                        "column": merge_field,
                        "target": table,
                        "value": "",
                        "reason": (
                            f"Airtable upsert missing merge field {merge_field!r} — "
                            "quarantined (refuse create invent on empty merge key)"
                        ),
                        "policy": "write_fail" if policy == "fail" else "write_quarantine",
                    })
                    continue
                kept.append(row)
            if dropped and policy == "fail":
                return WriteResult(
                    ok=False,
                    rows_written=written,
                    table_name=table,
                    target_schema=base_id,
                    checksum=digest.hexdigest()[:32] if written else "",
                    chunks_completed=chunks,
                    error=f"Airtable write blocked: upsert missing merge field {merge_field!r}",
                    rejected_details=all_rejected,
                    rejected_rows=len(all_rejected),
                    driver="airtable",
                )
            batch_dicts = kept

        url, method, payload = _batch_payload(
            batch_dicts,
            table_name=table,
            base_id=base_id,
            update=update,
            merge_field=merge_field,
        )
        if update and not merge_field:
            # Surface per-row id-less drops that PATCH filtering omitted.
            missing_ids = []
            for j, row in enumerate(batch_dicts):
                if not (row.get("id") or row.get("Id")):
                    missing_ids.append(j)
                    all_rejected.append({
                        "row": i + j + 1,
                        "column": "id",
                        "target": table,
                        "value": "",
                        "reason": (
                            "Airtable upsert/update missing record id — quarantined "
                            "(refuse create invent / silent skip)"
                        ),
                        "policy": "write_fail" if policy == "fail" else "write_quarantine",
                    })
            if missing_ids and policy == "fail":
                return WriteResult(
                    ok=False,
                    rows_written=written,
                    table_name=table,
                    target_schema=base_id,
                    checksum=digest.hexdigest()[:32] if written else "",
                    chunks_completed=chunks,
                    error="Airtable write blocked: upsert/update missing record id",
                    rejected_details=all_rejected,
                    rejected_rows=len(all_rejected),
                    driver="airtable",
                )
        if not payload["records"]:
            for j, row in enumerate(batch_dicts):
                # Avoid duplicate quarantine when already recorded above.
                if update and not merge_field and not (row.get("id") or row.get("Id")):
                    continue
                if update and merge_field:
                    merge_val = row.get(merge_field)
                    if merge_val is None or str(merge_val).strip() == "":
                        continue
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
                    written_ids.append(str(rec_id))
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

    _final_abort = reject_on_strict_policy(policy, all_rejected, "Airtable")
    if _final_abort:
        return WriteResult(
            ok=False,
            rows_written=written,
            table_name=table,
            target_schema=base_id,
            checksum=digest.hexdigest()[:32],
            chunks_completed=chunks or 1,
            error=_final_abort,
            rejected_details=all_rejected,
            rejected_rows=len(all_rejected),
            warnings=warnings[:20],
            driver="airtable",
        )

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
        meta={
            "reconcile_sample": produced_sample,
            "source_row_count": len(mapped_rows),
            "written_ids": written_ids[:500],
        },
    )
