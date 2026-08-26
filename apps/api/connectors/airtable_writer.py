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
    # Structured Meta payloads — JSON polarity (never invent open VARCHAR).
    if ftype in {
        "multipleAttachments",
        "multipleRecordLinks",
        "multipleCollaborators",
        "singleCollaborator",
        "createdBy",
        "lastModifiedBy",
    }:
        return "JSON"
    # Computed / opaque / unknown Admin types — refuse soft VARCHAR invent;
    # merge_saas_live_types fails closed unless Studio types the column.
    if ftype in {
        "formula",
        "rollup",
        "lookup",
        "multipleLookupValues",
        "button",
    }:
        return ""
    if not ftype:
        return ""
    return ""


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
) -> tuple[list[dict[str, Any]] | None, Exception | None]:
    """Live Meta schema when ``schema.bases:read`` is granted.

    Returns ``(fields, None)`` on success, ``(None, exc)`` on probe failure,
    or ``([], None)`` when the base is readable but the table has no fields.
    """
    url = f"https://{DEFAULT_HOST}/v0/meta/bases/{base_id}/tables"
    try:
        resp = request(method="GET", url=url, token=access_token, timeout=20)
        resp.raise_for_status()
        body = resp.json()
    except Exception as exc:
        return None, exc
    tables = body.get("tables") if isinstance(body, dict) else None
    if not isinstance(tables, list):
        return None, RuntimeError("Airtable Meta tables payload missing")
    want = (table_name or "").strip().lower()
    for t in tables:
        if not isinstance(t, dict):
            continue
        name = str(t.get("name") or "").strip()
        tid = str(t.get("id") or "").strip()
        if name.lower() == want or tid.lower() == want:
            fields = t.get("fields")
            if isinstance(fields, list):
                return list(fields), None
            return [], None
    return None, RuntimeError(
        f"Airtable table {table_name!r} not found in base Meta schema"
    )


def resolve_airtable_dest_types(
    target_cols: list[str],
    mappings: list[dict],
    column_types: dict[str, str],
    *,
    logical_types: list[str] | None = None,
    meta_fields: list[dict[str, Any]] | None = None,
    studio_types: dict[str, Any] | None = None,
) -> dict[str, str]:
    """Prefer live Meta field types; else Map/source carriers.

    When Meta fields are supplied or Studio typed carriers, never soft-fill
    unknown/computed types with Map ``VARCHAR`` — parity with the write-path
    ``merge_saas_live_types`` gate.
    """
    from connectors.saas_common import resolve_saas_live_or_map_dest_types

    live: dict[str, str] = {}
    for f in meta_fields or []:
        if not isinstance(f, dict):
            continue
        name = str(f.get("name") or "").strip()
        if name:
            live[name] = airtable_field_to_carrier(f)
    live = {k: v for k, v in live.items() if str(v or "").strip()}
    return resolve_saas_live_or_map_dest_types(
        target_cols,
        mappings,
        column_types,
        live_carriers=live,
        live_schema_present=meta_fields is not None,
        studio_types=studio_types,
        logical_types=logical_types,
        product="Airtable",
    )


def _batch_payload(
    rows: list[dict[str, Any]],
    *,
    table_name: str,
    base_id: str,
    update: bool,
    merge_field: str | None,
) -> tuple[str, str, dict[str, Any], list[int]]:
    """Return (url, method, payload, source_indices) for an Airtable batch request.

    ``source_indices[k]`` is the position in ``rows`` that produced
    ``payload["records"][k]``. The PATCH branch sends only the rows that carry a
    record id, so the two lists are not the same length, and a caller pairing
    them positionally attributes a failure to the wrong row — which is how the
    wrong ``mapped_row`` reached the DLQ and replay would have re-sent the wrong
    payload.
    """
    url = f"https://{DEFAULT_HOST}/v0/{base_id}/{table_name}"
    if update and merge_field:
        return (
            url,
            "PATCH",
            {
                "records": [{"fields": dict(r)} for r in rows],
                "performUpsert": {"fieldsToMergeOn": [merge_field]},
            },
            list(range(len(rows))),
        )
    if update:
        # PATCH only rows with Airtable record id — never fall through to POST
        # create invent for the whole batch when some/all ids are missing.
        records: list[dict[str, Any]] = []
        indices: list[int] = []
        for idx, r in enumerate(rows):
            rid = r.get("id") or r.get("Id")
            if not rid:
                continue
            records.append(
                {"id": rid, "fields": {k: v for k, v in r.items() if k.lower() != "id"}}
            )
            indices.append(idx)
        return url, "PATCH", {"records": records}, indices
    return (
        url,
        "POST",
        {"records": [{"fields": dict(r)} for r in rows]},
        list(range(len(rows))),
    )


def _id_from_response(body: Any) -> str | None:
    if isinstance(body, dict):
        return body.get("id") or body.get("Id")
    return None


def _present_fields(row: Any, target_cols: list[str]) -> dict[str, Any]:
    """Airtable cells the batch should carry — sentinels and blanks are absent.

    Airtable has no NULL: sending an empty string writes an empty cell, and
    sending DF_MISSING writes the sentinel's text. Omitting the key is the only
    way to leave a cell untouched, so the filter is a fidelity rule, not tidying.
    """
    from services.value_serializer import is_reader_null_cell

    def _present(value: Any) -> bool:
        if is_reader_null_cell(value):
            return False
        return not (isinstance(value, str) and not value.strip())

    if isinstance(row, dict):
        return {k: v for k, v in row.items() if _present(v)}
    return {
        c: row[j]
        for j, c in enumerate(target_cols)
        if j < len(row) and _present(row[j])
    }


def _quarantine_row(
    rejected: list[dict],
    *,
    batch_offset: int,
    row_index: int,
    column: str,
    table: str,
    reason: str,
    policy: str,
    values: dict[str, Any],
    mapped_row: Any,
    target_cols: list[str],
) -> None:
    """One writer-side quarantine record, numbered against the source row."""
    from connectors.writer_common import append_write_quarantine_detail

    append_write_quarantine_detail(
        rejected,
        {
            "row": batch_offset + row_index + 1,
            "column": column,
            "target": table,
            "value": "",
            "reason": reason,
            "policy": "write_fail" if policy == "fail" else "write_quarantine",
            "values": values,
        },
        mapped_row=mapped_row,
        target_cols=target_cols,
    )


def _drop_rows_missing_merge_field(
    batch_dicts: list[dict[str, Any]],
    batch: list[Any],
    *,
    merge_field: str,
    rejected: list[dict],
    batch_offset: int,
    table: str,
    policy: str,
    target_cols: list[str],
) -> tuple[list[dict[str, Any]], list[int], int]:
    """Keep only rows Airtable can match on; quarantine the rest.

    ``performUpsert`` with an empty merge value does not match — it creates a new
    record — so an unmatched row would silently become an invented insert.
    """
    kept: list[dict[str, Any]] = []
    kept_sources: list[int] = []
    dropped = 0
    for j, row in enumerate(batch_dicts):
        merge_val = row.get(merge_field)
        if merge_val is None or str(merge_val).strip() == "":
            dropped += 1
            _quarantine_row(
                rejected,
                batch_offset=batch_offset,
                row_index=j,
                column=merge_field,
                table=table,
                reason=(
                    f"Airtable upsert missing merge field {merge_field!r} — "
                    "quarantined (refuse create invent on empty merge key)"
                ),
                policy=policy,
                values=row,
                mapped_row=batch[j] if j < len(batch) else row,
                target_cols=target_cols,
            )
            continue
        kept.append(row)
        kept_sources.append(j)
    return kept, kept_sources, dropped


def _quarantine_rows_without_record_id(
    batch_dicts: list[dict[str, Any]],
    batch: list[Any],
    *,
    rejected: list[dict],
    batch_offset: int,
    table: str,
    policy: str,
    target_cols: list[str],
) -> int:
    """Update mode with no merge field can only address rows that carry an id."""
    missing = 0
    for j, row in enumerate(batch_dicts):
        if row.get("id") or row.get("Id"):
            continue
        missing += 1
        _quarantine_row(
            rejected,
            batch_offset=batch_offset,
            row_index=j,
            column="id",
            table=table,
            reason=(
                "Airtable upsert/update missing record id — quarantined "
                "(refuse create invent / silent skip)"
            ),
            policy=policy,
            values=row,
            mapped_row=batch[j] if j < len(batch) else row,
            target_cols=target_cols,
        )
    return missing


def _quarantine_empty_payload(
    batch_dicts: list[dict[str, Any]],
    batch: list[Any],
    *,
    update: bool,
    merge_field: str | None,
    rejected: list[dict],
    batch_offset: int,
    table: str,
    policy: str,
    target_cols: list[str],
) -> None:
    """A batch that produced no records must not vanish from the ledger."""
    for j, row in enumerate(batch_dicts):
        # Skip what the id / merge-field passes above already recorded.
        if update and not merge_field and not (row.get("id") or row.get("Id")):
            continue
        if update and merge_field:
            merge_val = row.get(merge_field)
            if merge_val is None or str(merge_val).strip() == "":
                continue
        _quarantine_row(
            rejected,
            batch_offset=batch_offset,
            row_index=j,
            column=merge_field or "id",
            table=table,
            reason=(
                "Airtable batch produced zero records — update mode "
                "requires an id (or merge field); refuse silent skip"
            ),
            policy=policy,
            values=row,
            mapped_row=batch[j] if j < len(batch) else row,
            target_cols=target_cols,
        )


def _quarantine_failed_request(
    payload: dict[str, Any],
    payload_sources: list[int],
    batch_dicts: list[dict[str, Any]],
    batch_sources: list[int],
    batch: list[Any],
    *,
    reason: str,
    rejected: list[dict],
    batch_offset: int,
    table: str,
    policy: str,
    target_cols: list[str],
) -> None:
    """Quarantine exactly the rows this request carried, resolved to their source.

    Walking ``batch`` positionally against ``payload["records"]`` attached the
    wrong mapped_row and row number to the failure, so the DLQ named records
    that never left and replay would have re-sent their values.
    """
    from connectors.writer_common import append_write_quarantine_detail

    recs = payload.get("records") if isinstance(payload, dict) else []
    recs = recs if isinstance(recs, list) else []
    for pos, dict_idx in enumerate(payload_sources):
        src_idx = batch_sources[dict_idx] if dict_idx < len(batch_sources) else dict_idx
        payload_rec: dict[str, Any] = {}
        if pos < len(recs) and isinstance(recs[pos], dict):
            payload_rec = dict(recs[pos].get("fields") or recs[pos])
        append_write_quarantine_detail(
            rejected,
            {
                "row": batch_offset + src_idx + 1,
                "column": "",
                "target": table,
                "value": "",
                "reason": reason,
                "policy": policy,
                "values": payload_rec
                or (batch_dicts[dict_idx] if dict_idx < len(batch_dicts) else {}),
            },
            mapped_row=batch[src_idx] if src_idx < len(batch) else {},
            target_cols=target_cols,
        )


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
    written = 0
    chunks = 0
    digest = hashlib.sha256()
    all_rejected: list[dict] = []
    warnings: list[str] = []

    def _fail(
        error: str,
        *,
        checksum: str = "",
        with_rejected: bool = False,
        count_rejected: bool = True,
        with_warnings: bool = False,
    ) -> WriteResult:
        """Every refusal reports the same ledger — rows written so far, then why."""
        extra: dict[str, Any] = {}
        if with_rejected:
            extra["rejected_details"] = all_rejected
            if count_rejected:
                extra["rejected_rows"] = len(all_rejected)
        if with_warnings:
            extra["warnings"] = warnings
        return WriteResult(
            ok=False,
            rows_written=written,
            table_name=table,
            target_schema=base_id,
            checksum=checksum,
            chunks_completed=chunks,
            error=error,
            driver="airtable",
            **extra,
        )

    if not base_id or not table:
        return _fail(
            "Airtable write requires a base id (database) and table name (table_name)."
        )

    access_token = token(api_key, connection_string, username, password)
    if not access_token:
        return _fail("Airtable personal access token is required.")

    target_cols, logical_types = resolve_target_columns(
        mappings, column_types, preserve_case=True
    )
    policy = transform_error_policy(error_policy)
    # Live Meta when PAT has schema.bases:read — typed DECIMAL/VARCHAR(n)
    # before batch create invents bad cells (HubSpot/SF Describe class).
    live_dest = _kwargs.get("destination_column_types")
    meta_fields, meta_exc = _fetch_table_fields(base_id, table, access_token)
    from connectors.saas_common import gate_saas_describe

    gate = gate_saas_describe(
        product="Airtable",
        object_name=table,
        fields=meta_fields,
        exc=meta_exc,
        target_cols=target_cols,
        studio_types=live_dest if isinstance(live_dest, dict) else None,
    )
    if not gate.ok:
        return _fail(gate.error)
    from connectors.saas_common import merge_saas_live_types

    meta_fields = gate.fields
    live: dict[str, str] = {}
    for f in meta_fields or []:
        if not isinstance(f, dict):
            continue
        name = str(f.get("name") or "").strip()
        if name:
            carrier = str(airtable_field_to_carrier(f) or "").strip()
            if carrier:
                live[name] = carrier
    dest_types, cov_err = merge_saas_live_types(
        live,
        target_cols,
        studio_types=live_dest if isinstance(live_dest, dict) else None,
        product="Airtable",
    )
    if cov_err:
        return _fail(cov_err)
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
    tgt_types = [str(dest_types.get(c) or "").strip() for c in target_cols]
    mapped_rows = apply_write_quarantine_matrix(
        mapped_rows,
        target_cols,
        tgt_types,
        rejected_details,
        policy,
        dialect_label="Airtable",
        mappings=mappings,
    )
    all_rejected = rejected_details
    _map_abort = reject_on_strict_policy(policy, rejected_details, 'Airtable', transform_errors)
    if _map_abort:
        return _fail(
            _map_abort or f"Transform errors: {'; '.join(transform_errors[:3])}",
            with_rejected=True,
            count_rejected=False,
        )

    mode = (write_mode or "upsert").lower()
    upsert_modes = {"upsert", "merge", "update", "overwrite"}
    update = mode in upsert_modes
    merge_field = None
    if conflict_columns:
        merge_field = str(conflict_columns[0]).strip() or None

    all_rejected = list(rejected_details)
    written_ids: list[str] = []
    produced_sample: list[dict[str, Any]] = []

    for i in range(0, len(mapped_rows), _BATCH):
        batch = mapped_rows[i : i + _BATCH]
        batch_dicts = [_present_fields(row, target_cols) for row in batch]
        produced_sample.extend(
            dict(d) for d in batch_dicts[: max(0, 50 - len(produced_sample))]
        )

        # Position in ``batch`` that each entry of ``batch_dicts`` came from.
        # The filters below drop rows from one list and not the other, so this is
        # what keeps a failure attributable to the row that caused it.
        batch_sources = list(range(len(batch_dicts)))

        if update and merge_field:
            batch_dicts, batch_sources, dropped = _drop_rows_missing_merge_field(
                batch_dicts,
                batch,
                merge_field=merge_field,
                rejected=all_rejected,
                batch_offset=i,
                table=table,
                policy=policy,
                target_cols=target_cols,
            )
            if dropped and policy == "fail":
                return _fail(
                    f"Airtable write blocked: upsert missing merge field {merge_field!r}",
                    checksum=digest.hexdigest()[:32] if written else "",
                    with_rejected=True,
                )

        url, method, payload, payload_sources = _batch_payload(
            batch_dicts,
            table_name=table,
            base_id=base_id,
            update=update,
            merge_field=merge_field,
        )
        if update and not merge_field:
            # Surface per-row id-less drops that PATCH filtering omitted.
            missing_ids = _quarantine_rows_without_record_id(
                batch_dicts,
                batch,
                rejected=all_rejected,
                batch_offset=i,
                table=table,
                policy=policy,
                target_cols=target_cols,
            )
            if missing_ids and policy == "fail":
                return _fail(
                    "Airtable write blocked: upsert/update missing record id",
                    checksum=digest.hexdigest()[:32] if written else "",
                    with_rejected=True,
                )
        if not payload["records"]:
            _quarantine_empty_payload(
                batch_dicts,
                batch,
                update=update,
                merge_field=merge_field,
                rejected=all_rejected,
                batch_offset=i,
                table=table,
                policy=policy,
                target_cols=target_cols,
            )
            if policy == "fail":
                return _fail(
                    "Airtable write blocked: empty batch (missing record id)",
                    checksum=digest.hexdigest()[:32] if written else "",
                    with_rejected=True,
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
            reason = humanize_http_error(exc, "airtable")
            if is_auth_error(exc):
                return _fail(
                    reason,
                    checksum=digest.hexdigest()[:32],
                    with_rejected=True,
                    with_warnings=True,
                )
            _quarantine_failed_request(
                payload,
                payload_sources,
                batch_dicts,
                batch_sources,
                batch,
                reason=reason,
                rejected=all_rejected,
                batch_offset=i,
                table=table,
                policy=policy,
                target_cols=target_cols,
            )
            if policy == "fail":
                return _fail(
                    f"Airtable write failed for batch starting at row {i}: {reason}",
                    checksum=digest.hexdigest()[:32],
                    with_rejected=True,
                    with_warnings=True,
                )
            warnings.append(f"batch {i}: {reason}")

        chunks += 1
        if on_checkpoint:
            on_checkpoint(i + len(batch), written, 1)

    _final_abort = reject_on_strict_policy(policy, all_rejected, "Airtable")
    if _final_abort:
        chunks = chunks or 1
        warnings = warnings[:20]
        return _fail(
            _final_abort,
            checksum=digest.hexdigest()[:32],
            with_rejected=True,
            with_warnings=True,
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
