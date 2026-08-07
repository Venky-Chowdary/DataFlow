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
    reject_on_strict_policy,
    WriteResult,
    apply_write_quarantine_matrix,
    build_mapped_rows_with_details,
    gate8_writer_meta,
    resolve_target_columns,
    transform_error_policy,
)

DEFAULT_HOST = "login.salesforce.com"
API_VERSION = "v58.0"
_CHUNK = 200
_SF_ID_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ012345"


def _sf_field_api_name(name: str) -> str:
    """Keep Salesforce API names including custom ``__c`` / ``__r`` suffixes.

    Generic SQL ``sanitize_identifier`` collapses ``__`` → ``_``, which invents
    wrong field API names and breaks External Id conflict columns on write.
    """
    import re

    raw = (name or "").strip()
    if not raw:
        return raw
    # Allow only Salesforce-legal identifier chars; never collapse underscores.
    s = re.sub(r"[^a-zA-Z0-9_]", "_", raw)
    if not s or s[0].isdigit():
        s = f"f_{s or 'field'}"
    return s[:255]


def _require_instance_url(host: str) -> str:
    """Refuse login/test hosts — Composite APIs need the org instance URL."""
    host_l = (host or "").strip().lower()
    if not host_l or "login.salesforce.com" in host_l or "test.salesforce.com" in host_l:
        raise ValueError(
            "Salesforce Host must be the org instance URL "
            "(https://yourorg.my.salesforce.com), not login.salesforce.com"
        )
    return base_url(host, DEFAULT_HOST)


def coerce_salesforce_id_wire(value: Any) -> str | None:
    """Normalize Salesforce Id / reference to 18-char case-safe form.

    Research: CASESAFEID — 15-char case-sensitive IDs append a 3-char checksum
    encoding uppercase positions (Informatica / Data Loader class). Refuse
    invent for lengths other than 15/18 or non-base62 payloads.
    """
    if value is None:
        return None
    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        return value
    if isinstance(value, (bytes, bytearray, memoryview, dict, list, tuple, bool, int, float)):
        raise ValueError(
            f"Salesforce Id cannot bind {type(value).__name__} — refuse invent"
        )
    text = str(value).strip()
    if not text:
        raise ValueError(
            "empty Salesforce Id — refuse silent NULL invent "
            "(quarantine or omit lookup upstream)"
        )
    if len(text) == 18:
        # Validate checksum matches 15-char body when body is well-formed.
        body = text[:15]
        if not _looks_like_salesforce_id_body(body):
            raise ValueError(
                f"Salesforce Id {text!r} is not a valid 18-char Id — refuse invent"
            )
        expected = _salesforce_id_checksum_suffix(body)
        if text[15:] != expected:
            raise ValueError(
                f"Salesforce Id checksum mismatch — refuse invent: {text!r}"
            )
        return text
    if len(text) == 15:
        if not _looks_like_salesforce_id_body(text):
            raise ValueError(
                f"Salesforce Id {text!r} is not a valid 15-char Id — refuse invent"
            )
        return text + _salesforce_id_checksum_suffix(text)
    raise ValueError(
        f"Salesforce Id length {len(text)} not in {{15,18}} — refuse invent"
    )


def _looks_like_salesforce_id_body(text: str) -> bool:
    if len(text) != 15:
        return False
    return all(
        ("0" <= c <= "9") or ("A" <= c <= "Z") or ("a" <= c <= "z") for c in text
    )


def _salesforce_id_checksum_suffix(body15: str) -> str:
    """Append CASESAFEID checksum for a 15-char Salesforce Id body."""
    out: list[str] = []
    for i in range(3):
        flags = 0
        for j in range(5):
            ch = body15[i * 5 + j]
            if "A" <= ch <= "Z":
                flags |= 1 << j
        out.append(_SF_ID_ALPHABET[flags])
    return "".join(out)


def _salesforce_picklist_labels(field: dict[str, Any]) -> list[str]:
    """Ordered active picklist values from Describe (value preferred over label)."""
    raw = field.get("picklistValues") or field.get("picklist_values") or []
    if not isinstance(raw, list):
        return []
    labels: list[str] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, dict):
            continue
        if item.get("active") is False:
            continue
        lab = str(item.get("value") or item.get("label") or "").strip()
        if not lab or lab in seen:
            continue
        seen.add(lab)
        labels.append(lab)
        # Bound domain size — huge org picklists stay VARCHAR + length guard.
        if len(labels) >= 256:
            break
    return labels


def salesforce_field_to_carrier(field: dict[str, Any]) -> str:
    """Map Salesforce Describe field metadata to a bounded logical carrier.

    Bulk API fails (STRING_TOO_LONG) when values exceed length — never truncate
    silently (Data Loader / SOAP AllowFieldTruncation honesty).

    Picklist / multipicklist keep closed ENUM/SET domains when Describe supplies
    ``picklistValues`` (Informatica / MuleSoft class reverse-ETL fidelity).
    Id / reference stay ``VARCHAR(18)`` (Salesforce 15/18-char ID contract).
    """
    from services.type_system import (
        format_enum_domain_carrier,
        format_set_domain_carrier,
    )

    ftype = str(field.get("type") or "string").strip().lower()
    length = field.get("length")
    precision = field.get("precision")
    scale = field.get("scale")
    try:
        length_n = int(length) if length is not None else None
    except (TypeError, ValueError):
        length_n = None
    try:
        prec_n = int(precision) if precision is not None else None
    except (TypeError, ValueError):
        prec_n = None
    try:
        scale_n = int(scale) if scale is not None else 0
    except (TypeError, ValueError):
        scale_n = 0

    # Salesforce Id / Lookup / Master-Detail — fixed 15/18-char case-safe IDs.
    if ftype in {"id", "reference", "masterrecord"}:
        return "VARCHAR(18)"

    if ftype == "picklist":
        labels = _salesforce_picklist_labels(field)
        if labels:
            return format_enum_domain_carrier(labels)
        if length_n and length_n > 0:
            return f"VARCHAR({length_n})"
        return "VARCHAR"

    if ftype == "multipicklist":
        # SOAP multipicklist is semicolon-delimited; SET domain when values known.
        labels = _salesforce_picklist_labels(field)
        if labels:
            return format_set_domain_carrier(labels)
        if length_n and length_n > 0:
            return f"VARCHAR({length_n})"
        return "VARCHAR"

    if ftype in {
        "string",
        "textarea",
        "phone",
        "email",
        "url",
        "encryptedstring",
        "combobox",
        "datacategorygroupreference",
        "junctionidlist",
    }:
        if length_n and length_n > 0:
            return f"VARCHAR({length_n})"
        return "VARCHAR"

    # Compound address / geolocation / polymorphic anyType — structured envelope.
    if ftype in {"address", "location", "complexvalue", "anytype"}:
        return "JSON"

    if ftype in {"double", "currency", "percent", "number"}:
        if prec_n and prec_n > 0:
            return f"DECIMAL({prec_n},{max(0, scale_n)})"
        return "DECIMAL"
    if ftype == "int":
        return "INTEGER"
    if ftype == "long":
        return "BIGINT"
    if ftype == "boolean":
        return "BOOLEAN"
    if ftype == "date":
        return "DATE"
    if ftype == "datetime":
        # Salesforce Datetime is a UTC instant (SOAP/Bulk) — TIMESTAMPTZ polarity
        # matches Airbyte timestamp_with_timezone, not naive DATETIME invent.
        return "TIMESTAMPTZ"
    if ftype == "time":
        return "TIME"
    if ftype == "base64":
        return "BINARY"
    # Unknown SOAP type — refuse soft VARCHAR invent (Studio / remap required).
    return ""


def resolve_salesforce_dest_types(
    target_cols: list[str],
    mappings: list[dict],
    column_types: dict[str, str],
    *,
    describe_fields: list[dict[str, Any]] | None = None,
) -> dict[str, str]:
    """Prefer live Describe length/precision; else Map/source carriers.

    When Describe fields are supplied, never soft-fill unknown SOAP types with
    Map ``VARCHAR`` — parity with the write-path ``merge_saas_live_types`` gate.
    """
    live: dict[str, str] = {}
    for f in describe_fields or []:
        name = str(f.get("name") or "").strip()
        if not name:
            continue
        carrier = salesforce_field_to_carrier(f)
        if str(carrier or "").strip():
            live[name] = carrier
    if describe_fields is not None:
        from connectors.saas_common import merge_saas_live_types

        merged, _err = merge_saas_live_types(
            live,
            list(target_cols or []),
            studio_types=None,
            product="Salesforce",
        )
        return merged
    out: dict[str, str] = {}
    for i, col in enumerate(target_cols):
        mapped = ""
        if i < len(mappings):
            mapped = str(
                mappings[i].get("target_type")
                or mappings[i].get("dest_type")
                or ""
            )
        src = str(mappings[i].get("source") or "") if i < len(mappings) else ""
        out[col] = mapped or column_types.get(src) or column_types.get(col) or "VARCHAR"
    return out


def _normalize_salesforce_id_cells(
    mapped_rows: list[tuple],
    target_cols: list[str],
    target_types: list[str],
    rejected_details: list[dict[str, Any]],
    policy: str,
) -> list[tuple]:
    """Expand 15-char Ids to 18-char on Id/reference VARCHAR(18) columns."""
    from connectors.writer_common import append_write_quarantine_detail
    from services.value_serializer import cell_to_string, is_missing_sentinel

    id_cols = [
        i
        for i, col in enumerate(target_cols)
        if i < len(target_types)
        and str(target_types[i]).upper().replace(" ", "").startswith("VARCHAR(18)")
        and (col in {"Id", "id"} or col.endswith("Id"))
    ]
    if not id_cols or policy == "fail":
        return mapped_rows

    out: list[tuple] = []
    for row_idx, row in enumerate(mapped_rows):
        cells = list(row)
        hold_out = False
        for col_idx in id_cols:
            if col_idx >= len(cells) or cells[col_idx] is None:
                continue
            if is_missing_sentinel(cells[col_idx]):
                continue
            try:
                cells[col_idx] = coerce_salesforce_id_wire(cells[col_idx])
            except ValueError:
                sample = cell_to_string(cells[col_idx])[:120]
                append_write_quarantine_detail(
                    rejected_details,
                    {
                        "row": row_idx + 1,
                        "column": target_cols[col_idx],
                        "target": target_cols[col_idx],
                        "value": sample,
                        "reason": (
                            "invalid Salesforce Id — quarantined "
                            "(expect 15/18-char case-safe Id)"
                        ),
                        "policy": "write_quarantine",
                        "chars": [],
                    },
                    mapped_row=cells,
                    target_cols=target_cols,
                )
                if policy == "coerce_null":
                    cells[col_idx] = None
                else:
                    hold_out = True
                    break
        if hold_out:
            continue
        out.append(tuple(cells))
    return out


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
    try:
        url_base = _require_instance_url(host)
    except ValueError as exc:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=sobject,
            target_schema="",
            checksum="",
            chunks_completed=0,
            error=str(exc),
            driver="salesforce",
        )

    target_cols, _ = resolve_target_columns(mappings, column_types, preserve_case=True)
    # Rebuild targets with Salesforce API-name honesty (__c / __r preserved).
    sf_cols: list[str] = []
    for m in mappings or []:
        try:
            from services.mapping_constraints import is_intentional_omit

            if is_intentional_omit(m):
                continue
        except Exception:
            pass  # intentional omit helper optional
        tgt = _sf_field_api_name(str(m.get("target") or ""))
        if tgt and tgt not in sf_cols:
            sf_cols.append(tgt)
    if sf_cols:
        target_cols = sf_cols
    # Conflict columns must use the same API-name polarity as Map targets.
    if conflict_columns:
        conflict_columns = [
            _sf_field_api_name(str(c)) for c in conflict_columns if str(c).strip()
        ]
    policy = transform_error_policy(error_policy)
    # Live Describe when credentials allow — VARCHAR(n)/DECIMAL(p,s) for Bulk fit.
    # Never degrade to Map VARCHAR invent on Describe failure (HubSpot class).
    describe_fields: list[dict[str, Any]] | None = None
    describe_warning = ""
    live_dest = _kwargs.get("destination_column_types")
    studio_live = isinstance(live_dest, dict) and all(
        str(live_dest.get(c) or "").strip() for c in target_cols if c
    )
    try:
        from connectors.saas_common import is_auth_error
        from connectors.salesforce import describe_sobject

        cfg = {
            "host": host,
            "username": username,
            "password": password,
            "connection_string": connection_string,
            "api_key": api_key,
            "database": database,
            "table": sobject,
        }
        describe_fields = describe_sobject(cfg, sobject)
    except Exception as exc:
        if is_auth_error(exc):
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=sobject,
                target_schema="",
                checksum="",
                chunks_completed=0,
                error=(
                    f"Salesforce Describe auth failed: {exc} — "
                    "refuse Map VARCHAR bind (empty→null invent risk)."
                ),
                driver="salesforce",
            )
        if not studio_live:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=sobject,
                target_schema="",
                checksum="",
                chunks_completed=0,
                error=(
                    f"Salesforce Describe unavailable ({exc}) and Studio did not "
                    "type all mapped fields — refuse Map VARCHAR bind "
                    "(empty→null invent risk). Re-run destination schema "
                    "introspect or refresh org credentials."
                ),
                driver="salesforce",
            )
        describe_fields = None
        describe_warning = (
            f"Salesforce Describe unavailable ({exc}); using Studio-typed "
            "carriers only for this write"
        )

    if describe_fields is not None and len(describe_fields) == 0:
        if not studio_live:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=sobject,
                target_schema="",
                checksum="",
                chunks_completed=0,
                error=(
                    f"Salesforce Describe returned no fields for {sobject!r} — "
                    "refuse Map VARCHAR bind (empty→null invent risk). Confirm "
                    "object API name and Describe access."
                ),
                driver="salesforce",
            )
        # Studio typed all fields — treat empty Describe as unavailable.
        describe_fields = None
        describe_warning = (
            f"Salesforce Describe returned no fields for {sobject!r}; using "
            "Studio-typed carriers only for this write"
        )
    # Skip formula / non-writable fields so a live demo does not explode on
    # Calculated fields the Map step may have suggested from Describe.
    skip_targets: set[str] = set()
    mode = (write_mode or "upsert").lower()
    ext_field = ""
    if conflict_columns:
        ext_field = str(conflict_columns[0]).strip()
    elif "Id" in target_cols:
        ext_field = "Id"
    elif "ExternalId" in target_cols:
        ext_field = "ExternalId"
    # Upsert/update by Id needs Id present even though it is not createable.
    identity_keep = {ext_field} if ext_field and mode in {"upsert", "update"} else set()
    if describe_fields:
        by_name = {str(f.get("name") or ""): f for f in describe_fields if f.get("name")}
        if ext_field and ext_field != "Id" and ext_field in by_name:
            meta = by_name[ext_field]
            if not (meta.get("externalId") or meta.get("idLookup")):
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=sobject,
                    target_schema="",
                    checksum="",
                    chunks_completed=0,
                    error=(
                        f"Conflict column `{ext_field}` is not an External Id / idLookup "
                        "on this Salesforce object — set Destination → Advanced primary key "
                        "to a real External Id field (or Id for PATCH update)"
                    ),
                    driver="salesforce",
                )
        for f in describe_fields:
            name = str(f.get("name") or "")
            if not name or name in identity_keep:
                continue
            if f.get("calculated"):
                skip_targets.add(name)
                continue
            if mode in {"update", "upsert"} and ext_field == "Id":
                # PATCH-by-Id: keep updateable fields + Id; drop non-updateable.
                if name != "Id" and not f.get("updateable", True):
                    skip_targets.add(name)
            elif mode == "update":
                if not f.get("updateable", True):
                    skip_targets.add(name)
            elif not f.get("createable", True):
                skip_targets.add(name)
    if skip_targets:
        target_cols = [c for c in target_cols if c not in skip_targets]
        mappings = [
            m
            for m in mappings
            if (m.get("target") or m.get("source") or "") not in skip_targets
        ]
    if mode in {"upsert", "update"} and ext_field and ext_field not in target_cols:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=sobject,
            target_schema="",
            checksum="",
            chunks_completed=0,
            error=(
                f"Conflict column `{ext_field}` is not mapped — include it in Map "
                "or change Destination → Advanced primary key"
            ),
            driver="salesforce",
        )
    if describe_fields:
        live: dict[str, str] = {}
        for f in describe_fields:
            name = str(f.get("name") or "").strip()
            if name:
                live[name] = salesforce_field_to_carrier(f)
        live = {k: v for k, v in live.items() if str(v or "").strip()}
        from connectors.saas_common import merge_saas_live_types

        dest_types, cov_err = merge_saas_live_types(
            live,
            target_cols,
            studio_types=live_dest if isinstance(live_dest, dict) else None,
            product="Salesforce",
        )
        if cov_err:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=sobject,
                target_schema="",
                checksum="",
                chunks_completed=0,
                error=cov_err,
                driver="salesforce",
            )
    else:
        from connectors.saas_common import merge_saas_live_types

        dest_types, cov_err = merge_saas_live_types(
            {},
            target_cols,
            studio_types=live_dest if isinstance(live_dest, dict) else None,
            product="Salesforce",
        )
        if cov_err:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=sobject,
                target_schema="",
                checksum="",
                chunks_completed=0,
                error=cov_err,
                driver="salesforce",
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
        dest_kind="salesforce",
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
        dialect_label="Salesforce",
        mappings=mappings,
    )
    mapped_rows = _normalize_salesforce_id_cells(
        mapped_rows,
        target_cols,
        tgt_types,
        rejected_details,
        policy,
    )
    _map_abort = reject_on_strict_policy(policy, rejected_details, 'Salesforce', transform_errors)
    if _map_abort:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=sobject,
            target_schema="",
            checksum="",
            chunks_completed=0,
            error=_map_abort or f"Transform errors: {'; '.join(transform_errors[:3])}",
            rejected_details=rejected_details,
            driver="salesforce",
        )

    if mode in {"upsert", "update"} and not ext_field:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=sobject,
            target_schema="",
            checksum="",
            chunks_completed=0,
            error=(
                "Salesforce upsert/update requires conflict_columns — set Destination → "
                "Advanced primary key to Id or an External Id field"
            ),
            driver="salesforce",
        )

    written = 0
    chunks = 0
    digest = hashlib.sha256()
    written_ids: list[str] = []

    def _submit(
        records: list[dict[str, Any]],
        *,
        method: str,
        endpoint: str,
        row_base: int,
        mapped_batch: list[Any] | None = None,
    ) -> None:
        nonlocal written, chunks
        if not records:
            return
        body = {"allOrNone": False, "records": [
            {"attributes": {"type": sobject}, **r} for r in records
        ]}
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
                    f"Salesforce returned invalid result for row {row_base + idx}"
                )
            if item.get("success"):
                written += 1
                rid = str(item.get("id", "") or "")
                digest.update((rid or str(idx)).encode())
                if rid:
                    written_ids.append(rid)
            else:
                errs = item.get("errors") or [{"message": "unknown Salesforce error"}]
                msg = errs[0].get("message", str(errs[0])) if isinstance(errs[0], dict) else str(errs[0])
                from connectors.writer_common import append_write_quarantine_detail

                rec = records[idx] if idx < len(records) else {}
                # Original mapped row preserves DF_MISSING / NULL; CRM rec omits them.
                src_mapped: Any
                if mapped_batch is not None and idx < len(mapped_batch):
                    src_mapped = mapped_batch[idx]
                else:
                    src_mapped = tuple(
                        rec.get(c) if isinstance(rec, dict) else None for c in target_cols
                    )
                append_write_quarantine_detail(
                    rejected_details,
                    {
                        "row": row_base + idx + 1,
                        "column": "",
                        "target": sobject,
                        "value": "",
                        "reason": msg,
                        "policy": "write_fail" if policy == "fail" else "write_quarantine",
                        "values": rec if isinstance(rec, dict) else {},
                    },
                    mapped_row=src_mapped,
                    target_cols=target_cols,
                )
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

    try:
        for i in range(0, len(mapped_rows), chunk):
            batch = mapped_rows[i : i + chunk]
            records = []
            mapped_batch: list[Any] = []
            for row in batch:
                if isinstance(row, dict):
                    pairs = row.items()
                    mapped_src = tuple((row or {}).get(c) for c in target_cols)
                else:
                    pairs = zip(target_cols, row)
                    mapped_src = row if isinstance(row, (tuple, list)) else tuple(
                        row[j] if j < len(row) else None for j in range(len(target_cols))
                    )
                from connectors.writer_common import omit_missing_fields

                # STOP_COLUMN / coerce_null → DF_MISSING must omit, never leak
                # "__DF_MISSING__" into Salesforce field values.
                rec = omit_missing_fields(pairs)
                rec.pop("attributes", None)
                records.append(rec)
                mapped_batch.append(mapped_src)
            if not records:
                continue

            if write_mode in {"upsert", "update"} and ext_field and ext_field != "Id":
                endpoint = (
                    f"{url_base}/services/data/{API_VERSION}/composite/sobjects/"
                    f"{sobject}/{ext_field}"
                )
                _submit(
                    records,
                    method="PATCH",
                    endpoint=endpoint,
                    row_base=i,
                    mapped_batch=mapped_batch,
                )
            elif write_mode in {"upsert", "update"} and ext_field == "Id":
                # Id present → PATCH update; Id missing → quarantine (never POST
                # invent duplicates under at-least-once retry — HubSpot/Stripe class).
                collections = f"{url_base}/services/data/{API_VERSION}/composite/sobjects"
                with_id: list[dict[str, Any]] = []
                with_id_mapped: list[Any] = []
                without_id: list[tuple[int, dict[str, Any], Any]] = []
                for idx, rec in enumerate(records):
                    if rec.get("Id"):
                        with_id.append(rec)
                        with_id_mapped.append(mapped_batch[idx])
                    else:
                        without_id.append((idx, rec, mapped_batch[idx]))
                if with_id:
                    _submit(
                        with_id,
                        method="PATCH",
                        endpoint=collections,
                        row_base=i,
                        mapped_batch=with_id_mapped,
                    )
                if without_id:
                    from connectors.writer_common import append_write_quarantine_detail

                    for idx, rec, src_mapped in without_id:
                        append_write_quarantine_detail(
                            rejected_details,
                            {
                                "row": i + idx + 1,
                                "column": "Id",
                                "target": "Id",
                                "value": None,
                                "reason": (
                                    "Salesforce upsert/update requires Id — quarantined "
                                    "(refuse POST invent duplicates)"
                                ),
                                "policy": (
                                    "write_fail" if policy == "fail" else "write_quarantine"
                                ),
                                "values": rec,
                            },
                            mapped_row=src_mapped,
                            target_cols=target_cols,
                        )
                    if policy == "fail":
                        raise RuntimeError(
                            f"Salesforce upsert/update missing Id on {len(without_id)} record(s)"
                        )
            else:
                endpoint = f"{url_base}/services/data/{API_VERSION}/composite/sobjects"
                _submit(
                    records,
                    method="POST",
                    endpoint=endpoint,
                    row_base=i,
                    mapped_batch=mapped_batch,
                )
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

    _final_abort = reject_on_strict_policy(policy, rejected_details, "Salesforce")
    if _final_abort:
        return WriteResult(
            ok=False,
            rows_written=written,
            table_name=sobject,
            target_schema="",
            checksum=digest.hexdigest()[:16] if written else "",
            chunks_completed=chunks,
            error=_final_abort,
            rejected_details=rejected_details,
            rejected_rows=len(rejected_details),
            driver="salesforce",
        )

    warnings: list[str] = []
    if describe_warning:
        warnings.append(describe_warning)
    if skip_targets:
        warnings.append(
            "Skipped non-writable Salesforce fields: "
            + ", ".join(sorted(skip_targets)[:12])
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
        warnings=warnings or None,
        driver="salesforce",
        meta=gate8_writer_meta(mapped_rows, target_cols, written_ids),
    )
