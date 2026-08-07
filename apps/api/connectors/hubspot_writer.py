"""HubSpot reverse-ETL writer — CRM batch upsert by idProperty.

Uses ``/crm/v3/objects/{object}/batch/upsert`` for idempotent activation.
Failed records are quarantined via rejected_details.
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
    resolve_mapping_dest_types,
    resolve_target_columns,
    transform_error_policy,
)

DEFAULT_HOST = "api.hubapi.com"
_CHUNK = 100
# HubSpot Properties API: string type limited to 65,536 characters.
_HUBSPOT_STRING_CHARS = 65_536
# Unformatted numbers: ≤38 total digits, ≤10 right of decimal (HubSpot KB).
_HUBSPOT_NUMBER_PRECISION = 38
_HUBSPOT_NUMBER_SCALE = 10


def _hubspot_enumeration_options(prop: dict[str, Any]) -> list[str]:
    """Ordered active HubSpot enumeration *internal* values (not display labels).

    Hightouch / Census reverse-ETL: enumeration properties only accept internal
    ``options[].value`` — case-sensitive; labels must not invent wire.
    """
    raw = prop.get("options") or prop.get("optionsList") or []
    if not isinstance(raw, list):
        return []
    values: list[str] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, dict):
            continue
        if item.get("hidden") is True:
            continue
        val = str(item.get("value") or "").strip()
        if not val or val in seen:
            continue
        seen.add(val)
        values.append(val)
        if len(values) >= 256:
            break
    return values


def coerce_hubspot_datetime_wire(value: Any) -> str | None:
    """Normalize HubSpot ``datetime`` property wire to epoch-millis string.

    HubSpot CRM Properties API stores datetime as UTC milliseconds since epoch
    (Airbyte / Census class). Refuse invent for unparseable values — never
    silently stringify a bad ISO fragment into a CRM cell.
    """
    if value is None:
        return None
    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        return value
    if isinstance(value, bool):
        raise ValueError("HubSpot datetime cannot bind bool — refuse invent")
    if isinstance(value, (int, float)):
        n = int(value)
        # Seconds vs millis heuristic (same as sql_temporal).
        if n < 10_000_000_000:
            n *= 1000
        if n < 0:
            raise ValueError("HubSpot datetime epoch cannot be negative — refuse invent")
        return str(n)
    text = str(value).strip()
    if not text:
        # Omit empty CRM datetime — never invent JSON/API null wipe on upsert.
        # Callers must not write None into property payload; raise so quarantine/
        # omit path owns the cell (parity with Notion empty url).
        raise ValueError(
            "empty HubSpot datetime — refuse silent NULL invent "
            "(quarantine or omit property upstream)"
        )
    if text.isdigit():
        return coerce_hubspot_datetime_wire(int(text))
    from connectors.sql_temporal import coerce_sql_temporal

    coerced = coerce_sql_temporal(value, "TIMESTAMPTZ")
    from datetime import datetime

    if not isinstance(coerced, datetime):
        raise ValueError(
            f"HubSpot datetime cannot parse {text[:64]!r} — refuse invent"
        )
    if coerced.tzinfo is None:
        from datetime import timezone as _tz

        coerced = coerced.replace(tzinfo=_tz.utc)
    return str(int(coerced.timestamp() * 1000))


def coerce_hubspot_date_wire(value: Any) -> str | None:
    """Normalize HubSpot ``date`` property to ``YYYY-MM-DD`` (midnight UTC day)."""
    if value is None:
        return None
    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        return value
    if isinstance(value, str) and not value.strip():
        raise ValueError(
            "empty HubSpot date — refuse silent NULL invent "
            "(quarantine or omit property upstream)"
        )
    from connectors.sql_temporal import coerce_sql_temporal
    from datetime import date, datetime

    coerced = coerce_sql_temporal(value, "DATE")
    if isinstance(coerced, datetime):
        return coerced.date().isoformat()
    if isinstance(coerced, date):
        return coerced.isoformat()
    raise ValueError(f"HubSpot date cannot parse {value!r} — refuse invent")


def hubspot_property_to_carrier(prop: dict[str, Any]) -> str:
    """Map HubSpot Properties Describe → quarantine carrier.

    Prefer validation ``MAX_LENGTH`` when present (Property Validations API /
    embedded rules); else platform string cap 65_536. Numbers use DECIMAL(38,10)
    so excess scale / non-numeric values quarantine instead of silent IEEE invent.

    Enumeration (select/radio/checkbox) keeps closed ENUM/SET domains from
    ``options`` internal values — never invent open VARCHAR (Hightouch class).
    """
    from services.type_system import (
        format_enum_domain_carrier,
        format_set_domain_carrier,
    )

    ptype = str(prop.get("type") or "").strip().lower()
    field_type = str(prop.get("fieldType") or "").strip().lower()
    hint = str(prop.get("numberDisplayHint") or "").strip().lower()
    max_len = prop.get("maxLength") or prop.get("max_length")
    if max_len is None:
        # Property Validations may surface as ruleArguments on the describe row.
        for rule in prop.get("validationRules") or prop.get("validation_rules") or []:
            if not isinstance(rule, dict):
                continue
            rtype = str(rule.get("ruleType") or rule.get("type") or "").upper()
            if rtype == "MAX_LENGTH":
                args = rule.get("ruleArguments") or rule.get("arguments") or []
                if args:
                    try:
                        max_len = int(args[0])
                    except (TypeError, ValueError):
                        max_len = None
                break
    length_n: int | None = None
    if max_len is not None:
        try:
            length_n = max(1, min(_HUBSPOT_STRING_CHARS, int(max_len)))
        except (TypeError, ValueError):
            length_n = None

    if ptype in {"bool", "boolean"} or field_type == "booleancheckbox":
        return "BOOLEAN"
    if ptype == "date":
        # HubSpot ``date`` is midnight UTC; ``datetime`` is epoch millis UTC.
        return "DATE"
    if ptype == "datetime":
        # Epoch-millis UTC instant — TIMESTAMPTZ polarity (Airbyte/SF parity).
        return "TIMESTAMPTZ"
    if ptype == "number" or field_type == "number":
        if hint == "currency":
            return "DECIMAL(38,2)"
        if hint == "percentage":
            return "DECIMAL(38,4)"
        return f"DECIMAL({_HUBSPOT_NUMBER_PRECISION},{_HUBSPOT_NUMBER_SCALE})"
    if ptype == "enumeration" or field_type in {"select", "radio", "checkbox"}:
        labels = _hubspot_enumeration_options(prop)
        if labels:
            # checkbox = multi-select (semicolon wire); select/radio = single.
            if field_type == "checkbox":
                return format_set_domain_carrier(labels)
            return format_enum_domain_carrier(labels)
        return "VARCHAR(256)"
    if ptype in {"string", "phone_number"} or field_type in {
        "text",
        "textarea",
        "html",
        "phonenumber",
        "file",
    }:
        if length_n:
            return f"VARCHAR({length_n})"
        return f"VARCHAR({_HUBSPOT_STRING_CHARS})"
    if ptype == "json":
        # HubSpot internal JSON property polarity — never invent open VARCHAR.
        return "JSON"
    if ptype == "object_coordinates":
        # Internal object-reference text — bounded string, not unbounded invent.
        if length_n:
            return f"VARCHAR({length_n})"
        return f"VARCHAR({_HUBSPOT_STRING_CHARS})"
    # Unknown / new Properties API type tokens — refuse soft VARCHAR invent;
    # merge_saas_live_types fails closed unless Studio types the column.
    return ""


def resolve_hubspot_dest_types(
    target_cols: list[str],
    mappings: list[dict],
    column_types: dict[str, str],
    *,
    logical_types: list[str] | None = None,
    describe_props: list[dict[str, Any]] | None = None,
    studio_types: dict[str, Any] | None = None,
) -> dict[str, str]:
    """Prefer live Properties Describe; else Map/source carriers.

    When Describe props are supplied (including empty list after a successful
    probe), never soft-fill missing/unknown Meta types with Map ``VARCHAR`` —
    parity with ``merge_saas_live_types`` on the write path (Studio included).
    """
    live: dict[str, str] = {}
    for p in describe_props or []:
        if not isinstance(p, dict):
            continue
        name = str(p.get("name") or "").strip()
        if name:
            live[name] = hubspot_property_to_carrier(p)
    live = {k: v for k, v in live.items() if str(v or "").strip()}
    if describe_props is not None:
        from connectors.saas_common import merge_saas_live_types

        merged, _err = merge_saas_live_types(
            live,
            list(target_cols or []),
            studio_types=studio_types if isinstance(studio_types, dict) else None,
            product="HubSpot",
        )
        # Return covered carriers only — never Map VARCHAR invent for gaps.
        return merged
    return resolve_mapping_dest_types(
        target_cols,
        mappings,
        column_types,
        logical_types=logical_types,
        live_types=None,
        default="VARCHAR",
    )


def _normalize_hubspot_temporal_cells(
    mapped_rows: list[tuple],
    target_cols: list[str],
    target_types: list[str],
    rejected_details: list[dict[str, Any]],
    policy: str,
) -> list[tuple]:
    """Convert DATE/TIMESTAMPTZ cells to HubSpot CRM wire (YYYY-MM-DD / epoch ms)."""
    from connectors.writer_common import append_write_quarantine_detail
    from services.value_serializer import cell_to_string, is_missing_sentinel

    temporal_cols: list[tuple[int, str]] = []
    for i, typ in enumerate(target_types):
        upper = str(typ or "").upper()
        if upper.startswith("TIMESTAMPTZ") or upper in {"DATETIME", "TIMESTAMP"}:
            temporal_cols.append((i, "datetime"))
        elif upper.startswith("DATE") and "TIME" not in upper:
            temporal_cols.append((i, "date"))
    if not temporal_cols:
        return mapped_rows
    # ``fail`` must still convert. Skipping the whole pass under the strictest
    # policy shipped raw ISO strings into CRM properties that expect
    # YYYY-MM-DD / epoch millis — the loop below already holds bad cells out and
    # the caller turns any rejected_details into a hard failure.

    out: list[tuple] = []
    for row_idx, row in enumerate(mapped_rows):
        cells = list(row)
        hold_out = False
        for col_idx, kind in temporal_cols:
            if col_idx >= len(cells) or cells[col_idx] is None:
                continue
            if is_missing_sentinel(cells[col_idx]):
                continue
            try:
                if kind == "datetime":
                    cells[col_idx] = coerce_hubspot_datetime_wire(cells[col_idx])
                else:
                    cells[col_idx] = coerce_hubspot_date_wire(cells[col_idx])
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
                            f"invalid HubSpot {kind} wire — quarantined "
                            "(expect ISO / epoch millis)"
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
    obj = (table_name or object_name({"table": table_name, "database": database}, "contacts")).strip()
    access = token(api_key, connection_string, username, password)
    if not access:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=obj,
            target_schema="",
            checksum="",
            chunks_completed=0,
            error="HubSpot private app token is required for reverse-ETL writes",
            driver="hubspot",
        )

    target_cols, logical_types = resolve_target_columns(
        mappings, column_types, preserve_case=True
    )
    policy = transform_error_policy(error_policy)
    # Live Properties Describe when scopes allow — VARCHAR(65536)/DECIMAL(38,10)
    # before batch upsert invents bad CRM cells (Salesforce Describe class).
    describe_props: list[dict[str, Any]] | None = None
    live_dest = _kwargs.get("destination_column_types")
    studio_live = isinstance(live_dest, dict) and all(
        str(live_dest.get(c) or "").strip() for c in target_cols if c
    )
    try:
        from connectors.hubspot import describe_properties
        from connectors.saas_common import is_auth_error

        describe_props = describe_properties(
            {
                "host": host,
                "username": username,
                "password": password,
                "connection_string": connection_string,
                "api_key": api_key,
                "database": database,
                "table": obj,
            },
            obj,
        )
    except Exception as exc:
        if is_auth_error(exc):
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=obj,
                target_schema="",
                checksum="",
                chunks_completed=0,
                error=(
                    f"HubSpot Properties Describe auth failed: {exc} — "
                    "refuse Map VARCHAR bind (empty→null invent risk)."
                ),
                driver="hubspot",
            )
        if not studio_live:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=obj,
                target_schema="",
                checksum="",
                chunks_completed=0,
                error=(
                    f"HubSpot Properties Describe unavailable ({exc}) and Studio "
                    "did not type all mapped fields — refuse Map VARCHAR bind "
                    "(empty→null invent risk). Re-run destination schema introspect "
                    "or refresh CRM property scopes."
                ),
                driver="hubspot",
            )
        describe_props = None

    if describe_props is not None and len(describe_props) == 0:
        if not studio_live:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=obj,
                target_schema="",
                checksum="",
                chunks_completed=0,
                error=(
                    f"HubSpot Properties Describe returned no properties for {obj!r} — "
                    "refuse Map VARCHAR bind (empty→null invent risk). Confirm object "
                    "type and CRM scopes."
                ),
                driver="hubspot",
            )
        describe_props = None

    if describe_props:
        live: dict[str, str] = {}
        for p in describe_props:
            if not isinstance(p, dict):
                continue
            name = str(p.get("name") or "").strip()
            if name:
                live[name] = hubspot_property_to_carrier(p)
        live = {k: v for k, v in live.items() if str(v or "").strip()}
        from connectors.saas_common import merge_saas_live_types

        dest_types, cov_err = merge_saas_live_types(
            live,
            target_cols,
            studio_types=live_dest if isinstance(live_dest, dict) else None,
            product="HubSpot",
        )
        if cov_err:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=obj,
                target_schema="",
                checksum="",
                chunks_completed=0,
                error=cov_err,
                driver="hubspot",
            )
    else:
        # Studio-typed fallback only after Describe failed non-auth / empty.
        from connectors.saas_common import merge_saas_live_types

        dest_types, cov_err = merge_saas_live_types(
            {},
            target_cols,
            studio_types=live_dest if isinstance(live_dest, dict) else None,
            product="HubSpot",
        )
        if cov_err:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=obj,
                target_schema="",
                checksum="",
                chunks_completed=0,
                error=cov_err,
                driver="hubspot",
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
        dest_kind="hubspot",
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
        dialect_label="HubSpot",
        mappings=mappings,
    )
    mapped_rows = _normalize_hubspot_temporal_cells(
        mapped_rows,
        target_cols,
        tgt_types,
        rejected_details,
        policy,
    )
    _map_abort = reject_on_strict_policy(policy, rejected_details, 'HubSpot', transform_errors)
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
            driver="hubspot",
        )

    id_property = ""
    mode_l = (write_mode or "upsert").lower()
    upsert_modes = {"upsert", "merge", "update", "overwrite", "replace"}
    if conflict_columns:
        id_property = str(conflict_columns[0]).strip()
        if not id_property and mode_l in upsert_modes:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=obj,
                target_schema="",
                checksum="",
                chunks_completed=0,
                error=(
                    "HubSpot upsert requires conflict_columns/primary_key — "
                    "refuse inventing default 'email'"
                ),
                rejected_details=rejected_details,
                driver="hubspot",
            )
    elif "hs_object_id" in target_cols:
        id_property = "hs_object_id"
    elif "id" in target_cols:
        id_property = "id"
    elif mode_l in upsert_modes:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=obj,
            target_schema="",
            checksum="",
            chunks_completed=0,
            error=(
                "HubSpot upsert requires conflict_columns/primary_key — "
                "refuse inventing default 'email' (wrong-object upsert)"
            ),
            rejected_details=rejected_details,
            driver="hubspot",
        )

    url = f"{base_url(host, DEFAULT_HOST)}/crm/v3/objects/{obj}/batch/upsert"
    written = 0
    chunks = 0
    digest = hashlib.sha256()
    written_ids: list[str] = []

    from services.value_serializer import is_missing_sentinel

    try:
        for i in range(0, len(mapped_rows), chunk):
            batch = mapped_rows[i : i + chunk]
            inputs = []
            # Parallel original mapped rows — CRM props omit DF_MISSING/None.
            input_mapped_rows: list[Any] = []
            for row in batch:
                if isinstance(row, dict):
                    pairs = row.items()
                    mapped_src = tuple((row or {}).get(c) for c in target_cols)
                else:
                    pairs = zip(target_cols, row)
                    mapped_src = row if isinstance(row, (tuple, list)) else tuple(
                        row[j] if j < len(row) else None for j in range(len(target_cols))
                    )
                props = {
                    k: str(v)
                    for k, v in pairs
                    if v is not None
                    and not is_missing_sentinel(v)
                    and str(v) != ""
                }
                id_val = props.pop(id_property, None)
                # Never invent identity from a different property while idProperty
                # stays configured (wrong-object upsert / create).
                if not id_val:
                    from connectors.writer_common import append_write_quarantine_detail

                    append_write_quarantine_detail(
                        rejected_details,
                        {
                            "row": i + len(inputs) + 1,
                            "column": id_property,
                            "target": id_property,
                            "value": None,
                            "reason": (
                                f"Missing idProperty '{id_property}' for HubSpot upsert"
                            ),
                            "policy": "write_fail" if policy == "fail" else "write_quarantine",
                            "values": props,
                        },
                        mapped_row=mapped_src,
                        target_cols=target_cols,
                    )
                    if policy == "fail":
                        raise RuntimeError(
                            f"Missing idProperty '{id_property}' for HubSpot upsert"
                        )
                    continue
                inputs.append({
                    "idProperty": id_property,
                    "id": str(id_val),
                    "properties": props,
                })
                input_mapped_rows.append(mapped_src)
            if not inputs:
                continue

            # HubSpot batch upsert; create path for insert-only mode
            endpoint = url if write_mode != "insert" else (
                f"{base_url(host, DEFAULT_HOST)}/crm/v3/objects/{obj}/batch/create"
            )
            body = {"inputs": inputs} if write_mode != "insert" else {
                "inputs": [{"properties": inp["properties"]} for inp in inputs]
            }
            resp = request(method="POST", url=endpoint, token=access, data=body, timeout=120)
            data = resp.json() if resp.content else {}
            if not isinstance(data, dict):
                raise RuntimeError("HubSpot returned an invalid batch response")
            results = data.get("results") or []
            errors = data.get("errors") or []
            if not isinstance(results, list) or not isinstance(errors, list):
                raise RuntimeError("HubSpot batch response lacks result arrays")
            accounted = len(results) + len(errors)
            if accounted < len(inputs):
                raise RuntimeError(
                    f"HubSpot acknowledged only {accounted} of {len(inputs)} records"
                )
            written += len(results)
            for r in results:
                rid = str(r.get("id", "") or "")
                digest.update(rid.encode())
                if rid:
                    written_ids.append(rid)
            for err_i, err in enumerate(errors):
                from connectors.writer_common import append_write_quarantine_detail

                ctx = err.get("context") if isinstance(err.get("context"), dict) else {}
                # Prefer the submitted input row — HubSpot context is metadata, not props.
                src_props: dict[str, Any] = {}
                matched_idx: int | None = None
                err_ids = {
                    str(x) for x in (ctx.get("ids") or []) if x is not None
                }
                if err_ids:
                    for inp_i, inp in enumerate(inputs):
                        if str(inp.get("id") or "") in err_ids:
                            matched_idx = inp_i
                            break
                if matched_idx is None and err_i < len(inputs):
                    matched_idx = err_i
                matched_inp = inputs[matched_idx] if matched_idx is not None else None
                if matched_inp is not None:
                    src_props = dict(matched_inp.get("properties") or {})
                    src_props[id_property] = matched_inp.get("id")
                # Original mapped batch row — preserves DF_MISSING / NULL polarity.
                mapped_src: Any = tuple()
                if matched_idx is not None and matched_idx < len(input_mapped_rows):
                    mapped_src = input_mapped_rows[matched_idx]
                append_write_quarantine_detail(
                    rejected_details,
                    {
                        "row": i + err_i + 1,
                        "column": "",
                        "target": obj,
                        "value": "",
                        "reason": str(err.get("message") or err),
                        "policy": "write_fail" if policy == "fail" else "write_quarantine",
                        "values": src_props,
                    },
                    mapped_row=mapped_src,
                    target_cols=target_cols,
                )
            if errors and policy == "fail":
                raise RuntimeError(
                    f"HubSpot rejected {len(errors)} record(s); "
                    "strict error policy blocks partial activation"
                )
            chunks += 1
            if on_checkpoint:
                on_checkpoint(written, len(mapped_rows), chunks)
    except Exception as exc:
        return WriteResult(
            ok=False,
            rows_written=written,
            table_name=obj,
            target_schema="",
            checksum=digest.hexdigest()[:16],
            chunks_completed=chunks,
            error=humanize_http_error(exc, "hubspot"),
            rejected_details=rejected_details,
            driver="hubspot",
        )

    _final_abort = reject_on_strict_policy(policy, rejected_details, "HubSpot")
    if _final_abort:
        return WriteResult(
            ok=False,
            rows_written=written,
            table_name=obj,
            target_schema="",
            checksum=digest.hexdigest()[:16] if written else "",
            chunks_completed=chunks,
            error=_final_abort,
            rejected_details=rejected_details,
            rejected_rows=len(rejected_details),
            driver="hubspot",
        )

    return WriteResult(
        ok=True,
        rows_written=written,
        table_name=obj,
        target_schema="",
        checksum=digest.hexdigest()[:16] if written else "",
        chunks_completed=chunks,
        rejected_details=rejected_details,
        rejected_rows=len(rejected_details),
        driver="hubspot",
        meta=gate8_writer_meta(mapped_rows, target_cols, written_ids),
    )
