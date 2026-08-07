"""DynamoDB table writer — BatchWriteItem loads."""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable

from connectors.aws_common import boto3_client
from connectors.writer_common import WriteResult as _WriteResult
from connectors.writer_common import (
    build_mapped_rows_with_details,
    _coerced_null_row_count,
    gate8_writer_meta,
    resolve_target_columns,
    row_checksum,
    transform_error_policy,
)


@dataclass
class WriteResult(_WriteResult):
    driver: str = "boto3"


def _pick_hash_key(columns: list[str], mappings: list[dict]) -> str:
    """Choose a DynamoDB hash key from column names or mapping targets.

    Prefers explicit identity names, then ``*_id`` columns, then identity-like
    mapping targets.  This is a deterministic helper used by tests and the writer
    when no explicit conflict_columns are supplied.
    """
    preferred = {"id", "_id", "uuid", "key", "pk", "sk"}
    lower_map = {c.lower(): c for c in columns}
    for name in preferred:
        if name in lower_map:
            return lower_map[name]
    for c in columns:
        if c.lower().endswith("_id"):
            return c
    for m in mappings:
        target = m.get("target") or m.get("source", "")
        if target and (target.lower() in preferred or target.lower().endswith("_id")):
            return target
    return columns[0] if columns else "id"


def _to_dynamo_value(value: Any, source_type: str) -> Any:
    """Convert transform-engine values to DynamoDB-serializable native types."""
    if value is None:
        return None
    # Reader envelopes: {"_df_ddb_set": "SS"|"NS"|"BS", "v": [...]} (also accept "items").
    if isinstance(value, dict) and value.get("_df_ddb_set") in {"SS", "NS", "BS"}:
        kind = value["_df_ddb_set"]
        items = value.get("v")
        if items is None:
            items = value.get("items") or []
        if kind == "SS":
            return {str(x) for x in items}
        if kind == "NS":
            return {Decimal(str(x)) for x in items}
        if kind == "BS":
            from connectors.sql_bind import coerce_binary_wire

            return {coerce_binary_wire(x) if not isinstance(x, (bytes, bytearray)) else bytes(x) for x in items}
    if isinstance(value, str) and value.startswith("{") and "_df_ddb_set" in value:
        try:
            parsed = json.loads(value, parse_float=Decimal)
            if isinstance(parsed, dict) and parsed.get("_df_ddb_set"):
                return _to_dynamo_value(parsed, source_type)
        except Exception:
            pass
    upper = source_type.upper()
    if upper in {"BOOLEAN", "BOOL", "BIT"}:
        from connectors.sql_bind import coerce_boolean_wire

        coerced = coerce_boolean_wire(value)
        if coerced is not None and not isinstance(coerced, bool):
            raise ValueError(
                f"DynamoDB BOOLEAN refused unrecognized value {value!r} "
                "(would invent True from non-empty string)"
            )
        return coerced
    # Advertise numeric DDL as DynamoDB N — never silently store typed numbers as S.
    if upper in {
        "DECIMAL", "NUMERIC", "NUMBER", "INTEGER", "BIGINT", "SMALLINT",
        "TINYINT", "FLOAT", "DOUBLE", "REAL", "INT", "INT64", "FLOAT64",
    }:
        from connectors.sql_bind import coerce_decimal_wire

        return coerce_decimal_wire(value, ddl_type=upper)
    if upper in {"JSON", "OBJECT", "ARRAY", "VARIANT", "SET"}:
        if isinstance(value, (dict, list, set)):
            return value
        if isinstance(value, str):
            try:
                def _reject(name: str) -> None:
                    raise ValueError(f"non-finite JSON constant: {name}")

                return json.loads(
                    value, parse_float=Decimal, parse_constant=_reject
                )
            except (json.JSONDecodeError, ValueError) as exc:
                raise ValueError(
                    f"DynamoDB JSON refused {value!r} "
                    "(refuse silent string invent)"
                ) from exc
        raise ValueError(
            f"DynamoDB JSON refused {value!r} "
            "(refuse silent pass-through invent)"
        )
    if upper in {
        "DATE",
        "TIME",
        "DATETIME",
        "TIMESTAMP",
        "TIMESTAMPTZ",
        "DATETIMEOFFSET",
        "TIMESTAMP_NTZ",
        "TIMESTAMP_LTZ",
        "TIMESTAMP_TZ",
    }:
        from connectors.sql_temporal import coerce_sql_temporal

        if isinstance(value, str) and not str(value).strip():
            raise ValueError(
                f"DynamoDB {upper} refused empty string {value!r} "
                "(refuse silent null invent / attribute wipe)"
            )
        return coerce_sql_temporal(value, upper)
    if upper in {"BINARY", "BLOB", "BYTEA", "VARBINARY"}:
        if isinstance(value, bytes):
            return value
        from connectors.sql_bind import coerce_binary_wire

        # Fail-closed — never UTF-8-invent bytes (Airbyte/Fivetran class).
        return coerce_binary_wire(value)
    return value


def _to_attr(value: Any, source_type: str) -> dict:
    from boto3.dynamodb.types import TypeSerializer

    ser = TypeSerializer()
    native = _to_dynamo_value(value, source_type)
    # TypeSerializer turns set[str] → SS, set[Decimal] → NS, set[bytes] → BS.
    return ser.serialize(native)


def _coerce_dynamo_cell(
    value: Any,
    *,
    col: str,
    logical_type: str,
    key_types: dict[str, str],
) -> Any:
    """Apply Dynamo key-type / binary wire coercion before AttributeValue encode."""
    attr_type = key_types.get(col)
    if attr_type == "S":
        if value is None or (
            isinstance(value, str) and value.strip() == ""
        ):
            raise ValueError(
                f"DynamoDB key type S refused {value!r} for {col!r} — "
                "refuse silent empty-string invent (HASH/RANGE identity)"
            )
        return str(value)
    if attr_type == "N":
        if value is None or (isinstance(value, str) and value.strip() == ""):
            raise ValueError(
                f"DynamoDB key type N refused {value!r} for {col!r} — "
                "refuse silent null invent (HASH/RANGE identity)"
            )
        try:
            return Decimal(value)
        except Exception as exc:
            raise ValueError(
                f"DynamoDB key type N refused {value!r} "
                "(refuse silent pass-through invent)"
            ) from exc
    if attr_type == "B":
        from connectors.sql_bind import coerce_binary_wire

        if isinstance(value, str):
            return coerce_binary_wire(value)
        if value is not None and not isinstance(value, (bytes, bytearray)):
            return coerce_binary_wire(value)
        return value
    return value


def _sparse_update_item(
    client: Any,
    table: str,
    *,
    key_attrs: dict[str, Any],
    set_attrs: dict[str, Any],
) -> None:
    """UpdateItem SET present attrs only — PutItem would wipe omitted fields."""
    if not set_attrs:
        # Key-only sparse image: ensure the item exists without clearing attrs.
        client.update_item(
            TableName=table,
            Key=key_attrs,
            UpdateExpression="SET #df_touch = if_not_exists(#df_touch, :z)",
            ExpressionAttributeNames={"#df_touch": "__df_touch"},
            ExpressionAttributeValues={":z": {"N": "0"}},
        )
        return
    names: dict[str, str] = {}
    values: dict[str, Any] = {}
    parts: list[str] = []
    for i, (col, attr) in enumerate(set_attrs.items()):
        nk = f"#c{i}"
        vk = f":v{i}"
        names[nk] = col
        values[vk] = attr
        parts.append(f"{nk} = {vk}")
    client.update_item(
        TableName=table,
        Key=key_attrs,
        UpdateExpression="SET " + ", ".join(parts),
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
    )


def write_mapped_rows(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    schema: str,
    connection_string: str,
    ssl: bool,
    table_name: str,
    headers: list[str],
    data_rows: list[list[str]],
    mappings: list[dict],
    column_types: dict[str, str],
    on_checkpoint: Callable[[int, int, int], None] | None = None,
    create_table: bool = True,
    error_policy: str | None = None,
    backfill_new_fields: bool = False,
    endpoint_url: str = "",
    conflict_columns: list[str] | None = None,
    **_kwargs: Any,
) -> WriteResult:
    del schema, ssl, backfill_new_fields
    policy = transform_error_policy(error_policy)
    table = table_name or database
    cfg = {
        "host": host,
        "port": port,
        "username": username,
        "password": password,
        "connection_string": connection_string,
        "endpoint_url": endpoint_url,
    }
    target_cols, logical_types = resolve_target_columns(mappings, column_types, preserve_case=True)
    from connectors.writer_common import resolve_mapping_dest_types

    live_dest = _kwargs.get("destination_column_types")
    dest_types = resolve_mapping_dest_types(
        target_cols,
        mappings,
        column_types,
        logical_types=logical_types,
        live_types=live_dest if isinstance(live_dest, dict) else None,
    )
    mapped_rows, errors, rejected_details = build_mapped_rows_with_details(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=target_cols,
        column_types=column_types,
        dest_types=dest_types,
        preserve_case=True,
        error_policy=policy,
        dest_kind="dynamodb",
        destination_pk_columns=list(conflict_columns or []) or None,
        destination_column_nullability=_kwargs.get("destination_column_nullability"),
    )
    tgt_types = [str(dest_types.get(c, logical_types[i] if i < len(logical_types) else "VARCHAR") or "VARCHAR") for i, c in enumerate(target_cols)]
    from connectors.writer_common import apply_write_quarantine_matrix, reject_on_strict_policy

    mapped_rows = apply_write_quarantine_matrix(
        mapped_rows,
        target_cols,
        tgt_types,
        rejected_details,
        policy,
        dialect_label="DynamoDB",
        mappings=mappings,
    )
    _map_abort = reject_on_strict_policy(policy, rejected_details, "DynamoDB", errors)
    if _map_abort:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table,
            target_schema=host or "",
            checksum="",
            chunks_completed=0,
            error=_map_abort or f"Transform errors: {'; '.join(errors[:3])}",
            warnings=errors[:10],
            rejected_rows=len({d["row"] for d in rejected_details}),
            rejected_details=list(rejected_details),
        )

    client = boto3_client("dynamodb", cfg)
    if create_table:
        _ensure_table(
            client,
            table,
            target_cols,
            mappings,
            logical_types,
            conflict_columns=conflict_columns or _kwargs.get("conflict_columns"),
        )

    key_types = _table_key_types(client, table)
    if not key_types:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table,
            target_schema=host or "",
            checksum="",
            chunks_completed=0,
            error=(
                f"DynamoDB table {table!r} key schema unavailable — refuse PutItem "
                "without HASH/RANGE identity (describe_table failed or empty KeySchema). "
                "Re-check table name/permissions; never soft-skip key preflight."
            ),
            rejected_details=list(rejected_details),
        )

    written = 0
    batch_size = 25
    # Filter out rows with incomplete Dynamo key components before BatchWrite.
    from services.value_serializer import is_missing_sentinel

    valid_rows: list[list[Any]] = []
    for row_idx, row in enumerate(mapped_rows):
        key_ok = True
        for key_col, attr_type in key_types.items():
            if key_col not in target_cols:
                key_ok = False
                detail = {
                    "row": row_idx + 1,
                    "column": key_col,
                    "target": key_col,
                    "value": "",
                    "reason": (
                        f"DynamoDB key attribute `{key_col}` is not mapped — "
                        "refuse silent PutItem without HASH/RANGE identity"
                    ),
                    "policy": "write_fail" if policy == "fail" else "write_quarantine",
                    "chars": [],
                }
                rejected_details.append(detail)
                if policy == "fail":
                    return WriteResult(
                        ok=False,
                        rows_written=0,
                        table_name=table,
                        target_schema=host or "",
                        checksum="",
                        chunks_completed=0,
                        error=detail["reason"],
                        rejected_rows=len({d["row"] for d in rejected_details}),
                        rejected_details=list(rejected_details),
                    )
                break
            i = target_cols.index(key_col)
            value = row[i] if i < len(row) else None
            from services.cdc_identity import is_present_cdc_row_key

            if not is_present_cdc_row_key(value) or (
                attr_type == "S" and isinstance(value, str) and value.strip() == ""
            ):
                key_ok = False
                detail = {
                    "row": row_idx + 1,
                    "column": key_col,
                    "target": key_col,
                    "value": "" if value is None else str(value)[:120],
                    "reason": (
                        f"DynamoDB key attribute `{key_col}` missing/empty — "
                        "refuse silent empty-string identity collapse"
                    ),
                    "policy": "write_fail" if policy == "fail" else "write_quarantine",
                    "chars": [],
                }
                rejected_details.append(detail)
                if policy == "fail":
                    return WriteResult(
                        ok=False,
                        rows_written=0,
                        table_name=table,
                        target_schema=host or "",
                        checksum="",
                        chunks_completed=0,
                        error=detail["reason"],
                        rejected_rows=len({d["row"] for d in rejected_details}),
                        rejected_details=list(rejected_details),
                    )
                break
        if key_ok:
            valid_rows.append(row)

    chunks = max(1, (len(valid_rows) + batch_size - 1) // batch_size) if valid_rows else 1
    from connectors.writer_common import row_has_missing_sentinel

    try:
        for chunk_idx in range(0 if not valid_rows else chunks):
            if not valid_rows:
                break
            slice_rows = valid_rows[chunk_idx * batch_size : (chunk_idx + 1) * batch_size]
            request_items = []
            chunk_written = 0
            for row in slice_rows:
                sparse = row_has_missing_sentinel(row)
                item: dict[str, Any] = {}
                for i, col in enumerate(target_cols):
                    value = row[i]
                    # STOP_COLUMN / coerce_null omit — never PutItem the sentinel string.
                    if is_missing_sentinel(value):
                        continue
                    # Wire coerce must use live dest_types (tgt_types), never Map-only
                    # logical_types — VARCHAR stamp + live INTEGER invents empty strings.
                    wire_type = tgt_types[i] if i < len(tgt_types) else logical_types[i]
                    value = _coerce_dynamo_cell(
                        value, col=col, logical_type=wire_type, key_types=key_types
                    )
                    # HASH/RANGE encode type must match KeySchema S/N/B — never let a
                    # live INTEGER stamp re-serialize a string key as AttributeValue N.
                    if col in key_types:
                        encode_type = {
                            "S": "VARCHAR",
                            "N": "DECIMAL",
                            "B": "BINARY",
                        }.get(key_types[col], wire_type)
                    else:
                        encode_type = wire_type
                    item[col] = _to_attr(value, encode_type)

                if sparse:
                    # BatchWrite PutItem replaces the whole item — sparse CDC /
                    # STOP_COLUMN must UpdateItem SET present attrs only.
                    key_attrs = {k: item[k] for k in key_types if k in item}
                    if len(key_attrs) != len(key_types):
                        # Defense-in-depth: key preflight should have held these out.
                        rejected_details.append(
                            {
                                "row": "",
                                "column": ",".join(key_types),
                                "target": table,
                                "value": "",
                                "reason": (
                                    "DynamoDB sparse UpdateItem skipped — incomplete "
                                    "HASH/RANGE key after DF_MISSING omit"
                                ),
                                "policy": "write_quarantine",
                                "chars": [],
                            }
                        )
                        continue
                    set_attrs = {k: v for k, v in item.items() if k not in key_types}
                    _sparse_update_item(
                        client, table, key_attrs=key_attrs, set_attrs=set_attrs
                    )
                    chunk_written += 1
                else:
                    request_items.append({"PutRequest": {"Item": item}})
                    chunk_written += 1
            if request_items:
                _batch_write_with_retry(client, table, request_items)
            written += chunk_written
            if on_checkpoint:
                on_checkpoint(chunk_idx + 1, chunks, written)

        from connectors.aws_common import resolve_endpoint_url, resolve_region

        region = resolve_region(cfg)
        endpoint = resolve_endpoint_url(cfg)
        _final_abort = reject_on_strict_policy(policy, rejected_details, "DynamoDB")
        if _final_abort:
            return WriteResult(
                ok=False,
                rows_written=written,
                table_name=table,
                target_schema=endpoint or region,
                checksum="",
                chunks_completed=chunks if valid_rows else 0,
                error=_final_abort,
                warnings=errors[:10],
                rejected_rows=len({d["row"] for d in rejected_details}),
                rejected_details=list(rejected_details),
            )
        return WriteResult(
            ok=True,
            rows_written=written,
            table_name=table,
            target_schema=endpoint or region,
            checksum=row_checksum(
                valid_rows,
                target_cols,
                dest_db_type="dynamodb",
                dest_types=dest_types,
            ),
            chunks_completed=chunks if valid_rows else 0,
            warnings=errors[:10],
            rejected_rows=len({d["row"] for d in rejected_details}),
            rejected_details=list(rejected_details),
            coerced_null_rows=_coerced_null_row_count(rejected_details, policy),
            meta=gate8_writer_meta(valid_rows, target_cols),
        )
    except Exception as exc:
        return WriteResult(
            ok=False, rows_written=written, table_name=table, target_schema=host or "",
            checksum="", chunks_completed=0, error=str(exc),
            rejected_details=list(rejected_details) if "rejected_details" in locals() else [],
            rejected_rows=len(rejected_details) if "rejected_details" in locals() else 0,
        )


def _table_key_types(client, table: str) -> dict[str, str]:
    """Return key attribute names -> DynamoDB type ('S', 'N', 'B') for an existing table."""
    from botocore.exceptions import ClientError
    try:
        info = client.describe_table(TableName=table)["Table"]
        attrs = {a["AttributeName"]: a["AttributeType"] for a in info.get("AttributeDefinitions", [])}
        keys = {}
        for ks in info.get("KeySchema", []):
            name = ks["AttributeName"]
            if name in attrs:
                keys[name] = attrs[name]
        return keys
    except ClientError:
        return {}


def _attr_type_for_logical(logical: str) -> str:
    upper = (logical or "").upper()
    if upper.startswith("DECIMAL") or upper in {
        "INTEGER", "NUMERIC", "FLOAT", "DOUBLE", "LONG", "BIGINT", "NUMBER",
    }:
        return "N"
    if upper in {"BINARY", "BLOB", "BYTEA", "VARBINARY"}:
        return "B"
    return "S"


def _resolve_key_schema(
    target_cols: list[str],
    mappings: list[dict],
    *,
    conflict_columns: list[str] | None,
    source_types: list[str] | None,
) -> list[tuple[str, str, str]]:
    """Return [(name, KeyType, AttrType), ...] for create-table.

    Prefer explicit conflict_columns (HASH, optional RANGE). Refuse inventing a
    key from an arbitrary first column when no identity metadata is available.
    """
    if conflict_columns:
        from connectors.writer_common import resolve_conflict_targets

        conflict = resolve_conflict_targets(conflict_columns, target_cols, strict=True)
    else:
        conflict = []
    if not conflict:
        # Legacy soft path only when a clear identity name exists.
        preferred = {"id", "_id", "pk", "sk", "uuid", "key"}
        lower_map = {c.lower(): c for c in target_cols}
        for name in preferred:
            if name in lower_map:
                conflict = [lower_map[name]]
                break
        if not conflict:
            for c in target_cols:
                if c.lower().endswith("_id"):
                    conflict = [c]
                    break
    if not conflict:
        raise ValueError(
            "DynamoDB create-table requires conflict_columns (HASH[, RANGE]) "
            "or a clear identity column (id/_id/*_id); refusing to invent a key "
            "from the first mapped column"
        )
    keys: list[tuple[str, str, str]] = []
    for i, col in enumerate(conflict[:2]):
        logical = ""
        if source_types and col in target_cols:
            logical = source_types[target_cols.index(col)] or ""
        keys.append((col, "HASH" if i == 0 else "RANGE", _attr_type_for_logical(logical)))
    return keys


def _ensure_table(
    client,
    table: str,
    target_cols: list[str],
    mappings: list[dict],
    source_types: list[str] | None = None,
    *,
    conflict_columns: list[str] | None = None,
) -> None:
    from botocore.exceptions import ClientError

    try:
        client.describe_table(TableName=table)
        return
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") != "ResourceNotFoundException":
            raise
    if not target_cols:
        raise ValueError(f"DynamoDB table `{table}` does not exist and no columns were provided to create it.")
    key_schema = _resolve_key_schema(
        target_cols,
        mappings,
        conflict_columns=conflict_columns,
        source_types=source_types,
    )
    # Deduplicate attribute definitions (HASH/RANGE may share names only once).
    attr_defs = []
    seen: set[str] = set()
    for name, _kt, at in key_schema:
        if name in seen:
            continue
        seen.add(name)
        attr_defs.append({"AttributeName": name, "AttributeType": at})
    client.create_table(
        TableName=table,
        AttributeDefinitions=attr_defs,
        KeySchema=[{"AttributeName": name, "KeyType": kt} for name, kt, _at in key_schema],
        BillingMode="PAY_PER_REQUEST",
    )
    waiter = client.get_waiter("table_exists")
    waiter.wait(TableName=table)


def _batch_write_with_retry(client, table: str, request_items: list[dict], retries: int = 5) -> None:
    pending = {table: request_items}
    for _ in range(retries):
        if not pending.get(table):
            return
        resp = client.batch_write_item(RequestItems=pending)
        pending = resp.get("UnprocessedItems") or {}
    if pending.get(table):
        raise RuntimeError(f"DynamoDB batch write left {len(pending[table])} unprocessed items")
