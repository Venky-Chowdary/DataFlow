"""Redis writer — store records as JSON strings under a key prefix."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable

from services.error_handling import format_exception_message
from services.primary_key import (
    infer_redis_conflict_columns,
    pick_redis_identity_column,
)
from services.sync_cursor import is_overwrite_sync
from services.value_serializer import json_default, sanitize_json_value

from connectors.redis_reader import _redis_client
from connectors.writer_common import WriteResult as _WriteResult
from connectors.writer_common import (
    build_mapped_rows_with_details,
    gate8_writer_meta,
    resolve_target_columns,
    row_checksum,
    sanitize_identifier,
    transform_error_policy,
)

logger = logging.getLogger(__name__)


def _fetch_redis_physical_types(
    client: Any,
    prefix: str,
    target_cols: list[str],
    *,
    sample_limit: int = 50,
) -> tuple[dict[str, str], int]:
    """Sample existing JSON docs under ``prefix:*`` for live carriers.

    Returns ``(physical, docs_sampled)``. Empty keyspace or non-JSON values
    yield ``docs_sampled == 0`` — callers fail-closed when keys exist but no
    JSON docs could be typed.
    """
    from services.schema_introspect import (
        _finalize_mongodb_type,
        _sample_logical_type,
    )

    wanted = {str(c) for c in target_cols if c}
    if not wanted:
        return {}, 0
    pattern = f"{prefix}:*"
    type_counts: dict[str, dict[str, int]] = {c: {} for c in wanted}
    sampled = 0
    cursor = 0
    try:
        while sampled < int(sample_limit):
            cursor, keys = client.scan(
                cursor=cursor,
                match=pattern,
                count=min(32, max(1, int(sample_limit) - sampled)),
            )
            for key in keys or []:
                if sampled >= int(sample_limit):
                    break
                raw = client.get(key)
                if not raw:
                    continue
                try:
                    if isinstance(raw, (bytes, bytearray)):
                        raw = raw.decode("utf-8")
                    doc = json.loads(raw)
                except (TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError):
                    continue
                if not isinstance(doc, dict):
                    continue
                sampled += 1
                for key_name, val in doc.items():
                    if key_name not in type_counts or val is None:
                        continue
                    if isinstance(val, Decimal):
                        inferred = "DECIMAL"
                    else:
                        inferred = _sample_logical_type(val, key_name)
                    if not inferred:
                        continue
                    tc = type_counts[key_name]
                    tc[inferred] = int(tc.get(inferred, 0)) + 1
            if int(cursor) == 0:
                break
    except Exception:
        logger.debug("Redis physical type sample failed", exc_info=True)
        return {}, -1

    physical: dict[str, str] = {}
    for col, counts in type_counts.items():
        if not counts:
            continue
        carrier = _finalize_mongodb_type(counts)
        if carrier:
            physical[col] = carrier
            physical.setdefault(col.lower(), carrier)
            physical.setdefault(col.upper(), carrier)
    return physical, sampled


def _redis_rematerialize_if_physical_differs(
    *,
    physical: dict[str, str],
    dest_types: dict[str, str],
    target_cols: list[str],
    headers: list[str],
    data_rows: list,
    mappings: list,
    column_types: dict[str, str] | None,
    logical_types: list[str],
    policy: Any,
    conflict_columns: list[str] | None = None,
    destination_column_nullability: Any = None,
    force_remap: bool = False,
) -> tuple[list[tuple], list[str], list[dict], dict[str, str]] | None:
    """Rebuild mapped rows when live Redis JSON carriers differ from Map stamps.

    ``force_remap`` covers deferred Map under partial Studio (invent risk).
    """
    from connectors.writer_common import rematerialize_live_dest_types

    live_dest_types = rematerialize_live_dest_types(
        physical, list(target_cols or []), product="Redis"
    )
    if live_dest_types is None:
        return None
    carriers_differ = any(
        str(dest_types.get(c) or "").strip().upper()
        != str(live_dest_types.get(c) or "").strip().upper()
        for c in target_cols
    )
    if not carriers_differ and not force_remap:
        return None
    mapped_rows, transform_errors, rejected_details = build_mapped_rows_with_details(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=target_cols,
        column_types=column_types,
        dest_types=live_dest_types,
        preserve_case=True,
        error_policy=policy,
        dest_kind="redis",
        destination_pk_columns=list(conflict_columns or []) or None,
        destination_column_nullability=destination_column_nullability,
    )
    return (
        mapped_rows,
        list(transform_errors or []),
        rejected_details,
        live_dest_types,
    )


#: Keys asked for per SCAN call. Redis treats COUNT as a hint about how much
#: work to do per call, not a page size, so this bounds latency per round trip
#: rather than the answer.
_SCAN_COUNT = 1000
#: SCAN calls before the probe gives up. With the count above this covers a
#: keyspace of roughly ten million keys before reporting "unknown".
_SCAN_MAX_CALLS = 10_000


def _redis_prefix_key_count_hint(
    client: Any, prefix: str, *, probe: int = _SCAN_COUNT
) -> int:
    """Return >0 when any ``prefix:*`` key exists; 0 if empty; -1 if unknown.

    Redis SCAN may return a non-zero cursor with an empty key batch, so proving
    a prefix is *absent* means walking the cursor until it wraps. Proving it is
    present stops at the first match.

    The budget used to be 64 calls of 8 keys, which cannot complete a pass over
    more than about 512 keys — so on any Redis holding more than that, the probe
    reported "unknown", the writer fail-closed, and every Redis destination
    refused. It only ever passed because a test instance was nearly empty. The
    budget is now sized for a real keyspace, and exhausting it still reports
    unknown rather than guessing empty: reading "no keys" off an incomplete scan
    would bind Map VARCHAR over live typed documents.
    """
    try:
        cursor = 0
        for _ in range(_SCAN_MAX_CALLS):
            cursor, keys = client.scan(
                cursor=cursor, match=f"{prefix}:*", count=int(probe)
            )
            if keys:
                return len(keys)
            if int(cursor) == 0:
                return 0
        # Budget exhausted without cursor wrap — unknown, fail closed.
        return -1
    except Exception:
        return -1


@dataclass
class WriteResult(_WriteResult):
    driver: str = "redis-py"


def _normalize_redis_typed_doc(
    doc: dict[str, Any],
    target_cols: list[str],
    logical_types: list[str],
) -> dict[str, Any]:
    """Fail-closed typed normalize before JSON SET.

    Invalid UUID/BINARY/BOOL/FLOAT/DECIMAL/INT raise so the writer quarantines —
    never invent bytes, 0.0, or empty-string numerics.
    """
    import base64

    from connectors.sql_bind import (
        coerce_binary_wire,
        coerce_boolean_wire,
        coerce_decimal_wire,
        coerce_float_wire,
        coerce_integer_wire,
        coerce_uuid_wire,
    )

    out = dict(doc)
    for col, typ in zip(target_cols, logical_types):
        if col not in out or out[col] is None:
            continue
        upper = (typ or "").upper()
        if upper in {"UUID", "UNIQUEIDENTIFIER", "GUID"}:
            out[col] = coerce_uuid_wire(out[col])
        elif upper in {"BINARY", "BLOB", "BYTEA", "VARBINARY"}:
            raw = coerce_binary_wire(out[col])
            out[col] = base64.b64encode(raw).decode("ascii") if raw is not None else None
        elif upper in {"BOOLEAN", "BOOL"}:
            coerced = coerce_boolean_wire(out[col], as_int=False)
            if not isinstance(coerced, bool):
                raise ValueError(
                    f"Redis BOOLEAN refused unrecognized value {out[col]!r} "
                    "(refuse invent via bool())"
                )
            out[col] = coerced
        elif upper in {
            "FLOAT",
            "FLOAT16",
            "FLOAT32",
            "FLOAT64",
            "DOUBLE",
            "REAL",
            "HALF",
            "HALFFLOAT",
        }:
            out[col] = coerce_float_wire(out[col], ddl_type=upper or "FLOAT")
        elif upper in {
            "DECIMAL",
            "NUMERIC",
            "NUMBER",
            "BIGNUMERIC",
            "MONEY",
            "SMALLMONEY",
        } or upper.startswith(("DECIMAL(", "NUMERIC(", "NUMBER(")):
            out[col] = coerce_decimal_wire(out[col], ddl_type=upper or "DECIMAL")
        elif upper in {
            "INTEGER",
            "INT",
            "BIGINT",
            "SMALLINT",
            "TINYINT",
            "MEDIUMINT",
            "LONG",
            "SERIAL",
            "BIGSERIAL",
            "INT2",
            "INT4",
            "INT8",
        }:
            out[col] = coerce_integer_wire(out[col], ddl_type=upper or "INTEGER")
    return out


def _redis_row_to_doc(
    target_cols: list[str],
    row: tuple | list,
) -> dict[str, Any]:
    """Build Redis JSON doc with null-polarity honesty.

    * ``DF_MISSING`` / STOP_COLUMN → omit key (sparse merge keeps prior JSON)
    * ``None`` / ``SQL_NULL_SENTINEL`` → JSON ``null`` (explicit wipe)
    """
    from services.value_serializer import SQL_NULL_SENTINEL, is_missing_sentinel

    doc: dict[str, Any] = {}
    for c, v in zip(target_cols, row):
        if is_missing_sentinel(v):
            continue
        if v is None or v == SQL_NULL_SENTINEL:
            doc[c] = None
        else:
            doc[c] = v
    return doc


# Thin aliases — tests/engine may import these names from the writer module.
_infer_redis_conflict_columns = infer_redis_conflict_columns
_pick_redis_identity_column = pick_redis_identity_column


def _resolve_redis_key_id(
    doc: dict[str, Any],
    target_cols: list[str],
    conflict_columns: list[str] | None,
    row_index: int,
) -> tuple[str | None, str]:
    """Return (key_id, identity_column) — None key_id means identity missing.

    Identity ranking matches Validate via ``services.primary_key`` (never prefer
    ``capital`` over ``code``).
    """
    del row_index  # retained for call-site compatibility / future diagnostics
    cols = list(conflict_columns or [])
    if not cols:
        picked = pick_redis_identity_column(list(target_cols))
        cols = [picked] if picked else []
    if not cols:
        return None, ""

    parts: list[str] = []
    for col in cols:
        val = doc.get(col)
        from services.cdc_identity import is_present_cdc_row_key

        if not is_present_cdc_row_key(val):
            return None, col
        parts.append(str(val))
    return "|".join(parts), cols[0]


def _clear_redis_prefix(client: Any, prefix: str) -> None:
    """Delete all existing keys under ``prefix:*`` for a full-refresh overwrite."""
    if not prefix:
        return
    pattern = f"{prefix}:*"
    # Delete in small chunks to avoid blocking Redis on large keyspaces.
    batch: list[Any] = []
    for key in client.scan_iter(match=pattern, count=500):
        batch.append(key)
        if len(batch) >= 1000:
            client.delete(*batch)
            batch.clear()
    if batch:
        client.delete(*batch)


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
    conflict_columns: list[str] | None = None,
    write_mode: str = "upsert",
    sync_mode: str = "",
    **_kwargs: Any,
) -> WriteResult:
    del create_table, backfill_new_fields
    file_batch_idx = int(_kwargs.pop("file_batch_idx", 0) or 0)
    policy = transform_error_policy(error_policy)
    prefix = table_name or schema or "dataflow"
    cfg = {
        "host": host,
        "port": port,
        "database": database,
        "username": username,
        "password": password,
        "connection_string": connection_string,
        "ssl": ssl,
    }
    target_cols, logical_types = resolve_target_columns(mappings, column_types, preserve_case=True)
    from connectors.writer_common import resolve_studio_or_map_dest_types

    live_dest = _kwargs.get("destination_column_types")
    dest_types, studio_err = resolve_studio_or_map_dest_types(
        target_cols,
        mappings,
        column_types,
        logical_types=logical_types,
        studio_types=live_dest if isinstance(live_dest, dict) else None,
        product="Redis",
    )

    # A full refresh deletes every key under the prefix before writing, so the
    # shapes currently there are not a schema this write has to respect — unlike
    # a SQL TRUNCATE, which keeps the columns and their declared types. Binding
    # against them refused overwrites onto a prefix whose old documents simply
    # held different fields.
    replaces_keyspace = is_overwrite_sync(sync_mode) or write_mode in {
        "overwrite",
        "replace",
        "truncate",
    }

    # Connect before Map bind — sample existing JSON docs under prefix so live
    # INTEGER/BOOL carriers win over Map VARCHAR (empty→null invent cliff).
    client = _redis_client(cfg)
    key_hint = _redis_prefix_key_count_hint(client, prefix)
    errors: list[str] = []
    rejected_details: list[dict] = []
    mapped_data_cols = [c for c in target_cols if c]
    studio_live = isinstance(live_dest, dict) and all(
        str(live_dest.get(c) or "").strip() for c in mapped_data_cols
    )
    # Empty prefix: partial Studio must not soft-bind Map VARCHAR.
    if studio_err and key_hint == 0:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=prefix,
            target_schema=f"db{database or 0}",
            checksum="",
            chunks_completed=0,
            error=studio_err,
        )
    if key_hint < 0 and mapped_data_cols:
        # Probe failure must never take the empty-prefix Map path — even when
        # Studio typed all fields (unknown populated keyspace invent cliff).
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=prefix,
            target_schema=f"db{database or 0}",
            checksum="",
            chunks_completed=0,
            error=(
                f"Redis prefix {prefix!r} keyspace probe failed — refuse Map "
                "VARCHAR bind without live JSON types (empty→null invent risk). "
                "Re-check Redis connectivity and retry."
            ),
        )
    if key_hint > 0:
        physical, docs_sampled = _fetch_redis_physical_types(client, prefix, target_cols)
        if docs_sampled <= 0 and mapped_data_cols and not studio_live:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=prefix,
                target_schema=f"db{database or 0}",
                checksum="",
                chunks_completed=0,
                error=(
                    f"Redis prefix {prefix!r} has existing keys but live JSON "
                    "types were unavailable — refuse Map VARCHAR bind "
                    "(empty→null invent risk). Re-run destination schema "
                    "introspect and retry."
                ),
            )
        # Partial JSON sample: Studio may fill gaps; else require_physical
        # (same bar as Mongo/Dynamo — never soft-bind Map VARCHAR on missing fields).
        if mapped_data_cols and docs_sampled > 0 and not replaces_keyspace:
            from connectors.writer_common import require_physical_types_for_existing_table

            effective_physical = dict(physical)
            if isinstance(live_dest, dict):
                for c in mapped_data_cols:
                    if (
                        effective_physical.get(c)
                        or effective_physical.get(str(c).lower())
                        or effective_physical.get(str(c).upper())
                    ):
                        continue
                    st = str(live_dest.get(c) or "").strip()
                    if st:
                        effective_physical[c] = st
            phys_err = require_physical_types_for_existing_table(
                table_existed=True,
                physical=effective_physical,
                dialect_label="Redis",
                target_cols=mapped_data_cols,
            )
            if phys_err:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=prefix,
                    target_schema=f"db{database or 0}",
                    checksum="",
                    chunks_completed=0,
                    error=phys_err,
                )
            physical = effective_physical
        _force_remap = bool(studio_err)
        remat = _redis_rematerialize_if_physical_differs(
            physical=physical,
            dest_types=dest_types,
            target_cols=target_cols,
            headers=headers,
            data_rows=data_rows,
            mappings=mappings,
            column_types=column_types,
            logical_types=logical_types,
            policy=policy,
            conflict_columns=conflict_columns,
            destination_column_nullability=_kwargs.get(
                "destination_column_nullability"
            ),
            force_remap=_force_remap,
        )
        if remat is not None:
            mapped_rows, errors, rejected_details, dest_types = remat
        elif _force_remap:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=prefix,
                target_schema=f"db{database or 0}",
                checksum="",
                chunks_completed=0,
                error=(
                    "Redis live JSON carriers incomplete for mapped fields — "
                    "refuse Map VARCHAR rematerialize invent. Re-run "
                    "destination schema introspect and retry."
                ),
            )
        else:
            mapped_rows, errors, rejected_details = build_mapped_rows_with_details(
                headers=headers,
                data_rows=data_rows,
                mappings=mappings,
                target_cols=target_cols,
                column_types=column_types,
                dest_types=dest_types,
                preserve_case=True,
                error_policy=policy,
                dest_kind="redis",
                destination_pk_columns=list(conflict_columns or []) or None,
                destination_column_nullability=_kwargs.get(
                    "destination_column_nullability"
                ),
            )
    else:
        mapped_rows, errors, rejected_details = build_mapped_rows_with_details(
            headers=headers,
            data_rows=data_rows,
            mappings=mappings,
            target_cols=target_cols,
            column_types=column_types,
            dest_types=dest_types,
            preserve_case=True,
            error_policy=policy,
            dest_kind="redis",
            destination_pk_columns=list(conflict_columns or []) or None,
            destination_column_nullability=_kwargs.get(
                "destination_column_nullability"
            ),
        )

    from connectors.writer_common import apply_write_quarantine_matrix, reject_on_strict_policy

    # Partial Studio: never soft-fill quarantine carriers from Map logicals
    # after force_remap (empty→NULL invent on typed Redis JSON fields).
    if studio_err:
        missing = [
            c
            for c in target_cols
            if c and not str(dest_types.get(c) or "").strip()
        ]
        if missing:
            sample = ", ".join(repr(c) for c in missing[:12])
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=prefix,
                target_schema=f"db{database or 0}",
                checksum="",
                chunks_completed=0,
                error=(
                    f"Redis mapped field(s) {sample} lack live carriers under "
                    "partial Studio — refuse Map logical quarantine invent. "
                    "Re-run destination schema introspect and retry."
                ),
            )
        tgt_types = [str(dest_types.get(c) or "").strip() for c in target_cols]
    else:
        tgt_types = [
            str(
                dest_types.get(c)
                or (logical_types[i] if i < len(logical_types) else "")
                or ""
            )
            for i, c in enumerate(target_cols)
        ]
    mapped_rows = apply_write_quarantine_matrix(
        mapped_rows,
        target_cols,
        tgt_types,
        rejected_details,
        policy,
        dialect_label="Redis",
        mappings=list(mappings or []) or None,
    )
    _map_abort = reject_on_strict_policy(policy, rejected_details, "Redis", errors)
    if _map_abort:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=prefix,
            target_schema=f"db{database or 0}",
            checksum="",
            chunks_completed=0,
            error=_map_abort or f"Transform errors: {'; '.join(errors[:3])}",
            warnings=errors[:10],
            rejected_rows=len({d.get("row") for d in rejected_details if d.get("row") is not None}),
            rejected_details=list(rejected_details),
        )

    try:
        conflict = _infer_redis_conflict_columns(target_cols, mappings, conflict_columns)
    except ValueError as exc:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=prefix,
            target_schema=f"db{database or 0}",
            checksum="",
            chunks_completed=0,
            error=str(exc),
            warnings=errors[:10],
            rejected_rows=len({d.get("row") for d in rejected_details if d.get("row") is not None}),
            rejected_details=list(rejected_details),
        )
    from connectors.writer_common import partition_dense_upsert_rows

    if conflict:
        mapped_rows = partition_dense_upsert_rows(
            mapped_rows,
            conflict,
            target_cols=target_cols,
            rejected_details=rejected_details,
            policy=policy,
        )
    try:
        # Full-refresh overwrite must replace the destination keyspace once per job,
        # not once per chunk. Only the first chunk clears stale keys.
        if file_batch_idx in (0, 1) and replaces_keyspace:
            _clear_redis_prefix(client, prefix)

        written = 0
        seen_keys: dict[str, int] = {}
        for i, row in enumerate(mapped_rows):
            # DF_MISSING omit vs explicit SQL NULL → JSON null (no stale merge invent).
            doc = _redis_row_to_doc(target_cols, row)
            try:
                doc = _normalize_redis_typed_doc(doc, target_cols, tgt_types)
            except (ValueError, TypeError) as cell_exc:
                msg = format_exception_message(cell_exc)
                if policy == "fail":
                    return WriteResult(
                        ok=False,
                        rows_written=written,
                        table_name=prefix,
                        target_schema=f"db{database or 0}",
                        checksum="",
                        chunks_completed=0,
                        error=msg,
                        warnings=errors[:10],
                        rejected_rows=len({d["row"] for d in rejected_details}) + 1,
                        rejected_details=list(rejected_details)
                        + [
                            {
                                "row": i + 1,
                                "column": "",
                                "target": "",
                                "value": "",
                                "reason": msg,
                                "policy": "write_fail",
                                "chars": [],
                            }
                        ],
                    )
                rejected_details.append(
                    {
                        "row": i + 1,
                        "column": "",
                        "target": "",
                        "value": "",
                        "reason": msg,
                        "policy": "write_quarantine",
                        "chars": [],
                    }
                )
                errors.append(msg)
                continue
            key_id, id_col = _resolve_redis_key_id(doc, target_cols, conflict, row_index=i)
            if key_id is None:
                msg = (
                    f"Redis identity missing for conflict_columns={conflict}"
                    if conflict
                    else "Redis identity missing — no id-like column found in mapping"
                )
                if policy == "fail":
                    return WriteResult(
                        ok=False,
                        rows_written=written,
                        table_name=prefix,
                        target_schema=f"db{database or 0}",
                        checksum="",
                        chunks_completed=0,
                        error=msg,
                        warnings=errors[:10],
                        rejected_rows=len({d["row"] for d in rejected_details}) + 1,
                        rejected_details=list(rejected_details)
                        + [
                            {
                                "row": i + 1,
                                "column": id_col or "",
                                "target": id_col or "",
                                "value": "",
                                "reason": msg,
                                "policy": "write_fail",
                                "chars": [],
                            }
                        ],
                    )
                rejected_details.append(
                    {
                        "row": i + 1,
                        "column": id_col or "",
                        "target": id_col or "",
                        "value": "",
                        "reason": msg,
                        "policy": "write_quarantine",
                        "chars": [],
                    }
                )
                continue

            key = f"{prefix}:{sanitize_identifier(str(key_id), preserve_case=True)}"
            if key in seen_keys:
                prev = seen_keys[key]
                msg = (
                    f"Duplicate Redis key '{key}' for rows {prev + 1} and {i + 1} "
                    f"(conflict on '{id_col}'). Use a unique primary key or deduplicate the source."
                )
                return WriteResult(
                    ok=False,
                    rows_written=written,
                    table_name=prefix,
                    target_schema=f"db{database or 0}",
                    checksum="",
                    chunks_completed=0,
                    error=msg,
                    warnings=errors[:10],
                    rejected_rows=len({d["row"] for d in rejected_details}),
                    rejected_details=list(rejected_details),
                )
            seen_keys[key] = i

            try:
                # Pre-sanitize so extreme Decimals never raise mid-dumps.
                from connectors.writer_common import row_has_missing_sentinel

                # Sparse STOP_COLUMN / CDC / NULL omit: merge onto existing JSON so
                # omitted fields are not wiped by a full-key SET.
                needs_merge = (
                    not replaces_keyspace
                    and (
                        row_has_missing_sentinel(row)
                        or len(doc) < len(target_cols)
                    )
                )
                if needs_merge:
                    existing_raw = client.get(key)
                    if existing_raw:
                        try:
                            existing = json.loads(existing_raw)
                        except (TypeError, ValueError, json.JSONDecodeError):
                            existing = None
                        if isinstance(existing, dict):
                            doc = {**existing, **doc}
                safe_doc = sanitize_json_value(doc)
                client.set(key, json.dumps(safe_doc, default=json_default, allow_nan=False))
                written += 1
            except Exception as cell_exc:
                msg = format_exception_message(cell_exc)
                if policy == "fail":
                    return WriteResult(
                        ok=False,
                        rows_written=written,
                        table_name=prefix,
                        target_schema=f"db{database or 0}",
                        checksum="",
                        chunks_completed=0,
                        error=msg,
                        warnings=errors[:10],
                        rejected_rows=len({d["row"] for d in rejected_details}) + 1,
                        rejected_details=list(rejected_details)
                        + [
                            {
                                "row": i + 1,
                                "column": id_col,
                                "target": id_col,
                                "value": str(key_id)[:120],
                                "reason": msg,
                                "policy": "write_fail",
                                "chars": [],
                            }
                        ],
                    )
                rejected_details.append(
                    {
                        "row": i + 1,
                        "column": id_col,
                        "target": id_col,
                        "value": str(key_id)[:120],
                        "reason": msg,
                        "policy": "write_quarantine",
                        "chars": [],
                    }
                )
                errors.append(msg)
        if on_checkpoint:
            on_checkpoint(1, 1, written)
        # Re-check FAIL_JOB / strict after mid-write identity/type rejects.
        _final_abort = reject_on_strict_policy(policy, rejected_details, "Redis")
        if _final_abort:
            return WriteResult(
                ok=False,
                # Honest count: keys may already be written before abort-class reject.
                rows_written=written,
                table_name=prefix,
                target_schema=f"db{database or 0}",
                checksum="",
                chunks_completed=0,
                error=_final_abort,
                warnings=errors[:10],
                rejected_rows=len({d["row"] for d in rejected_details}),
                rejected_details=list(rejected_details),
            )
        return WriteResult(
            ok=True,
            rows_written=written,
            table_name=prefix,
            target_schema=f"db{database or 0}",
            checksum=row_checksum(
                mapped_rows,
                target_cols,
                dest_db_type="redis",
            ),
            chunks_completed=1,
            warnings=errors[:10],
            rejected_rows=len({d["row"] for d in rejected_details}),
            rejected_details=list(rejected_details),
            meta=gate8_writer_meta(mapped_rows, target_cols),
        )
    except Exception as exc:
        return WriteResult(
            ok=False,
            rows_written=written if "written" in locals() else 0,
            table_name=prefix,
            target_schema=f"db{database or 0}" if "database" in locals() else "",
            checksum="",
            chunks_completed=0,
            error=format_exception_message(exc),
            rejected_details=list(rejected_details) if "rejected_details" in locals() else [],
            rejected_rows=len(rejected_details) if "rejected_details" in locals() else 0,
        )
    finally:
        client.close()
