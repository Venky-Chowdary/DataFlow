"""MongoDB bulk writer — CSV file to collection with checkpoint batches."""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Callable
from dataclasses import dataclass
from decimal import InvalidOperation
from typing import Any

from services.brand_env import getenv_brand

from connectors.writer_common import (
    CHUNK_SIZE,
    DF_LSN_COL,
    _coerced_null_row_count,
    _rejected_row_count,
    build_mapped_rows_with_details,
    compare_lsn,
    gate8_writer_meta,
    lsn_is_newer,
    resolve_target_columns,
    row_checksum,
    reject_on_strict_policy,
    sanitize_identifier,
    transform_error_policy,
)
from connectors.writer_common import (
    WriteResult as _WriteResult,
)

logger = logging.getLogger(__name__)

# MongoDB commands handle ~1000-document batches most reliably through proxies
# and serverless tiers. 20k-document single calls can hit socket/proxy limits.
MONGO_WRITE_BATCH_SIZE = int(getenv_brand("MONGO_BATCH_SIZE", "1000"))


def _fetch_mongo_physical_types(
    coll: Any,
    target_cols: list[str],
    *,
    sample_limit: int = 50,
) -> tuple[dict[str, str], Exception | None]:
    """Sample live BSON types for mapped fields (majority vote).

    Empty collections return ``({}, None)`` — Map stamps remain authoritative
    for create-new. Probe failures return ``({}, exc)`` so writers can
    fail-closed on auth (never soft-empty → Map invent).
    """
    from services.schema_introspect import (
        _finalize_mongodb_type,
        _sample_logical_type,
    )

    wanted = {str(c) for c in target_cols if c}
    if not wanted:
        return {}, None
    type_counts: dict[str, dict[str, int]] = {c: {} for c in wanted}
    try:
        for doc in coll.find().limit(int(sample_limit)):
            if not isinstance(doc, dict):
                continue
            for key, val in doc.items():
                if key not in wanted:
                    continue
                inferred = _sample_logical_type(val, key)
                if not inferred or val is None:
                    continue
                tc = type_counts[key]
                tc[inferred] = int(tc.get(inferred, 0)) + 1
    except Exception as exc:
        logger.debug("Mongo physical type sample failed", exc_info=True)
        return {}, exc
    physical: dict[str, str] = {}
    for col, counts in type_counts.items():
        if not counts:
            continue
        carrier = _finalize_mongodb_type(counts)
        if carrier:
            physical[col] = carrier
            physical.setdefault(col.lower(), carrier)
            physical.setdefault(col.upper(), carrier)
    return physical, None


def _mongo_rematerialize_if_physical_differs(
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
) -> tuple[list[tuple], list[str], list[dict], dict[str, str]] | None:
    """Rebuild mapped rows from source when live BSON carriers differ from Map."""
    from connectors.writer_common import resolve_mapping_dest_types

    if not physical:
        return None
    live_dest_types = resolve_mapping_dest_types(
        target_cols,
        mappings,
        column_types,
        logical_types=logical_types,
        live_types=physical,
    )
    carriers_differ = any(
        str(dest_types.get(c) or "").strip().upper()
        != str(live_dest_types.get(c) or "").strip().upper()
        for c in target_cols
    )
    if not carriers_differ:
        return None
    mapped_rows, transform_errors, rejected_details = build_mapped_rows_with_details(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=target_cols,
        column_types=column_types,
        dest_types=live_dest_types,
        error_policy=policy,
        preserve_case=True,
        dest_kind="mongodb",
        destination_pk_columns=list(conflict_columns or []) or None,
        destination_column_nullability=destination_column_nullability,
    )
    return (
        mapped_rows,
        list(transform_errors or []),
        rejected_details,
        live_dest_types,
    )


@dataclass
class WriteResult(_WriteResult):
    driver: str = "pymongo"


def _connection_string(
    host: str,
    port: int,
    username: str,
    password: str,
    connection_string: str,
    database: str = "",
    ssl: bool = False,
    auth_source: str = "",
) -> str:
    from connectors.mongodb_common import normalize_mongodb_connection_string

    return normalize_mongodb_connection_string(
        connection_string,
        database=database,
        host=host,
        port=port,
        username=username,
        password=password,
        ssl=ssl,
        auth_source=auth_source,
    )


def _idempotent_insert_many(coll, docs: list[dict]) -> int:
    """Insert documents; duplicate-key errors count as already-present.

    Documents without ``_id`` are refused — never invent a content-hash PK
    (silent identity invent collapses natural keys and hides collisions).
    Map ``_id`` or use upsert with ``conflict_columns``.
    """
    from pymongo.errors import BulkWriteError

    missing_id = [i for i, doc in enumerate(docs) if "_id" not in doc]
    if missing_id:
        raise ValueError(
            f"MongoDB insert refused {len(missing_id)} document(s) without `_id` — "
            "refuse content-hash PK invent (map `_id` or upsert with conflict_columns)"
        )

    try:
        result = coll.insert_many(docs, ordered=False)
        return len(result.inserted_ids)
    except BulkWriteError as bwe:
        details = bwe.details or {}
        write_errors = details.get("writeErrors", [])
        non_dup = [e for e in write_errors if e.get("code") != 11000]
        if non_dup:
            raise
        # All errors were duplicate keys; those rows are already present.
        return len(docs)


def write_mapped_rows(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    schema: str,  # usually not used in MongoDB, but we keep signature consistent
    connection_string: str,
    ssl: bool,
    table_name: str,  # collection name
    headers: list[str],
    data_rows: list[list[str]],
    mappings: list[dict],
    column_types: dict[str, str],
    on_checkpoint: Callable[[int, int, int], None] | None = None,
    create_table: bool = True,
    error_policy: str | None = None,
    write_mode: str = "insert",
    conflict_columns: list[str] | None = None,
    backfill_new_fields: bool = False,
    auth_source: str = "",
    **_kwargs: Any,
) -> WriteResult:
    del backfill_new_fields
    try:
        import pymongo
        from pymongo import MongoClient  # noqa: F401
    except ImportError:
        from connectors.driver_guard import require_driver, stub_writes_allowed
        from connectors.stub_writer import simulate_stub_write

        if not stub_writes_allowed():
            return WriteResult(
                ok=False, rows_written=0, table_name=table_name, target_schema=schema or "db",
                checksum="", chunks_completed=0,
                error=require_driver("pymongo", "pymongo"),
                driver="none",
            )
        rows, checksum, chunks = simulate_stub_write(
            data_rows=data_rows, table_name=table_name, target_schema=schema or "db",
            on_checkpoint=on_checkpoint,
        )
        return WriteResult(
            ok=True, rows_written=rows, table_name=table_name, target_schema=schema or "db",
            checksum=checksum, chunks_completed=chunks, driver="stub",
        )

    target_cols, logical_types = resolve_target_columns(mappings, column_types, preserve_case=True)
    if not target_cols:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema=schema or "db",
            checksum="",
            chunks_completed=0,
            error="No column mappings",
        )

    collection_name = sanitize_identifier(table_name, preserve_case=True)
    db_name = database or schema or "test"
    # Prefer Studio-probed live field types over Map stamps (invent cliff).
    from connectors.writer_common import resolve_mapping_dest_types

    live_dest = _kwargs.get("destination_column_types")
    dest_types = resolve_mapping_dest_types(
        target_cols,
        mappings,
        column_types,
        logical_types=logical_types,
        live_types=live_dest if isinstance(live_dest, dict) else None,
    )
    policy = transform_error_policy(error_policy)

    try:
        from connectors.mongodb_common import _mongo_client

        conn_str = _connection_string(host, port, username, password, connection_string, database, ssl, auth_source)
        # Reuse a cached MongoClient per connection string to avoid paying the
        # connection handshake cost on every batch.
        client = _mongo_client(conn_str)

        db = client[db_name]
        existing = set(db.list_collection_names(filter={"name": collection_name}))
        collection_existed = collection_name in existing
        if not create_table and not collection_existed:
            # Mongo creates collections on first write — deny-create must probe first.
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=collection_name,
                target_schema=db_name,
                checksum="",
                chunks_completed=0,
                error=(
                    f"MongoDB collection {collection_name!r} is missing "
                    "and create_table is disabled"
                ),
            )
        coll = db[collection_name]

        # Existing collection: sample BSON carriers and rematerialize when they
        # differ from Map/Studio stamps (VARCHAR→Decimal128 invent cliff).
        if collection_existed:
            from connectors.saas_common import is_auth_error

            physical, sample_exc = _fetch_mongo_physical_types(coll, target_cols)
            if sample_exc is not None and is_auth_error(sample_exc):
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=collection_name,
                    target_schema=db_name,
                    checksum="",
                    chunks_completed=0,
                    error=(
                        f"MongoDB BSON sample auth failed: {sample_exc} — "
                        "refuse Map VARCHAR bind (empty→NULL invent risk)."
                    ),
                )
            if not physical:
                # Empty sample on a non-empty / unknown-count collection — refuse
                # Map-only bind unless Studio typed every mapped field.
                try:
                    doc_count = int(coll.estimated_document_count())
                except Exception:
                    try:
                        doc_count = int(coll.count_documents({}))
                    except Exception:
                        doc_count = -1
                mapped_data_cols = [c for c in target_cols if c and c != "_id"]
                studio_live = isinstance(live_dest, dict) and all(
                    str(live_dest.get(c) or "").strip() for c in mapped_data_cols
                )
                # Unknown count (-1) on existing collection: fail closed like SQL.
                if (doc_count > 0 or doc_count < 0) and mapped_data_cols and not studio_live:
                    return WriteResult(
                        ok=False,
                        rows_written=0,
                        table_name=collection_name,
                        target_schema=db_name,
                        checksum="",
                        chunks_completed=0,
                        error=(
                            f"MongoDB collection {collection_name!r} exists but "
                            "live BSON types were unavailable for all mapped fields — "
                            "refuse Map VARCHAR bind (empty→NULL invent risk). "
                            "Re-run destination schema introspect and retry."
                        ),
                    )
            remat = _mongo_rematerialize_if_physical_differs(
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
            )
            if remat is not None:
                mapped_rows, transform_errors, rejected_details, dest_types = remat
            else:
                mapped_rows, transform_errors, rejected_details = (
                    build_mapped_rows_with_details(
                        headers=headers,
                        data_rows=data_rows,
                        mappings=mappings,
                        target_cols=target_cols,
                        column_types=column_types,
                        dest_types=dest_types,
                        error_policy=policy,
                        preserve_case=True,
                        dest_kind="mongodb",
                        destination_pk_columns=list(conflict_columns or []) or None,
                        destination_column_nullability=_kwargs.get(
                            "destination_column_nullability"
                        ),
                    )
                )
        else:
            mapped_rows, transform_errors, rejected_details = build_mapped_rows_with_details(
                headers=headers,
                data_rows=data_rows,
                mappings=mappings,
                target_cols=target_cols,
                column_types=column_types,
                dest_types=dest_types,
                error_policy=policy,
                preserve_case=True,
                dest_kind="mongodb",
                destination_pk_columns=list(conflict_columns or []) or None,
                destination_column_nullability=_kwargs.get("destination_column_nullability"),
            )
        coerced_null_rows = _coerced_null_row_count(rejected_details, policy)

        tgt_types = [
            str(dest_types.get(c, logical_types[i] if i < len(logical_types) else "VARCHAR") or "VARCHAR")
            for i, c in enumerate(target_cols)
        ]
        from connectors.writer_common import apply_write_quarantine_matrix

        mapped_rows = apply_write_quarantine_matrix(
            mapped_rows,
            target_cols,
            tgt_types,
            rejected_details,
            policy,
            dialect_label="MongoDB",
            mappings=mappings,
        )
        rejected_rows = _rejected_row_count(data_rows, mapped_rows, rejected_details, policy)
        # FAIL_JOB / strict abort after matrix — matrix may add abort-class rejects.
        _map_abort = reject_on_strict_policy(policy, rejected_details, "MongoDB", transform_errors)
        if _map_abort:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=collection_name,
                target_schema=db_name,
                checksum="",
                chunks_completed=0,
                error=_map_abort or f"Transform errors: {'; '.join(transform_errors[:3])}",
                rejected_rows=rejected_rows,
                rejected_details=list(rejected_details),
                warnings=transform_errors,
            )

        from datetime import date as _date
        from datetime import datetime as _datetime
        from datetime import time as _time

        from bson.binary import Binary
        from bson.decimal128 import Decimal128

        transform_by_col = {
            sanitize_identifier(m.get("target") or m.get("source"), preserve_case=True): (m.get("transform") or "")
            for m in mappings
        }

        def _to_bson(value: Any, stype: str, transform: str = "") -> Any:
            from services.value_serializer import is_missing_sentinel

            # Preserve DF_MISSING through coercion so sparse upsert can omit
            # the field — never convert the sentinel into a live BSON value.
            if is_missing_sentinel(value):
                return value
            if value is None:
                return None
            upper = stype.upper()
            # An explicit mapping transform overrides the inferred source type so
            # values like decimal strings are stored as the correct BSON type.
            if upper in {"FLOAT", "DOUBLE", "FLOAT64", "REAL", "FLOAT32", "FLOAT16"}:
                from connectors.sql_bind import coerce_float_wire

                return coerce_float_wire(value, ddl_type=upper or "FLOAT")
            if transform in {"decimal", "currency", "percentage"} or upper in {"DECIMAL", "NUMERIC", "NUMBER", "BIGNUMERIC"}:
                if isinstance(value, str) and not str(value).strip():
                    raise ValueError(
                        f"MongoDB DECIMAL refused empty string {value!r} "
                        "(refuse silent Decimal128 invent / field wipe)"
                    )
                from connectors.sql_bind import coerce_decimal_wire

                coerced = coerce_decimal_wire(value, ddl_type=upper or "DECIMAL")
                return Decimal128(str(coerced))
            if transform == "integer" or upper in {"INTEGER", "INT", "BIGINT", "SMALLINT", "TINYINT", "LONG", "SERIAL", "BIGSERIAL"}:
                from decimal import Decimal

                if isinstance(value, bool):
                    iv = int(value)
                elif isinstance(value, int):
                    iv = value
                elif isinstance(value, float):
                    if not value.is_integer():
                        raise ValueError(
                            f"cannot coerce non-integral float {value!r} to INTEGER "
                            "without truncation"
                        )
                    iv = int(value)
                elif isinstance(value, Decimal):
                    if value != value.to_integral_value():
                        raise ValueError(
                            f"cannot coerce non-integral decimal {value!r} to INTEGER "
                            "without truncation"
                        )
                    iv = int(value)
                elif isinstance(value, str):
                    try:
                        iv = int(value)
                    except (ValueError, TypeError) as exc:
                        raise ValueError(
                            f"MongoDB INTEGER refused {value!r} "
                            "(refuse silent string invent)"
                        ) from exc
                else:
                    try:
                        iv = int(value)
                    except (ValueError, TypeError) as exc:
                        raise ValueError(
                            f"MongoDB INTEGER refused {value!r} "
                            "(refuse silent pass-through invent)"
                        ) from exc
                # BSON supports signed 64-bit ints; fall back to Decimal128 or
                # string when a value overflows.
                if iv > 2**63 - 1 or iv < -(2**63):
                    try:
                        return Decimal128(str(iv))
                    except (InvalidOperation, ValueError, TypeError):
                        return str(iv)
                return iv
            if transform == "boolean" or upper in {"BOOLEAN", "BOOL"}:
                from connectors.sql_bind import coerce_boolean_wire

                coerced = coerce_boolean_wire(value, as_int=False)
                if not isinstance(coerced, bool):
                    raise ValueError(
                        f"MongoDB BOOLEAN refused {value!r} "
                        "(refuse invent via pass-through)"
                    )
                return coerced
            if upper == "DATE" and transform != "datetime":
                from connectors.sql_temporal import coerce_sql_temporal
                from datetime import timezone as _tz

                coerced = coerce_sql_temporal(value, "DATE")
                # DATE → BSON Date as UTC midnight (calendar date instant), never
                # leave naive for PyMongo local-TZ invent.
                if isinstance(coerced, _datetime):
                    d = coerced.date()
                    return _datetime(d.year, d.month, d.day, tzinfo=_tz.utc)
                if isinstance(coerced, _date):
                    return _datetime(
                        coerced.year, coerced.month, coerced.day, tzinfo=_tz.utc
                    )
                raise ValueError(
                    f"MongoDB DATE refused {value!r} (refuse silent pass-through invent)"
                )
            # A "DATE" carrier with a datetime transform is Mongo's single BSON
            # date, which stores a full instant. Narrowing it truncated every
            # timestamp to midnight even though the carrier could hold the time.
            if upper in {"DATE"} or upper in {
                "DATETIME", "TIMESTAMP", "TIMESTAMP_TZ", "TIMESTAMPTZ",
                "TIMESTAMP_LTZ", "TIMESTAMP_NTZ",
            }:
                from connectors.sql_temporal import coerce_sql_temporal

                coerced = coerce_sql_temporal(value, "DATETIME")
                if isinstance(coerced, _datetime):
                    # BSON Date is a UTC instant — never invent UTC on a naive
                    # wall-clock (would silently shift polarity). Require offset/Z.
                    if coerced.tzinfo is None:
                        raise ValueError(
                            "MongoDB date/time refused naive wall-clock — "
                            "provide offset/Z (refuse silent UTC invent)"
                        )
                    from datetime import timezone as _tz

                    return coerced.astimezone(_tz.utc)
                raise ValueError(
                    f"MongoDB DATETIME refused {value!r} "
                    "(refuse silent pass-through invent)"
                )
            if upper in {"BINARY", "BYTEA", "BLOB", "VARBINARY"}:
                from connectors.sql_bind import coerce_binary_wire
                from bson.binary import Binary as _Bin

                if isinstance(value, bytes):
                    return _Bin(value)
                # Fail-closed — never UTF-8-invent bytes.
                return _Bin(coerce_binary_wire(value))
            if upper in {"JSON", "OBJECT", "ARRAY", "VARIANT"}:
                if isinstance(value, (dict, list)):
                    return value
                if isinstance(value, str):
                    try:
                        def _reject(name: str) -> None:
                            raise ValueError(f"non-finite JSON constant: {name}")

                        return json.loads(value, parse_constant=_reject)
                    except (json.JSONDecodeError, TypeError, ValueError) as exc:
                        raise ValueError(
                            f"MongoDB JSON refused {value!r} "
                            "(refuse silent string invent)"
                        ) from exc
                raise ValueError(
                    f"MongoDB JSON refused {value!r} "
                    "(refuse silent pass-through invent)"
                )
            if upper in {"UUID", "UNIQUEIDENTIFIER", "GUID"}:
                from connectors.sql_bind import coerce_uuid_wire

                return coerce_uuid_wire(value)
            if upper in {"OBJECTID", "OBJECT_ID"}:
                from bson.objectid import ObjectId as _Oid

                if isinstance(value, _Oid):
                    return value
                text = str(value).strip()
                if len(text) == 24 and _Oid.is_valid(text):
                    return _Oid(text)
                raise ValueError(
                    f"cannot coerce {value!r} to MongoDB ObjectId "
                    "(expect 24-char hex)"
                )
            if upper == "TIME":
                return str(value)
            return value

        # BSON coercion is fail-closed (ObjectId hex, non-integral ints, binary
        # wire). Letting a ValueError escape aborted the whole write with
        # rows_written=0, so one bad cell lost every good row. Hold the row out
        # and record why instead — quarantine, never silent drop, never abort.
        from connectors.writer_common import (
            append_write_quarantine_detail,
            partition_dense_upsert_rows,
        )
        from services.value_serializer import cell_to_string as _cell_to_string

        if write_mode == "upsert" and conflict_columns:
            pk_for_part = [str(c) for c in conflict_columns if str(c) and str(c) in target_cols]
            if pk_for_part:
                # Partition before BSON coerce / same-key fold — empty keys must
                # never collapse onto None/"" and mass-touch destination docs.
                mapped_rows = partition_dense_upsert_rows(
                    mapped_rows,
                    pk_for_part,
                    target_cols=target_cols,
                    rejected_details=rejected_details,
                    policy=policy,
                )

        typed_rows: list[tuple] = []
        for row_idx, row in enumerate(mapped_rows):
            cells: list[Any] = []
            hold_out = False
            # Coerce with live-aware dest types (tgt_types), not Map-only
            # logical_types — otherwise quarantine can pass live DDL while
            # BSON invents a different wire shape (Bugbot invent cliff).
            for i, (v, t) in enumerate(zip(row, tgt_types)):
                col = target_cols[i] if i < len(target_cols) else f"col_{i}"
                try:
                    cells.append(_to_bson(v, t, transform_by_col.get(col, "")))
                except (ValueError, TypeError, InvalidOperation) as exc:
                    append_write_quarantine_detail(
                        rejected_details,
                        {
                            "row": row_idx + 1,
                            "column": col,
                            "target": col,
                            "value": _cell_to_string(v)[:120],
                            "reason": f"BSON coercion rejected the value — {exc}",
                            "policy": "coerce_null" if policy == "coerce_null" else "write_quarantine",
                            "chars": [],
                        },
                        mapped_row=list(row),
                        target_cols=target_cols,
                    )
                    if policy == "coerce_null":
                        cells.append(None)
                        continue
                    hold_out = True
                    break
            if hold_out:
                continue
            typed_rows.append(tuple(cells))

        _bson_abort = reject_on_strict_policy(
            policy, rejected_details, "MongoDB", transform_errors=None
        )
        if _bson_abort:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=collection_name,
                target_schema=db_name,
                checksum="",
                chunks_completed=0,
                error=_bson_abort,
                rejected_rows=_rejected_row_count(
                    data_rows, typed_rows, rejected_details, policy
                ),
                rejected_details=rejected_details,
            )
        rejected_rows = _rejected_row_count(data_rows, typed_rows, rejected_details, policy)

        total = len(typed_rows)
        # MongoDB writes are split into smaller server-friendly batches.
        mongo_batch_size = max(1, min(MONGO_WRITE_BATCH_SIZE, CHUNK_SIZE))
        chunks = max(1, (total + mongo_batch_size - 1) // mongo_batch_size)
        written = 0
        skipped_total = 0

        for chunk_idx in range(chunks):
            start = chunk_idx * mongo_batch_size
            batch = typed_rows[start : start + mongo_batch_size]
            if not batch:
                break

            # Convert row tuples to documents; omit missing-field sentinels.
            # Track sparse rows so upsert uses $set (never ReplaceOne — that
            # would delete destination keys for fields absent in the CDC image).
            from services.value_serializer import is_missing_sentinel

            from connectors.writer_common import row_has_missing_sentinel

            docs: list[dict[str, Any]] = []
            sparse_flags: list[bool] = []
            for row in batch:
                sparse = row_has_missing_sentinel(row)
                sparse_flags.append(sparse)
                docs.append(
                    {
                        k: v
                        for k, v in dict(zip(target_cols, row)).items()
                        if not is_missing_sentinel(v)
                    }
                )

            # Preserve MongoDB ObjectId identity when a 24-char hex _id is present.
            from bson.objectid import ObjectId

            for doc in docs:
                if "_id" in doc and isinstance(doc["_id"], str):
                    v = doc["_id"]
                    if len(v) == 24 and ObjectId.is_valid(v):
                        try:
                            doc["_id"] = ObjectId(v)
                        except Exception as exc:
                            logger.warning("Exception suppressed: %s", exc, exc_info=exc)

            if write_mode == "upsert" and conflict_columns:
                from pymongo import ReplaceOne, UpdateOne

                requested_keys = [str(c) for c in conflict_columns if str(c)]
                pk_cols = [c for c in requested_keys if c in target_cols]
                missing_keys = [c for c in requested_keys if c not in target_cols]
                if not pk_cols or missing_keys:
                    return WriteResult(
                        ok=False,
                        rows_written=written,
                        table_name=collection_name,
                        target_schema=db_name,
                        checksum="",
                        chunks_completed=chunk_idx,
                        error=(
                            "MongoDB upsert requires mapped conflict columns; missing: "
                            f"{missing_keys or requested_keys}"
                        ),
                        rejected_rows=len(rejected_details),
                        rejected_details=list(rejected_details),
                        warnings=transform_errors,
                    )

                # Deduplicate within the batch on the conflict key, keeping the
                # highest _df_lsn so ``bulk_write(ordered=False)`` does not apply
                # same-PK updates in an undefined order.
                # Pair each doc with its sparse flag through dedupe.
                paired = list(zip(docs, sparse_flags))
                if DF_LSN_COL in target_cols:
                    best_docs: dict[tuple, tuple[dict[str, Any], bool]] = {}
                    for doc, sparse in paired:
                        key = tuple(doc.get(c) for c in pk_cols)
                        prev = best_docs.get(key)
                        if prev is None or compare_lsn(doc.get(DF_LSN_COL), prev[0].get(DF_LSN_COL)) >= 0:
                            best_docs[key] = (doc, sparse)
                    paired = list(best_docs.values())
                else:
                    seen_docs: dict[tuple, tuple[dict[str, Any], bool]] = {}
                    for doc, sparse in paired:
                        key = tuple(doc.get(c) for c in pk_cols)
                        seen_docs[key] = (doc, sparse)
                    paired = list(seen_docs.values())
                docs = [p[0] for p in paired]
                sparse_flags = [p[1] for p in paired]

                # Pre-fetch existing _df_lsn values for the batch keys so stale
                # redelivery cannot regress destination state under CDC.
                existing_lsn: dict[tuple, Any] = {}
                use_lsn_guard = DF_LSN_COL in target_cols
                if use_lsn_guard:
                    try:
                        batch_filters = []
                        for doc in docs:
                            filt = {c: doc.get(c) for c in pk_cols}
                            if not any(v in (None, "") for v in filt.values()):
                                batch_filters.append(filt)
                        if batch_filters:
                            projection = {DF_LSN_COL: 1}
                            projection.update({c: 1 for c in pk_cols})
                            for existing in coll.find({"$or": batch_filters}, projection):
                                key = tuple(existing.get(c) for c in pk_cols)
                                existing_lsn[key] = existing.get(DF_LSN_COL)
                    except pymongo.errors.PyMongoError as exc:
                        # Fail closed: without the prior LSN map we cannot prove
                        # idempotency, so we must not write this batch.
                        return WriteResult(
                            ok=False,
                            rows_written=written,
                            rows_skipped=skipped_total,
                            table_name=collection_name,
                            target_schema=db_name,
                            checksum="",
                            chunks_completed=chunk_idx,
                            error=(
                                "MongoDB CDC LSN prefetch failed; cannot guarantee "
                                f"idempotent delivery: {exc}"
                            ),
                            rejected_rows=len(rejected_details),
                            rejected_details=list(rejected_details),
                            warnings=transform_errors,
                        )

                ops = []
                skipped_stale = 0
                for doc_idx, (doc, sparse) in enumerate(zip(docs, sparse_flags)):
                    filt = {c: doc.get(c) for c in pk_cols}
                    if any(v in (None, "") for v in filt.values()):
                        missing_cols = [c for c, v in filt.items() if v in (None, "")]
                        detail = {
                            "row": start + doc_idx + 1,
                            "column": ",".join(missing_cols),
                            "target": ",".join(pk_cols),
                            "value": "",
                            "reason": (
                                "Mongo upsert skipped — incomplete conflict key "
                                f"{missing_cols}; refuse silent omit"
                            ),
                            "policy": "write_fail" if policy == "fail" else "write_quarantine",
                            "chars": [],
                        }
                        rejected_details.append(detail)
                        if policy == "fail":
                            return WriteResult(
                                ok=False,
                                rows_written=written,
                                table_name=collection_name,
                                target_schema=db_name,
                                checksum="",
                                chunks_completed=chunk_idx,
                                error=detail["reason"],
                                rejected_rows=len(rejected_details),
                                rejected_details=list(rejected_details),
                                warnings=transform_errors,
                            )
                        continue
                    if use_lsn_guard:
                        incoming_lsn = doc.get(DF_LSN_COL)
                        key = tuple(doc.get(c) for c in pk_cols)
                        prior_lsn = existing_lsn.get(key)
                        if incoming_lsn is not None and not lsn_is_newer(incoming_lsn, prior_lsn):
                            skipped_stale += 1
                            continue
                    if sparse:
                        # Partial CDC image: $set present fields only.
                        # ReplaceOne would delete omitted destination keys.
                        ops.append(UpdateOne(filt, {"$set": doc}, upsert=True))
                    else:
                        ops.append(ReplaceOne(filt, doc, upsert=True))
                if ops:
                    coll.bulk_write(ops, ordered=False)
                written += len(ops)
                skipped_total += skipped_stale
            else:
                written += _idempotent_insert_many(coll, docs)
            if on_checkpoint:
                on_checkpoint(chunk_idx + 1, chunks, written + skipped_total)

        # Re-check FAIL_JOB / strict after mid-write incomplete-PK quarantine.
        _final_abort = reject_on_strict_policy(
            policy, rejected_details, "MongoDB", transform_errors
        )
        if _final_abort:
            return WriteResult(
                ok=False,
                # Honest count: earlier chunks may already be committed.
                rows_written=written,
                rows_skipped=skipped_total,
                table_name=collection_name,
                target_schema=db_name,
                checksum="",
                chunks_completed=chunks,
                error=_final_abort,
                rejected_rows=len(rejected_details),
                rejected_details=list(rejected_details),
                coerced_null_rows=coerced_null_rows,
                warnings=transform_errors,
            )
        return WriteResult(
            ok=True,
            rows_written=written,
            rows_skipped=skipped_total,
            table_name=collection_name,
            target_schema=db_name,
            checksum=row_checksum(
                mapped_rows,
                target_cols,
                dest_db_type="mongodb",
            ),
            chunks_completed=chunks,
            rejected_rows=max(rejected_rows, len(data_rows) - written - skipped_total),
            rejected_details=list(rejected_details),
            coerced_null_rows=coerced_null_rows,
            warnings=transform_errors,
            meta=gate8_writer_meta(mapped_rows, target_cols),
        )
    except (pymongo.errors.PyMongoError, ValueError, TypeError, KeyError, OSError) as exc:
        return WriteResult(
            ok=False,
            rows_written=written if "written" in locals() else 0,
            table_name=table_name,
            target_schema=schema or "db",
            checksum="",
            chunks_completed=0,
            error=str(exc),
            rejected_details=list(rejected_details) if "rejected_details" in locals() else [],
            rejected_rows=len(rejected_details) if "rejected_details" in locals() else 0,
            warnings=transform_errors if "transform_errors" in locals() else [],
        )
