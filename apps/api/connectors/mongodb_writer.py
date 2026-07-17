"""MongoDB bulk writer — CSV file to collection with checkpoint batches."""

from __future__ import annotations

import base64
import hashlib
import json
import os
from dataclasses import dataclass, field
from typing import Any, Callable

from connectors.writer_common import (
    CHUNK_SIZE,
    build_mapped_rows,
    resolve_target_columns,
    row_checksum,
    sanitize_identifier,
    transform_error_policy,
)

# MongoDB commands handle ~1000-document batches most reliably through proxies
# and serverless tiers. 20k-document single calls can hit socket/proxy limits.
MONGO_WRITE_BATCH_SIZE = int(os.getenv("DATAFLOW_MONGO_BATCH_SIZE", "1000"))


@dataclass
class WriteResult:
    ok: bool
    rows_written: int
    table_name: str
    target_schema: str
    checksum: str
    chunks_completed: int
    error: str | None = None
    driver: str = "pymongo"
    rejected_rows: int = 0
    warnings: list[str] = field(default_factory=list)


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
    """Insert documents, treating duplicate-key errors as already-present rows.

    Every document without an explicit ``_id`` gets a deterministic content hash
    as its primary key so retries and resumable chunks produce the same _id and
    do not create duplicates.
    """
    from pymongo.errors import BulkWriteError

    for doc in docs:
        if "_id" not in doc:
            id_input = json.dumps(
                {k: v for k, v in doc.items() if k != "_id"},
                sort_keys=True,
                default=str,
            )
            doc["_id"] = hashlib.sha256(id_input.encode("utf-8")).hexdigest()

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
    dest_types = {target_cols[i]: logical_types[i] for i in range(len(target_cols))}
    policy = transform_error_policy(error_policy)

    try:
        from connectors.mongodb_common import _mongo_client

        conn_str = _connection_string(host, port, username, password, connection_string, database, ssl, auth_source)
        # Reuse a cached MongoClient per connection string to avoid paying the
        # connection handshake cost on every batch.
        client = _mongo_client(conn_str)

        db = client[db_name]
        coll = db[collection_name]

        mapped_rows, transform_errors = build_mapped_rows(
            headers=headers,
            data_rows=data_rows,
            mappings=mappings,
            target_cols=target_cols,
            column_types=column_types,
            dest_types=dest_types,
            error_policy=policy,
            preserve_case=True,
        )
        rejected_rows = len(data_rows) - len(mapped_rows)
        if transform_errors and policy == "fail":
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=collection_name,
                target_schema=db_name,
                checksum="",
                chunks_completed=0,
                error=f"Transform errors: {'; '.join(transform_errors[:3])}",
                rejected_rows=rejected_rows,
                warnings=transform_errors,
            )
        
        from bson.binary import Binary
        from bson.decimal128 import Decimal128
        from datetime import date as _date, datetime as _datetime, time as _time

        def _to_bson(value: Any, stype: str) -> Any:
            if value is None:
                return None
            upper = stype.upper()
            if upper in {"INTEGER", "INT", "BIGINT", "SMALLINT", "TINYINT", "LONG", "SERIAL", "BIGSERIAL"}:
                try:
                    iv = int(value)
                except (ValueError, TypeError):
                    return value
                # BSON supports signed 64-bit ints; fall back to Decimal128 or
                # string when a value overflows.
                if iv > 2**63 - 1 or iv < -(2**63):
                    try:
                        return Decimal128(str(iv))
                    except Exception:
                        return str(iv)
                return iv
            if upper in {"BOOLEAN", "BOOL"}:
                text = str(value).strip().lower()
                if text in {"true", "t", "yes", "y", "1"}:
                    return True
                if text in {"false", "f", "no", "n", "0"}:
                    return False
                return value
            if upper in {"DECIMAL", "NUMERIC"}:
                return Decimal128(str(value))
            if upper == "DATE":
                if isinstance(value, _datetime):
                    return value
                if isinstance(value, _date):
                    return _datetime.combine(value, _time.min)
                text = value.strip() if isinstance(value, str) else str(value)
                for fmt in (
                    "%Y-%m-%d",
                    "%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%dT%H:%M:%S.%f",
                    "%Y-%m-%dT%H:%M:%S.%fZ",
                    "%Y-%m-%dT%H:%M:%S.%f%z",
                    "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%d %H:%M:%S.%f",
                    "%m/%d/%Y",
                    "%d/%m/%Y",
                    "%Y%m%d",
                ):
                    try:
                        return _datetime.strptime(text, fmt)
                    except ValueError:
                        continue
                return value
            if upper in {"DATETIME", "TIMESTAMP", "TIMESTAMP_TZ", "TIMESTAMPTZ"}:
                if isinstance(value, _datetime):
                    return value
                text = value.strip() if isinstance(value, str) else str(value)
                text = text.replace("Z", "+00:00")
                try:
                    return _datetime.fromisoformat(text)
                except ValueError:
                    pass
                for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S%z"):
                    try:
                        return _datetime.strptime(text, fmt)
                    except ValueError:
                        continue
                return value
            if upper in {"BINARY", "BYTEA", "BLOB"}:
                if isinstance(value, bytes):
                    return Binary(value)
                if isinstance(value, str):
                    try:
                        return Binary(base64.b64decode(value, validate=True))
                    except Exception:
                        return Binary(value.encode("utf-8"))
                return value
            if upper in {"JSON", "OBJECT", "ARRAY", "VARIANT"}:
                if isinstance(value, (dict, list)):
                    return value
                if isinstance(value, str):
                    try:
                        return json.loads(value, parse_constant=lambda v: None)
                    except Exception:
                        return value
                return value
            if upper == "UUID":
                return str(value)
            if upper == "TIME":
                return str(value)
            return value

        typed_rows: list[tuple] = []
        for row in mapped_rows:
            typed_rows.append(tuple(_to_bson(v, t) for v, t in zip(row, logical_types)))

        total = len(typed_rows)
        # MongoDB writes are split into smaller server-friendly batches.
        mongo_batch_size = max(1, min(MONGO_WRITE_BATCH_SIZE, CHUNK_SIZE))
        chunks = max(1, (total + mongo_batch_size - 1) // mongo_batch_size)
        written = 0

        for chunk_idx in range(chunks):
            start = chunk_idx * mongo_batch_size
            batch = typed_rows[start : start + mongo_batch_size]
            if not batch:
                break

            # Convert row tuples to documents
            docs = [dict(zip(target_cols, row)) for row in batch]

            # Preserve MongoDB ObjectId identity when a 24-char hex _id is present.
            from bson.objectid import ObjectId

            for doc in docs:
                if "_id" in doc and isinstance(doc["_id"], str):
                    v = doc["_id"]
                    if len(v) == 24 and ObjectId.is_valid(v):
                        try:
                            doc["_id"] = ObjectId(v)
                        except Exception:
                            pass

            if write_mode == "upsert" and conflict_columns:
                from pymongo import ReplaceOne

                pk = conflict_columns[0]
                if pk in target_cols:
                    ops = [
                        ReplaceOne({pk: doc[pk]}, doc, upsert=True)
                        for doc in docs
                        if doc.get(pk) not in (None, "")
                    ]
                    if ops:
                        coll.bulk_write(ops, ordered=False)
                    written += len(ops)
                else:
                    written += _idempotent_insert_many(coll, docs)
            else:
                written += _idempotent_insert_many(coll, docs)
            if on_checkpoint:
                on_checkpoint(chunk_idx + 1, chunks, written)

        return WriteResult(
            ok=True,
            rows_written=written,
            table_name=collection_name,
            target_schema=db_name,
            checksum=row_checksum(mapped_rows, target_cols),
            chunks_completed=chunks,
            rejected_rows=len(data_rows) - written,
            warnings=transform_errors,
        )
    except Exception as exc:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema=schema or "db",
            checksum="",
            chunks_completed=0,
            error=str(exc),
        )
