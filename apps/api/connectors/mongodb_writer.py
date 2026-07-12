"""MongoDB bulk writer — CSV file to collection with checkpoint batches."""

from __future__ import annotations

import base64
import json
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
    host: str, port: int, username: str, password: str, connection_string: str
) -> str:
    if connection_string:
        return connection_string
    if username and password:
        return f"mongodb://{username}:{password}@{host}:{port or 27017}/"
    return f"mongodb://{host}:{port or 27017}/"


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
) -> WriteResult:
    try:
        from pymongo import MongoClient
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

    target_cols, source_types = resolve_target_columns(mappings, column_types)
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

    collection_name = sanitize_identifier(table_name)
    db_name = database or schema or "test"
    policy = transform_error_policy(error_policy)

    try:
        conn_str = _connection_string(host, port, username, password, connection_string)
        client = MongoClient(conn_str, serverSelectionTimeoutMS=10000)
        
        # Test connection
        client.admin.command('ping')

        db = client[db_name]
        coll = db[collection_name]

        mapped_rows, transform_errors = build_mapped_rows(
            headers=headers,
            data_rows=data_rows,
            mappings=mappings,
            target_cols=target_cols,
            column_types=column_types,
            error_policy=policy,
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
                    return int(value)
                except (ValueError, TypeError):
                    return value
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
                if isinstance(value, _date):
                    return value
                text = value.strip() if isinstance(value, str) else str(value)
                for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y%m%d"):
                    try:
                        return _datetime.strptime(text, fmt).date()
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
                        return json.loads(value)
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
            typed_rows.append(tuple(_to_bson(v, t) for v, t in zip(row, source_types)))

        total = len(typed_rows)
        chunks = max(1, (total + CHUNK_SIZE - 1) // CHUNK_SIZE)
        written = 0

        for chunk_idx in range(chunks):
            start = chunk_idx * CHUNK_SIZE
            batch = typed_rows[start : start + CHUNK_SIZE]
            if not batch:
                break

            # Convert row tuples to documents
            docs = [dict(zip(target_cols, row)) for row in batch]

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
                    coll.insert_many(docs, ordered=False)
                    written += len(docs)
            else:
                coll.insert_many(docs, ordered=False)
                written += len(docs)
            if on_checkpoint:
                on_checkpoint(chunk_idx + 1, chunks, written)

        client.close()
        return WriteResult(
            ok=True,
            rows_written=written,
            table_name=collection_name,
            target_schema=db_name,
            checksum=row_checksum(mapped_rows),
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
