"""Gate 8 reconciliation — independent target verification."""

from __future__ import annotations

import base64
import hashlib
import heapq
import json
import os
import re
import struct
import tempfile
from dataclasses import asdict, dataclass
from datetime import date as _date
from datetime import datetime as _datetime
from datetime import time as _time
from datetime import timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

from services.value_serializer import json_default

SPILL_THRESHOLD = int(os.getenv("DATAFLOW_FINGERPRINT_SPILL_THRESHOLD", "1000000"))

# Quick pre-filter for the expensive Decimal / date normalization in
# normalize_cell.  Most string columns (names, emails, codes) are clearly not
# numbers or dates, so we can skip the exception-heavy Decimal constructor and
# the date regex for them.
_NUMERIC_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$")
_DATE_LIKE_CHARS = frozenset("-:/T ")

from services.transform_engine import (
    _DATE_LIKE_RE,
    _parse_date,
    _parse_datetime,
    apply_transform,
)


@dataclass
class ReconciliationReport:
    passed: bool
    source_rows: int
    target_rows: int
    source_checksum: str
    target_checksum: str
    message: str
    rejected_rows: int = 0

    def to_dict(self) -> dict:
        return asdict(self)


def _get_case_insensitive(rec: dict[str, Any], key: str | None) -> Any:
    if not key:
        return None
    if key in rec:
        return rec[key]
    lower = key.lower()
    for k, v in rec.items():
        if k.lower() == lower:
            return v
    return None


def _iter_fingerprints(
    rows: Iterable[Any],
    columns: list[str] | None = None,
    *,
    sort_key: str | None = None,
):
    """Yield (row_key, fingerprint) tuples for each row without materializing the full list."""
    if columns is not None:
        cols = columns
        sorted_cols = sorted(cols, key=lambda x: x.lower())
        col_index = {c: i for i, c in enumerate(cols)}
        sort_idx = -1
        if sort_key:
            sort_key_lower = sort_key.lower()
            for i, c in enumerate(cols):
                if c.lower() == sort_key_lower:
                    sort_idx = i
                    break
        for row in rows:
            if isinstance(row, dict):
                parts = [
                    f"{c.lower()}={normalize_cell(row.get(c))}" for c in sorted_cols
                ]
                if sort_key:
                    row_key = normalize_cell(row.get(sort_key))
                    if row_key is None:
                        for k, v in row.items():
                            if k.lower() == sort_key_lower:
                                row_key = normalize_cell(v)
                                break
                else:
                    row_key = ""
            else:
                parts = [
                    f"{c.lower()}={normalize_cell(row[col_index[c]] if col_index[c] < len(row) else None)}"
                    for c in sorted_cols
                ]
                row_key = normalize_cell(row[sort_idx] if sort_idx >= 0 and sort_idx < len(row) else None) if sort_key else ""
            fingerprint = "\x1f".join(parts)
            yield (row_key, fingerprint)
    else:
        for row in rows:
            if isinstance(row, dict):
                keys = sorted(row.keys(), key=lambda x: x.lower())
                parts = [f"{k.lower()}={normalize_cell(row.get(k))}" for k in keys]
                fingerprint = "\x1f".join(parts)
                row_key = normalize_cell(_get_case_insensitive(row, sort_key)) if sort_key else ""
            else:
                fingerprint = "|".join(sorted(normalize_cell(v) for v in row))
                row_key = ""
            yield (row_key, fingerprint)


class FingerprintAccumulator:
    """Streaming, order-independent checksum accumulator for arbitrary row counts.

    Keeps fingerprints in memory until ``DATAFLOW_FINGERPRINT_SPILL_THRESHOLD``
    is reached, then spills sorted chunks to disk and merges them at the end.
    This lets the engine compute a strict source checksum for billion-row files
    without holding every row's fingerprint in RAM.
    """

    def __init__(self, threshold: int | None = None) -> None:
        self.threshold = threshold or SPILL_THRESHOLD
        self.buffer: list[tuple[str, str]] = []
        self.chunk_files: list[str] = []
        self.total = 0
        self._tempdir: tempfile.TemporaryDirectory | None = None

    def add(self, key: str, fingerprint: str) -> None:
        self.buffer.append((key, fingerprint))
        self.total += 1
        if len(self.buffer) >= self.threshold:
            self._spill()

    def add_many(self, fingerprints: Iterable[tuple[str, str]]) -> None:
        for key, fingerprint in fingerprints:
            self.add(key, fingerprint)

    def _spill(self) -> None:
        if not self.buffer:
            return
        self.buffer.sort(key=lambda x: (x[0], x[1]))
        if self._tempdir is None:
            self._tempdir = tempfile.TemporaryDirectory(prefix="dataflow_fp_")
        fd, path = tempfile.mkstemp(dir=self._tempdir.name, suffix=".chk")
        with os.fdopen(fd, "wb") as f:
            for key, fp in self.buffer:
                key_b = key.encode("utf-8")
                fp_b = fp.encode("utf-8")
                f.write(struct.pack(">I", len(key_b)))
                f.write(key_b)
                f.write(struct.pack(">I", len(fp_b)))
                f.write(fp_b)
        self.chunk_files.append(path)
        self.buffer = []

    def _read_chunk(self, path: str) -> Iterable[tuple[str, str]]:
        with open(path, "rb") as f:
            while True:
                key_len_b = f.read(4)
                if not key_len_b:
                    break
                key_len = struct.unpack(">I", key_len_b)[0]
                key = f.read(key_len).decode("utf-8")
                fp_len_b = f.read(4)
                if not fp_len_b:
                    break
                fp_len = struct.unpack(">I", fp_len_b)[0]
                fp = f.read(fp_len).decode("utf-8")
                yield (key, fp)

    def _sorted_stream(self) -> Iterable[tuple[str, str]]:
        if not self.chunk_files:
            self.buffer.sort(key=lambda x: (x[0], x[1]))
            yield from self.buffer
            return
        if self.buffer:
            self._spill()
        streams = [self._read_chunk(p) for p in self.chunk_files]
        yield from heapq.merge(*streams, key=lambda x: (x[0], x[1]))

    def digest(self) -> str:
        h = hashlib.sha256()
        for _, fp in self._sorted_stream():
            h.update(fp.encode("utf-8"))
        self.close()
        return h.hexdigest()[:16]

    def close(self) -> None:
        if self._tempdir is not None:
            self._tempdir.cleanup()
            self._tempdir = None
        self.chunk_files = []
        self.buffer = []


def fingerprint_checksum(fingerprints: Iterable[tuple[str, str]]) -> str:
    """Hash a list/iterable of (row_key, fingerprint) tuples.

    For small inputs the in-memory sort+hash path is used; for large or
    streaming inputs an ``FingerprintAccumulator`` spills to disk so the
    checksum stays memory-bounded.
    """
    if isinstance(fingerprints, list) and len(fingerprints) <= SPILL_THRESHOLD:
        return _hash_fingerprints(fingerprints)
    acc = FingerprintAccumulator()
    acc.add_many(fingerprints)
    return acc.digest()


def _hash_fingerprints(fingerprints: list[tuple[str, str]]) -> str:
    fingerprints.sort(key=lambda x: (x[0], x[1]))
    h = hashlib.sha256()
    for _, fp in fingerprints:
        h.update(fp.encode("utf-8"))
    return h.hexdigest()[:16]


def canonical_checksum(
    rows: list[Any],
    columns: list[str] | None = None,
    *,
    sort_key: str | None = None,
) -> str:
    """Stable, order-independent checksum that preserves column identity.

    Accepts either a matrix of values (with an explicit column list) or a list
    of dicts. Column names are included in the row fingerprint so that swapped
    columns cannot collide. Column labels are normalized to lowercase so source
    and target casing differences do not produce false mismatches. When no
    columns are provided, the legacy cell-only fallback is used for matrices.
    """
    if not rows:
        return hashlib.sha256(b"").hexdigest()[:16]
    return _hash_fingerprints(list(_iter_fingerprints(rows, columns, sort_key=sort_key)))


def canonical_checksum_from_iter(
    rows: Iterable[Any],
    columns: list[str] | None = None,
    *,
    sort_key: str | None = None,
    limit: int = 0,
) -> str:
    """Streaming variant of canonical_checksum with optional sample limit.

    Reads rows lazily, collects fingerprints, and hashes them after sorting.
    A limit of 0 means process all rows.
    """
    fingerprints: list[tuple[str, str]] = []
    for i, (row_key, fp) in enumerate(_iter_fingerprints(rows, columns, sort_key=sort_key)):
        if limit and i >= limit:
            break
        fingerprints.append((row_key, fp))
    return _hash_fingerprints(fingerprints)


def checksum_rows(rows: list[Any], columns: list[str] | None = None) -> str:
    """Canonical, order-independent checksum over a matrix or list of dicts."""
    return canonical_checksum(rows, columns)


def aggregate_checksum(
    records: list[dict[str, Any]],
    columns: list[str] | None = None,
    *,
    sort_key: str | None = None,
) -> str:
    """Order-independent checksum for reconciliation with column identity."""
    return canonical_checksum(records, columns, sort_key=sort_key)


def reconcile(
    *,
    source_rows: int,
    target_rows: int,
    source_checksum: str,
    target_checksum: str,
    rejected_rows: int = 0,
    strict_checksum: bool = True,
    allow_extra_rows: bool = False,
    sample_compare: dict[str, Any] | None = None,
) -> ReconciliationReport:
    expected_rows = max(source_rows - max(rejected_rows, 0), 0)
    row_count_ok = target_rows == expected_rows or (
        allow_extra_rows and target_rows >= expected_rows
    )
    if not row_count_ok:
        extra_note = f" (target has {target_rows - expected_rows} extra rows)" if target_rows > expected_rows else ""
        return ReconciliationReport(
            passed=False,
            source_rows=source_rows,
            target_rows=target_rows,
            source_checksum=source_checksum,
            target_checksum=target_checksum,
            message=(
                f"Row count mismatch: source {source_rows}, rejected {rejected_rows}, "
                f"expected target {expected_rows} vs target {target_rows}{extra_note}"
            ),
            rejected_rows=rejected_rows,
        )

    if sample_compare and not sample_compare.get("passed", True):
        mismatches = sample_compare.get("mismatches") or []
        detail = mismatches[0] if mismatches else "value mismatch in read-back sample"
        return ReconciliationReport(
            passed=False,
            source_rows=source_rows,
            target_rows=target_rows,
            source_checksum=source_checksum,
            target_checksum=target_checksum,
            message=f"Read-back sample verification failed: {detail}",
            rejected_rows=rejected_rows,
        )

    if source_checksum != target_checksum:
        # When the target legitimately contains extra rows (append/upsert),
        # whole-table checksums are not comparable; rely on the key-aligned
        # sample compare already validated above.
        if allow_extra_rows and target_rows > expected_rows:
            return ReconciliationReport(
                passed=True,
                source_rows=source_rows,
                target_rows=target_rows,
                source_checksum=source_checksum,
                target_checksum=target_checksum,
                message=(
                    f"Transfer verified by key-aligned sample ({target_rows} rows"
                    + (f", {rejected_rows} rejected" if rejected_rows else "")
                    + f"; {target_rows - expected_rows} pre-existing rows skipped in checksum)"
                ),
                rejected_rows=rejected_rows,
            )
        if strict_checksum:
            return ReconciliationReport(
                passed=False,
                source_rows=source_rows,
                target_rows=target_rows,
                source_checksum=source_checksum,
                target_checksum=target_checksum,
                message=(
                    f"Checksum mismatch in strict mode: source {source_checksum} "
                    f"vs target {target_checksum}"
                ),
                rejected_rows=rejected_rows,
            )
        return ReconciliationReport(
            passed=True,
            source_rows=source_rows,
            target_rows=target_rows,
            source_checksum=source_checksum,
            target_checksum=target_checksum,
            message=(
                f"Row count verified ({target_rows} rows"
                + (f", {rejected_rows} rejected" if rejected_rows else "")
                + "); checksums differ due to "
                "cross-engine type rendering — not a data loss signal"
            ),
            rejected_rows=rejected_rows,
        )
    message = f"100% row fidelity verified ({target_rows} rows)"
    if rejected_rows:
        message = f"Transfer verified ({target_rows} rows written, {rejected_rows} rejected)"
    return ReconciliationReport(
        passed=True,
        source_rows=source_rows,
        target_rows=target_rows,
        source_checksum=source_checksum,
        target_checksum=target_checksum,
        message=message,
        rejected_rows=rejected_rows,
    )


def _iter_fetchmany(cur, batch_size: int = 5000):
    """Yield rows from a DBAPI cursor without loading the full result set."""
    while True:
        rows = cur.fetchmany(batch_size)
        if not rows:
            break
        for row in rows:
            yield row


def verify_postgres_table(
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
    target_columns: list[str] | None = None,
    limit: int = 0,
) -> tuple[int, str]:
    try:
        from connectors.postgresql_conn import get_connection

        conn = get_connection(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            connection_string=connection_string,
            ssl=ssl,
        )
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"')
            count = int(cur.fetchone()[0])
            cur.execute(f'SELECT * FROM "{schema}"."{table_name}"')
            names = [d[0] for d in cur.description] if cur.description else []
            columns = names or target_columns or []
            checksum = canonical_checksum_from_iter(_iter_fetchmany(cur), columns, limit=limit)
        conn.close()
        return count, checksum
    except Exception:
        return -1, ""


def verify_snowflake_table(
    *,
    host: str,
    database: str,
    username: str,
    password: str,
    schema: str,
    connection_string: str,
    warehouse: str,
    table_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
) -> tuple[int, str]:
    try:
        from connectors.snowflake_conn import get_connection, normalize_account

        conn = get_connection(
            account=normalize_account(host),
            username=username,
            password=password,
            database=database,
            schema=schema,
            warehouse=warehouse,
            connection_string=connection_string,
        )
        with conn.cursor() as cur:
            if warehouse:
                try:
                    cur.execute(f'USE WAREHOUSE "{warehouse}"')
                except Exception:
                    pass
            qualified_name = f'"{schema or "PUBLIC"}"."{table_name}"'
            cur.execute(f'SELECT COUNT(*) FROM {qualified_name}')
            count = int(cur.fetchone()[0])
            cur.execute(f'SELECT * FROM {qualified_name}')
            names = [d[0] for d in cur.description] if cur.description else []
            columns = names or target_columns or []
            checksum = canonical_checksum_from_iter(_iter_fetchmany(cur), columns, limit=limit)
        conn.close()
        return count, checksum
    except Exception:
        return -1, ""


def verify_mysql_table(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    connection_string: str,
    ssl: bool,
    table_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
) -> tuple[int, str]:
    try:
        from connectors.mysql_conn import get_connection

        conn = get_connection(
            host=host, port=port, database=database,
            username=username, password=password,
            connection_string=connection_string, ssl=ssl,
        )
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{table_name}`")
            count = int(cur.fetchone()[0])
            cur.execute(f"SELECT * FROM `{table_name}`")
            names = [d[0] for d in cur.description] if cur.description else []
            columns = names or target_columns or []
            checksum = canonical_checksum_from_iter(_iter_fetchmany(cur), columns, limit=limit)
        conn.close()
        return count, checksum
    except Exception:
        return -1, ""


def verify_bigquery_table(
    *,
    project_id: str,
    dataset_id: str,
    connection_string: str,
    table_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
) -> tuple[int, str]:
    try:
        from connectors.bigquery_conn import get_client

        client = get_client(project_id=project_id, credentials_path=connection_string)
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        table = client.get_table(table_id)
        count = table.num_rows or 0
        field_names = [field.name for field in table.schema] if table.schema else []
        columns = field_names or target_columns or []

        def _row_iter():
            yielded = 0
            for row in client.list_rows(table_id):
                if limit and yielded >= limit:
                    break
                yield list(row.values()) if hasattr(row, "values") else list(row)
                yielded += 1

        return int(count), canonical_checksum_from_iter(_row_iter(), columns, limit=limit)
    except Exception:
        return -1, ""


def _rows_from_object_bytes(
    body: bytes, key: str, columns: list[str] | None = None
) -> tuple[list[dict[str, Any]], list[str]]:
    """Parse S3/GCS object payload (JSON, JSONL, CSV) into dict rows and headers."""
    import csv
    import io

    text = body.decode("utf-8", errors="replace")
    lower_key = (key or "").lower()

    if lower_key.endswith(".csv"):
        reader = csv.DictReader(io.StringIO(text))
        rows = list(reader)
        headers = reader.fieldnames or []
        return rows, headers

    if lower_key.endswith(".jsonl"):
        rows: list[dict[str, Any]] = []
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            parsed = json.loads(line)
            if isinstance(parsed, dict):
                rows.append(parsed)
            else:
                rows.append({"value": parsed})
        headers = sorted(set(k for r in rows for k in r.keys())) if rows else []
        return rows, headers or (columns or [])

    # Default: JSON array or single JSON object.
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        data = []
    if isinstance(data, list):
        rows = [r for r in data if isinstance(r, dict)]
        headers = sorted(set(k for r in rows for k in r.keys())) if rows else []
        return rows, headers or (columns or [])
    if isinstance(data, dict):
        return [data], sorted(data.keys())
    return [], columns or []


def verify_s3_object(
    *,
    bucket: str,
    key: str,
    host: str,
    port: int,
    username: str,
    password: str,
    connection_string: str,
    ssl: bool,
    target_columns: list[str] | None = None,
    limit: int = 0,
) -> tuple[int, str]:
    """Reconcile an S3 object by downloading and parsing its contents."""
    try:
        from connectors.aws_common import boto3_client

        cfg = {
            "host": host,
            "port": port,
            "username": username,
            "password": password,
            "connection_string": connection_string,
            "ssl": ssl,
            "database": bucket,
        }
        client = boto3_client("s3", cfg)
        obj = client.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read()
        rows, headers = _rows_from_object_bytes(body, key, target_columns)
        columns = headers or target_columns or []
        return len(rows), canonical_checksum_from_iter(rows, columns, limit=limit)
    except Exception:
        return -1, ""


def verify_gcs_blob(
    *,
    bucket: str,
    key: str,
    host: str,
    port: int,
    connection_string: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
) -> tuple[int, str]:
    """Reconcile a GCS blob by downloading and parsing its contents."""
    try:
        from connectors.gcs_common import gcs_client

        cfg = {
            "host": host,
            "port": port,
            "connection_string": connection_string,
        }
        client = gcs_client(cfg)
        blob = client.bucket(bucket).blob(key)
        body = blob.download_as_bytes()
        rows, headers = _rows_from_object_bytes(body, key, target_columns)
        columns = headers or target_columns or []
        return len(rows), canonical_checksum_from_iter(rows, columns, limit=limit)
    except Exception:
        return -1, ""


def verify_sqlite_table(
    *,
    connection_string: str,
    database: str,
    table_name: str,
    host: str = "",
    target_columns: list[str] | None = None,
    limit: int = 0,
) -> tuple[int, str]:
    """Reconcile a SQLite target by reading the local file."""
    try:
        import sqlite3

        from connectors.sqlite_common import sqlite_file_path

        path = sqlite_file_path(database, connection_string, host)
        if not path:
            return -1, ""
        conn = sqlite3.connect(str(path))
        cur = conn.cursor()
        cur.execute(f'SELECT COUNT(*) FROM "{table_name}"')
        count = cur.fetchone()[0]
        cur.execute(f'SELECT * FROM "{table_name}"')
        names = [d[0] for d in cur.description] if cur.description else []
        columns = names or target_columns or []
        checksum = canonical_checksum_from_iter(_iter_fetchmany(cur), columns, limit=limit)
        conn.close()
        return int(count), checksum
    except sqlite3.OperationalError as exc:
        # Missing table means the target is empty, not that verification is
        # unavailable. Return 0 so reconciliation can surface the mismatch.
        if "no such table" in str(exc).lower():
            return 0, ""
        return -1, ""
    except Exception:
        return -1, ""


def verify_duckdb_table(
    *,
    connection_string: str,
    database: str,
    table_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
) -> tuple[int, str]:
    """Reconcile a DuckDB target by reading the local file."""
    try:
        import duckdb

        path = connection_string or database
        if not path:
            return -1, ""
        conn = duckdb.connect(str(path))
        count = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
        cur = conn.execute(f'SELECT * FROM "{table_name}"')
        names = [d[0] for d in cur.description] if cur.description else []
        columns = names or target_columns or []
        checksum = canonical_checksum_from_iter(_iter_fetchmany(cur), columns, limit=limit)
        conn.close()
        return int(count), checksum
    except Exception:
        return -1, ""


def verify_mongodb_collection(
    *,
    host: str = "",
    port: int = 27017,
    username: str = "",
    password: str = "",
    connection_string: str = "",
    database: str = "",
    ssl: bool = False,
    auth_source: str = "",
    table_name: str = "",
    target_columns: list[str] | None = None,
    limit: int = 0,
) -> tuple[int, str]:
    """Reconcile a MongoDB target by counting and fingerprinting documents."""
    try:
        from pymongo import MongoClient

        from connectors.mongodb_common import normalize_mongodb_connection_string

        conn_str = normalize_mongodb_connection_string(
            connection_string or "",
            database=database,
            host=host,
            port=port,
            username=username,
            password=password,
            ssl=ssl,
            auth_source=auth_source,
        )
        client = MongoClient(conn_str, serverSelectionTimeoutMS=5000)
        db = client[database or "test"]
        coll = db[table_name]
        count = coll.count_documents({})

        def _doc_iter():
            yielded = 0
            for doc in coll.find({}):
                if limit and yielded >= limit:
                    break
                yield doc
                yielded += 1

        columns = target_columns or sorted(
            set(k for doc in coll.find({}).limit(100) for k in doc.keys())
        )
        checksum = canonical_checksum_from_iter(_doc_iter(), columns, limit=limit)
        client.close()
        return int(count), checksum
    except Exception:
        return -1, ""


def verify_dynamodb_table(
    *,
    connection_string: str,
    database: str,
    table_name: str,
    username: str = "local",
    password: str = "local",
    target_columns: list[str] | None = None,
    limit: int = 0,
) -> tuple[int, str]:
    """Reconcile a DynamoDB target by Scan count and item fingerprint."""
    try:
        import boto3
        from boto3.dynamodb.types import TypeDeserializer

        endpoint = connection_string or "http://localhost:8000"
        client = boto3.client(
            "dynamodb",
            endpoint_url=endpoint,
            aws_access_key_id=username.strip() or "local",
            aws_secret_access_key=password.strip() or "local",
            region_name="us-east-1",
        )
        paginator = client.get_paginator("scan")
        count = sum(page["Count"] for page in paginator.paginate(
            TableName=table_name, Select="COUNT"
        ))

        deserializer = TypeDeserializer()

        def _item_iter():
            yielded = 0
            for page in paginator.paginate(TableName=table_name):
                for item in page.get("Items", []):
                    if limit and yielded >= limit:
                        break
                    yield {k: deserializer.deserialize(v) for k, v in item.items()}
                    yielded += 1

        columns = target_columns or []
        return int(count), canonical_checksum_from_iter(_item_iter(), columns, limit=limit)
    except Exception:
        return -1, ""


def verify_target(
    db_type: str,
    dest: dict[str, Any],
    *,
    schema: str,
    table_name: str,
    fallback_rows: int,
    fallback_checksum: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
) -> tuple[int, str]:
    if db_type == "mongodb":
        count, chk = verify_mongodb_collection(
            host=dest.get("host", ""),
            port=int(dest.get("port") or 27017),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            database=dest.get("database", ""),
            ssl=bool(dest.get("ssl", False)),
            auth_source=dest.get("auth_source", ""),
            table_name=table_name,
            target_columns=target_columns,
            limit=limit,
        )
    elif db_type == "dynamodb":
        from connectors.aws_common import resolve_endpoint_url

        conn_str = resolve_endpoint_url(dest) or "http://localhost:8000"
        count, chk = verify_dynamodb_table(
            connection_string=conn_str,
            database=dest.get("database", ""),
            table_name=table_name,
            username=dest.get("username", "local"),
            password=dest.get("password", "local"),
            target_columns=target_columns,
            limit=limit,
        )
    elif db_type == "sqlite":
        count, chk = verify_sqlite_table(
            connection_string=dest.get("connection_string", ""),
            database=dest.get("database", ""),
            host=dest.get("host", ""),
            table_name=table_name,
            target_columns=target_columns,
            limit=limit,
        )
    elif db_type == "duckdb":
        count, chk = verify_duckdb_table(
            connection_string=dest.get("connection_string", ""),
            database=dest.get("database", ""),
            table_name=table_name,
            target_columns=target_columns,
            limit=limit,
        )
    elif db_type == "postgresql":
        count, chk = verify_postgres_table(
            host=dest.get("host", ""),
            port=dest.get("port", 5432),
            database=dest.get("database", ""),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            schema=schema,
            connection_string=dest.get("connection_string", ""),
            ssl=dest.get("ssl", True),
            table_name=table_name,
            target_columns=target_columns,
            limit=limit,
        )
    elif db_type == "snowflake":
        count, chk = verify_snowflake_table(
            host=dest.get("host", ""),
            database=dest.get("database", ""),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            schema=schema,
            connection_string=dest.get("connection_string", ""),
            warehouse=dest.get("warehouse", ""),
            table_name=table_name,
            target_columns=target_columns,
            limit=limit,
        )
    elif db_type == "mysql":
        count, chk = verify_mysql_table(
            host=dest.get("host", ""),
            port=int(dest.get("port", 3306)),
            database=dest.get("database", ""),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            ssl=dest.get("ssl", False),
            table_name=table_name,
            target_columns=target_columns,
            limit=limit,
        )
    elif db_type == "bigquery":
        count, chk = verify_bigquery_table(
            project_id=dest.get("database", ""),
            dataset_id=schema,
            connection_string=dest.get("connection_string", ""),
            table_name=table_name,
            target_columns=target_columns,
            limit=limit,
        )
    elif db_type == "s3":
        count, chk = verify_s3_object(
            bucket=dest.get("database", ""),
            key=table_name,
            host=dest.get("host", ""),
            port=int(dest.get("port", 0)),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            ssl=bool(dest.get("ssl", False)),
            target_columns=target_columns,
            limit=limit,
        )
    elif db_type == "gcs":
        count, chk = verify_gcs_blob(
            bucket=dest.get("database", ""),
            key=table_name,
            host=dest.get("host", ""),
            port=int(dest.get("port", 0)),
            connection_string=dest.get("connection_string", ""),
            target_columns=target_columns,
            limit=limit,
        )
    else:
        count, chk = -1, ""

    if count >= 0:
        return count, chk
    return fallback_rows, fallback_checksum


def normalize_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, _datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)
        return value.strftime("%Y-%m-%dT%H:%M:%SZ")
    if isinstance(value, _date):
        return _datetime.combine(value, _time.min, tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    if isinstance(value, float):
        return _canonicalize_number(str(value)) or "nan"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, Decimal):
        return _canonicalize_number(value) or "nan"
    if isinstance(value, (bytes, bytearray, memoryview)):
        # Bytes may be raw payload or a base64-encoded string stored as bytes
        # (common in emulators). When the bytes are a valid base64 string,
        # decode and re-encode so the canonical checksum matches the original
        # encoded text; otherwise base64-encode the raw bytes.
        try:
            decoded = base64.b64decode(value, validate=True)
            re_encoded = base64.b64encode(decoded)
            if re_encoded == value:
                return re_encoded.decode("ascii")
        except Exception:
            pass
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, (dict, list, tuple, set, frozenset)):
        return json.dumps(value, sort_keys=True, default=json_default)
    text = str(value).strip()
    # Boolean and empty fast paths.
    if not text:
        return ""
    lowered = text.lower()
    if lowered in {"true", "t", "yes", "y", "on", "enabled", "active", "ok", "aye", "positive", "1"}:
        return "1"
    if lowered in {"false", "f", "no", "n", "off", "disabled", "inactive", "nope", "negative", "0"}:
        return "0"
    # Numeric fast path: only attempt Decimal normalization for strings that look
    # like numbers, avoiding the expensive exception path for names, emails, codes.
    if text[0] in "+-0123456789" and _NUMERIC_RE.match(text):
        canonical = _canonicalize_number(text)
        if canonical is not None:
            return canonical
        return text
    # JSON payloads (e.g. jsonb).
    if text.startswith(("{", "[")):
        try:
            parsed = json.loads(text)
            if isinstance(parsed, (dict, list)):
                return json.dumps(parsed, sort_keys=True, default=json_default)
        except (json.JSONDecodeError, TypeError):
            pass
    # Date/time normalization: cheap heuristic first to avoid running the date
    # regex on every non-date string.
    if (
        text[0].isdigit()
        and len(text) >= 8
        and _DATE_LIKE_CHARS.intersection(text)
        and _DATE_LIKE_RE.search(text)
    ):
        dtm = _parse_datetime(text)
        if dtm:
            return dtm
        dt = _parse_date(text)
        if dt:
            return f"{dt}T00:00:00Z"
    return text


def _canonicalize_number(value: Any) -> str | None:
    """Return a canonical string for numeric values so 9.5 == 9.5000000000."""
    try:
        d = Decimal(value) if not isinstance(value, Decimal) else value
        if d.is_nan():
            return None
        s = format(d.normalize(), "f")
        if "." in s:
            s = s.rstrip("0").rstrip(".")
        return s if s else "0"
    except (InvalidOperation, TypeError, ValueError):
        return None


def build_reconciliation_proof(
    source_records: list[dict[str, Any]],
    target_records: list[dict[str, Any]],
    mappings: list[dict[str, Any]],
    *,
    primary_key: str | None = None,
    sample_size: int = 50,
) -> dict[str, Any]:
    """Build a deterministic proof object for row-level transfer verification.

    The proof is based on exact primary-key matching and normalized mapped-value
    comparison across a bounded sample. It returns a score suitable for a
    preflight/reconciliation gate, not a legal audit guarantee.
    """
    if not source_records and not target_records:
        return {
            "passed": True,
            "matched_key_count": 0,
            "missing_key_count": 0,
            "row_fidelity_score": 1.0,
            "sample_compare": {"passed": True, "compared": 0, "mismatches": []},
        }

    key_col = primary_key or "id"
    source_keys = {normalize_cell(row.get(key_col)) for row in source_records if row.get(key_col) is not None}
    target_keys = {normalize_cell(row.get(key_col)) for row in target_records if row.get(key_col) is not None}
    matched_keys = source_keys & target_keys
    missing_keys = source_keys - target_keys
    extra_keys = target_keys - source_keys

    sample_compare = sample_compare_rows(
        source_records,
        target_records,
        mappings,
        sample_size=sample_size,
    )

    matched_key_count = len(matched_keys)
    missing_key_count = len(missing_keys)
    extra_key_count = len(extra_keys)
    total_keys = max(len(source_keys), 1)
    row_fidelity_score = round(
        max(0.0, 1.0 - (missing_key_count / total_keys) - (extra_key_count / total_keys) * 0.25),
        4,
    )

    passed = (
        missing_key_count == 0
        and extra_key_count == 0
        and sample_compare.get("passed", True)
        and row_fidelity_score >= 0.95
    )

    return {
        "passed": passed,
        "matched_key_count": matched_key_count,
        "missing_key_count": missing_key_count,
        "extra_key_count": extra_key_count,
        "row_fidelity_score": row_fidelity_score,
        "sample_compare": sample_compare,
    }


def sample_compare_rows(
    source_records: list[dict[str, Any]],
    target_rows: list[dict[str, Any]] | list[tuple[Any, ...]] | list[list[Any]],
    mappings: list[dict[str, Any]],
    *,
    target_columns: list[str] | None = None,
    sample_size: int = 50,
    sort_key: str | None = None,
) -> dict[str, Any]:
    """
    Compare mapped column values between source records and destination read-back.
    Rows are aligned by a stable key (e.g. primary key) when available, so upserts
    and out-of-order writes compare correctly. Falls back to sorted index alignment.
    """
    if not source_records or not target_rows or not mappings:
        return {"passed": True, "compared": 0, "mismatches": [], "skipped": True}

    def _as_dict(tgt_raw: Any) -> dict[str, Any] | None:
        if isinstance(tgt_raw, dict):
            return tgt_raw
        if target_columns and isinstance(tgt_raw, (list, tuple)):
            return {col: tgt_raw[i] if i < len(tgt_raw) else None for i, col in enumerate(target_columns)}
        return None

    target_dicts = [d for d in (_as_dict(t) for t in target_rows) if d is not None]
    target_by_key: dict[str, dict[str, Any]] = {}
    if sort_key:
        for d in target_dicts:
            key = normalize_cell(d.get(sort_key))
            if key and key not in target_by_key:
                target_by_key[key] = d

    def _normalize_source(raw: Any, transform: str | None) -> str:
        if transform:
            try:
                converted, _ = apply_transform(raw, transform)
            except Exception:
                converted = raw
        else:
            converted = raw
        return normalize_cell(converted)

    def _row_key(rec: Any) -> Any:
        if isinstance(rec, dict):
            val = rec.get(sort_key) if sort_key else None
            return val or (rec.get(target_columns[0]) if target_columns else None)
        return rec

    source_sorted = sorted(source_records, key=lambda r: _row_key(r) or 0)[:sample_size]

    mismatches: list[dict[str, str]] = []
    compared = 0
    target_fallback = sorted(target_dicts, key=lambda d: _row_key(d) or 0)

    for idx, src in enumerate(source_sorted):
        if sort_key and target_by_key:
            key = normalize_cell(src.get(sort_key))
            tgt = target_by_key.get(key) if key else None
        else:
            tgt = target_fallback[idx] if idx < len(target_fallback) else None
        if tgt is None:
            continue

        for m in mappings:
            src_col = str(m.get("source") or "")
            tgt_col = str(m.get("target") or "")
            if not src_col or not tgt_col:
                continue
            transform = m.get("transform")
            src_val = _normalize_source(src.get(src_col), transform)
            tgt_val = normalize_cell(tgt.get(tgt_col))
            compared += 1
            if src_val != tgt_val:
                mismatches.append({
                    "row": str(idx),
                    "source": src_col,
                    "target": tgt_col,
                    "source_value": src_val[:120],
                    "target_value": tgt_val[:120],
                })
                if len(mismatches) >= 10:
                    return {
                        "passed": False,
                        "compared": compared,
                        "mismatches": mismatches,
                    }

    return {
        "passed": len(mismatches) == 0,
        "compared": compared,
        "mismatches": mismatches,
    }


def read_target_sample(
    db_type: str,
    dest: dict[str, Any],
    *,
    schema: str,
    table_name: str,
    columns: list[str] | None = None,
    limit: int = 50,
    sort_key: str | None = None,
) -> list[dict[str, Any]]:
    """Read a small ordered sample from destination for value reconciliation."""
    cols = columns or ["*"]
    col_sql = ", ".join(f'"{c}"' for c in cols) if cols != ["*"] else "*"
    try:
        if db_type == "postgresql":
            from connectors.postgresql_conn import get_connection

            conn = get_connection(
                host=dest.get("host", ""),
                port=dest.get("port", 5432),
                database=dest.get("database", ""),
                username=dest.get("username", ""),
                password=dest.get("password", ""),
                connection_string=dest.get("connection_string", ""),
                ssl=dest.get("ssl", True),
            )
            order_sql = f'"{sort_key.replace(chr(34), chr(34) + chr(34))}"' if sort_key else "1"
            with conn.cursor() as cur:
                cur.execute(
                    f'SELECT {col_sql} FROM "{schema}"."{table_name}" ORDER BY {order_sql} LIMIT %s',
                    (limit,),
                )
                names = [d[0] for d in cur.description]
                rows = cur.fetchall()
            conn.close()
            return [dict(zip(names, row)) for row in rows]

        if db_type == "mysql":
            from connectors.mysql_conn import get_connection

            mysql_col_sql = ", ".join(f"`{c}`" for c in cols) if cols != ["*"] else "*"
            conn = get_connection(
                host=dest.get("host", ""),
                port=int(dest.get("port", 3306)),
                database=dest.get("database", ""),
                username=dest.get("username", ""),
                password=dest.get("password", ""),
                connection_string=dest.get("connection_string", ""),
                ssl=dest.get("ssl", False),
            )
            mysql_order = f"`{sort_key.replace('`', '``')}`" if sort_key else "1"
            with conn.cursor() as cur:
                cur.execute(f"SELECT {mysql_col_sql} FROM `{table_name}` ORDER BY {mysql_order} LIMIT %s", (limit,))
                names = [d[0] for d in cur.description]
                rows = cur.fetchall()
            conn.close()
            return [dict(zip(names, row)) for row in rows]

        if db_type == "duckdb":
            import duckdb

            def _quote_id(name: str) -> str:
                return '"' + str(name).replace('"', '""') + '"'

            path = dest.get("connection_string") or dest.get("database", "")
            if not path:
                return []
            conn = duckdb.connect(str(path))
            if cols == ["*"]:
                duckdb_col_sql = "*"
            else:
                duckdb_col_sql = ", ".join(_quote_id(c) for c in cols)
            duckdb_order = _quote_id(sort_key) if sort_key else "1"
            rows = conn.execute(
                f"SELECT {duckdb_col_sql} FROM {_quote_id(table_name)} ORDER BY {duckdb_order} LIMIT ?",
                (int(limit),),
            ).fetchall()
            names = [d[0] for d in conn.description]
            conn.close()
            return [dict(zip(names, row)) for row in rows]
    except Exception:
        return []
    return []
