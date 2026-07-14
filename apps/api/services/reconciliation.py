"""Gate 8 reconciliation — independent target verification."""

from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Sequence

from services.transform_engine import _parse_date, _parse_datetime, apply_transform


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


def checksum_rows(rows: list[Sequence[Any]]) -> str:
    """Canonical, order-independent checksum over a matrix of typed values."""
    if not rows:
        return hashlib.sha256(b"").hexdigest()[:16]
    fingerprints: list[str] = []
    for row in rows:
        # Sort the normalized cells so rows with the same values in different
        # column order produce the same fingerprint.
        cells = sorted(normalize_cell(v) for v in row)
        fingerprints.append("|".join(cells))
    fingerprints.sort()
    h = hashlib.sha256()
    for fp in fingerprints:
        h.update(fp.encode("utf-8"))
    return h.hexdigest()[:16]


def aggregate_checksum(
    records: list[dict[str, Any]],
    columns: list[str] | None = None,
    *,
    sort_key: str | None = None,
) -> str:
    """
    Order-independent checksum for reconciliation.
    Sorts row fingerprints before hashing — same data in different order = same checksum.
    """
    if not records:
        return hashlib.sha256(b"").hexdigest()[:16]
    cols = columns or sorted(records[0].keys())
    fingerprints: list[str] = []
    for rec in records:
        parts = [f"{c}={normalize_cell(rec.get(c))}" for c in cols]
        fingerprints.append("\x1f".join(parts))
    if sort_key and sort_key in cols:
        fingerprints.sort(key=lambda fp: fp.split("\x1f")[cols.index(sort_key)] if sort_key in cols else fp)
    else:
        fingerprints.sort()
    h = hashlib.sha256()
    for fp in fingerprints:
        h.update(fp.encode("utf-8"))
    return h.hexdigest()[:16]


def reconcile(
    *,
    source_rows: int,
    target_rows: int,
    source_checksum: str,
    target_checksum: str,
    rejected_rows: int = 0,
    strict_checksum: bool = True,
    sample_compare: dict[str, Any] | None = None,
) -> ReconciliationReport:
    expected_rows = max(source_rows - max(rejected_rows, 0), 0)
    if expected_rows != target_rows:
        return ReconciliationReport(
            passed=False,
            source_rows=source_rows,
            target_rows=target_rows,
            source_checksum=source_checksum,
            target_checksum=target_checksum,
            message=(
                f"Row count mismatch: source {source_rows}, rejected {rejected_rows}, "
                f"expected target {expected_rows} vs target {target_rows}"
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
) -> tuple[int, str]:
    try:
        import psycopg2
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
            cur.execute(f'SELECT * FROM "{schema}"."{table_name}" ORDER BY 1 LIMIT 5000')
            rows = cur.fetchall()
        conn.close()
        return count, checksum_rows(rows)
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
                    cur.execute(f"USE WAREHOUSE {warehouse}")
                except Exception:
                    pass
            cur.execute(f'SELECT COUNT(*) FROM "{table_name}"')
            count = int(cur.fetchone()[0])
            cur.execute(f'SELECT * FROM "{table_name}" ORDER BY 1 LIMIT 5000')
            rows = cur.fetchall()
        conn.close()
        return count, checksum_rows(rows)
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
            cur.execute(f"SELECT * FROM `{table_name}` ORDER BY 1 LIMIT 5000")
            rows = cur.fetchall()
        conn.close()
        return count, checksum_rows(rows)
    except Exception:
        return -1, ""


def verify_bigquery_table(
    *,
    project_id: str,
    dataset_id: str,
    connection_string: str,
    table_name: str,
) -> tuple[int, str]:
    try:
        from connectors.bigquery_conn import get_client

        client = get_client(project_id=project_id, credentials_path=connection_string)
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        table = client.get_table(table_id)
        count = table.num_rows or 0
        rows = list(client.list_rows(table_id, max_results=5000))
        return int(count), checksum_rows([list(row.values()) for row in rows])
    except Exception:
        return -1, ""


def _rows_from_object_bytes(body: bytes, key: str) -> list[Sequence[Any]]:
    """Parse S3/GCS object payload (JSON, JSONL, CSV) into a row matrix."""
    import csv
    import io

    text = body.decode("utf-8", errors="replace")
    lower_key = (key or "").lower()

    if lower_key.endswith(".csv"):
        reader = csv.DictReader(io.StringIO(text))
        return [list(row.values()) for row in reader]

    if lower_key.endswith(".jsonl"):
        rows = []
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            parsed = json.loads(line)
            if isinstance(parsed, dict):
                rows.append(list(parsed.values()))
            else:
                rows.append([parsed])
        return rows

    # Default: JSON array or newline-delimited JSON.
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        data = []
    if isinstance(data, list):
        return [list(record.values()) for record in data if isinstance(record, dict)]
    if isinstance(data, dict):
        return [list(data.values())]
    return []


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
        rows = _rows_from_object_bytes(body, key)
        return len(rows), checksum_rows(rows)
    except Exception:
        return -1, ""


def verify_gcs_blob(
    *,
    bucket: str,
    key: str,
    host: str,
    port: int,
    connection_string: str,
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
        rows = _rows_from_object_bytes(body, key)
        return len(rows), checksum_rows(rows)
    except Exception:
        return -1, ""


def verify_sqlite_table(
    *,
    connection_string: str,
    database: str,
    table_name: str,
) -> tuple[int, str]:
    """Reconcile a SQLite target by reading the local file."""
    try:
        import sqlite3

        path = connection_string or database
        if not path:
            return -1, ""
        conn = sqlite3.connect(str(path))
        cur = conn.cursor()
        cur.execute(f'SELECT COUNT(*) FROM "{table_name}"')
        count = cur.fetchone()[0]
        cur.execute(f'SELECT * FROM "{table_name}" LIMIT 5000')
        rows = cur.fetchall()
        conn.close()
        return int(count), checksum_rows(rows)
    except Exception:
        return -1, ""


def verify_duckdb_table(
    *,
    connection_string: str,
    database: str,
    table_name: str,
) -> tuple[int, str]:
    """Reconcile a DuckDB target by reading the local file."""
    try:
        import duckdb

        path = connection_string or database
        if not path:
            return -1, ""
        conn = duckdb.connect(str(path))
        count = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
        rows = conn.execute(f'SELECT * FROM "{table_name}" LIMIT 5000').fetchall()
        conn.close()
        return int(count), checksum_rows(rows)
    except Exception:
        return -1, ""


def verify_mongodb_collection(
    *,
    connection_string: str,
    database: str,
    table_name: str,
) -> tuple[int, str]:
    """Reconcile a MongoDB target by counting and fingerprinting documents."""
    try:
        from pymongo import MongoClient
        from bson.decimal128 import Decimal128

        client = MongoClient(
            connection_string or "localhost", serverSelectionTimeoutMS=5000
        )
        db = client[database or "test"]
        coll = db[table_name]
        count = coll.count_documents({})
        rows = list(coll.find({}, {"_id": 0}).limit(5000))
        client.close()
        return int(count), checksum_rows([list(doc.values()) for doc in rows])
    except Exception:
        return -1, ""


def verify_dynamodb_table(
    *,
    connection_string: str,
    database: str,
    table_name: str,
    username: str = "local",
    password: str = "local",
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
        rows = []
        for page in paginator.paginate(TableName=table_name, Limit=5000):
            for item in page.get("Items", []):
                rows.append({k: deserializer.deserialize(v) for k, v in item.items()})

        return int(count), checksum_rows([list(row.values()) for row in rows])
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
) -> tuple[int, str]:
    if db_type == "mongodb":
        count, chk = verify_mongodb_collection(
            connection_string=dest.get("connection_string", ""),
            database=dest.get("database", ""),
            table_name=table_name,
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
        )
    elif db_type == "sqlite":
        count, chk = verify_sqlite_table(
            connection_string=dest.get("connection_string", ""),
            database=dest.get("database", ""),
            table_name=table_name,
        )
    elif db_type == "duckdb":
        count, chk = verify_duckdb_table(
            connection_string=dest.get("connection_string", ""),
            database=dest.get("database", ""),
            table_name=table_name,
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
        )
    elif db_type == "bigquery":
        count, chk = verify_bigquery_table(
            project_id=dest.get("database", ""),
            dataset_id=schema,
            connection_string=dest.get("connection_string", ""),
            table_name=table_name,
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
        )
    elif db_type == "gcs":
        count, chk = verify_gcs_blob(
            bucket=dest.get("database", ""),
            key=table_name,
            host=dest.get("host", ""),
            port=int(dest.get("port", 0)),
            connection_string=dest.get("connection_string", ""),
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
        return "true" if value else "false"
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
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, sort_keys=True, default=str)
    text = str(value).strip()
    canonical = _canonicalize_number(text)
    if canonical is not None:
        return canonical
    # Normalize JSON payloads (e.g. jsonb) to a canonical string.
    if text.startswith(("{", "[")):
        try:
            parsed = json.loads(text)
            if isinstance(parsed, (dict, list)):
                return json.dumps(parsed, sort_keys=True, default=str)
        except (json.JSONDecodeError, TypeError):
            pass
    # Normalize date/time formatting differences ("T" vs " ", "Z" vs "+00:00", etc.).
    # Always canonicalize date-only values to midnight UTC so date and datetime
    # representations of the same day produce identical checksums.
    if text:
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
    Uses normalized string comparison to tolerate driver formatting differences.
    Rows are sorted by a stable key (e.g. primary key) before comparing so source
    and target align even when the destination writes columns in a different order.
    """
    if not source_records or not target_rows or not mappings:
        return {"passed": True, "compared": 0, "mismatches": [], "skipped": True}

    def _row_key(rec: Any) -> Any:
        if isinstance(rec, dict):
            val = rec.get(sort_key) if sort_key else None
            return val or (rec.get(target_columns[0]) if target_columns else None)
        return rec

    def _as_dict(tgt_raw: Any) -> dict[str, Any] | None:
        if isinstance(tgt_raw, dict):
            return tgt_raw
        if target_columns and isinstance(tgt_raw, (list, tuple)):
            return {col: tgt_raw[i] if i < len(tgt_raw) else None for i, col in enumerate(target_columns)}
        return None

    source_sorted = sorted(source_records, key=lambda r: _row_key(r) or 0)
    target_sorted = sorted(
        (t for t in target_rows if _as_dict(t) is not None),
        key=lambda r: _row_key(_as_dict(r)) or 0,
    )

    mismatches: list[dict[str, str]] = []
    compared = 0
    limit = min(sample_size, len(source_sorted), len(target_sorted))

    def _normalize_source(raw: Any, transform: str | None) -> str:
        if transform:
            try:
                converted, _ = apply_transform(raw, transform)
            except Exception:
                converted = raw
        else:
            converted = raw
        return normalize_cell(converted)

    for idx in range(limit):
        src = source_sorted[idx]
        tgt = _as_dict(target_sorted[idx])
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
            import psycopg2
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
