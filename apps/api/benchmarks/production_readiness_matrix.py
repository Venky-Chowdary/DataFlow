"""Production-readiness connector matrix.

Exercises the UniversalTransferEngine across heterogeneous sources and destinations
that are available in the local docker-compose stack (Postgres, MySQL, MongoDB,
MinIO, DynamoDB via moto, Snowflake via fakesnow) plus SQLite and file exports.

Run from the repo root:
    source .venv/bin/activate
    DATAFLOW_JOB_STORE=memory DATAFLOW_DISABLE_OBJECT_STORE=1 \
    DATAFLOW_DATA_DIR=/tmp/dataflow-matrix \
    python -m apps.api.benchmarks.production_readiness_matrix

Environment (optional overrides):
    DATAFLOW_MATRIX_ROWS_SQLITE=5000000
    DATAFLOW_MATRIX_ROWS_PG=1000000
    DATAFLOW_MATRIX_ROWS_MYSQL=1000000
    DATAFLOW_MATRIX_ROWS_CROSS=100000
    DATAFLOW_MATRIX_ROWS_CLOUD=100000
    DATAFLOW_MATRIX_ROWS_DYNAMODB=10000
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import fakesnow

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import csv  # noqa: E402
from services.value_serializer import sanitize_json_value  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_job_id() -> str:
    return os.urandom(12).hex()


@dataclass
class MatrixResult:
    leg: str
    source: str
    destination: str
    rows: int
    success: bool
    elapsed_seconds: float = 0.0
    records_per_second: float = 0.0
    peak_memory_bytes: int = 0
    error: str = ""
    row_count_verified: int | None = None
    checksum_match: bool | None = None
    destination_summary: dict[str, Any] = field(default_factory=dict)


@dataclass
class Report:
    started_at: str = field(default_factory=_now)
    environment: dict[str, Any] = field(default_factory=dict)
    results: list[MatrixResult] = field(default_factory=list)


# -----------------------------------------------------------------------------
# Local docker-compose endpoint defaults
# -----------------------------------------------------------------------------
_SQLITE_PATH = "/tmp/dataflow_matrix.db"

_PG = {
    "host": "localhost",
    "port": 5432,
    "database": "dataflow",
    "username": "dataflow",
    "password": "dataflow",
    "schema": "public",
}

_MYSQL = {
    "host": "localhost",
    "port": 3306,
    "database": "dataflow",
    "username": "root",
    "password": "dataflow",
}

_MONGO = {
    "host": "localhost",
    "port": 27017,
    "database": "dataflow_matrix",
    "collection": "data",
}

_MINIO = {
    "host": "localhost",
    "port": 9000,
    "username": "dataflow",
    "password": "dataflowsecret",
    "database": "dataflow-matrix",
    "endpoint_url": "http://localhost:9000",
    "path_style": True,
    "region": "us-east-1",
}

_DYNAMODB = {
    "host": "us-east-1",
    "port": 5555,
    "username": "AKIA",
    "password": "secret",
    "endpoint_url": "http://localhost:5555",
    "region": "us-east-1",
    "database": "dataflow_matrix",
}

_SNOWFLAKE = {
    "host": "test.snowflakecomputing.com",
    "port": 443,
    "username": "dataflow",
    "password": "dataflow",
    "database": "dataflow",
    "warehouse": "dataflow_wh",
    "schema": "public",
}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default


def _csv_path(rows: int, seed: int = 42) -> str:
    fd, path = tempfile.mkstemp(suffix=".csv", prefix="dataflow_matrix_")
    with os.fdopen(fd, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(["id", "amount", "status", "created_at"])
        for i in range(rows):
            row_id = seed + i
            writer.writerow([
                row_id,
                f"{((row_id * 1000) % 100000) / 100:.2f}",
                "active" if i % 3 == 0 else "pending",
                "2024-01-01T00:00:00Z",
            ])
    return path


def _run_file_to_db(
    *,
    target_label: str,
    dest_format: str,
    dest_kwargs: dict[str, Any],
    table: str,
    rows: int,
    seed: int = 42,
    validation_mode: str = "strict",
) -> MatrixResult:
    path = _csv_path(rows, seed=seed)
    try:
        request = TransferRequest(
            source=EndpointConfig(kind="file", format="csv"),
            source_content=b"",
            source_path=path,
            source_filename=f"matrix_{rows}.csv",
            destination=EndpointConfig(
                kind="database",
                format=dest_format,
                table=table,
                **dest_kwargs,
            ),
            sync_mode="full_refresh_overwrite",
            skip_preflight=True,
            validation_mode=validation_mode,
        )
        result = _execute(label=f"file_csv->{dest_format}", src="file_csv", dst=dest_format, request=request, rows=rows)
    finally:
        Path(path).unlink(missing_ok=True)
    return result


def _run_db_to_db(
    *,
    label: str,
    src_format: str,
    src_kwargs: dict[str, Any],
    src_table: str,
    dst_format: str,
    dst_kwargs: dict[str, Any],
    dst_table: str,
    rows: int,
) -> MatrixResult:
    request = TransferRequest(
        source=EndpointConfig(
            kind="database",
            format=src_format,
            table=src_table,
            **src_kwargs,
        ),
        destination=EndpointConfig(
            kind="database",
            format=dst_format,
            table=dst_table,
            **dst_kwargs,
        ),
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
    )
    return _execute(label=label, src=src_format, dst=dst_format, request=request, rows=rows)


def _run_db_to_file(
    *,
    label: str,
    src_format: str,
    src_kwargs: dict[str, Any],
    src_table: str,
    file_format: str,
    output_path: str,
    rows: int,
) -> MatrixResult:
    request = TransferRequest(
        source=EndpointConfig(
            kind="database",
            format=src_format,
            table=src_table,
            **src_kwargs,
        ),
        destination=EndpointConfig(
            kind="file_export",
            format=file_format,
            output_path=output_path,
            table=src_table,
        ),
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
    )
    return _execute(label=label, src=src_format, dst=f"file:{file_format}", request=request, rows=rows)


def _execute(
    *,
    label: str,
    src: str,
    dst: str,
    request: TransferRequest,
    rows: int,
) -> MatrixResult:
    engine = UniversalTransferEngine()
    start = time.monotonic()
    result = engine.execute_tracked(request, _new_job_id())
    elapsed = time.monotonic() - start
    return MatrixResult(
        leg=label,
        source=src,
        destination=dst,
        rows=rows,
        success=result.success,
        elapsed_seconds=round(elapsed, 3),
        records_per_second=round(result.records_transferred / elapsed, 1) if elapsed else 0.0,
        peak_memory_bytes=result.peak_memory_bytes,
        error=result.error or "",
        destination_summary=result.destination_summary,
    )


# -----------------------------------------------------------------------------
# Verification helpers
# -----------------------------------------------------------------------------
def _verify_sqlite_table(path: str, table: str, expected: int) -> int:
    conn = sqlite3.connect(path)
    try:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    finally:
        conn.close()
    return count


def _verify_postgres_table(table: str, expected: int) -> int:
    import psycopg2

    conn = psycopg2.connect(
        host=_PG["host"],
        port=_PG["port"],
        database=_PG["database"],
        user=_PG["username"],
        password=_PG["password"],
    )
    conn.autocommit = True
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        return cur.fetchone()[0]
    finally:
        conn.close()


def _verify_mysql_table(table: str, expected: int) -> int:
    import pymysql

    conn = pymysql.connect(
        host=_MYSQL["host"],
        port=_MYSQL["port"],
        database=_MYSQL["database"],
        user=_MYSQL["username"],
        password=_MYSQL["password"],
    )
    conn.autocommit(True)
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        return cur.fetchone()[0]
    finally:
        conn.close()


def _verify_mongo_collection(db_name: str, collection: str, expected: int) -> int:
    from pymongo import MongoClient

    client = MongoClient(f"mongodb://localhost:27017/?directConnection=true", serverSelectionTimeoutMS=5000)
    try:
        return client[db_name][collection].count_documents({})
    finally:
        client.close()


def _verify_s3_object(bucket: str, key: str) -> int:
    import boto3
    from botocore.config import Config

    client = boto3.client(
        "s3",
        endpoint_url=_MINIO["endpoint_url"],
        aws_access_key_id=_MINIO["username"],
        aws_secret_access_key=_MINIO["password"],
        config=Config(s3={"addressing_style": "path"}),
        region_name=_MINIO["region"],
    )
    try:
        obj = client.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read()
        return len(body.decode("utf-8").strip().splitlines()) - 1  # minus header
    except Exception:
        return -1


def _verify_dynamodb_table(table: str, expected: int) -> int:
    import boto3

    client = boto3.client(
        "dynamodb",
        endpoint_url=_DYNAMODB["endpoint_url"],
        aws_access_key_id=_DYNAMODB["username"],
        aws_secret_access_key=_DYNAMODB["password"],
        region_name=_DYNAMODB["region"],
    )
    try:
        resp = client.scan(TableName=table, Select="COUNT")
        return resp.get("Count", -1)
    except Exception:
        return -1


def _verify_snowflake_table(table: str, expected: int) -> int:
    """Verify row count in fakesnow. Caller must have fakesnow.patch() active."""
    import snowflake.connector

    conn = snowflake.connector.connect(
        account=_SNOWFLAKE["host"].replace(".snowflakecomputing.com", ""),
        user=_SNOWFLAKE["username"],
        password=_SNOWFLAKE["password"],
        database=_SNOWFLAKE["database"],
        schema=_SNOWFLAKE["schema"],
        warehouse=_SNOWFLAKE["warehouse"],
    )
    try:
        cur = conn.cursor()
        cur.execute(f'SELECT COUNT(*) FROM "{table}"')
        return cur.fetchone()[0]
    finally:
        conn.close()


# -----------------------------------------------------------------------------
# Matrix legs
# -----------------------------------------------------------------------------
def _seed_source_table(
    *,
    target: str,
    table: str,
    rows: int,
    seed: int = 42,
) -> None:
    """Seed a source table by loading a generated CSV into it."""
    dest_kwargs: dict[str, Any]
    if target == "postgresql":
        dest_kwargs = _PG
    elif target == "mysql":
        dest_kwargs = _MYSQL
    elif target == "sqlite":
        dest_kwargs = {"connection_string": _SQLITE_PATH}
    else:
        raise ValueError(f"Unsupported seed target: {target}")

    result = _run_file_to_db(
        target_label=target,
        dest_format=target,
        dest_kwargs=dest_kwargs,
        table=table,
        rows=rows,
        seed=seed,
    )
    if not result.success:
        raise RuntimeError(f"Could not seed {target} source table {table}: {result.error}")


def _prepare_sqlite() -> None:
    Path(_SQLITE_PATH).unlink(missing_ok=True)


def _matrix_sqlite(report: Report, rows: int) -> None:
    _prepare_sqlite()
    table = f"df_sqlite_bench_{uuid.uuid4().hex[:8]}"
    r = _run_file_to_db(
        target_label="sqlite",
        dest_format="sqlite",
        dest_kwargs={"connection_string": _SQLITE_PATH},
        table=table,
        rows=rows,
    )
    if r.success:
        r.row_count_verified = _verify_sqlite_table(_SQLITE_PATH, table, rows)
        r.checksum_match = r.row_count_verified == rows
    report.results.append(r)


def _matrix_postgres(report: Report, rows: int) -> None:
    table = f"pg_bench_{uuid.uuid4().hex[:8]}"
    r = _run_file_to_db(
        target_label="postgresql",
        dest_format="postgresql",
        dest_kwargs=_PG,
        table=table,
        rows=rows,
    )
    if r.success:
        r.row_count_verified = _verify_postgres_table(table, rows)
        r.checksum_match = r.row_count_verified == rows
    report.results.append(r)


def _matrix_mysql(report: Report, rows: int) -> None:
    table = f"mysql_bench_{uuid.uuid4().hex[:8]}"
    r = _run_file_to_db(
        target_label="mysql",
        dest_format="mysql",
        dest_kwargs=_MYSQL,
        table=table,
        rows=rows,
    )
    if r.success:
        r.row_count_verified = _verify_mysql_table(table, rows)
        r.checksum_match = r.row_count_verified == rows
    report.results.append(r)


def _matrix_mongodb(report: Report, rows: int) -> None:
    db_name = f"dataflow_matrix_{uuid.uuid4().hex[:8]}"
    collection = "data"
    r = _run_file_to_db(
        target_label="mongodb",
        dest_format="mongodb",
        dest_kwargs={**_MONGO, "database": db_name, "collection": collection},
        table=collection,
        rows=rows,
    )
    if r.success:
        r.row_count_verified = _verify_mongo_collection(db_name, collection, rows)
        r.checksum_match = r.row_count_verified == rows
    report.results.append(r)


def _matrix_s3(report: Report, rows: int) -> None:
    key = f"matrix/{uuid.uuid4().hex[:8]}/data.csv"
    r = _run_file_to_db(
        target_label="s3",
        dest_format="s3",
        dest_kwargs={**_MINIO, "database": _MINIO["database"]},
        table=key,
        rows=rows,
    )
    if r.success:
        r.row_count_verified = _verify_s3_object(_MINIO["database"], key)
        r.checksum_match = r.row_count_verified == rows
    report.results.append(r)


def _matrix_dynamodb(report: Report, rows: int) -> None:
    table = f"dynamodb_bench_{uuid.uuid4().hex[:8]}"
    r = _run_file_to_db(
        target_label="dynamodb",
        dest_format="dynamodb",
        dest_kwargs=_DYNAMODB,
        table=table,
        rows=rows,
    )
    if r.success:
        r.row_count_verified = _verify_dynamodb_table(table, rows)
        r.checksum_match = r.row_count_verified == rows
    report.results.append(r)


def _matrix_snowflake(report: Report, rows: int) -> None:
    table = f"sf_bench_{uuid.uuid4().hex[:8]}"
    with fakesnow.patch():
        r = _run_file_to_db(
            target_label="snowflake",
            dest_format="snowflake",
            dest_kwargs=_SNOWFLAKE,
            table=table,
            rows=rows,
        )
        if r.success:
            actual_table = r.destination_summary.get("table", table.lower())
            r.row_count_verified = _verify_snowflake_table(actual_table, rows)
            r.checksum_match = r.row_count_verified == rows
    report.results.append(r)


def _matrix_cross_db(report: Report, rows: int) -> None:
    src_pg = f"df_pg_src_{uuid.uuid4().hex[:8]}"
    src_mysql = f"df_mysql_src_{uuid.uuid4().hex[:8]}"
    src_sqlite = f"df_sqlite_src_{uuid.uuid4().hex[:8]}"

    _seed_source_table(target="postgresql", table=src_pg, rows=rows, seed=1)
    _seed_source_table(target="mysql", table=src_mysql, rows=rows, seed=2)
    _seed_source_table(target="sqlite", table=src_sqlite, rows=rows, seed=4)

    legs = [
        ("postgresql", _PG, src_pg, "mysql", _MYSQL, f"pg_to_mysql_{uuid.uuid4().hex[:8]}"),
        ("mysql", _MYSQL, src_mysql, "postgresql", _PG, f"mysql_to_pg_{uuid.uuid4().hex[:8]}"),
        ("postgresql", _PG, src_pg, "sqlite", {"connection_string": _SQLITE_PATH}, f"pg_to_sqlite_{uuid.uuid4().hex[:8]}"),
        ("mysql", _MYSQL, src_mysql, "sqlite", {"connection_string": _SQLITE_PATH}, f"mysql_to_sqlite_{uuid.uuid4().hex[:8]}"),
        ("sqlite", {"connection_string": _SQLITE_PATH}, src_sqlite, "postgresql", _PG, f"sqlite_to_pg_{uuid.uuid4().hex[:8]}"),
    ]
    for src_fmt, src_kwargs, src_table, dst_fmt, dst_kwargs, dst_table in legs:
        label = f"{src_fmt}:{src_table}->{dst_fmt}:{dst_table}"
        r = _run_db_to_db(
            label=label,
            src_format=src_fmt,
            src_kwargs=src_kwargs,
            src_table=src_table,
            dst_format=dst_fmt,
            dst_kwargs=dst_kwargs,
            dst_table=dst_table,
            rows=rows,
        )
        report.results.append(r)


def _matrix_file_exports(report: Report, rows: int) -> None:
    src_table = f"pg_export_src_{uuid.uuid4().hex[:8]}"
    _seed_source_table(target="postgresql", table=src_table, rows=rows, seed=3)
    export_dir = _API_ROOT / "exports"
    export_dir.mkdir(parents=True, exist_ok=True)
    for fmt in ("csv", "json", "parquet"):
        ext = "jsonl" if fmt == "json" else fmt
        path = str(export_dir / f"export_{fmt}_{uuid.uuid4().hex[:8]}.{ext}")
        r = _run_db_to_file(
            label=f"postgresql->{fmt}",
            src_format="postgresql",
            src_kwargs=_PG,
            src_table=src_table,
            file_format=fmt,
            output_path=path,
            rows=rows,
        )
        if r.success and Path(path).exists():
            if fmt == "parquet":
                try:
                    import pyarrow.parquet as pq
                    r.row_count_verified = pq.read_table(path).num_rows
                except Exception:
                    r.row_count_verified = -1
            elif fmt == "csv":
                with open(path, "r", encoding="utf-8") as f:
                    content = f.read()
                r.row_count_verified = len(content.strip().splitlines()) - 1
            else:
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    r.row_count_verified = len(data) if isinstance(data, list) else -1
                except Exception:
                    r.row_count_verified = -1
            r.checksum_match = r.row_count_verified == rows
        report.results.append(r)


# -----------------------------------------------------------------------------
# Entry point
# -----------------------------------------------------------------------------
def main() -> Report:
    os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
    os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

    report = Report(
        environment={
            "DATAFLOW_JOB_STORE": os.environ.get("DATAFLOW_JOB_STORE"),
            "DATAFLOW_DATA_DIR": os.environ.get("DATAFLOW_DATA_DIR"),
            "DATAFLOW_DISABLE_OBJECT_STORE": os.environ.get("DATAFLOW_DISABLE_OBJECT_STORE"),
        }
    )

    rows_sqlite = _env_int("DATAFLOW_MATRIX_ROWS_SQLITE", 5_000_000)
    rows_pg = _env_int("DATAFLOW_MATRIX_ROWS_PG", 1_000_000)
    rows_mysql = _env_int("DATAFLOW_MATRIX_ROWS_MYSQL", 1_000_000)
    rows_cross = _env_int("DATAFLOW_MATRIX_ROWS_CROSS", 100_000)
    rows_cloud = _env_int("DATAFLOW_MATRIX_ROWS_CLOUD", 100_000)
    rows_dynamodb = _env_int("DATAFLOW_MATRIX_ROWS_DYNAMODB", 10_000)

    print("=== DataFlow production readiness matrix ===", flush=True)
    print(f"SQLite rows: {rows_sqlite:,}", flush=True)
    print(f"Postgres rows: {rows_pg:,}", flush=True)
    print(f"MySQL rows: {rows_mysql:,}", flush=True)
    print(f"Cross-DB rows: {rows_cross:,}", flush=True)
    print(f"Cloud (Mongo/S3/Snowflake) rows: {rows_cloud:,}", flush=True)
    print(f"DynamoDB rows: {rows_dynamodb:,}", flush=True)

    _matrix_sqlite(report, rows_sqlite)
    _matrix_postgres(report, rows_pg)
    _matrix_mysql(report, rows_mysql)
    _matrix_mongodb(report, rows_cloud)
    _matrix_s3(report, rows_cloud)
    _matrix_dynamodb(report, rows_dynamodb)
    _matrix_snowflake(report, rows_cloud)
    _matrix_cross_db(report, rows_cross)
    _matrix_file_exports(report, rows_cross)

    report.environment["finished_at"] = _now()
    return report


if __name__ == "__main__":
    report = main()
    summary = {
        "started_at": report.started_at,
        "finished_at": report.environment.get("finished_at"),
        "environment": report.environment,
        "total_legs": len(report.results),
        "passed": sum(1 for r in report.results if r.success),
        "failed": sum(1 for r in report.results if not r.success),
        "verified": sum(1 for r in report.results if r.checksum_match),
        "results": [asdict(r) for r in report.results],
    }
    out = json.dumps(summary, indent=2, default=sanitize_json_value)
    report_path = Path(os.environ.get("DATAFLOW_DATA_DIR", "/tmp/dataflow-matrix")) / "report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(out, encoding="utf-8")
    print(out)

    failed = [r for r in report.results if not r.success]
    if failed:
        print(f"\nFAILED LEGS: {len(failed)}")
        for f in failed:
            print(f"  {f.leg}: {f.error}")
        raise SystemExit(1)
