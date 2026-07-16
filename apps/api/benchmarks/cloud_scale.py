"""Real-cloud scale harness for Snowflake, BigQuery, and S3.

Usage (from repo root):
    python -m apps.api.benchmarks.cloud_scale

Environment variables:
    DATAFLOW_BENCHMARK_SNOWFLAKE_ACCOUNT/USER/PASSWORD/DATABASE/WAREHOUSE/SCHEMA
    DATAFLOW_BENCHMARK_BIGQUERY_PROJECT/DATASET/KEY_PATH
    DATAFLOW_BENCHMARK_S3_ENDPOINT/BUCKET/ACCESS_KEY/SECRET_KEY/REGION

If no credentials are provided the harness prints a skip message and exits 0.
"""

from __future__ import annotations

import csv
import io
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ScaleResult:
    target: str
    rows: int
    success: bool
    elapsed_seconds: float = 0.0
    records_per_second: float = 0.0
    peak_memory_bytes: int = 0
    error: str = ""
    destination_summary: dict[str, Any] = field(default_factory=dict)


def _has_env(prefix: str, keys: list[str]) -> bool:
    return all(os.environ.get(f"{prefix}_{k}") for k in keys)


def _snowflake_config() -> dict[str, Any] | None:
    keys = ["ACCOUNT", "USER", "PASSWORD", "DATABASE", "WAREHOUSE", "SCHEMA"]
    if not _has_env("DATAFLOW_BENCHMARK_SNOWFLAKE", keys):
        return None
    return {
        "account": os.environ["DATAFLOW_BENCHMARK_SNOWFLAKE_ACCOUNT"],
        "username": os.environ["DATAFLOW_BENCHMARK_SNOWFLAKE_USER"],
        "password": os.environ["DATAFLOW_BENCHMARK_SNOWFLAKE_PASSWORD"],
        "database": os.environ["DATAFLOW_BENCHMARK_SNOWFLAKE_DATABASE"],
        "warehouse": os.environ["DATAFLOW_BENCHMARK_SNOWFLAKE_WAREHOUSE"],
        "schema": os.environ["DATAFLOW_BENCHMARK_SNOWFLAKE_SCHEMA"],
    }


def _bigquery_config() -> dict[str, Any] | None:
    keys = ["PROJECT", "DATASET"]
    if not _has_env("DATAFLOW_BENCHMARK_BIGQUERY", keys):
        return None
    cfg = {
        "project": os.environ["DATAFLOW_BENCHMARK_BIGQUERY_PROJECT"],
        "dataset": os.environ["DATAFLOW_BENCHMARK_BIGQUERY_DATASET"],
    }
    if os.environ.get("DATAFLOW_BENCHMARK_BIGQUERY_KEY_PATH"):
        cfg["service_account"] = os.environ["DATAFLOW_BENCHMARK_BIGQUERY_KEY_PATH"]
    return cfg


def _s3_config() -> dict[str, Any] | None:
    keys = ["ENDPOINT", "BUCKET", "ACCESS_KEY", "SECRET_KEY"]
    if not _has_env("DATAFLOW_BENCHMARK_S3", keys):
        return None
    return {
        "host": os.environ["DATAFLOW_BENCHMARK_S3_ENDPOINT"],
        "database": os.environ["DATAFLOW_BENCHMARK_S3_BUCKET"],
        "username": os.environ["DATAFLOW_BENCHMARK_S3_ACCESS_KEY"],
        "password": os.environ["DATAFLOW_BENCHMARK_S3_SECRET_KEY"],
        "region": os.environ.get("DATAFLOW_BENCHMARK_S3_REGION", "us-east-1"),
    }


def generate_csv(rows: int, *, seed: int = 42) -> bytes:
    """Generate a deterministic CSV with integer, decimal, string, timestamp columns."""
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(["id", "amount", "status", "created_at"])
    for i in range(rows):
        row_id = seed + i
        writer.writerow([
            row_id,
            f"{((row_id * 1000) % 100000) / 100:.2f}",
            "active" if i % 3 == 0 else "pending",
            "2024-01-01T00:00:00Z",
        ])
    return buf.getvalue().encode("utf-8")


def _run_transfer(
    *,
    target_label: str,
    dest_format: str,
    dest_kwargs: dict[str, Any],
    table_or_key: str,
    rows: int,
) -> ScaleResult:
    import sys
    from pathlib import Path

    api_root = Path(__file__).resolve().parents[2]
    if str(api_root) not in sys.path:
        sys.path.insert(0, str(api_root))

    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    content = generate_csv(rows)
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_content=content,
        source_filename=f"benchmark_{rows}.csv",
        destination=EndpointConfig(
            kind="database",
            format=dest_format,
            table=table_or_key,
            **dest_kwargs,
        ),
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="strict",
    )

    start = time.monotonic()
    result = UniversalTransferEngine().execute_tracked(
        request, f"bench_{target_label}_{rows}_{uuid.uuid4().hex[:8]}"
    )
    elapsed = time.monotonic() - start
    return ScaleResult(
        target=target_label,
        rows=rows,
        success=result.success,
        elapsed_seconds=round(elapsed, 3),
        records_per_second=round(result.records_transferred / elapsed, 1) if elapsed else 0.0,
        peak_memory_bytes=result.peak_memory_bytes,
        error=result.error or "",
        destination_summary=result.destination_summary,
    )


def benchmark_snowflake(rows: int = 1_000_000) -> ScaleResult:
    cfg = _snowflake_config()
    if not cfg:
        return ScaleResult(target="snowflake", rows=rows, success=False, error="No credentials")
    return _run_transfer(
        target_label="snowflake",
        dest_format="snowflake",
        dest_kwargs=cfg,
        table_or_key=f"dataflow_bench_{uuid.uuid4().hex[:8]}",
        rows=rows,
    )


def benchmark_bigquery(rows: int = 1_000_000) -> ScaleResult:
    cfg = _bigquery_config()
    if not cfg:
        return ScaleResult(target="bigquery", rows=rows, success=False, error="No credentials")
    return _run_transfer(
        target_label="bigquery",
        dest_format="bigquery",
        dest_kwargs=cfg,
        table_or_key=f"dataflow_bench_{uuid.uuid4().hex[:8]}",
        rows=rows,
    )


def benchmark_s3(rows: int = 1_000_000) -> ScaleResult:
    cfg = _s3_config()
    if not cfg:
        return ScaleResult(target="s3", rows=rows, success=False, error="No credentials")
    return _run_transfer(
        target_label="s3",
        dest_format="s3",
        dest_kwargs=cfg,
        table_or_key=f"dataflow_bench_{uuid.uuid4().hex[:8]}.csv",
        rows=rows,
    )


def benchmark_sqlite(rows: int = 100_000, *, db_path: str | None = None) -> ScaleResult:
    """Local, credential-free baseline benchmark using SQLite."""
    import os
    import sqlite3
    import tempfile

    target = "sqlite"
    path = db_path or tempfile.mktemp(suffix=".db")
    content = generate_csv(rows)
    try:
        result = _run_transfer(
            target_label=target,
            dest_format="sqlite",
            dest_kwargs={"connection_string": path},
            table_or_key="dataflow_bench",
            rows=rows,
        )
        conn = sqlite3.connect(path)
        try:
            count = conn.execute("SELECT COUNT(*) FROM dataflow_bench").fetchone()[0]
            result.destination_summary["row_count"] = count
            result.destination_summary["verified"] = count == rows
        finally:
            conn.close()
        return result
    finally:
        if os.path.exists(path) and db_path is None:
            os.unlink(path)


def run_local_benchmark(rows: int = 100_000, *, db_path: str | None = None) -> dict[str, Any]:
    """Run the credential-free local benchmark and return a standardized report."""
    import os

    os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
    os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    result = benchmark_sqlite(rows, db_path=db_path)
    return {
        "target": result.target,
        "rows": result.rows,
        "success": result.success,
        "elapsed_seconds": result.elapsed_seconds,
        "records_per_second": result.records_per_second,
        "peak_memory_bytes": result.peak_memory_bytes,
        "peak_memory_mb": round(result.peak_memory_bytes / (1024 * 1024), 1),
        "destination_summary": result.destination_summary,
        "error": result.error,
        "timestamp": _now(),
    }


def _now() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


def run_all(rows: int = 1_000_000) -> list[ScaleResult]:
    return [
        benchmark_sqlite(rows),
        benchmark_snowflake(rows),
        benchmark_bigquery(rows),
        benchmark_s3(rows),
    ]


if __name__ == "__main__":
    import json

    rows = int(os.environ.get("DATAFLOW_BENCHMARK_ROWS", "1000000"))
    results = run_all(rows)
    print(json.dumps([r.__dict__ for r in results], indent=2, default=str))
    any_ran = any(r.error != "No credentials" for r in results)
    if not any_ran:
        print("No cloud credentials configured; benchmarks skipped.")
    failed = [r for r in results if not r.success and r.error != "No credentials"]
    if failed:
        print(f"Failed benchmarks: {failed}")
        raise SystemExit(1)
