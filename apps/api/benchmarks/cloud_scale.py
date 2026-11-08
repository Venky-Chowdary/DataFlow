"""Real-cloud scale harness for Snowflake, BigQuery, and S3.

Usage (from repo root):
    python -m apps.api.benchmarks.cloud_scale
    python -m apps.api.benchmarks.cloud_scale --indexed-keyset --rows 20000

Environment variables:
    DATAFLOW_BENCHMARK_SNOWFLAKE_ACCOUNT/USER/PASSWORD/DATABASE/WAREHOUSE/SCHEMA
    DATAFLOW_BENCHMARK_BIGQUERY_PROJECT/DATASET/KEY_PATH
    DATAFLOW_BENCHMARK_S3_ENDPOINT/BUCKET/ACCESS_KEY/SECRET_KEY/REGION
    DATAFLOW_KEYSET_BENCHMARK_URL (optional PostgreSQL URL for --indexed-keyset)

If no credentials are provided the harness prints a skip message and exits 0.
"""

from __future__ import annotations

import csv
import io
import os
import sys
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.brand_env import getenv_brand
from services.value_serializer import sanitize_json_value


def _new_job_id() -> str:
    """Return a 24-character hex ObjectId-compatible job id."""
    return os.urandom(12).hex()


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
    account = os.environ["DATAFLOW_BENCHMARK_SNOWFLAKE_ACCOUNT"]
    if ".snowflakecomputing.com" not in account:
        account = f"{account}.snowflakecomputing.com"
    return {
        "host": account,
        "port": 443,
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
    key_path = getenv_brand("BENCHMARK_BIGQUERY_KEY_PATH")
    if key_path:
        cfg["service_account"] = key_path
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
        "region": getenv_brand("BENCHMARK_S3_REGION", "us-east-1"),
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
    job_id: str | None = None,
) -> ScaleResult:
    import sys
    from pathlib import Path

    api_root = Path(__file__).resolve().parents[1]
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
    result = UniversalTransferEngine().execute_tracked(request, job_id or _new_job_id())
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


def benchmark_sqlite(
    rows: int = 100_000, *, db_path: str | None = None, job_id: str | None = None
) -> ScaleResult:
    """Local, credential-free baseline benchmark using SQLite."""
    import os
    import sqlite3
    import tempfile

    target = "sqlite"
    path = db_path or tempfile.mktemp(suffix=".db")
    try:
        result = _run_transfer(
            target_label=target,
            dest_format="sqlite",
            dest_kwargs={"connection_string": path},
            table_or_key="dataflow_bench",
            rows=rows,
            job_id=job_id,
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


def run_local_benchmark(
    rows: int = 100_000, *, db_path: str | None = None, job_id: str | None = None
) -> dict[str, Any]:
    """Run the credential-free local benchmark and return a standardized report.

    Sets the in-memory job store for THIS process. The hosted API never calls
    it in-process (``services.benchmark_runs`` runs it in a child process): the
    defaults it sets would otherwise switch off the object store for every
    later transfer the API runs.
    """
    import os

    os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
    os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    result = benchmark_sqlite(rows, db_path=db_path, job_id=job_id)
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


def run_indexed_keyset_benchmark(
    rows: int = 2_000, *, key_width: int = 3, page_size: int = 128,
    repetitions: int = 3, timeout_seconds: float = 15,
    database_url: str | None = None,
) -> dict[str, Any]:
    """Compare query-only indexed seeks on SQLite or opt-in PostgreSQL.

    Query timings include SQLAlchemy execution and fetching, but exclude fixture
    loading, plan collection and correctness checks. Each pass verifies the
    exact successor range after the midpoint bookmark.
    """
    import json
    import math
    import statistics
    import sqlalchemy as sa

    from services.keyset_pagination import encode_keyset_bookmark, sqlalchemy_keyset_clause

    for name, value, minimum, maximum in [
        ("rows", rows, 2, 1_000_000), ("key_width", key_width, 2, 16),
        ("page_size", page_size, 1, 10_000), ("repetitions", repetitions, 1, 10),
    ]:
        if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
            raise ValueError(f"{name} must be an integer in [{minimum}, {maximum}]")
    if (
        isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float))
        or not math.isfinite(timeout_seconds) or not 0 < timeout_seconds <= 600
    ):
        raise ValueError("timeout_seconds must be finite and in (0, 600]")
    url = sa.engine.make_url(database_url or "sqlite://")
    backend = url.get_backend_name()
    if backend not in ("sqlite", "postgresql"):
        raise ValueError("indexed keyset benchmark supports only SQLite and PostgreSQL")
    if backend == "sqlite" and url.database not in (None, "", ":memory:"):
        raise ValueError("SQLite benchmark requires a temporary in-memory database")

    deadline = time.monotonic() + timeout_seconds
    started = time.perf_counter()

    def check_deadline() -> None:
        if time.monotonic() >= deadline:
            raise TimeoutError("indexed keyset benchmark deadline exceeded")

    def prepare_statement(connection) -> None:
        check_deadline()
        if backend == "postgresql":
            remaining_ms = max(1, math.ceil((deadline - time.monotonic()) * 1000))
            connection.execute(
                sa.text("SELECT set_config('statement_timeout', :timeout, true)"),
                {"timeout": f"{remaining_ms}ms"},
            )

    def pg_index_search(value: Any) -> bool:
        if isinstance(value, list):
            return any(pg_index_search(child) for child in value)
        if isinstance(value, dict):
            if value.get("Node Type") in ("Index Scan", "Index Only Scan", "Bitmap Index Scan"):
                if value.get("Index Cond"):
                    return True
            return any(pg_index_search(child) for child in value.values() if isinstance(child, (list, dict)))
        return False

    divisors = [10 ** (key_width - position - 1) for position in range(key_width)]

    def keys(ordinal: int) -> tuple[int, ...]:
        return tuple(
            ordinal // divisor if position == 0 else (ordinal // divisor) % 10
            for position, divisor in enumerate(divisors)
        )

    options = {"connect_args": {"connect_timeout": max(1, math.ceil(timeout_seconds))}} if backend == "postgresql" else {}
    engine = sa.create_engine(url, **options)
    names = [f"key_{position}" for position in range(key_width)]
    table_name = f"_df_keyset_{uuid.uuid4().hex}" if backend == "postgresql" else "keyset_fixture"
    table = sa.Table(table_name, sa.MetaData(), *[
        sa.Column(name, sa.Integer, primary_key=True) for name in names
    ], prefixes=["TEMPORARY"] if backend == "postgresql" else [],
        **({"postgresql_on_commit": "DROP"} if backend == "postgresql" else {}))
    first = rows // 2
    initial_bookmark = encode_keyset_bookmark(keys(first - 1))
    modes = {"native_tuple": "postgresql", "portable_or": "sqlite"}
    algorithms: dict[str, Any] = {}

    def query(dialect: str, bookmark: str):
        clause = sqlalchemy_keyset_clause(sa, list(table.c), bookmark, dialect=dialect)
        return sa.select(table).where(clause).order_by(*table.c).limit(page_size), clause

    try:
        with engine.begin() as connection:
            driver_connection = connection.connection.driver_connection
            if backend == "sqlite":
                driver_connection.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
            try:
                prepare_statement(connection)
                table.create(connection, checkfirst=False)
                for offset in range(0, rows, 1000):
                    prepare_statement(connection)
                    connection.execute(table.insert(), [
                        dict(zip(names, keys(ordinal)))
                        for ordinal in range(offset, min(rows, offset + 1000))
                    ])
                prepare_statement(connection)
                if backend == "postgresql":
                    connection.exec_driver_sql("ANALYZE " + engine.dialect.identifier_preparer.format_table(table))
                fixture_seconds = time.perf_counter() - started
                prepare_statement(connection)
                version = connection.exec_driver_sql(
                    "SELECT sqlite_version()" if backend == "sqlite" else "SHOW server_version"
                ).scalar_one()
                for name, dialect in modes.items():
                    prepare_statement(connection)
                    statement, clause = query(dialect, initial_bookmark)
                    compiled = statement.compile(dialect=engine.dialect)
                    params = (
                        tuple(compiled.params[parameter] for parameter in compiled.positiontup)
                        if compiled.positiontup is not None else compiled.params
                    )
                    if backend == "sqlite":
                        plan_rows = connection.exec_driver_sql(
                            "EXPLAIN QUERY PLAN " + str(compiled), params,
                        ).all()
                        plan = [{"id": row[0], "parent": row[1], "detail": row[3]} for row in plan_rows]
                        uses_index_search = any(row["detail"].startswith("SEARCH ") for row in plan)
                    else:
                        plan = connection.exec_driver_sql(
                            "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + str(compiled), params,
                        ).scalar_one()
                        if isinstance(plan, str):
                            plan = json.loads(plan)
                        uses_index_search = pg_index_search(plan)
                    algorithms[name] = {
                        "predicate_dialect": dialect,
                        "sql": str(compiled), "plan": plan,
                        "bind_count": len(clause.compile(dialect=engine.dialect).params),
                        "query_bind_count": len(compiled.params),
                        "uses_index_search": uses_index_search,
                        "query_seconds_samples": [],
                    }

                for repetition in range(repetitions + 1):
                    order = list(modes) if repetition % 2 == 0 else list(reversed(modes))
                    for name in order:
                        expected = first
                        bookmark = initial_bookmark
                        query_seconds = 0.0
                        pages = 0
                        while True:
                            prepare_statement(connection)
                            statement, _ = query(modes[name], bookmark)
                            query_started = time.perf_counter()
                            page = connection.execute(statement).all()
                            query_seconds += time.perf_counter() - query_started
                            pages += 1
                            check_deadline()
                            if not page:
                                break
                            for row in page:
                                ordinal = 0
                                for component in row:
                                    ordinal = ordinal * 10 + int(component)
                                if ordinal != expected or expected >= rows:
                                    raise ValueError(f"keyset correctness failed in {name} at row {expected}")
                                expected += 1
                            bookmark = encode_keyset_bookmark(tuple(page[-1]))
                        if expected != rows:
                            raise ValueError(f"keyset correctness failed in {name}: missing successors")
                        algorithms[name]["verified_rows"] = expected - first
                        algorithms[name]["pages"] = pages
                        if repetition:
                            algorithms[name]["query_seconds_samples"].append(query_seconds)

                for result in algorithms.values():
                    result["median_query_seconds"] = statistics.median(result["query_seconds_samples"])
                check_deadline()
                return {
                    "success": True,
                    "evidence_level": "sqlite_local_query_benchmark" if backend == "sqlite" else "postgresql_query_benchmark",
                    "database_engine": backend, "database_version": version,
                    "fixture_storage": "temporary_in_memory_sqlite" if backend == "sqlite" else "temporary_postgresql_table",
                    "fixture_rows": rows, "key_width": key_width, "page_size": page_size,
                    "fixture_ddl": str(sa.schema.CreateTable(table).compile(dialect=engine.dialect)),
                    "fixture_seconds": fixture_seconds,
                    "benchmark_elapsed_seconds": time.perf_counter() - started,
                    "repetitions": repetitions, "warmup_runs": 1, "execution_order": "alternating",
                    "scope": {"first_ordinal": first, "last_ordinal": rows - 1},
                    "verified_successors": rows - first, "algorithms": algorithms,
                    "plan_scope": "initial_midpoint_bookmark", "migration_throughput_evidence": False,
                    "timing_scope": {
                        "measured": "sqlalchemy_execute_and_fetch",
                        "excluded": [
                            "fixture_setup", "plan_collection", "statement_construction",
                            "deadline_setup", "correctness_verification", "migration_write_and_reconciliation",
                        ],
                    },
                }
            except sa.exc.DBAPIError as exc:
                if time.monotonic() >= deadline:
                    raise TimeoutError("indexed keyset benchmark deadline exceeded") from exc
                raise
            finally:
                if backend == "sqlite":
                    driver_connection.set_progress_handler(None, 0)
    finally:
        engine.dispose()


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
    import argparse
    import json

    parser = argparse.ArgumentParser(description="Scale transfers or compare indexed query predicates.")
    parser.add_argument("--indexed-keyset", action="store_true", help="Run a query-only SQLite or opt-in PostgreSQL benchmark")
    parser.add_argument("--rows", type=int)
    parser.add_argument("--key-width", type=int, default=3)
    parser.add_argument("--page-size", type=int, default=128)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--timeout-seconds", type=float, default=15)
    options = parser.parse_args()
    if options.indexed_keyset:
        try:
            report = run_indexed_keyset_benchmark(
                rows=options.rows if options.rows is not None else 2_000,
                key_width=options.key_width, page_size=options.page_size,
                repetitions=options.repetitions, timeout_seconds=options.timeout_seconds,
                database_url=os.environ.get("DATAFLOW_KEYSET_BENCHMARK_URL"),
            )
        except Exception as exc:
            print(json.dumps({
                "success": False, "error_type": type(exc).__name__,
                "error": str(exc) if isinstance(exc, (ValueError, TimeoutError)) else "Benchmark failed; no result certified",
            }))
            raise SystemExit(1) from exc
        print(json.dumps(report, indent=2, default=sanitize_json_value))
        raise SystemExit(0)
    rows = options.rows if options.rows is not None else int(getenv_brand("BENCHMARK_ROWS", "1000000"))
    results = run_all(rows)
    print(json.dumps([r.__dict__ for r in results], indent=2, default=sanitize_json_value))
    any_ran = any(r.error != "No credentials" for r in results)
    if not any_ran:
        print("No cloud credentials configured; benchmarks skipped.")
    failed = [r for r in results if not r.success and r.error != "No credentials"]
    if failed:
        print(f"Failed benchmarks: {failed}")
        raise SystemExit(1)
