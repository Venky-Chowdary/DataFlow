"""Live cloud connector matrix harness.

Exercises the UniversalTransferEngine across heterogeneous sources and real cloud
destinations (Snowflake by default) and reports timing + row fidelity.

Usage (from repo root):
    python -m apps.api.benchmarks.cloud_connector_matrix

Environment:
    DATAFLOW_BENCHMARK_SNOWFLAKE_ACCOUNT/USER/PASSWORD/DATABASE/WAREHOUSE/SCHEMA

The harness skips any leg whose source or destination is not reachable.
"""

from __future__ import annotations

import json
import os
import socket
import tempfile
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_job_id() -> str:
    return os.urandom(12).hex()


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


def _reachable(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _generate_rows(count: int, *, seed: int = 42) -> list[dict[str, Any]]:
    """Return rows covering integer, decimal, boolean, timestamp, JSON, string and array types."""
    rows: list[dict[str, Any]] = []
    for i in range(count):
        row_id = seed + i
        rows.append({
            "id": row_id,
            "amount": f"{((row_id * 1000) % 100000) / 100:.2f}",
            "active": i % 3 == 0,
            "created_at": "2024-01-01T00:00:00Z",
            "name": f"user_{row_id}",
            "meta": {"seq": i, "ok": i % 2 == 0},
            "tags": ["a", "b"] if i % 2 == 0 else ["c"],
        })
    return rows


def _pg_conn():
    import psycopg2
    return psycopg2.connect(host="localhost", port=5432, database="dataflow", user="dataflow", password="dataflow")


def _mysql_conn():
    import pymysql
    return pymysql.connect(host="localhost", port=3306, user="dataflow", password="dataflow", database="dataflow")


def _mongo_client():
    from pymongo import MongoClient
    return MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=5000)


def _sf_conn(cfg: dict[str, Any]):
    import snowflake.connector
    return snowflake.connector.connect(
        account=cfg["host"].replace(".snowflakecomputing.com", ""),
        user=cfg["username"],
        password=cfg["password"],
        database=cfg["database"],
        schema=cfg["schema"],
        warehouse=cfg["warehouse"],
    )


@dataclass
class MatrixResult:
    source: str
    destination: str
    rows: int
    success: bool
    elapsed_seconds: float = 0.0
    records_per_second: float = 0.0
    error: str = ""
    source_checksum: str = ""
    target_checksum: str = ""


@dataclass
class Report:
    started_at: str = field(default_factory=_now)
    results: list[MatrixResult] = field(default_factory=list)


def _run_transfer(source: dict[str, Any], destination: dict[str, Any], rows: int) -> MatrixResult:
    import sys
    from pathlib import Path

    api_root = Path(__file__).resolve().parents[1]
    if str(api_root) not in sys.path:
        sys.path.insert(0, str(api_root))

    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    src = EndpointConfig(kind="database", format=source["format"], **{k: v for k, v in source.items() if k != "format"})
    dst = EndpointConfig(kind=destination["kind"], format=destination["format"], **{k: v for k, v in destination.items() if k not in ("kind", "format")})
    request = TransferRequest(
        source=src,
        destination=dst,
        sync_mode="full_refresh_overwrite",
        stream_contracts=[{"name": source.get("table") or source.get("collection") or "data", "sync_mode": "full_refresh_overwrite", "primary_key": "id", "selected": True}],
        skip_preflight=False,
    )
    engine = UniversalTransferEngine()
    start = time.monotonic()
    result = engine.execute_tracked(request, _new_job_id())
    elapsed = time.monotonic() - start
    return MatrixResult(
        source=f"{source.get('format')}:{source.get('table') or source.get('collection')}",
        destination=f"{destination.get('kind')}:{destination.get('format')}:{destination.get('table') or destination.get('collection')}",
        rows=rows,
        success=result.success,
        elapsed_seconds=round(elapsed, 3),
        records_per_second=round(result.records_transferred / elapsed, 1) if elapsed else 0.0,
        error=result.error or "",
        source_checksum=result.reconciliation.get("source_checksum", ""),
        target_checksum=result.reconciliation.get("target_checksum", ""),
    )


def _seed_postgres(table: str, rows: list[dict[str, Any]]) -> None:
    import psycopg2
    from psycopg2.extras import Json
    conn = _pg_conn()
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {table}")
    cur.execute(
        f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, amount TEXT, active BOOLEAN, created_at TEXT, name TEXT, meta JSONB, tags JSONB)"
    )
    for r in rows:
        cur.execute(
            f"INSERT INTO {table} VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (r["id"], r["amount"], r["active"], r["created_at"], r["name"], Json(r["meta"]), Json(r["tags"])),
        )
    conn.close()


def _seed_mysql(table: str, rows: list[dict[str, Any]]) -> None:
    import json
    conn = _mysql_conn()
    conn.autocommit(True)
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {table}")
    cur.execute(
        f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, amount VARCHAR(50), active BOOLEAN, created_at VARCHAR(50), name VARCHAR(100), meta JSON, tags JSON)"
    )
    for r in rows:
        cur.execute(
            f"INSERT INTO {table} VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (r["id"], r["amount"], r["active"], r["created_at"], r["name"], json.dumps(r["meta"]), json.dumps(r["tags"])),
        )
    conn.close()


def _seed_mongodb(db_name: str, collection: str, rows: list[dict[str, Any]]) -> None:
    client = _mongo_client()
    client.drop_database(db_name)
    client[db_name][collection].insert_many(rows)
    client.close()


def _seed_snowflake(sf_cfg: dict[str, Any], table: str, rows: list[dict[str, Any]]) -> None:
    import snowflake.connector
    conn = _sf_conn(sf_cfg)
    cur = conn.cursor()
    cur.execute(f'DROP TABLE IF EXISTS "{table}"')
    cur.execute(
        f'CREATE TABLE "{table}" (id BIGINT, amount VARCHAR(50), active BOOLEAN, created_at VARCHAR(50), name VARCHAR(100), meta VARIANT, tags VARIANT)'
    )
    for r in rows:
        cur.execute(
            f'INSERT INTO "{table}" SELECT %s, %s, %s, %s, %s, parse_json(%s), parse_json(%s)',
            (r["id"], r["amount"], r["active"], r["created_at"], r["name"], json.dumps(r["meta"]), json.dumps(r["tags"])),
        )
    conn.close()


def _cleanup_postgres(table: str) -> None:
    try:
        conn = _pg_conn()
        conn.autocommit = True
        conn.cursor().execute(f"DROP TABLE IF EXISTS {table}")
        conn.close()
    except Exception:
        pass


def _cleanup_mysql(table: str) -> None:
    try:
        conn = _mysql_conn()
        conn.autocommit(True)
        conn.cursor().execute(f"DROP TABLE IF EXISTS {table}")
        conn.close()
    except Exception:
        pass


def _cleanup_mongodb(db_name: str) -> None:
    try:
        _mongo_client().drop_database(db_name)
    except Exception:
        pass


def _cleanup_snowflake(sf_cfg: dict[str, Any], table: str) -> None:
    try:
        conn = _sf_conn(sf_cfg)
        conn.cursor().execute(f'DROP TABLE IF EXISTS "{table}"')
        conn.close()
    except Exception:
        pass


def _ensure_postgres_db() -> bool:
    try:
        import psycopg2
        conn = psycopg2.connect(host="localhost", port=5432, database="dataflow", user="dataflow", password="dataflow")
        conn.close()
        return True
    except Exception:
        return False


def _ensure_mysql_db() -> bool:
    try:
        import pymysql
        conn = pymysql.connect(host="localhost", port=3306, user="dataflow", password="dataflow", database="dataflow")
        conn.close()
        return True
    except Exception:
        return False


def main(rows: int = 1000) -> None:
    sf_cfg = _snowflake_config()
    if not sf_cfg:
        print(json.dumps({"error": "Set DATAFLOW_BENCHMARK_SNOWFLAKE_* environment variables"}, indent=2))
        raise SystemExit(1)

    rows_list = _generate_rows(rows)
    sf_src_table = f"matrix_sf_src_{uuid.uuid4().hex[:8]}"
    sf_dst_prefix = f"matrix_sf_dst_{uuid.uuid4().hex[:8]}"
    pg_table = f"matrix_pg_{uuid.uuid4().hex[:8]}"
    mysql_table = f"matrix_mysql_{uuid.uuid4().hex[:8]}"
    mongo_db = f"matrix_mongo_{uuid.uuid4().hex[:8]}"
    mongo_collection = "data"

    report = Report()
    try:
        # Snowflake source table reused for Snowflake -> * legs
        _seed_snowflake(sf_cfg, sf_src_table, rows_list)

        # Postgres <-> Snowflake
        if _ensure_postgres_db():
            _seed_postgres(pg_table, rows_list)
            try:
                report.results.append(_run_transfer(
                    {"format": "postgresql", "host": "localhost", "port": 5432, "database": "dataflow", "username": "dataflow", "password": "dataflow", "schema": "public", "table": pg_table},
                    {"kind": "database", "format": "snowflake", "table": f"{sf_dst_prefix}_from_pg", **sf_cfg},
                    rows,
                ))
            finally:
                _cleanup_postgres(pg_table)

            try:
                report.results.append(_run_transfer(
                    {"format": "snowflake", "host": sf_cfg["host"], "port": 443, "database": sf_cfg["database"], "username": sf_cfg["username"], "password": sf_cfg["password"], "schema": sf_cfg["schema"], "warehouse": sf_cfg["warehouse"], "table": sf_src_table},
                    {"kind": "database", "format": "postgresql", "host": "localhost", "port": 5432, "database": "dataflow", "username": "dataflow", "password": "dataflow", "schema": "public", "table": f"{pg_table}_from_sf"},
                    rows,
                ))
            finally:
                _cleanup_postgres(f"{pg_table}_from_sf")

        # MySQL <-> Snowflake
        if _ensure_mysql_db():
            _seed_mysql(mysql_table, rows_list)
            try:
                report.results.append(_run_transfer(
                    {"format": "mysql", "host": "localhost", "port": 3306, "database": "dataflow", "username": "dataflow", "password": "dataflow", "schema": "dataflow", "table": mysql_table},
                    {"kind": "database", "format": "snowflake", "table": f"{sf_dst_prefix}_from_mysql", **sf_cfg},
                    rows,
                ))
            finally:
                _cleanup_mysql(mysql_table)

            try:
                report.results.append(_run_transfer(
                    {"format": "snowflake", "host": sf_cfg["host"], "port": 443, "database": sf_cfg["database"], "username": sf_cfg["username"], "password": sf_cfg["password"], "schema": sf_cfg["schema"], "warehouse": sf_cfg["warehouse"], "table": sf_src_table},
                    {"kind": "database", "format": "mysql", "host": "localhost", "port": 3306, "database": "dataflow", "username": "dataflow", "password": "dataflow", "schema": "dataflow", "table": f"{mysql_table}_from_sf"},
                    rows,
                ))
            finally:
                _cleanup_mysql(f"{mysql_table}_from_sf")

        # MongoDB <-> Snowflake
        if _reachable("localhost", 27017):
            _seed_mongodb(mongo_db, mongo_collection, rows_list)
            try:
                report.results.append(_run_transfer(
                    {"format": "mongodb", "host": "localhost", "port": 27017, "database": mongo_db, "collection": mongo_collection},
                    {"kind": "database", "format": "snowflake", "table": f"{sf_dst_prefix}_from_mongo", **sf_cfg},
                    rows,
                ))
            finally:
                _cleanup_mongodb(mongo_db)

            try:
                report.results.append(_run_transfer(
                    {"format": "snowflake", "host": sf_cfg["host"], "port": 443, "database": sf_cfg["database"], "username": sf_cfg["username"], "password": sf_cfg["password"], "schema": sf_cfg["schema"], "warehouse": sf_cfg["warehouse"], "table": sf_src_table},
                    {"kind": "database", "format": "mongodb", "host": "localhost", "port": 27017, "database": mongo_db, "collection": f"{mongo_collection}_from_sf"},
                    rows,
                ))
            finally:
                _cleanup_mongodb(mongo_db)

        # Snowflake -> CSV file export
        try:
            export_dir = Path(__file__).resolve().parents[1] / "exports"
            export_dir.mkdir(parents=True, exist_ok=True)
            export_path = export_dir / f"matrix_export_{uuid.uuid4().hex[:8]}.csv"
            report.results.append(_run_transfer(
                {"format": "snowflake", "host": sf_cfg["host"], "port": 443, "database": sf_cfg["database"], "username": sf_cfg["username"], "password": sf_cfg["password"], "schema": sf_cfg["schema"], "warehouse": sf_cfg["warehouse"], "table": sf_src_table},
                {"kind": "file_export", "format": "csv", "output_path": str(export_path), "table": f"{sf_dst_prefix}_export"},
                rows,
            ))
            if export_path.exists():
                export_path.unlink()
        except Exception as exc:
            report.results.append(MatrixResult(source="snowflake", destination="file_export:csv", rows=rows, success=False, error=str(exc)))
    finally:
        _cleanup_snowflake(sf_cfg, sf_src_table)
        for r in report.results:
            if r.success and "snowflake" in r.destination and "from" in r.destination:
                table = r.destination.split(":")[-1]
                _cleanup_snowflake(sf_cfg, table)

    passed = [r for r in report.results if r.success]
    failed = [r for r in report.results if not r.success]
    summary = {
        "started_at": report.started_at,
        "finished_at": _now(),
        "rows_per_leg": rows,
        "total_legs": len(report.results),
        "passed": len(passed),
        "failed": len(failed),
        "results": [asdict(r) for r in report.results],
    }
    print(json.dumps(summary, indent=2, default=str))
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    rows = int(os.environ.get("DATAFLOW_BENCHMARK_ROWS", "1000"))
    main(rows)
