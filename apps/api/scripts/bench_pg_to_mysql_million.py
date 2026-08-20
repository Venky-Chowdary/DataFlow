"""Time a real PostgreSQL→MySQL append through the production stream engine.

This runs ``src.transfer.stream.stream_database_transfer`` — the same path a UI
job takes — against live services, then prints the engine's own phase profile,
the reconcile posture it recorded, and a destination ``COUNT(*)`` so a timing
number can never be quoted without its row-conservation proof.

Environment:

    BENCH_ROWS        rows to seed / move (default 1000000)
    BENCH_DEST        destination table name (default bench_dest)
    BENCH_PROFILE     ``1`` to add a cProfile of the hot path (slows the run;
                      diagnostic only, never quote a profiled run as throughput)
    BENCH_DUMP_SUMMARY ``1`` to print every scalar the engine returned
    PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD   source overrides
    MYSQL_HOST/MYSQL_PORT/MYSQL_DATABASE/MYSQL_USER/MYSQL_PASSWORD  dest overrides

Usage:

    cd apps/api && python scripts/bench_pg_to_mysql_million.py
"""

from __future__ import annotations

import cProfile
import io
import json
import os
import pstats
import sys
import time
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

import psycopg2  # noqa: E402
import pymysql  # noqa: E402

PG = {
    "host": os.environ.get("PGHOST", "127.0.0.1"),
    "port": int(os.environ.get("PGPORT", "5433")),
    "dbname": os.environ.get("PGDATABASE", "dataflow"),
    "user": os.environ.get("PGUSER", "postgres"),
    "password": os.environ.get("PGPASSWORD", "postgres"),
}
MY = {
    "host": os.environ.get("MYSQL_HOST", "127.0.0.1"),
    "port": int(os.environ.get("MYSQL_PORT", "3307")),
    "user": os.environ.get("MYSQL_USER", "root"),
    "password": os.environ.get("MYSQL_PASSWORD", "dataflow"),
    "database": os.environ.get("MYSQL_DATABASE", "dataflow"),
}

ROWS = int(os.environ.get("BENCH_ROWS", "1000000"))
SRC_TABLE = f"bench_emp_{ROWS}"

COLUMNS: list[tuple[str, str]] = [
    ("employee_id", "VARCHAR(32)"),
    ("first_name", "VARCHAR(64)"),
    ("last_name", "VARCHAR(64)"),
    ("department", "VARCHAR(64)"),
    ("job_title", "VARCHAR(64)"),
    ("age", "BIGINT"),
    ("salary_amount", "BIGINT"),
    ("hire_date", "DATE"),
    ("employment_status", "VARCHAR(32)"),
    ("work_location", "VARCHAR(64)"),
]

REPORTED_SUMMARY_KEYS = (
    "chunk_size",
    "batches",
    "source_row_count",
    "source_row_count_source",
    "source_snapshot_guarantee",
    "pagination_mode",
    "reread_pagination",
    "checksum",
    "checksum_mode",
    "checksum_note",
    "source_independently_reread",
    "rejected_rows",
    "coerced_null_rows",
    "rows_skipped",
    "rejected_details_total",
    "target_rows_before",
)


def seed_source() -> None:
    """Create and fill the source fixture unless it already holds ``ROWS``."""
    conn = psycopg2.connect(**PG)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = %s",
            (SRC_TABLE,),
        )
        if cur.fetchone()[0]:
            cur.execute(f"SELECT count(*) FROM {SRC_TABLE}")
            if cur.fetchone()[0] == ROWS:
                print(f"seed: {SRC_TABLE} already has {ROWS} rows")
                conn.close()
                return
            cur.execute(f"DROP TABLE {SRC_TABLE}")
        cur.execute(
            f"""
            CREATE TABLE {SRC_TABLE} (
              employee_id varchar(32) PRIMARY KEY,
              first_name varchar(64),
              last_name varchar(64),
              department varchar(64),
              job_title varchar(64),
              age bigint,
              salary_amount bigint,
              hire_date date,
              employment_status varchar(32),
              work_location varchar(64)
            )
            """
        )
        started = time.monotonic()
        cur.execute(
            f"""
            INSERT INTO {SRC_TABLE}
            SELECT
              'EMP' || lpad(i::text, 7, '0'),
              'First' || (i % 9973),
              'Last' || (i % 7919),
              (ARRAY['Operations','Finance','Engineering','Sales','HR'])[1 + i % 5],
              (ARRAY['Analyst','Manager','Engineer','Director','Clerk'])[1 + i % 5],
              22 + (i % 39),
              30000 + (i % 90000),
              DATE '2005-01-01' + (i % 7000),
              (ARRAY['Active','Inactive','Leave'])[1 + i % 3],
              (ARRAY['Mumbai','Pune','Delhi','Bengaluru','Hyderabad'])[1 + i % 5]
            FROM generate_series(1, {ROWS}) AS s(i)
            """
        )
        print(f"seed: inserted {ROWS} rows in {time.monotonic() - started:.1f}s")
    conn.close()


def reset_destination(table: str) -> None:
    conn = pymysql.connect(**MY)
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
    conn.commit()
    conn.close()


def destination_count(table: str) -> int:
    conn = pymysql.connect(**MY)
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM `{table}`")
        count = int(cur.fetchone()[0])
    conn.close()
    return count


def run(dest_table: str, *, profile: bool) -> None:
    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    source = EndpointConfig.from_dict(
        "database",
        {
            "format": "postgresql",
            "host": PG["host"],
            "port": PG["port"],
            "database": PG["dbname"],
            "username": PG["user"],
            "password": PG["password"],
            "table": SRC_TABLE,
            "schema": "public",
        },
    )
    destination = EndpointConfig.from_dict(
        "database",
        {
            "format": "mysql",
            "host": MY["host"],
            "port": MY["port"],
            "database": MY["database"],
            "username": MY["user"],
            "password": MY["password"],
            "table": dest_table,
        },
    )
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)

    # The engine refuses to stream without a durable checkpoint row, so the job
    # record has to exist before the run — that fail-closed behaviour is part of
    # what is being measured.
    job_id = f"bench-{dest_table}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    profiler = cProfile.Profile() if profile else None
    started = time.monotonic()
    if profiler is not None:
        profiler.enable()
    rows, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        None,
        sync_mode="full_refresh_append",
        job_id=job_id,
    )
    if profiler is not None:
        profiler.disable()
    elapsed = time.monotonic() - started

    print(
        f"\n=== {dest_table}: {rows} rows in {elapsed:.1f}s "
        f"= {rows / elapsed:,.0f} rows/s"
    )
    print(json.dumps(summary.get("phase_profile", {}), indent=2)[:2500])
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")

    if os.environ.get("BENCH_DUMP_SUMMARY") == "1":
        scalars = {
            key: value
            for key, value in summary.items()
            if value is None or isinstance(value, (str, int, float, bool))
        }
        print("summary scalars:", json.dumps(scalars, indent=2, default=str)[:4000])

    landed = destination_count(dest_table)
    print(f"destination COUNT(*): {landed} (source rows: {ROWS})")
    print(f"row conservation: {'OK' if landed == ROWS else 'MISMATCH'}")

    if profiler is not None:
        buffer = io.StringIO()
        pstats.Stats(profiler, stream=buffer).sort_stats("cumulative").print_stats(35)
        print(buffer.getvalue()[:6000])


if __name__ == "__main__":
    seed_source()
    table = os.environ.get("BENCH_DEST", "bench_dest")
    reset_destination(table)
    run(table, profile=os.environ.get("BENCH_PROFILE", "0") == "1")
