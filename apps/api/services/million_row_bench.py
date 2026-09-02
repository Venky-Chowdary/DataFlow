"""PostgreSQL → MySQL volume run through the production stream engine.

The CLI script and pytest smoke both call ``run_pg_mysql_volume`` — one
algorithm, dest ``COUNT(*)`` required, no invented green.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

import psycopg2
import pymysql

from services.million_row_proof import (
    assert_clean_conservation,
    discover_oltp_pair,
    ensure_memory_job_store_if_mongo_down,
    row_conservation,
    skip_reason_if_unreachable,
    write_million_proof,
)

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

BENCH_PK = "employee_id"
BENCH_CURSOR = "hire_date"

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
    "load_method",
    "copy_workers",
)


def seed_source(pg: dict[str, Any], table: str, rows: int) -> None:
    conn = psycopg2.connect(
        host=pg["host"],
        port=pg["port"],
        user=pg["user"],
        password=pg["password"],
        dbname=pg["dbname"],
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = %s",
            (table,),
        )
        if cur.fetchone()[0]:
            cur.execute(f'SELECT count(*) FROM "{table}"')
            if cur.fetchone()[0] == rows:
                print(f"seed: {table} already has {rows} rows")
                conn.close()
                return
            cur.execute(f'DROP TABLE "{table}"')
        cur.execute(
            f"""
            CREATE TABLE "{table}" (
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
            INSERT INTO "{table}"
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
            FROM generate_series(1, {int(rows)}) AS s(i)
            """
        )
        print(f"seed: inserted {rows} rows in {time.monotonic() - started:.1f}s")
    conn.close()


def reset_destination(mysql: dict[str, Any], table: str) -> None:
    conn = pymysql.connect(
        host=mysql["host"],
        port=mysql["port"],
        user=mysql["user"],
        password=mysql["password"],
        database=mysql["database"],
    )
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
    conn.commit()
    conn.close()


def destination_count(mysql: dict[str, Any], table: str) -> int:
    conn = pymysql.connect(
        host=mysql["host"],
        port=mysql["port"],
        user=mysql["user"],
        password=mysql["password"],
        database=mysql["database"],
    )
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM `{table}`")
        count = int(cur.fetchone()[0])
    conn.close()
    return count


def ensure_mysql_local_infile(mysql: dict[str, Any]) -> str:
    """Best-effort lab enable of ``local_infile``. Writer never SET GLOBAL.

    Correctness does not depend on this — INSERT is the fallback. Speed
    measurements need it on so LOAD DATA actually fires.
    """
    def _on(raw: object) -> bool:
        return str(raw).strip().lower() in {"1", "on", "true"}

    try:
        conn = pymysql.connect(
            host=mysql["host"],
            port=mysql["port"],
            user=mysql["user"],
            password=mysql["password"],
            database=mysql["database"],
            connect_timeout=3,
        )
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT @@GLOBAL.local_infile")
                if _on(cur.fetchone()[0]):
                    return "already_on"
        finally:
            conn.close()
    except Exception as exc:
        already_err = str(exc)[:120]
    else:
        already_err = ""

    root_password = mysql.get("password") or "dataflow"
    try:
        conn = pymysql.connect(
            host=mysql["host"],
            port=mysql["port"],
            user="root",
            password=root_password,
            database=mysql["database"],
            autocommit=True,
            connect_timeout=3,
        )
        try:
            with conn.cursor() as cur:
                cur.execute("SET GLOBAL local_infile = 1")
                cur.execute("SELECT @@GLOBAL.local_infile")
                if _on(cur.fetchone()[0]):
                    return "enabled_as_root"
        finally:
            conn.close()
    except Exception as exc:
        suffix = f" ({already_err})" if already_err else ""
        return f"off:{exc}{suffix}"[:200]
    return "off:SET GLOBAL did not stick"


def run_pg_mysql_volume(
    *,
    rows: int,
    dest_table: str,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    skip = skip_reason_if_unreachable()
    if skip:
        raise RuntimeError(skip)

    pair = discover_oltp_pair()
    assert pair is not None
    pg, mysql = pair
    src_table = f"bench_emp_{rows}"
    job_store = ensure_memory_job_store_if_mongo_down()

    seed_source(pg, src_table, rows)
    if not keep_dest:
        reset_destination(mysql, dest_table)
    local_infile = ensure_mysql_local_infile(mysql)
    print(f"mysql local_infile: {local_infile}")

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer
    from services.sync_cursor import requires_incremental

    source = EndpointConfig.from_dict(
        "database",
        {
            "format": "postgresql",
            "host": pg["host"],
            "port": pg["port"],
            "database": pg["dbname"],
            "username": pg["user"],
            "password": pg["password"],
            "table": src_table,
            "schema": "public",
        },
    )
    destination = EndpointConfig.from_dict(
        "database",
        {
            "format": "mysql",
            "host": mysql["host"],
            "port": mysql["port"],
            "database": mysql["database"],
            "username": mysql["user"],
            "password": mysql["password"],
            "table": dest_table,
        },
    )
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)

    job_id = f"bench-{dest_table}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    contracts: list[dict[str, object]] | None = None
    if requires_incremental(sync_mode) or sync_mode in {"upsert", "mirror"}:
        contracts = [{
            "name": "bench",
            "selected": True,
            "sync_mode": sync_mode,
            "cursor_field": BENCH_CURSOR,
            "primary_key": BENCH_PK,
        }]

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        None,
        sync_mode=sync_mode,
        stream_contracts=contracts,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = destination_count(mysql, dest_table)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "postgresql→mysql",
        "sync_mode": sync_mode,
        "source_table": src_table,
        "dest_table": dest_table,
        "pg_port": pg["port"],
        "mysql_port": mysql["port"],
        "job_store": job_store,
        "job_id": job_id,
        "mysql_local_infile": local_infile,
        "load_method": summary.get("load_method"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "phase_profile": summary.get("phase_profile"),
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest_table} [{sync_mode}]: {transferred} rows in {elapsed:.1f}s "
        f"= {rps:,.0f} rows/s"
    )
    print(json.dumps(summary.get("phase_profile", {}), indent=2)[:2500])
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination COUNT(*): {landed} (source rows: {rows})")
    print(f"row conservation: {conservation['verdict']}")

    if proof_path:
        write_million_proof(proof_path, report)

    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )

    persist_volume_history(
        pg=pg,
        mysql=mysql,
        src_table=src_table,
        dest_table=dest_table,
        job_id=job_id,
        rejected_rows=rejected,
        row_count=landed,
    )

    return report


def persist_volume_history(
    *,
    pg: dict[str, Any],
    mysql: dict[str, Any],
    src_table: str,
    dest_table: str,
    job_id: str,
    rejected_rows: int,
    row_count: int,
) -> None:
    """Stamp the volume route into load history so Validate can measure a rate."""
    try:
        from services.data_quality_history import (
            history_endpoint_from_config,
            profile_batch,
            save_profile,
        )

        save_profile(
            history_endpoint_from_config(
                {
                    "host": pg["host"],
                    "port": pg["port"],
                    "database": pg["dbname"],
                    "schema": "public",
                },
                kind="database",
                format="postgresql",
                table=src_table,
            ),
            history_endpoint_from_config(
                {
                    "host": mysql["host"],
                    "port": mysql["port"],
                    "database": mysql["database"],
                },
                kind="database",
                format="mysql",
                table=dest_table,
            ),
            profile_batch([], dict(COLUMNS)),
            job_id=job_id,
            rejected_rows=rejected_rows,
            row_count=row_count,
        )
    except Exception as exc:
        print(f"load-history save_profile skipped: {exc}")
