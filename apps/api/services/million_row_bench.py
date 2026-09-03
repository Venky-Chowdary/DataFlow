"""PostgreSQL → MySQL volume run through the production stream engine.

The CLI script and pytest smoke both call ``run_pg_mysql_volume`` — one
algorithm, dest ``COUNT(*)`` required, no invented green. MySQL→PostgreSQL
and MySQL→MySQL identity benches share the same conservation owner.
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
    "engine_source_checksum",
    "engine_target_checksum",
    "source_independently_reread",
    "rejected_rows",
    "coerced_null_rows",
    "rows_skipped",
    "rejected_details_total",
    "target_rows_before",
    "load_method",
    "copy_workers",
    "copy_partitions",
    "partitions_skipped",
    "shard_mode",
    "copy_split",
    "iceberg_write",
    "iceberg_read",
    "mongo_write",
    "mongo_read",
    "sqlite_write",
    "sqlite_read",
    "s3_write",
    "s3_read",
    "tsv_encoder",
    "partition_proof",
    "proof_scope",
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
        # Keep 7-digit ids for the named 1M fixture. Wider pads at 10M+ keep
        # VARCHAR PK order aligned with the integer series for even PK shards.
        pad = max(7, len(str(int(rows))))
        chunk = 10_000_000
        inserted = 0
        for start in range(1, int(rows) + 1, chunk):
            end = min(start + chunk - 1, int(rows))
            cur.execute(
                f"""
                INSERT INTO "{table}"
                SELECT
                  'EMP' || lpad(i::text, {pad}, '0'),
                  'First' || (i % 9973),
                  'Last' || (i % 7919),
                  (ARRAY['Operations','Finance','Engineering','Sales','HR'])[1 + i % 5],
                  (ARRAY['Analyst','Manager','Engineer','Director','Clerk'])[1 + i % 5],
                  22 + (i % 39),
                  30000 + (i % 90000),
                  DATE '2005-01-01' + (i % 7000),
                  (ARRAY['Active','Inactive','Leave'])[1 + i % 3],
                  (ARRAY['Mumbai','Pune','Delhi','Bengaluru','Hyderabad'])[1 + i % 5]
                FROM generate_series({start}, {end}) AS s(i)
                """
            )
            inserted = end
            print(f"seed: {inserted}/{rows} rows")
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
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
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
        parts = report.get("partition_proof") or []
        for part in parts:
            if int(part.get("source_count") or 0) != int(part.get("dest_count") or 0):
                raise AssertionError(
                    "partition dest COUNT mismatch: "
                    f"{part}"
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


def reset_pg_destination(pg: dict[str, Any], table: str) -> None:
    conn = psycopg2.connect(
        host=pg["host"],
        port=pg["port"],
        user=pg["user"],
        password=pg["password"],
        dbname=pg["dbname"],
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
    conn.close()


def postgres_count(pg: dict[str, Any], table: str) -> int:
    conn = psycopg2.connect(
        host=pg["host"],
        port=pg["port"],
        user=pg["user"],
        password=pg["password"],
        dbname=pg["dbname"],
    )
    with conn.cursor() as cur:
        cur.execute(f'SELECT COUNT(*) FROM public."{table}"')
        count = int(cur.fetchone()[0])
    conn.close()
    return count


def run_mysql_pg_volume(
    *,
    rows: int,
    source_table: str,
    dest_table: str,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity MySQL→PostgreSQL through stream_database_transfer. Dest COUNT required."""
    skip = skip_reason_if_unreachable()
    if skip:
        raise RuntimeError(skip)

    pair = discover_oltp_pair()
    assert pair is not None
    pg, mysql = pair
    job_store = ensure_memory_job_store_if_mongo_down()

    conn = pymysql.connect(
        host=mysql["host"],
        port=mysql["port"],
        user=mysql["user"],
        password=mysql["password"],
        database=mysql["database"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_name = %s",
                (source_table,),
            )
            if not cur.fetchone()[0]:
                raise RuntimeError(
                    f"MySQL source {source_table!r} is missing — run the PG→MySQL "
                    "bench first so dest COUNT is a named fixture, not an invented table"
                )
            cur.execute(f"SELECT COUNT(*) FROM `{source_table}`")
            have = int(cur.fetchone()[0])
    finally:
        conn.close()
    if have != rows:
        raise RuntimeError(
            f"MySQL {source_table} has {have} rows, requested {rows}"
        )
    if not keep_dest:
        reset_pg_destination(pg, dest_table)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    source = EndpointConfig.from_dict(
        "database",
        {
            "format": "mysql",
            "host": mysql["host"],
            "port": mysql["port"],
            "database": mysql["database"],
            "username": mysql["user"],
            "password": mysql["password"],
            "table": source_table,
        },
    )
    destination = EndpointConfig.from_dict(
        "database",
        {
            "format": "postgresql",
            "host": pg["host"],
            "port": pg["port"],
            "database": pg["dbname"],
            "username": pg["user"],
            "password": pg["password"],
            "table": dest_table,
            "schema": "public",
        },
    )
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-mysql-pg-{dest_table}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = postgres_count(pg, dest_table)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "mysql→postgresql",
        "sync_mode": sync_mode,
        "source_table": source_table,
        "dest_table": dest_table,
        "pg_port": pg["port"],
        "mysql_port": mysql["port"],
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "tsv_encoder": summary.get("tsv_encoder"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest_table} mysql→pg [{sync_mode}]: {transferred} rows in {elapsed:.1f}s "
        f"= {rps:,.0f} rows/s"
    )
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
        for part in report.get("partition_proof") or []:
            if int(part.get("source_count") or 0) != int(part.get("dest_count") or 0):
                raise AssertionError(f"partition dest COUNT mismatch: {part}")
    return report


def run_mysql_mysql_volume(
    *,
    rows: int,
    source_table: str,
    dest_table: str,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity MySQL→MySQL through stream_database_transfer. Dest COUNT required."""
    skip = skip_reason_if_unreachable()
    if skip:
        raise RuntimeError(skip)

    pair = discover_oltp_pair()
    assert pair is not None
    _pg, mysql = pair
    job_store = ensure_memory_job_store_if_mongo_down()

    conn = pymysql.connect(
        host=mysql["host"],
        port=mysql["port"],
        user=mysql["user"],
        password=mysql["password"],
        database=mysql["database"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_name = %s",
                (source_table,),
            )
            if not cur.fetchone()[0]:
                raise RuntimeError(
                    f"MySQL source {source_table!r} is missing — run the PG→MySQL "
                    "bench first so dest COUNT is a named fixture, not an invented table"
                )
            cur.execute(f"SELECT COUNT(*) FROM `{source_table}`")
            have = int(cur.fetchone()[0])
    finally:
        conn.close()
    if have != rows:
        raise RuntimeError(
            f"MySQL {source_table} has {have} rows, requested {rows}"
        )
    if source_table.lower() == dest_table.lower():
        raise RuntimeError("MySQL→MySQL bench refuses copy onto the same table")
    if not keep_dest:
        reset_destination(mysql, dest_table)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    source = EndpointConfig.from_dict(
        "database",
        {
            "format": "mysql",
            "host": mysql["host"],
            "port": mysql["port"],
            "database": mysql["database"],
            "username": mysql["user"],
            "password": mysql["password"],
            "table": source_table,
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
    job_id = f"bench-mysql-mysql-{dest_table}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
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
        "route": "mysql→mysql",
        "sync_mode": sync_mode,
        "source_table": source_table,
        "dest_table": dest_table,
        "mysql_port": mysql["port"],
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest_table} mysql→mysql [{sync_mode}]: {transferred} rows in {elapsed:.1f}s "
        f"= {rps:,.0f} rows/s"
    )
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
        for part in report.get("partition_proof") or []:
            if int(part.get("source_count") or 0) != int(part.get("dest_count") or 0):
                raise AssertionError(f"partition dest COUNT mismatch: {part}")
    return report


def run_pg_pg_volume(
    *,
    rows: int,
    dest_table: str,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity PostgreSQL→PostgreSQL through stream_database_transfer. Dest COUNT required."""
    skip = skip_reason_if_unreachable()
    if skip:
        raise RuntimeError(skip)

    pair = discover_oltp_pair()
    assert pair is not None
    pg, _mysql = pair
    src_table = f"bench_emp_{rows}"
    if src_table.lower() == dest_table.lower():
        raise RuntimeError("PostgreSQL→PostgreSQL bench refuses copy onto the same table")
    job_store = ensure_memory_job_store_if_mongo_down()
    seed_source(pg, src_table, rows)
    if not keep_dest:
        reset_pg_destination(pg, dest_table)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

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
            "format": "postgresql",
            "host": pg["host"],
            "port": pg["port"],
            "database": pg["dbname"],
            "username": pg["user"],
            "password": pg["password"],
            "table": dest_table,
            "schema": "public",
        },
    )
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-pg-pg-{dest_table}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = postgres_count(pg, dest_table)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "postgresql→postgresql",
        "sync_mode": sync_mode,
        "source_table": src_table,
        "dest_table": dest_table,
        "pg_port": pg["port"],
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "partition_proof": summary.get("partition_proof"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "proof_scope": summary.get("proof_scope"),
        "engine_source_checksum": summary.get("engine_source_checksum"),
        "engine_target_checksum": summary.get("engine_target_checksum"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest_table} pg→pg [{sync_mode}]: {transferred} rows in {elapsed:.1f}s "
        f"= {rps:,.0f} rows/s"
    )
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
        src_ck = summary.get("engine_source_checksum")
        dst_ck = summary.get("engine_target_checksum")
        if src_ck and dst_ck and src_ck != dst_ck:
            raise AssertionError(f"engine checksum mismatch {src_ck} != {dst_ck}")
        if summary.get("load_method") != "copy_binary_server_to_server":
            raise AssertionError(
                f"expected binary COPY, got load_method={summary.get('load_method')!r}"
            )
        for part in report.get("partition_proof") or []:
            if int(part.get("source_count") or 0) != int(part.get("dest_count") or 0):
                raise AssertionError(f"partition dest COUNT mismatch: {part}")
    return report


def _sqlserver_cfg() -> dict[str, Any]:
    return {
        "host": "127.0.0.1",
        "port": 1433,
        "database": "dataflow",
        "schema": "dbo",
        "username": "sa",
        "password": "DataFlow_CDC_2022!",
        "trust_server_certificate": True,
        "encrypt": "yes",
    }


def _sqlserver_connect():
    import pymssql

    return pymssql.connect(
        server="127.0.0.1",
        port=1433,
        user="sa",
        password="DataFlow_CDC_2022!",
        database="dataflow",
        login_timeout=10,
        autocommit=True,
    )


def _sqlserver_count(conn: Any, table: str) -> int:
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM dbo.[{table}]")  # nosec B608
    return int(cur.fetchone()[0])


def _sqlserver_seed(conn: Any, table: str, rows: int) -> None:
    cur = conn.cursor()
    cur.execute(
        f"SELECT OBJECT_ID(N'dbo.{table}', 'U')"  # nosec B608
    )
    exists = cur.fetchone()[0] is not None
    if exists:
        have = _sqlserver_count(conn, table)
        if have == rows:
            return
        cur.execute(f"DROP TABLE IF EXISTS dbo.[{table}]")  # nosec B608
    cur.execute(
        f"CREATE TABLE dbo.[{table}] ("  # nosec B608
        "id BIGINT NOT NULL PRIMARY KEY, label NVARCHAR(32) NULL)"
    )
    cur.execute(
        f"""
        WITH n AS (
          SELECT TOP ({int(rows)}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS seq
          FROM sys.all_objects a CROSS JOIN sys.all_objects b
        )
        INSERT INTO dbo.[{table}] (id, label)
        SELECT seq, CONCAT(N'r', seq) FROM n
        """  # nosec B608
    )
    have = _sqlserver_count(conn, table)
    if have != rows:
        raise RuntimeError(f"SQL Server seed {table} has {have} rows, wanted {rows}")


def run_sqlserver_sqlserver_volume(
    *,
    rows: int,
    source_table: str,
    dest_table: str,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity SQL Server→SQL Server through stream_database_transfer. Dest COUNT required."""
    import socket

    try:
        socket.create_connection(("127.0.0.1", 1433), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"SQL Server 1433 not reachable: {exc}") from exc

    if source_table.lower() == dest_table.lower():
        raise RuntimeError("SQL Server→SQL Server bench refuses copy onto the same table")

    job_store = ensure_memory_job_store_if_mongo_down()
    conn = _sqlserver_connect()
    try:
        _sqlserver_seed(conn, source_table, rows)
        if not keep_dest:
            cur = conn.cursor()
            cur.execute(f"DROP TABLE IF EXISTS dbo.[{dest_table}]")  # nosec B608
    finally:
        conn.close()

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    ss = _sqlserver_cfg()
    source = EndpointConfig.from_dict(
        "database", {**ss, "format": "sqlserver", "table": source_table}
    )
    destination = EndpointConfig.from_dict(
        "database", {**ss, "format": "sqlserver", "table": dest_table}
    )
    mappings = [
        {"source": "id", "target": "id", "type": "integer", "transform": "none"},
        {"source": "label", "target": "label", "type": "string", "transform": "none"},
    ]
    schema = {"id": "integer", "label": "string"}
    job_id = f"bench-ss-ss-{dest_table}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    conn = _sqlserver_connect()
    try:
        landed = _sqlserver_count(conn, dest_table)
    finally:
        conn.close()
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "sqlserver→sqlserver",
        "sync_mode": sync_mode,
        "source_table": source_table,
        "dest_table": dest_table,
        "sqlserver_port": 1433,
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "sqlserver_isolation": summary.get("sqlserver_isolation"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest_table} sqlserver→sqlserver [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
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
        if summary.get("load_method") != "insert_select_sqlserver_same_instance":
            raise AssertionError(
                f"expected INSERT SELECT, got load_method={summary.get('load_method')!r}"
            )
        for part in report.get("partition_proof") or []:
            if int(part.get("source_count") or 0) != int(part.get("dest_count") or 0):
                raise AssertionError(f"partition dest COUNT mismatch: {part}")
    return report


def _oracle_password() -> str:
    env = (os.environ.get("DATAFLOW_ORACLE_PASSWORD") or "").strip()
    if env:
        return env
    path = Path("/tmp/df-desktop-lab/oracle_password")
    if path.is_file():
        return path.read_text().strip()
    return "dataflow"


def _oracle_cfg() -> dict[str, Any]:
    return {
        "host": "127.0.0.1",
        "port": 1521,
        "database": "XEPDB1",
        "schema": "DATAFLOW",
        "username": "dataflow",
        "password": _oracle_password(),
    }


def _oracle_connect():
    import oracledb

    return oracledb.connect(
        user="dataflow",
        password=_oracle_password(),
        dsn="127.0.0.1:1521/XEPDB1",
    )


def _oracle_drop(cur: Any, table: str) -> None:
    tbl = str(table).upper()
    cur.execute(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE "
        f"{tbl} PURGE'; EXCEPTION WHEN OTHERS THEN "
        "IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


def _oracle_count(conn: Any, table: str) -> int:
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {str(table).upper()}")  # nosec B608
    return int(cur.fetchone()[0])


def _oracle_seed(conn: Any, table: str, rows: int) -> None:
    tbl = str(table).upper()
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM user_tables WHERE table_name = :1",
        [tbl],
    )
    exists = int(cur.fetchone()[0]) > 0
    if exists:
        have = _oracle_count(conn, tbl)
        if have == rows:
            return
        _oracle_drop(cur, tbl)
    cur.execute(
        f"CREATE TABLE {tbl} (ID NUMBER NOT NULL PRIMARY KEY, LABEL VARCHAR2(32))"  # nosec B608
    )
    cur.execute(
        f"INSERT INTO {tbl} (ID, LABEL) "  # nosec B608
        f"SELECT LEVEL, 'r' || LEVEL FROM dual CONNECT BY LEVEL <= {int(rows)}"
    )
    conn.commit()
    have = _oracle_count(conn, tbl)
    if have != rows:
        raise RuntimeError(f"Oracle seed {tbl} has {have} rows, wanted {rows}")


def run_oracle_oracle_volume(
    *,
    rows: int,
    source_table: str,
    dest_table: str,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity Oracle→Oracle through stream_database_transfer. Dest COUNT required."""
    import socket

    try:
        socket.create_connection(("127.0.0.1", 1521), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"Oracle 1521 not reachable: {exc}") from exc

    src = str(source_table).upper()
    dest = str(dest_table).upper()
    if src == dest:
        raise RuntimeError("Oracle→Oracle bench refuses copy onto the same table")

    job_store = ensure_memory_job_store_if_mongo_down()
    conn = _oracle_connect()
    try:
        _oracle_seed(conn, src, rows)
        if not keep_dest:
            cur = conn.cursor()
            _oracle_drop(cur, dest)
            conn.commit()
    finally:
        conn.close()

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    ora = _oracle_cfg()
    source = EndpointConfig.from_dict(
        "database", {**ora, "format": "oracle", "table": src}
    )
    destination = EndpointConfig.from_dict(
        "database", {**ora, "format": "oracle", "table": dest}
    )
    mappings = [
        {"source": "id", "target": "id", "type": "integer", "transform": "none"},
        {"source": "label", "target": "label", "type": "string", "transform": "none"},
    ]
    schema = {"id": "integer", "label": "string"}
    job_id = f"bench-ora-ora-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    conn = _oracle_connect()
    try:
        landed = _oracle_count(conn, dest)
    finally:
        conn.close()
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "oracle→oracle",
        "sync_mode": sync_mode,
        "source_table": src,
        "dest_table": dest,
        "oracle_service": "XEPDB1",
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "oracle_lock": summary.get("oracle_lock"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} oracle→oracle [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
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
        if summary.get("load_method") != "insert_select_oracle_same_instance":
            raise AssertionError(
                f"expected INSERT SELECT, got load_method={summary.get('load_method')!r}"
            )
        for part in report.get("partition_proof") or []:
            if int(part.get("source_count") or 0) != int(part.get("dest_count") or 0):
                raise AssertionError(f"partition dest COUNT mismatch: {part}")
    return report


def _pg_local_cfg() -> dict[str, Any]:
    return {
        "host": "localhost",
        "port": 5432,
        "dbname": "dataflow",
        "user": "dataflow",
        "password": "dataflow",
    }


def run_pg_sqlserver_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity PostgreSQL→SQL Server through stream_database_transfer. Dest COUNT required."""
    import socket

    try:
        socket.create_connection(("127.0.0.1", 5432), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"PostgreSQL 5432 not reachable: {exc}") from exc
    try:
        socket.create_connection(("127.0.0.1", 1433), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"SQL Server 1433 not reachable: {exc}") from exc

    pg = _pg_local_cfg()
    src_table = source_table or f"bench_emp_{rows}"
    job_store = ensure_memory_job_store_if_mongo_down()
    seed_source(pg, src_table, rows)
    if not keep_dest:
        conn = _sqlserver_connect()
        try:
            cur = conn.cursor()
            cur.execute(f"DROP TABLE IF EXISTS dbo.[{dest_table}]")  # nosec B608
        finally:
            conn.close()

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    ss = _sqlserver_cfg()
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
        "database", {**ss, "format": "sqlserver", "table": dest_table}
    )
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-pg-ss-{dest_table}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    conn = _sqlserver_connect()
    try:
        landed = _sqlserver_count(conn, dest_table)
    finally:
        conn.close()
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "postgresql→sqlserver",
        "sync_mode": sync_mode,
        "source_table": src_table,
        "dest_table": dest_table,
        "pg_port": pg["port"],
        "sqlserver_port": 1433,
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(COLUMNS),
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest_table} postgresql→sqlserver [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
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
        if summary.get("load_method") != "copy_text_pg_to_sqlserver_fast_executemany":
            raise AssertionError(
                "expected COPY text + fast_executemany, "
                f"got load_method={summary.get('load_method')!r}"
            )
        for part in report.get("partition_proof") or []:
            if int(part.get("source_count") or 0) != int(part.get("dest_count") or 0):
                raise AssertionError(f"partition dest COUNT mismatch: {part}")
    return report


def run_sqlserver_pg_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity SQL Server→PostgreSQL through stream_database_transfer. Dest COUNT required."""
    import socket

    try:
        socket.create_connection(("127.0.0.1", 5432), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"PostgreSQL 5432 not reachable: {exc}") from exc
    try:
        socket.create_connection(("127.0.0.1", 1433), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"SQL Server 1433 not reachable: {exc}") from exc

    pg = _pg_local_cfg()
    src_table = source_table or "bench_ss_from_pg"
    job_store = ensure_memory_job_store_if_mongo_down()
    conn = _sqlserver_connect()
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT OBJECT_ID(N'dbo.{src_table}', 'U')")  # nosec B608
        exists = cur.fetchone()[0] is not None
        have = _sqlserver_count(conn, src_table) if exists else 0
    finally:
        conn.close()
    if have != rows:
        seed_source(pg, f"bench_emp_{rows}", rows)
        from services.copy_pg_sqlserver import copy_postgres_to_sqlserver

        copy_postgres_to_sqlserver(
            source_cfg={
                "host": pg["host"],
                "port": pg["port"],
                "database": pg["dbname"],
                "username": pg["user"],
                "password": pg["password"],
                "schema": "public",
            },
            source_schema="public",
            source_table=f"bench_emp_{rows}",
            dest_cfg=_sqlserver_cfg(),
            dest_table=src_table,
            pairs=[(name, name) for name, _ddl in COLUMNS],
            sqlserver_ddls=[ddl for _name, ddl in COLUMNS],
            replace_destination=True,
        )
    if not keep_dest:
        reset_pg_destination(pg, dest_table)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    ss = _sqlserver_cfg()
    source = EndpointConfig.from_dict(
        "database", {**ss, "format": "sqlserver", "table": src_table}
    )
    destination = EndpointConfig.from_dict(
        "database",
        {
            "format": "postgresql",
            "host": pg["host"],
            "port": pg["port"],
            "database": pg["dbname"],
            "username": pg["user"],
            "password": pg["password"],
            "table": dest_table,
            "schema": "public",
        },
    )
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-ss-pg-{dest_table}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = postgres_count(pg, dest_table)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "sqlserver→postgresql",
        "sync_mode": sync_mode,
        "source_table": src_table,
        "dest_table": dest_table,
        "pg_port": pg["port"],
        "sqlserver_port": 1433,
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "sqlserver_isolation": summary.get("sqlserver_isolation"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(COLUMNS),
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest_table} sqlserver→postgresql [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
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
        if summary.get("load_method") != "select_sqlserver_copy_from_stdin_pg":
            raise AssertionError(
                "expected SELECT + COPY FROM STDIN, "
                f"got load_method={summary.get('load_method')!r}"
            )
        for part in report.get("partition_proof") or []:
            if int(part.get("source_count") or 0) != int(part.get("dest_count") or 0):
                raise AssertionError(f"partition dest COUNT mismatch: {part}")
    return report


def run_pg_oracle_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity PostgreSQL→Oracle through stream_database_transfer. Dest COUNT required."""
    import socket

    try:
        socket.create_connection(("127.0.0.1", 5432), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"PostgreSQL 5432 not reachable: {exc}") from exc
    try:
        socket.create_connection(("127.0.0.1", 1521), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"Oracle 1521 not reachable: {exc}") from exc

    pg = _pg_local_cfg()
    src_table = source_table or f"bench_emp_{rows}"
    dest = str(dest_table).upper()
    job_store = ensure_memory_job_store_if_mongo_down()
    seed_source(pg, src_table, rows)
    if not keep_dest:
        conn = _oracle_connect()
        try:
            cur = conn.cursor()
            _oracle_drop(cur, dest)
            conn.commit()
        finally:
            conn.close()

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    ora = _oracle_cfg()
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
        "database", {**ora, "format": "oracle", "table": dest}
    )
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-pg-ora-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    conn = _oracle_connect()
    try:
        landed = _oracle_count(conn, dest)
    finally:
        conn.close()
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "postgresql→oracle",
        "sync_mode": sync_mode,
        "source_table": src_table,
        "dest_table": dest,
        "pg_port": pg["port"],
        "oracle_service": "XEPDB1",
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "empty_string_as_null_cells": summary.get("empty_string_as_null_cells"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(COLUMNS),
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} postgresql→oracle [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination COUNT(*): {landed} (source rows: {rows})")
    print(
        "empty_string_as_null_cells: "
        f"{summary.get('empty_string_as_null_cells')}"
    )
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        if summary.get("load_method") != "copy_text_pg_to_oracle_executemany":
            raise AssertionError(
                "expected COPY text + executemany, "
                f"got load_method={summary.get('load_method')!r}"
            )
        for part in report.get("partition_proof") or []:
            if int(part.get("source_count") or 0) != int(part.get("dest_count") or 0):
                raise AssertionError(f"partition dest COUNT mismatch: {part}")
    return report


def run_oracle_pg_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity Oracle→PostgreSQL through stream_database_transfer. Dest COUNT required."""
    import socket

    try:
        socket.create_connection(("127.0.0.1", 5432), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"PostgreSQL 5432 not reachable: {exc}") from exc
    try:
        socket.create_connection(("127.0.0.1", 1521), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"Oracle 1521 not reachable: {exc}") from exc

    pg = _pg_local_cfg()
    src_table = str(source_table or "BENCH_PG_ORA").upper()
    job_store = ensure_memory_job_store_if_mongo_down()
    conn = _oracle_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM user_tables WHERE table_name = :1",
            [src_table],
        )
        exists = int(cur.fetchone()[0]) > 0
        have = _oracle_count(conn, src_table) if exists else 0
    finally:
        conn.close()
    if have != rows:
        seed_source(pg, f"bench_emp_{rows}", rows)
        from services.copy_pg_oracle import copy_postgres_to_oracle
        from services.type_system import ddl_type

        copy_postgres_to_oracle(
            source_cfg={
                "host": pg["host"],
                "port": pg["port"],
                "database": pg["dbname"],
                "username": pg["user"],
                "password": pg["password"],
                "schema": "public",
            },
            source_schema="public",
            source_table=f"bench_emp_{rows}",
            dest_cfg=_oracle_cfg(),
            dest_table=src_table,
            pairs=[(name, name) for name, _ddl in COLUMNS],
            oracle_ddls=[ddl_type("oracle", ddl) for _name, ddl in COLUMNS],
            replace_destination=True,
        )
    if not keep_dest:
        reset_pg_destination(pg, dest_table)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    ora = _oracle_cfg()
    source = EndpointConfig.from_dict(
        "database", {**ora, "format": "oracle", "table": src_table}
    )
    destination = EndpointConfig.from_dict(
        "database",
        {
            "format": "postgresql",
            "host": pg["host"],
            "port": pg["port"],
            "database": pg["dbname"],
            "username": pg["user"],
            "password": pg["password"],
            "table": dest_table,
            "schema": "public",
        },
    )
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-ora-pg-{dest_table}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = postgres_count(pg, dest_table)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "oracle→postgresql",
        "sync_mode": sync_mode,
        "source_table": src_table,
        "dest_table": dest_table,
        "pg_port": pg["port"],
        "oracle_service": "XEPDB1",
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "oracle_lock": summary.get("oracle_lock"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(COLUMNS),
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest_table} oracle→postgresql [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
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
        if summary.get("load_method") != "select_oracle_copy_from_stdin_pg":
            raise AssertionError(
                "expected SELECT + COPY FROM STDIN, "
                f"got load_method={summary.get('load_method')!r}"
            )
        for part in report.get("partition_proof") or []:
            if int(part.get("source_count") or 0) != int(part.get("dest_count") or 0):
                raise AssertionError(f"partition dest COUNT mismatch: {part}")
    return report


def _mysql_local_cfg() -> dict[str, Any]:
    return {
        "host": "127.0.0.1",
        "port": 3306,
        "database": "dataflow",
        "username": "dataflow",
        "password": "dataflow",
        "user": "dataflow",
    }


def _mysql_has_rows(table: str, rows: int) -> bool:
    conn = pymysql.connect(
        host="127.0.0.1",
        port=3306,
        user="dataflow",
        password="dataflow",
        database="dataflow",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_name = %s",
                (table,),
            )
            if not cur.fetchone()[0]:
                return False
            cur.execute(f"SELECT COUNT(*) FROM `{table}`")  # nosec B608
            return int(cur.fetchone()[0]) == rows
    finally:
        conn.close()


def _ensure_mysql_employee_fixture(rows: int, mysql_table: str) -> str:
    """Use existing MySQL table or COPY the PG employee fixture into it."""
    if _mysql_has_rows(mysql_table, rows):
        return mysql_table
    pg = _pg_local_cfg()
    seed_source(pg, f"bench_emp_{rows}", rows)
    from connectors.mysql_writer import mysql_type
    from services.copy_pg_mysql import copy_postgres_to_mysql

    copy_postgres_to_mysql(
        source_cfg={
            "host": pg["host"],
            "port": pg["port"],
            "database": pg["dbname"],
            "username": pg["user"],
            "password": pg["password"],
            "schema": "public",
        },
        source_schema="public",
        source_table=f"bench_emp_{rows}",
        dest_cfg=_mysql_local_cfg(),
        dest_table=mysql_table,
        pairs=[(name, name) for name, _ddl in COLUMNS],
        mysql_ddls=[mysql_type(ddl) for _name, ddl in COLUMNS],
        replace_destination=True,
    )
    return mysql_table


def run_mysql_sqlserver_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity MySQL→SQL Server through stream_database_transfer. Dest COUNT required."""
    import socket

    try:
        socket.create_connection(("127.0.0.1", 3306), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"MySQL 3306 not reachable: {exc}") from exc
    try:
        socket.create_connection(("127.0.0.1", 1433), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"SQL Server 1433 not reachable: {exc}") from exc

    src_table = source_table or "bench_1m"
    job_store = ensure_memory_job_store_if_mongo_down()
    _ensure_mysql_employee_fixture(rows, src_table)
    if not keep_dest:
        conn = _sqlserver_connect()
        try:
            cur = conn.cursor()
            cur.execute(f"DROP TABLE IF EXISTS dbo.[{dest_table}]")  # nosec B608
        finally:
            conn.close()

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    mysql = _mysql_local_cfg()
    ss = _sqlserver_cfg()
    source = EndpointConfig.from_dict(
        "database", {**mysql, "format": "mysql", "table": src_table}
    )
    destination = EndpointConfig.from_dict(
        "database", {**ss, "format": "sqlserver", "table": dest_table}
    )
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-my-ss-{dest_table}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    conn = _sqlserver_connect()
    try:
        landed = _sqlserver_count(conn, dest_table)
    finally:
        conn.close()
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "mysql→sqlserver",
        "sync_mode": sync_mode,
        "source_table": src_table,
        "dest_table": dest_table,
        "mysql_port": 3306,
        "sqlserver_port": 1433,
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(COLUMNS),
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest_table} mysql→sqlserver [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
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
        if summary.get("load_method") != "select_mysql_fast_executemany_sqlserver":
            raise AssertionError(
                "expected SELECT + fast_executemany, "
                f"got load_method={summary.get('load_method')!r}"
            )
        for part in report.get("partition_proof") or []:
            if int(part.get("source_count") or 0) != int(part.get("dest_count") or 0):
                raise AssertionError(f"partition dest COUNT mismatch: {part}")
    return report


def run_sqlserver_mysql_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity SQL Server→MySQL through stream_database_transfer. Dest COUNT required."""
    import socket

    try:
        socket.create_connection(("127.0.0.1", 3306), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"MySQL 3306 not reachable: {exc}") from exc
    try:
        socket.create_connection(("127.0.0.1", 1433), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"SQL Server 1433 not reachable: {exc}") from exc

    src_table = source_table or "bench_ss_from_mysql"
    job_store = ensure_memory_job_store_if_mongo_down()
    conn = _sqlserver_connect()
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT OBJECT_ID(N'dbo.{src_table}', 'U')")  # nosec B608
        exists = cur.fetchone()[0] is not None
        have = _sqlserver_count(conn, src_table) if exists else 0
    finally:
        conn.close()
    if have != rows:
        mysql_src = _ensure_mysql_employee_fixture(rows, "bench_1m")
        from services.copy_mysql_sqlserver import copy_mysql_to_sqlserver
        from services.type_system import ddl_type

        copy_mysql_to_sqlserver(
            source_cfg=_mysql_local_cfg(),
            source_table=mysql_src,
            dest_cfg=_sqlserver_cfg(),
            dest_table=src_table,
            pairs=[(name, name) for name, _ddl in COLUMNS],
            sqlserver_ddls=[ddl_type("sqlserver", ddl) for _name, ddl in COLUMNS],
            replace_destination=True,
        )
    if not keep_dest:
        reset_destination(_mysql_local_cfg(), dest_table)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    mysql = _mysql_local_cfg()
    ss = _sqlserver_cfg()
    source = EndpointConfig.from_dict(
        "database", {**ss, "format": "sqlserver", "table": src_table}
    )
    destination = EndpointConfig.from_dict(
        "database", {**mysql, "format": "mysql", "table": dest_table}
    )
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-ss-my-{dest_table}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
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
        "route": "sqlserver→mysql",
        "sync_mode": sync_mode,
        "source_table": src_table,
        "dest_table": dest_table,
        "mysql_port": 3306,
        "sqlserver_port": 1433,
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "sqlserver_isolation": summary.get("sqlserver_isolation"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(COLUMNS),
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest_table} sqlserver→mysql [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
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
        if summary.get("load_method") != "select_sqlserver_load_data_mysql":
            raise AssertionError(
                "expected SELECT + STRICT LOAD DATA, "
                f"got load_method={summary.get('load_method')!r}"
            )
        for part in report.get("partition_proof") or []:
            if int(part.get("source_count") or 0) != int(part.get("dest_count") or 0):
                raise AssertionError(f"partition dest COUNT mismatch: {part}")
    return report


def run_mysql_oracle_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity MySQL→Oracle through stream_database_transfer. Dest COUNT required."""
    import socket

    try:
        socket.create_connection(("127.0.0.1", 3306), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"MySQL 3306 not reachable: {exc}") from exc
    try:
        socket.create_connection(("127.0.0.1", 1521), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"Oracle 1521 not reachable: {exc}") from exc

    src_table = source_table or "bench_1m"
    dest = str(dest_table).upper()
    job_store = ensure_memory_job_store_if_mongo_down()
    _ensure_mysql_employee_fixture(rows, src_table)
    if not keep_dest:
        conn = _oracle_connect()
        try:
            _oracle_drop(conn.cursor(), dest)
            conn.commit()
        finally:
            conn.close()

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    mysql = _mysql_local_cfg()
    ora = _oracle_cfg()
    source = EndpointConfig.from_dict(
        "database", {**mysql, "format": "mysql", "table": src_table}
    )
    destination = EndpointConfig.from_dict(
        "database", {**ora, "format": "oracle", "table": dest}
    )
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-my-ora-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    conn = _oracle_connect()
    try:
        landed = _oracle_count(conn, dest)
    finally:
        conn.close()
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "mysql→oracle",
        "sync_mode": sync_mode,
        "source_table": src_table,
        "dest_table": dest,
        "mysql_port": 3306,
        "oracle_service": "XEPDB1",
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "empty_string_as_null_cells": summary.get("empty_string_as_null_cells"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(COLUMNS),
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} mysql→oracle [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination COUNT(*): {landed} (source rows: {rows})")
    print(
        "empty_string_as_null_cells: "
        f"{summary.get('empty_string_as_null_cells')}"
    )
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        if summary.get("load_method") != "select_mysql_executemany_oracle":
            raise AssertionError(
                "expected SELECT + executemany, "
                f"got load_method={summary.get('load_method')!r}"
            )
        for part in report.get("partition_proof") or []:
            if int(part.get("source_count") or 0) != int(part.get("dest_count") or 0):
                raise AssertionError(f"partition dest COUNT mismatch: {part}")
    return report


def run_oracle_mysql_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity Oracle→MySQL through stream_database_transfer. Dest COUNT required."""
    import socket

    try:
        socket.create_connection(("127.0.0.1", 3306), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"MySQL 3306 not reachable: {exc}") from exc
    try:
        socket.create_connection(("127.0.0.1", 1521), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"Oracle 1521 not reachable: {exc}") from exc

    src_table = str(source_table or "BENCH_MY_ORA").upper()
    job_store = ensure_memory_job_store_if_mongo_down()
    conn = _oracle_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM user_tables WHERE table_name = :1",
            [src_table],
        )
        exists = int(cur.fetchone()[0]) > 0
        have = _oracle_count(conn, src_table) if exists else 0
    finally:
        conn.close()
    if have != rows:
        mysql_src = _ensure_mysql_employee_fixture(rows, "bench_1m")
        from services.copy_mysql_oracle import copy_mysql_to_oracle
        from services.type_system import ddl_type

        copy_mysql_to_oracle(
            source_cfg=_mysql_local_cfg(),
            source_table=mysql_src,
            dest_cfg=_oracle_cfg(),
            dest_table=src_table,
            pairs=[(name, name) for name, _ddl in COLUMNS],
            oracle_ddls=[ddl_type("oracle", ddl) for _name, ddl in COLUMNS],
            replace_destination=True,
        )
    if not keep_dest:
        reset_destination(_mysql_local_cfg(), dest_table)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    mysql = _mysql_local_cfg()
    ora = _oracle_cfg()
    source = EndpointConfig.from_dict(
        "database", {**ora, "format": "oracle", "table": src_table}
    )
    destination = EndpointConfig.from_dict(
        "database", {**mysql, "format": "mysql", "table": dest_table}
    )
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-ora-my-{dest_table}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
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
        "route": "oracle→mysql",
        "sync_mode": sync_mode,
        "source_table": src_table,
        "dest_table": dest_table,
        "mysql_port": 3306,
        "oracle_service": "XEPDB1",
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "oracle_lock": summary.get("oracle_lock"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(COLUMNS),
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest_table} oracle→mysql [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
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
        if summary.get("load_method") != "select_oracle_load_data_mysql":
            raise AssertionError(
                "expected SELECT + STRICT LOAD DATA, "
                f"got load_method={summary.get('load_method')!r}"
            )
        for part in report.get("partition_proof") or []:
            if int(part.get("source_count") or 0) != int(part.get("dest_count") or 0):
                raise AssertionError(f"partition dest COUNT mismatch: {part}")
    return report


def run_sqlserver_oracle_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity SQL Server→Oracle through stream_database_transfer. Dest COUNT required."""
    import socket

    try:
        socket.create_connection(("127.0.0.1", 1433), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"SQL Server 1433 not reachable: {exc}") from exc
    try:
        socket.create_connection(("127.0.0.1", 1521), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"Oracle 1521 not reachable: {exc}") from exc

    src_table = source_table or "bench_ss_from_mysql"
    dest = str(dest_table).upper()
    job_store = ensure_memory_job_store_if_mongo_down()
    conn = _sqlserver_connect()
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT OBJECT_ID(N'dbo.{src_table}', 'U')")  # nosec B608
        exists = cur.fetchone()[0] is not None
        have = _sqlserver_count(conn, src_table) if exists else 0
    finally:
        conn.close()
    if have != rows:
        mysql_src = _ensure_mysql_employee_fixture(rows, "bench_1m")
        from services.copy_mysql_sqlserver import copy_mysql_to_sqlserver
        from services.type_system import ddl_type

        copy_mysql_to_sqlserver(
            source_cfg=_mysql_local_cfg(),
            source_table=mysql_src,
            dest_cfg=_sqlserver_cfg(),
            dest_table=src_table,
            pairs=[(name, name) for name, _ddl in COLUMNS],
            sqlserver_ddls=[ddl_type("sqlserver", ddl) for _name, ddl in COLUMNS],
            replace_destination=True,
        )
    if not keep_dest:
        conn = _oracle_connect()
        try:
            _oracle_drop(conn.cursor(), dest)
            conn.commit()
        finally:
            conn.close()

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    ss = _sqlserver_cfg()
    ora = _oracle_cfg()
    source = EndpointConfig.from_dict(
        "database", {**ss, "format": "sqlserver", "table": src_table}
    )
    destination = EndpointConfig.from_dict(
        "database", {**ora, "format": "oracle", "table": dest}
    )
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-ss-ora-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    conn = _oracle_connect()
    try:
        landed = _oracle_count(conn, dest)
    finally:
        conn.close()
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "sqlserver→oracle",
        "sync_mode": sync_mode,
        "source_table": src_table,
        "dest_table": dest,
        "sqlserver_port": 1433,
        "oracle_service": "XEPDB1",
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "sqlserver_isolation": summary.get("sqlserver_isolation"),
        "empty_string_as_null_cells": summary.get("empty_string_as_null_cells"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(COLUMNS),
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} sqlserver→oracle [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination COUNT(*): {landed} (source rows: {rows})")
    print(
        "empty_string_as_null_cells: "
        f"{summary.get('empty_string_as_null_cells')}"
    )
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        if summary.get("load_method") != "select_sqlserver_executemany_oracle":
            raise AssertionError(
                "expected SELECT + executemany, "
                f"got load_method={summary.get('load_method')!r}"
            )
        for part in report.get("partition_proof") or []:
            if int(part.get("source_count") or 0) != int(part.get("dest_count") or 0):
                raise AssertionError(f"partition dest COUNT mismatch: {part}")
    return report


def run_oracle_sqlserver_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity Oracle→SQL Server through stream_database_transfer. Dest COUNT required."""
    import socket

    try:
        socket.create_connection(("127.0.0.1", 1433), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"SQL Server 1433 not reachable: {exc}") from exc
    try:
        socket.create_connection(("127.0.0.1", 1521), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"Oracle 1521 not reachable: {exc}") from exc

    src_table = str(source_table or "BENCH_SS_ORA").upper()
    job_store = ensure_memory_job_store_if_mongo_down()
    conn = _oracle_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM user_tables WHERE table_name = :1",
            [src_table],
        )
        exists = int(cur.fetchone()[0]) > 0
        have = _oracle_count(conn, src_table) if exists else 0
    finally:
        conn.close()
    if have != rows:
        ss_src = "bench_ss_from_mysql"
        ss_conn = _sqlserver_connect()
        try:
            cur = ss_conn.cursor()
            cur.execute(f"SELECT OBJECT_ID(N'dbo.{ss_src}', 'U')")  # nosec B608
            ss_exists = cur.fetchone()[0] is not None
            ss_have = _sqlserver_count(ss_conn, ss_src) if ss_exists else 0
        finally:
            ss_conn.close()
        if ss_have != rows:
            mysql_src = _ensure_mysql_employee_fixture(rows, "bench_1m")
            from services.copy_mysql_sqlserver import copy_mysql_to_sqlserver
            from services.type_system import ddl_type as _ddl_type

            copy_mysql_to_sqlserver(
                source_cfg=_mysql_local_cfg(),
                source_table=mysql_src,
                dest_cfg=_sqlserver_cfg(),
                dest_table=ss_src,
                pairs=[(name, name) for name, _ddl in COLUMNS],
                sqlserver_ddls=[_ddl_type("sqlserver", ddl) for _name, ddl in COLUMNS],
                replace_destination=True,
            )
        from services.copy_sqlserver_oracle import copy_sqlserver_to_oracle
        from services.type_system import ddl_type

        copy_sqlserver_to_oracle(
            source_cfg=_sqlserver_cfg(),
            source_table=ss_src,
            dest_cfg=_oracle_cfg(),
            dest_table=src_table,
            pairs=[(name, name) for name, _ddl in COLUMNS],
            oracle_ddls=[ddl_type("oracle", ddl) for _name, ddl in COLUMNS],
            replace_destination=True,
        )
    if not keep_dest:
        conn = _sqlserver_connect()
        try:
            cur = conn.cursor()
            cur.execute(f"DROP TABLE IF EXISTS dbo.[{dest_table}]")  # nosec B608
        finally:
            conn.close()

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    ss = _sqlserver_cfg()
    ora = _oracle_cfg()
    source = EndpointConfig.from_dict(
        "database", {**ora, "format": "oracle", "table": src_table}
    )
    destination = EndpointConfig.from_dict(
        "database", {**ss, "format": "sqlserver", "table": dest_table}
    )
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-ora-ss-{dest_table}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    conn = _sqlserver_connect()
    try:
        landed = _sqlserver_count(conn, dest_table)
    finally:
        conn.close()
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "oracle→sqlserver",
        "sync_mode": sync_mode,
        "source_table": src_table,
        "dest_table": dest_table,
        "sqlserver_port": 1433,
        "oracle_service": "XEPDB1",
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "oracle_lock": summary.get("oracle_lock"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(COLUMNS),
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest_table} oracle→sqlserver [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
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
        if summary.get("load_method") != "select_oracle_fast_executemany_sqlserver":
            raise AssertionError(
                "expected SELECT + fast_executemany, "
                f"got load_method={summary.get('load_method')!r}"
            )
        for part in report.get("partition_proof") or []:
            if int(part.get("source_count") or 0) != int(part.get("dest_count") or 0):
                raise AssertionError(f"partition dest COUNT mismatch: {part}")
    return report


def _iceberg_rest_uri() -> str:
    return os.environ.get("DATAFLOW_ICEBERG_REST_URI", "http://127.0.0.1:8181").rstrip("/")


def _iceberg_rest_warehouse() -> str:
    return os.environ.get(
        "DATAFLOW_ICEBERG_REST_WAREHOUSE", "file:///tmp/iceberg-rest-wh"
    )


def _iceberg_rest_cfg(table: str) -> dict[str, Any]:
    warehouse = _iceberg_rest_warehouse()
    return {
        "type": "iceberg",
        "format": "iceberg",
        "connection_string": _iceberg_rest_uri(),
        "warehouse": warehouse,
        "table": table,
        "schema": "default",
        "host": "127.0.0.1",
        "port": 8181,
        "database": "default",
        "extra": {"catalog_type": "rest", "warehouse": warehouse},
    }


def _iceberg_drop(table: str) -> None:
    from connectors.iceberg_catalog import load_catalog, parse_iceberg_catalog_config
    from pyiceberg.exceptions import NoSuchTableError

    cfg = _iceberg_rest_cfg(table)
    try:
        parsed = parse_iceberg_catalog_config(cfg)
        catalog = load_catalog(cfg)
        catalog.drop_table(parsed["namespace"] + (parsed["table_name"],))
    except NoSuchTableError:
        return


def _iceberg_count(table: str) -> int:
    from services.dest_precount import destination_row_count

    cfg = _iceberg_rest_cfg(table)
    n = destination_row_count("iceberg", cfg, schema="default", table_name=table)
    if n is None:
        raise RuntimeError("Iceberg dest COUNT unmeasured (snapshot/file footers)")
    return int(n)


def run_pg_iceberg_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity PostgreSQL→Iceberg through stream_database_transfer.

    Dest COUNT is file footers, never ``scan().count()``. Empty dest is CoW
    snapshot append, not MERGE INTO.
    """
    import socket
    from urllib.request import urlopen

    try:
        socket.create_connection(("127.0.0.1", 5432), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"PostgreSQL 5432 not reachable: {exc}") from exc
    rest = _iceberg_rest_uri()
    try:
        with urlopen(f"{rest}/v1/config", timeout=2) as resp:
            if int(getattr(resp, "status", 0) or 0) != 200:
                raise RuntimeError(f"Iceberg REST {rest}/v1/config not 200")
    except Exception as exc:
        raise RuntimeError(f"Iceberg REST not reachable at {rest}: {exc}") from exc

    pg = _pg_local_cfg()
    src_table = source_table or f"bench_emp_{rows}"
    dest = str(dest_table)
    job_store = ensure_memory_job_store_if_mongo_down()
    seed_source(pg, src_table, rows)
    if not keep_dest:
        _iceberg_drop(dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

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
    destination = EndpointConfig.from_dict("database", _iceberg_rest_cfg(dest))
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-pg-iceberg-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = _iceberg_count(dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "postgresql→iceberg",
        "sync_mode": sync_mode,
        "source_table": src_table,
        "dest_table": dest,
        "pg_port": pg["port"],
        "iceberg_rest": rest,
        "iceberg_warehouse": _iceberg_rest_warehouse(),
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "iceberg_write": summary.get("iceberg_write"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(COLUMNS),
        "dest_count_source": "iceberg_file_footers",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} postgresql→iceberg [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination COUNT (file footers): {landed} (source rows: {rows})")
    print(f"iceberg_write: {summary.get('iceberg_write')}")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "copy_csv_pg_to_iceberg_snapshot"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected COPY CSV + Iceberg snapshot, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def run_iceberg_pg_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity Iceberg→PostgreSQL through stream_database_transfer.

    Source COUNT is Iceberg file footers, never ``scan().count()``. Dest
    proof is PostgreSQL ``COUNT(*)``.
    """
    import socket
    from urllib.request import urlopen

    try:
        socket.create_connection(("127.0.0.1", 5432), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"PostgreSQL 5432 not reachable: {exc}") from exc
    rest = _iceberg_rest_uri()
    try:
        with urlopen(f"{rest}/v1/config", timeout=2) as resp:
            if int(getattr(resp, "status", 0) or 0) != 200:
                raise RuntimeError(f"Iceberg REST {rest}/v1/config not 200")
    except Exception as exc:
        raise RuntimeError(f"Iceberg REST not reachable at {rest}: {exc}") from exc

    pg = _pg_local_cfg()
    ice_src = str(source_table or "bench_pg_iceberg")
    dest = str(dest_table)
    job_store = ensure_memory_job_store_if_mongo_down()
    try:
        have = _iceberg_count(ice_src)
    except Exception:
        have = 0
    if have != rows:
        run_pg_iceberg_volume(
            rows=rows,
            dest_table=ice_src,
            source_table=f"bench_emp_{rows}",
            sync_mode="full_refresh_append",
            keep_dest=False,
            fail_closed=True,
            proof_path=None,
        )
    if not keep_dest:
        reset_pg_destination(pg, dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    source = EndpointConfig.from_dict("database", _iceberg_rest_cfg(ice_src))
    destination = EndpointConfig.from_dict(
        "database",
        {
            "format": "postgresql",
            "host": pg["host"],
            "port": pg["port"],
            "database": pg["dbname"],
            "username": pg["user"],
            "password": pg["password"],
            "table": dest,
            "schema": "public",
        },
    )
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-iceberg-pg-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = postgres_count(pg, dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "iceberg→postgresql",
        "sync_mode": sync_mode,
        "source_table": ice_src,
        "dest_table": dest,
        "pg_port": pg["port"],
        "iceberg_rest": rest,
        "iceberg_warehouse": _iceberg_rest_warehouse(),
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "iceberg_read": summary.get("iceberg_read"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(COLUMNS),
        "source_count_source": "iceberg_file_footers",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} iceberg→postgresql [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination COUNT(*): {landed} (source footer COUNT: {rows})")
    print(f"iceberg_read: {summary.get('iceberg_read')}")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "iceberg_parquet_copy_from_stdin_pg"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected Iceberg Parquet + COPY FROM STDIN, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def run_mysql_iceberg_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity MySQL→Iceberg through stream_database_transfer.

    Dest COUNT is file footers, never ``scan().count()``. Empty dest is CoW
    snapshot append, not MERGE INTO. MySQL has no COPY TO STDOUT — one
    consistent-snapshot SELECT is encoded as CSV.
    """
    import socket
    from urllib.request import urlopen

    try:
        socket.create_connection(("127.0.0.1", 3306), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"MySQL 3306 not reachable: {exc}") from exc
    rest = _iceberg_rest_uri()
    try:
        with urlopen(f"{rest}/v1/config", timeout=2) as resp:
            if int(getattr(resp, "status", 0) or 0) != 200:
                raise RuntimeError(f"Iceberg REST {rest}/v1/config not 200")
    except Exception as exc:
        raise RuntimeError(f"Iceberg REST not reachable at {rest}: {exc}") from exc

    src_table = source_table or "bench_1m"
    dest = str(dest_table)
    job_store = ensure_memory_job_store_if_mongo_down()
    _ensure_mysql_employee_fixture(rows, src_table)
    if not keep_dest:
        _iceberg_drop(dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    mysql = _mysql_local_cfg()
    source = EndpointConfig.from_dict(
        "database", {**mysql, "format": "mysql", "table": src_table}
    )
    destination = EndpointConfig.from_dict("database", _iceberg_rest_cfg(dest))
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-mysql-iceberg-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = _iceberg_count(dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "mysql→iceberg",
        "sync_mode": sync_mode,
        "source_table": src_table,
        "dest_table": dest,
        "mysql_port": 3306,
        "iceberg_rest": rest,
        "iceberg_warehouse": _iceberg_rest_warehouse(),
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "iceberg_write": summary.get("iceberg_write"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(COLUMNS),
        "dest_count_source": "iceberg_file_footers",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} mysql→iceberg [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination COUNT (file footers): {landed} (source rows: {rows})")
    print(f"iceberg_write: {summary.get('iceberg_write')}")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "select_mysql_csv_iceberg_snapshot"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected SELECT + CSV + Iceberg snapshot, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def run_iceberg_mysql_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity Iceberg→MySQL through stream_database_transfer.

    Source COUNT is Iceberg file footers, never ``scan().count()``. Dest
    proof is MySQL ``COUNT(*)``. Payload is snapshot Parquet + STRICT
    LOAD DATA, never ``scan().to_arrow()``.
    """
    import socket
    from urllib.request import urlopen

    try:
        socket.create_connection(("127.0.0.1", 3306), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"MySQL 3306 not reachable: {exc}") from exc
    rest = _iceberg_rest_uri()
    try:
        with urlopen(f"{rest}/v1/config", timeout=2) as resp:
            if int(getattr(resp, "status", 0) or 0) != 200:
                raise RuntimeError(f"Iceberg REST {rest}/v1/config not 200")
    except Exception as exc:
        raise RuntimeError(f"Iceberg REST not reachable at {rest}: {exc}") from exc

    ice_src = str(source_table or "bench_mysql_iceberg")
    dest = str(dest_table)
    job_store = ensure_memory_job_store_if_mongo_down()
    try:
        have = _iceberg_count(ice_src)
    except Exception:
        have = 0
    if have != rows:
        run_mysql_iceberg_volume(
            rows=rows,
            dest_table=ice_src,
            source_table="bench_1m",
            sync_mode="full_refresh_append",
            keep_dest=False,
            fail_closed=True,
            proof_path=None,
        )
    mysql = _mysql_local_cfg()
    if not keep_dest:
        reset_destination(mysql, dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    source = EndpointConfig.from_dict("database", _iceberg_rest_cfg(ice_src))
    destination = EndpointConfig.from_dict(
        "database", {**mysql, "format": "mysql", "table": dest}
    )
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-iceberg-mysql-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = destination_count(mysql, dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "iceberg→mysql",
        "sync_mode": sync_mode,
        "source_table": ice_src,
        "dest_table": dest,
        "mysql_port": 3306,
        "iceberg_rest": rest,
        "iceberg_warehouse": _iceberg_rest_warehouse(),
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "iceberg_read": summary.get("iceberg_read"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(COLUMNS),
        "source_count_source": "iceberg_file_footers",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} iceberg→mysql [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination COUNT(*): {landed} (source footer COUNT: {rows})")
    print(f"iceberg_read: {summary.get('iceberg_read')}")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "iceberg_parquet_load_data_mysql"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected Iceberg Parquet + STRICT LOAD DATA, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def _ensure_sqlserver_employee_fixture(rows: int, ss_table: str) -> str:
    """Use existing SQL Server 10-col table or COPY MySQL ``bench_1m`` into it."""
    conn = _sqlserver_connect()
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT OBJECT_ID(N'dbo.{ss_table}', 'U')")  # nosec B608
        exists = cur.fetchone()[0] is not None
        if exists and _sqlserver_count(conn, ss_table) == rows:
            return ss_table
    finally:
        conn.close()
    run_mysql_sqlserver_volume(
        rows=rows,
        dest_table=ss_table,
        source_table="bench_1m",
        sync_mode="full_refresh_overwrite",
        keep_dest=False,
        fail_closed=True,
        proof_path=None,
    )
    return ss_table


def run_sqlserver_iceberg_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity SQL Server→Iceberg through stream_database_transfer.

    Dest COUNT is file footers, never ``scan().count()``. Empty dest is CoW
    snapshot append, not MERGE INTO.
    """
    import socket
    from urllib.request import urlopen

    try:
        socket.create_connection(("127.0.0.1", 1433), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"SQL Server 1433 not reachable: {exc}") from exc
    rest = _iceberg_rest_uri()
    try:
        with urlopen(f"{rest}/v1/config", timeout=2) as resp:
            if int(getattr(resp, "status", 0) or 0) != 200:
                raise RuntimeError(f"Iceberg REST {rest}/v1/config not 200")
    except Exception as exc:
        raise RuntimeError(f"Iceberg REST not reachable at {rest}: {exc}") from exc

    src_table = source_table or "bench_ss_from_mysql"
    dest = str(dest_table)
    job_store = ensure_memory_job_store_if_mongo_down()
    _ensure_sqlserver_employee_fixture(rows, src_table)
    if not keep_dest:
        _iceberg_drop(dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    ss = _sqlserver_cfg()
    source = EndpointConfig.from_dict(
        "database", {**ss, "format": "sqlserver", "table": src_table}
    )
    destination = EndpointConfig.from_dict("database", _iceberg_rest_cfg(dest))
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-ss-iceberg-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = _iceberg_count(dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "sqlserver→iceberg",
        "sync_mode": sync_mode,
        "source_table": src_table,
        "dest_table": dest,
        "sqlserver_port": 1433,
        "iceberg_rest": rest,
        "iceberg_warehouse": _iceberg_rest_warehouse(),
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "iceberg_write": summary.get("iceberg_write"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(COLUMNS),
        "dest_count_source": "iceberg_file_footers",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} sqlserver→iceberg [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination COUNT (file footers): {landed} (source rows: {rows})")
    print(f"iceberg_write: {summary.get('iceberg_write')}")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "select_sqlserver_csv_iceberg_snapshot"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected SELECT + CSV + Iceberg snapshot, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def run_iceberg_sqlserver_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity Iceberg→SQL Server through stream_database_transfer.

    Source COUNT is Iceberg file footers, never ``scan().count()``. Dest
    proof is SQL Server ``COUNT(*)``.
    """
    import socket
    from urllib.request import urlopen

    try:
        socket.create_connection(("127.0.0.1", 1433), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"SQL Server 1433 not reachable: {exc}") from exc
    rest = _iceberg_rest_uri()
    try:
        with urlopen(f"{rest}/v1/config", timeout=2) as resp:
            if int(getattr(resp, "status", 0) or 0) != 200:
                raise RuntimeError(f"Iceberg REST {rest}/v1/config not 200")
    except Exception as exc:
        raise RuntimeError(f"Iceberg REST not reachable at {rest}: {exc}") from exc

    ice_src = str(source_table or "bench_ss_iceberg")
    dest = str(dest_table)
    job_store = ensure_memory_job_store_if_mongo_down()
    try:
        have = _iceberg_count(ice_src)
    except Exception:
        have = 0
    if have != rows:
        run_sqlserver_iceberg_volume(
            rows=rows,
            dest_table=ice_src,
            source_table="bench_ss_from_mysql",
            sync_mode="full_refresh_append",
            keep_dest=False,
            fail_closed=True,
            proof_path=None,
        )
    if not keep_dest:
        conn = _sqlserver_connect()
        try:
            cur = conn.cursor()
            cur.execute(f"DROP TABLE IF EXISTS dbo.[{dest}]")  # nosec B608
        finally:
            conn.close()

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    ss = _sqlserver_cfg()
    source = EndpointConfig.from_dict("database", _iceberg_rest_cfg(ice_src))
    destination = EndpointConfig.from_dict(
        "database", {**ss, "format": "sqlserver", "table": dest}
    )
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-iceberg-ss-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    conn = _sqlserver_connect()
    try:
        landed = _sqlserver_count(conn, dest)
    finally:
        conn.close()
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "iceberg→sqlserver",
        "sync_mode": sync_mode,
        "source_table": ice_src,
        "dest_table": dest,
        "sqlserver_port": 1433,
        "iceberg_rest": rest,
        "iceberg_warehouse": _iceberg_rest_warehouse(),
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "iceberg_read": summary.get("iceberg_read"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(COLUMNS),
        "source_count_source": "iceberg_file_footers",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} iceberg→sqlserver [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination COUNT(*): {landed} (source footer COUNT: {rows})")
    print(f"iceberg_read: {summary.get('iceberg_read')}")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "iceberg_parquet_fast_executemany_sqlserver"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected Iceberg Parquet + fast_executemany, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def _ensure_oracle_employee_fixture(rows: int, ora_table: str) -> str:
    """Use existing Oracle 10-col table or COPY MySQL ``bench_1m`` into it."""
    tbl = str(ora_table).upper()
    conn = _oracle_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM user_tables WHERE table_name = :1",
            [tbl],
        )
        exists = int(cur.fetchone()[0]) > 0
        if exists and _oracle_count(conn, tbl) == rows:
            return tbl
    finally:
        conn.close()
    run_mysql_oracle_volume(
        rows=rows,
        dest_table=tbl,
        source_table="bench_1m",
        sync_mode="full_refresh_overwrite",
        keep_dest=False,
        fail_closed=True,
        proof_path=None,
    )
    return tbl


def run_oracle_iceberg_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity Oracle→Iceberg through stream_database_transfer.

    Dest COUNT is file footers, never ``scan().count()``. Empty dest is CoW
    snapshot append, not MERGE INTO.
    """
    import socket
    from urllib.request import urlopen

    try:
        socket.create_connection(("127.0.0.1", 1521), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"Oracle 1521 not reachable: {exc}") from exc
    rest = _iceberg_rest_uri()
    try:
        with urlopen(f"{rest}/v1/config", timeout=2) as resp:
            if int(getattr(resp, "status", 0) or 0) != 200:
                raise RuntimeError(f"Iceberg REST {rest}/v1/config not 200")
    except Exception as exc:
        raise RuntimeError(f"Iceberg REST not reachable at {rest}: {exc}") from exc

    src_table = _ensure_oracle_employee_fixture(rows, source_table or "BENCH_MY_ORA")
    dest = str(dest_table)
    job_store = ensure_memory_job_store_if_mongo_down()
    if not keep_dest:
        _iceberg_drop(dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    ora = _oracle_cfg()
    source = EndpointConfig.from_dict(
        "database", {**ora, "format": "oracle", "table": src_table}
    )
    destination = EndpointConfig.from_dict("database", _iceberg_rest_cfg(dest))
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-ora-iceberg-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = _iceberg_count(dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "oracle→iceberg",
        "sync_mode": sync_mode,
        "source_table": src_table,
        "dest_table": dest,
        "oracle_dsn": "127.0.0.1:1521/XEPDB1",
        "iceberg_rest": rest,
        "iceberg_warehouse": _iceberg_rest_warehouse(),
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "iceberg_write": summary.get("iceberg_write"),
        "oracle_lock": summary.get("oracle_lock"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(COLUMNS),
        "dest_count_source": "iceberg_file_footers",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} oracle→iceberg [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination COUNT (file footers): {landed} (source rows: {rows})")
    print(f"iceberg_write: {summary.get('iceberg_write')}")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "select_oracle_csv_iceberg_snapshot"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected SELECT + CSV + Iceberg snapshot, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def run_iceberg_oracle_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity Iceberg→Oracle through stream_database_transfer.

    Source COUNT is Iceberg file footers, never ``scan().count()``. Dest
    proof is Oracle ``COUNT(*)``. VARCHAR2 empty-string→NULL is engine
    law, counted in ``empty_string_as_null_cells``.
    """
    import socket
    from urllib.request import urlopen

    try:
        socket.create_connection(("127.0.0.1", 1521), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"Oracle 1521 not reachable: {exc}") from exc
    rest = _iceberg_rest_uri()
    try:
        with urlopen(f"{rest}/v1/config", timeout=2) as resp:
            if int(getattr(resp, "status", 0) or 0) != 200:
                raise RuntimeError(f"Iceberg REST {rest}/v1/config not 200")
    except Exception as exc:
        raise RuntimeError(f"Iceberg REST not reachable at {rest}: {exc}") from exc

    ice_src = str(source_table or "bench_ora_iceberg")
    dest = str(dest_table).upper()
    job_store = ensure_memory_job_store_if_mongo_down()
    try:
        have = _iceberg_count(ice_src)
    except Exception:
        have = 0
    if have != rows:
        run_oracle_iceberg_volume(
            rows=rows,
            dest_table=ice_src,
            source_table="BENCH_MY_ORA",
            sync_mode="full_refresh_append",
            keep_dest=False,
            fail_closed=True,
            proof_path=None,
        )
    if not keep_dest:
        conn = _oracle_connect()
        try:
            _oracle_drop(conn.cursor(), dest)
            conn.commit()
        finally:
            conn.close()

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    ora = _oracle_cfg()
    source = EndpointConfig.from_dict("database", _iceberg_rest_cfg(ice_src))
    destination = EndpointConfig.from_dict(
        "database", {**ora, "format": "oracle", "table": dest}
    )
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-iceberg-ora-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    conn = _oracle_connect()
    try:
        landed = _oracle_count(conn, dest)
    finally:
        conn.close()
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "iceberg→oracle",
        "sync_mode": sync_mode,
        "source_table": ice_src,
        "dest_table": dest,
        "oracle_dsn": "127.0.0.1:1521/XEPDB1",
        "iceberg_rest": rest,
        "iceberg_warehouse": _iceberg_rest_warehouse(),
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "iceberg_read": summary.get("iceberg_read"),
        "empty_string_as_null_cells": summary.get("empty_string_as_null_cells") or 0,
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(COLUMNS),
        "source_count_source": "iceberg_file_footers",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} iceberg→oracle [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination COUNT(*): {landed} (source footer COUNT: {rows})")
    print(f"iceberg_read: {summary.get('iceberg_read')}")
    print(
        "empty_string_as_null_cells: "
        f"{summary.get('empty_string_as_null_cells') or 0}"
    )
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "iceberg_parquet_executemany_oracle"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected Iceberg Parquet + executemany, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def _mongo_cfg(collection: str) -> dict[str, Any]:
    return {
        "type": "mongodb",
        "format": "mongodb",
        "host": "127.0.0.1",
        "port": 27017,
        "database": "dataflow",
        "table": collection,
        "collection": collection,
    }


def _mongo_drop(collection: str) -> None:
    from pymongo import MongoClient

    client = MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=5000)
    try:
        client["dataflow"][collection].drop()
    finally:
        client.close()


def _mongo_count(collection: str) -> int:
    from services.dest_precount import destination_row_count

    n = destination_row_count(
        "mongodb", _mongo_cfg(collection), schema="", table_name=collection
    )
    if n is None:
        raise RuntimeError(f"Mongo dest COUNT unmeasured for {collection}")
    return int(n)


def run_pg_mongo_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity PostgreSQL→MongoDB through stream_database_transfer.

    Dest COUNT is ``count_documents({})``, never ``estimatedDocumentCount``.
    Empty dest is insert_many, not upsert.
    """
    import socket

    try:
        socket.create_connection(("127.0.0.1", 5432), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"PostgreSQL 5432 not reachable: {exc}") from exc
    try:
        socket.create_connection(("127.0.0.1", 27017), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"MongoDB 27017 not reachable: {exc}") from exc

    pg = _pg_local_cfg()
    src_table = source_table or f"bench_emp_{rows}"
    dest = str(dest_table)
    job_store = ensure_memory_job_store_if_mongo_down()
    seed_source(pg, src_table, rows)
    if not keep_dest:
        _mongo_drop(dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    source = EndpointConfig.from_dict(
        "database",
        {
            "format": "postgresql",
            "host": pg["host"],
            "port": pg["port"],
            "database": pg["dbname"],
            "username": pg["user"],
            "password": pg["password"],
            "schema": "public",
            "table": src_table,
        },
    )
    destination = EndpointConfig.from_dict("database", _mongo_cfg(dest))
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-pg-mongo-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = _mongo_count(dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "postgresql→mongodb",
        "sync_mode": sync_mode,
        "source_table": src_table,
        "dest_table": dest,
        "postgres_port": 5432,
        "mongo_port": 27017,
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "mongo_write": summary.get("mongo_write"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(COLUMNS),
        "dest_count_source": "mongodb_count_documents",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} postgresql→mongodb [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination count_documents: {landed} (source rows: {rows})")
    print(f"mongo_write: {summary.get('mongo_write')}")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "copy_text_pg_insert_many_mongo"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected COPY text + insert_many, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def run_mongo_pg_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity MongoDB→PostgreSQL through stream_database_transfer.

    Source COUNT is ``count_documents`` in a replica-set snapshot. Dest
    proof is PostgreSQL ``COUNT(*)``.
    """
    import socket

    try:
        socket.create_connection(("127.0.0.1", 5432), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"PostgreSQL 5432 not reachable: {exc}") from exc
    try:
        socket.create_connection(("127.0.0.1", 27017), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"MongoDB 27017 not reachable: {exc}") from exc

    mongo_src = str(source_table or "bench_pg_mongo")
    dest = str(dest_table)
    job_store = ensure_memory_job_store_if_mongo_down()
    try:
        have = _mongo_count(mongo_src)
    except Exception:
        have = 0
    if have != rows:
        run_pg_mongo_volume(
            rows=rows,
            dest_table=mongo_src,
            source_table=f"bench_emp_{rows}",
            sync_mode="full_refresh_append",
            keep_dest=False,
            fail_closed=True,
            proof_path=None,
        )
    if not keep_dest:
        pg = _pg_local_cfg()
        conn = psycopg2.connect(
            host=pg["host"],
            port=pg["port"],
            user=pg["user"],
            password=pg["password"],
            dbname=pg["dbname"],
        )
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS public."{dest}"')  # nosec B608
        finally:
            conn.close()

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    pg = _pg_local_cfg()
    source = EndpointConfig.from_dict("database", _mongo_cfg(mongo_src))
    destination = EndpointConfig.from_dict(
        "database",
        {
            "format": "postgresql",
            "host": pg["host"],
            "port": pg["port"],
            "database": pg["dbname"],
            "username": pg["user"],
            "password": pg["password"],
            "schema": "public",
            "table": dest,
        },
    )
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-mongo-pg-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    conn = psycopg2.connect(
        host=pg["host"],
        port=pg["port"],
        user=pg["user"],
        password=pg["password"],
        dbname=pg["dbname"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM public."{dest}"')  # nosec B608
            landed = int(cur.fetchone()[0])
    finally:
        conn.close()
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "mongodb→postgresql",
        "sync_mode": sync_mode,
        "source_table": mongo_src,
        "dest_table": dest,
        "postgres_port": 5432,
        "mongo_port": 27017,
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "mongo_read": summary.get("mongo_read"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(COLUMNS),
        "source_count_source": "mongodb_count_documents",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} mongodb→postgresql [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination COUNT(*): {landed} (source count_documents: {rows})")
    print(f"mongo_read: {summary.get('mongo_read')}")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "mongo_snapshot_find_copy_from_stdin_pg"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected Mongo snapshot find + COPY FROM STDIN, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def run_mysql_mongo_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity MySQL→MongoDB through stream_database_transfer.

    Dest COUNT is ``count_documents({})``, never ``estimatedDocumentCount``.
    Empty dest is insert_many, not upsert. MySQL has no COPY TO STDOUT —
    one consistent-snapshot SELECT is bound with insert_many.
    """
    import socket

    try:
        socket.create_connection(("127.0.0.1", 3306), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"MySQL 3306 not reachable: {exc}") from exc
    try:
        socket.create_connection(("127.0.0.1", 27017), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"MongoDB 27017 not reachable: {exc}") from exc

    src_table = source_table or "bench_1m"
    dest = str(dest_table)
    job_store = ensure_memory_job_store_if_mongo_down()
    _ensure_mysql_employee_fixture(rows, src_table)
    if not keep_dest:
        _mongo_drop(dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    mysql = _mysql_local_cfg()
    source = EndpointConfig.from_dict(
        "database", {**mysql, "format": "mysql", "table": src_table}
    )
    destination = EndpointConfig.from_dict("database", _mongo_cfg(dest))
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-mysql-mongo-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = _mongo_count(dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "mysql→mongodb",
        "sync_mode": sync_mode,
        "source_table": src_table,
        "dest_table": dest,
        "mysql_port": 3306,
        "mongo_port": 27017,
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "mongo_write": summary.get("mongo_write"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(COLUMNS),
        "dest_count_source": "mongodb_count_documents",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} mysql→mongodb [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination count_documents: {landed} (source rows: {rows})")
    print(f"mongo_write: {summary.get('mongo_write')}")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "select_mysql_insert_many_mongo"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected SELECT + insert_many, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def run_mongo_mysql_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity MongoDB→MySQL through stream_database_transfer.

    Source COUNT is ``count_documents`` in a replica-set snapshot. Dest
    proof is MySQL ``COUNT(*)``. Payload is snapshot find + STRICT
    LOAD DATA, never mongoexport.
    """
    import socket

    try:
        socket.create_connection(("127.0.0.1", 3306), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"MySQL 3306 not reachable: {exc}") from exc
    try:
        socket.create_connection(("127.0.0.1", 27017), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"MongoDB 27017 not reachable: {exc}") from exc

    mongo_src = str(source_table or "bench_mysql_mongo")
    dest = str(dest_table)
    job_store = ensure_memory_job_store_if_mongo_down()
    try:
        have = _mongo_count(mongo_src)
    except Exception:
        have = 0
    if have != rows:
        run_mysql_mongo_volume(
            rows=rows,
            dest_table=mongo_src,
            source_table="bench_1m",
            sync_mode="full_refresh_append",
            keep_dest=False,
            fail_closed=True,
            proof_path=None,
        )
    mysql = _mysql_local_cfg()
    ensure_mysql_local_infile(mysql)
    if not keep_dest:
        reset_destination(mysql, dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    source = EndpointConfig.from_dict("database", _mongo_cfg(mongo_src))
    destination = EndpointConfig.from_dict(
        "database", {**mysql, "format": "mysql", "table": dest}
    )
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-mongo-mysql-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = destination_count(mysql, dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "mongodb→mysql",
        "sync_mode": sync_mode,
        "source_table": mongo_src,
        "dest_table": dest,
        "mysql_port": 3306,
        "mongo_port": 27017,
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "mongo_read": summary.get("mongo_read"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(COLUMNS),
        "source_count_source": "mongodb_count_documents",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} mongodb→mysql [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination COUNT(*): {landed} (source count_documents: {rows})")
    print(f"mongo_read: {summary.get('mongo_read')}")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "mongo_snapshot_find_load_data_mysql"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected Mongo snapshot find + STRICT LOAD DATA, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def _sqlserver_drop_table(table: str) -> None:
    conn = _sqlserver_connect()
    try:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS dbo.[{table}]")  # nosec B608
    finally:
        conn.close()


def _sqlserver_dest_count(table: str) -> int:
    conn = _sqlserver_connect()
    try:
        return _sqlserver_count(conn, table)
    finally:
        conn.close()


def _oracle_drop_table(table: str) -> None:
    conn = _oracle_connect()
    try:
        _oracle_drop(conn.cursor(), str(table).upper())
        conn.commit()
    finally:
        conn.close()


def _oracle_dest_count(table: str) -> int:
    conn = _oracle_connect()
    try:
        return _oracle_count(conn, str(table).upper())
    finally:
        conn.close()


def run_sqlserver_mongo_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity SQL Server→MongoDB through stream_database_transfer.

    Dest COUNT is ``count_documents({})``, never ``estimatedDocumentCount``.
    Empty dest is insert_many, not upsert. Not BCP / mongoimport.
    """
    import socket

    try:
        socket.create_connection(("127.0.0.1", 1433), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"SQL Server 1433 not reachable: {exc}") from exc
    try:
        socket.create_connection(("127.0.0.1", 27017), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"MongoDB 27017 not reachable: {exc}") from exc

    src_table = source_table or "bench_ss_from_mysql"
    dest = str(dest_table)
    job_store = ensure_memory_job_store_if_mongo_down()
    _ensure_sqlserver_employee_fixture(rows, src_table)
    if not keep_dest:
        _mongo_drop(dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    ss = _sqlserver_cfg()
    source = EndpointConfig.from_dict(
        "database", {**ss, "format": "sqlserver", "table": src_table}
    )
    destination = EndpointConfig.from_dict("database", _mongo_cfg(dest))
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-ss-mongo-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = _mongo_count(dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "sqlserver→mongodb",
        "sync_mode": sync_mode,
        "source_table": src_table,
        "dest_table": dest,
        "sqlserver_port": 1433,
        "mongo_port": 27017,
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "mongo_write": summary.get("mongo_write"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(COLUMNS),
        "dest_count_source": "mongodb_count_documents",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} sqlserver→mongodb [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination count_documents: {landed} (source rows: {rows})")
    print(f"mongo_write: {summary.get('mongo_write')}")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "select_sqlserver_insert_many_mongo"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected SELECT + insert_many, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def run_mongo_sqlserver_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity MongoDB→SQL Server through stream_database_transfer.

    Source COUNT is ``count_documents`` in a replica-set snapshot. Dest
    proof is SQL Server ``COUNT(*)``. Payload is snapshot find +
    fast_executemany, never BCP / mongoexport.
    """
    import socket

    try:
        socket.create_connection(("127.0.0.1", 1433), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"SQL Server 1433 not reachable: {exc}") from exc
    try:
        socket.create_connection(("127.0.0.1", 27017), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"MongoDB 27017 not reachable: {exc}") from exc

    mongo_src = str(source_table or "bench_ss_mongo")
    dest = str(dest_table)
    job_store = ensure_memory_job_store_if_mongo_down()
    try:
        have = _mongo_count(mongo_src)
    except Exception:
        have = 0
    if have != rows:
        run_sqlserver_mongo_volume(
            rows=rows,
            dest_table=mongo_src,
            source_table="bench_ss_from_mysql",
            sync_mode="full_refresh_append",
            keep_dest=False,
            fail_closed=True,
            proof_path=None,
        )
    if not keep_dest:
        _sqlserver_drop_table(dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    ss = _sqlserver_cfg()
    source = EndpointConfig.from_dict("database", _mongo_cfg(mongo_src))
    destination = EndpointConfig.from_dict(
        "database", {**ss, "format": "sqlserver", "table": dest}
    )
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-mongo-ss-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = _sqlserver_dest_count(dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "mongodb→sqlserver",
        "sync_mode": sync_mode,
        "source_table": mongo_src,
        "dest_table": dest,
        "sqlserver_port": 1433,
        "mongo_port": 27017,
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "mongo_read": summary.get("mongo_read"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(COLUMNS),
        "source_count_source": "mongodb_count_documents",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} mongodb→sqlserver [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination COUNT(*): {landed} (source count_documents: {rows})")
    print(f"mongo_read: {summary.get('mongo_read')}")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "mongo_snapshot_find_fast_executemany_sqlserver"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected Mongo snapshot find + fast_executemany, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def run_oracle_mongo_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity Oracle→MongoDB through stream_database_transfer.

    Dest COUNT is ``count_documents({})``, never ``estimatedDocumentCount``.
    Empty dest is insert_many, not upsert. Not sqlldr / mongoimport.
    """
    import socket

    try:
        socket.create_connection(("127.0.0.1", 1521), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"Oracle 1521 not reachable: {exc}") from exc
    try:
        socket.create_connection(("127.0.0.1", 27017), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"MongoDB 27017 not reachable: {exc}") from exc

    src_table = _ensure_oracle_employee_fixture(rows, source_table or "BENCH_MY_ORA")
    dest = str(dest_table)
    job_store = ensure_memory_job_store_if_mongo_down()
    if not keep_dest:
        _mongo_drop(dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    ora = _oracle_cfg()
    source = EndpointConfig.from_dict(
        "database", {**ora, "format": "oracle", "table": src_table}
    )
    destination = EndpointConfig.from_dict("database", _mongo_cfg(dest))
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-ora-mongo-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = _mongo_count(dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "oracle→mongodb",
        "sync_mode": sync_mode,
        "source_table": src_table,
        "dest_table": dest,
        "oracle_port": 1521,
        "mongo_port": 27017,
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "mongo_write": summary.get("mongo_write"),
        "oracle_lock": summary.get("oracle_lock"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(COLUMNS),
        "dest_count_source": "mongodb_count_documents",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} oracle→mongodb [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination count_documents: {landed} (source rows: {rows})")
    print(f"mongo_write: {summary.get('mongo_write')}")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "select_oracle_insert_many_mongo"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected SHARE-lock SELECT + insert_many, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def run_mongo_oracle_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity MongoDB→Oracle through stream_database_transfer.

    Source COUNT is ``count_documents`` in a replica-set snapshot. Dest
    proof is Oracle ``COUNT(*)``. Payload is snapshot find + executemany,
    never sqlldr / mongoexport. VARCHAR2 ``''`` → NULL is engine law.
    """
    import socket

    try:
        socket.create_connection(("127.0.0.1", 1521), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"Oracle 1521 not reachable: {exc}") from exc
    try:
        socket.create_connection(("127.0.0.1", 27017), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"MongoDB 27017 not reachable: {exc}") from exc

    mongo_src = str(source_table or "bench_ora_mongo")
    dest = str(dest_table).upper()
    job_store = ensure_memory_job_store_if_mongo_down()
    try:
        have = _mongo_count(mongo_src)
    except Exception:
        have = 0
    if have != rows:
        run_oracle_mongo_volume(
            rows=rows,
            dest_table=mongo_src,
            source_table="BENCH_MY_ORA",
            sync_mode="full_refresh_append",
            keep_dest=False,
            fail_closed=True,
            proof_path=None,
        )
    if not keep_dest:
        _oracle_drop_table(dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    ora = _oracle_cfg()
    source = EndpointConfig.from_dict("database", _mongo_cfg(mongo_src))
    destination = EndpointConfig.from_dict(
        "database", {**ora, "format": "oracle", "table": dest}
    )
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-mongo-ora-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = _oracle_dest_count(dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "mongodb→oracle",
        "sync_mode": sync_mode,
        "source_table": mongo_src,
        "dest_table": dest,
        "oracle_port": 1521,
        "mongo_port": 27017,
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "mongo_read": summary.get("mongo_read"),
        "empty_string_as_null_cells": summary.get("empty_string_as_null_cells"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(COLUMNS),
        "source_count_source": "mongodb_count_documents",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} mongodb→oracle [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination COUNT(*): {landed} (source count_documents: {rows})")
    print(f"mongo_read: {summary.get('mongo_read')}")
    print(
        "empty_string_as_null_cells: "
        f"{summary.get('empty_string_as_null_cells')}"
    )
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "mongo_snapshot_find_executemany_oracle"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected Mongo snapshot find + executemany, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def run_mongo_mongo_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity MongoDB→MongoDB through stream_database_transfer.

    Source and dest COUNT are ``count_documents``, never
    ``estimatedDocumentCount``. Empty dest is insert_many, not upsert /
    ``$out``. Nested BSON is identity-safe. Same collection declines.
    """
    import socket

    try:
        socket.create_connection(("127.0.0.1", 27017), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"MongoDB 27017 not reachable: {exc}") from exc

    mongo_src = str(source_table or "bench_mysql_mongo")
    dest = str(dest_table)
    if mongo_src == dest:
        raise AssertionError("Mongo→Mongo bench refuses the same collection")
    job_store = ensure_memory_job_store_if_mongo_down()
    try:
        have = _mongo_count(mongo_src)
    except Exception:
        have = 0
    if have != rows:
        run_mysql_mongo_volume(
            rows=rows,
            dest_table=mongo_src,
            source_table="bench_1m",
            sync_mode="full_refresh_append",
            keep_dest=False,
            fail_closed=True,
            proof_path=None,
        )
    if not keep_dest:
        _mongo_drop(dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    source = EndpointConfig.from_dict("database", _mongo_cfg(mongo_src))
    destination = EndpointConfig.from_dict("database", _mongo_cfg(dest))
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-mongo-mongo-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = _mongo_count(dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "mongodb→mongodb",
        "sync_mode": sync_mode,
        "source_table": mongo_src,
        "dest_table": dest,
        "mongo_port": 27017,
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "mongo_read": summary.get("mongo_read"),
        "mongo_write": summary.get("mongo_write"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(COLUMNS),
        "dest_count_source": "mongodb_count_documents",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} mongodb→mongodb [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination count_documents: {landed} (source count_documents: {rows})")
    print(f"mongo_write: {summary.get('mongo_write')}")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "mongo_snapshot_find_insert_many_mongo"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected Mongo snapshot find + insert_many, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def run_iceberg_mongo_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity Iceberg→MongoDB through stream_database_transfer.

    Source COUNT is Iceberg file footers, never ``scan().count()``. Dest
    proof is ``count_documents({})``, never ``estimatedDocumentCount``.
    Payload is snapshot Parquet + insert_many, never ``scan().to_arrow()``
    / mongoimport. Empty dest is insert, not upsert.
    """
    import socket
    from urllib.request import urlopen

    try:
        socket.create_connection(("127.0.0.1", 27017), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"MongoDB 27017 not reachable: {exc}") from exc
    rest = _iceberg_rest_uri()
    try:
        with urlopen(f"{rest}/v1/config", timeout=2) as resp:
            if int(getattr(resp, "status", 0) or 0) != 200:
                raise RuntimeError(f"Iceberg REST {rest}/v1/config not 200")
    except Exception as exc:
        raise RuntimeError(f"Iceberg REST not reachable at {rest}: {exc}") from exc

    ice_src = str(source_table or "bench_mysql_iceberg")
    dest = str(dest_table)
    job_store = ensure_memory_job_store_if_mongo_down()
    try:
        have = _iceberg_count(ice_src)
    except Exception:
        have = 0
    if have != rows:
        run_mysql_iceberg_volume(
            rows=rows,
            dest_table=ice_src,
            source_table="bench_1m",
            sync_mode="full_refresh_append",
            keep_dest=False,
            fail_closed=True,
            proof_path=None,
        )
    if not keep_dest:
        _mongo_drop(dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    source = EndpointConfig.from_dict("database", _iceberg_rest_cfg(ice_src))
    destination = EndpointConfig.from_dict("database", _mongo_cfg(dest))
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-iceberg-mongo-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = _mongo_count(dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "iceberg→mongodb",
        "sync_mode": sync_mode,
        "source_table": ice_src,
        "dest_table": dest,
        "mongo_port": 27017,
        "iceberg_rest": rest,
        "iceberg_warehouse": _iceberg_rest_warehouse(),
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "iceberg_read": summary.get("iceberg_read"),
        "mongo_write": summary.get("mongo_write"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(COLUMNS),
        "source_count_source": "iceberg_file_footers",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} iceberg→mongodb [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination count_documents: {landed} (source footer COUNT: {rows})")
    print(f"iceberg_read: {summary.get('iceberg_read')}")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "iceberg_parquet_insert_many_mongo"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected Iceberg Parquet + insert_many, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def run_mongo_iceberg_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity MongoDB→Iceberg through stream_database_transfer.

    Source COUNT is ``count_documents`` in a replica-set snapshot. Dest
    proof is Iceberg file footers, never ``scan().count()``. Payload is
    snapshot find + CSV + catalog snapshot, never mongoexport /
    ``MERGE INTO``. Empty dest is CoW append.
    """
    import socket
    from urllib.request import urlopen

    try:
        socket.create_connection(("127.0.0.1", 27017), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"MongoDB 27017 not reachable: {exc}") from exc
    rest = _iceberg_rest_uri()
    try:
        with urlopen(f"{rest}/v1/config", timeout=2) as resp:
            if int(getattr(resp, "status", 0) or 0) != 200:
                raise RuntimeError(f"Iceberg REST {rest}/v1/config not 200")
    except Exception as exc:
        raise RuntimeError(f"Iceberg REST not reachable at {rest}: {exc}") from exc

    mongo_src = str(source_table or "bench_ice_mongo")
    dest = str(dest_table)
    job_store = ensure_memory_job_store_if_mongo_down()
    try:
        have = _mongo_count(mongo_src)
    except Exception:
        have = 0
    if have != rows:
        run_iceberg_mongo_volume(
            rows=rows,
            dest_table=mongo_src,
            source_table="bench_mysql_iceberg",
            sync_mode="full_refresh_append",
            keep_dest=False,
            fail_closed=True,
            proof_path=None,
        )
    if not keep_dest:
        _iceberg_drop(dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    source = EndpointConfig.from_dict("database", _mongo_cfg(mongo_src))
    destination = EndpointConfig.from_dict("database", _iceberg_rest_cfg(dest))
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-mongo-iceberg-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = _iceberg_count(dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "mongodb→iceberg",
        "sync_mode": sync_mode,
        "source_table": mongo_src,
        "dest_table": dest,
        "mongo_port": 27017,
        "iceberg_rest": rest,
        "iceberg_warehouse": _iceberg_rest_warehouse(),
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "mongo_read": summary.get("mongo_read"),
        "iceberg_write": summary.get("iceberg_write"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(COLUMNS),
        "source_count_source": "mongodb_count_documents",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} mongodb→iceberg [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination footer COUNT: {landed} (source count_documents: {rows})")
    print(f"mongo_read: {summary.get('mongo_read')}")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "mongo_snapshot_find_csv_iceberg_snapshot"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected Mongo snapshot find + Iceberg snapshot, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def run_iceberg_iceberg_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity Iceberg→Iceberg through stream_database_transfer.

    Source and dest COUNT are Iceberg file footers, never ``scan().count()``.
    Payload is snapshot Parquet + CoW append/overwrite, never
    ``scan().to_arrow()`` / ``MERGE INTO``. Same table declines. Empty dest
    is CoW snapshot append.
    """
    from urllib.request import urlopen

    rest = _iceberg_rest_uri()
    try:
        with urlopen(f"{rest}/v1/config", timeout=2) as resp:
            if int(getattr(resp, "status", 0) or 0) != 200:
                raise RuntimeError(f"Iceberg REST {rest}/v1/config not 200")
    except Exception as exc:
        raise RuntimeError(f"Iceberg REST not reachable at {rest}: {exc}") from exc

    ice_src = str(source_table or "bench_mysql_iceberg")
    dest = str(dest_table)
    if ice_src.strip().lower() == dest.strip().lower():
        raise RuntimeError("Iceberg→Iceberg bench refuses the same table")
    job_store = ensure_memory_job_store_if_mongo_down()
    try:
        have = _iceberg_count(ice_src)
    except Exception:
        have = 0
    if have != rows:
        run_mysql_iceberg_volume(
            rows=rows,
            dest_table=ice_src,
            source_table="bench_1m",
            sync_mode="full_refresh_append",
            keep_dest=False,
            fail_closed=True,
            proof_path=None,
        )
    if not keep_dest:
        _iceberg_drop(dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    source = EndpointConfig.from_dict("database", _iceberg_rest_cfg(ice_src))
    destination = EndpointConfig.from_dict("database", _iceberg_rest_cfg(dest))
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-iceberg-iceberg-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = _iceberg_count(dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "iceberg→iceberg",
        "sync_mode": sync_mode,
        "source_table": ice_src,
        "dest_table": dest,
        "iceberg_rest": rest,
        "iceberg_warehouse": _iceberg_rest_warehouse(),
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "iceberg_read": summary.get("iceberg_read"),
        "iceberg_write": summary.get("iceberg_write"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(COLUMNS),
        "source_count_source": "iceberg_file_footers",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} iceberg→iceberg [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination footer COUNT: {landed} (source footer COUNT: {rows})")
    print(f"iceberg_read: {summary.get('iceberg_read')}")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "iceberg_snapshot_parquet_cow_iceberg"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected Iceberg snapshot Parquet + CoW snapshot, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def _sqlite_bench_dir() -> Path:
    root = Path(os.environ.get("DATAFLOW_SQLITE_BENCH_DIR", "/tmp/dataflow-sqlite-bench"))
    root.mkdir(parents=True, exist_ok=True)
    return root


def _sqlite_cfg(database: str | Path, table: str) -> dict[str, Any]:
    return {
        "type": "sqlite",
        "format": "sqlite",
        "database": str(database),
        "table": table,
    }


def _sqlite_physical_columns() -> list[tuple[str, str]]:
    """SQLite COPY mappings: DATE rematerializes to TEXT (no DATE affinity)."""
    out: list[tuple[str, str]] = []
    for name, ddl in COLUMNS:
        base = ddl.split("(", 1)[0].strip().upper()
        if base == "DATE":
            out.append((name, "TEXT"))
        else:
            out.append((name, ddl))
    return out


def _sqlite_count(database: str | Path, table: str) -> int:
    from services.dest_precount import destination_row_count

    n = destination_row_count(
        "sqlite",
        _sqlite_cfg(database, table),
        schema="",
        table_name=table,
    )
    if n is None:
        raise RuntimeError(f"SQLite dest COUNT unmeasured for {table}")
    return int(n)


def _sqlite_drop_table(database: str | Path, table: str) -> None:
    import sqlite3

    path = Path(database)
    if not path.exists():
        return
    conn = sqlite3.connect(path)
    try:
        conn.execute(f'DROP TABLE IF EXISTS "{table}"')
        conn.commit()
    finally:
        conn.close()


def run_pg_sqlite_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    dest_database: str | Path | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity PostgreSQL→SQLite through stream_database_transfer.

    Dest COUNT is ``SELECT COUNT(*)``. DATE lands as SQLite TEXT (no DATE
    affinity — engine law, not a row drop). Empty dest is executemany
    insert, not upsert / ``.import``. TIMESTAMP / BYTEA / JSONB decline.
    """
    import socket

    try:
        socket.create_connection(("127.0.0.1", 5432), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"PostgreSQL 5432 not reachable: {exc}") from exc

    pg = _pg_local_cfg()
    src_table = source_table or f"bench_emp_{rows}"
    dest = str(dest_table)
    dest_db = Path(dest_database or (_sqlite_bench_dir() / "src.db"))
    dest_db.parent.mkdir(parents=True, exist_ok=True)
    job_store = ensure_memory_job_store_if_mongo_down()
    seed_source(pg, src_table, rows)
    if not keep_dest:
        _sqlite_drop_table(dest_db, dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    source = EndpointConfig.from_dict(
        "database",
        {
            "format": "postgresql",
            "host": pg["host"],
            "port": pg["port"],
            "database": pg["dbname"],
            "username": pg["user"],
            "password": pg["password"],
            "schema": "public",
            "table": src_table,
        },
    )
    destination = EndpointConfig.from_dict("database", _sqlite_cfg(dest_db, dest))
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-pg-sqlite-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = _sqlite_count(dest_db, dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "postgresql→sqlite",
        "sync_mode": sync_mode,
        "source_table": src_table,
        "dest_table": dest,
        "dest_database": str(dest_db),
        "pg_port": 5432,
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "sqlite_write": summary.get("sqlite_write"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(COLUMNS),
        "dest_count_source": "sqlite_count_star",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} postgresql→sqlite [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination COUNT(*): {landed} (source snapshot COUNT: {rows})")
    print("DATE lands as TEXT (SQLite has no DATE affinity).")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "copy_text_pg_executemany_sqlite"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected PostgreSQL COPY text + SQLite executemany, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def run_sqlite_sqlite_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    source_database: str | Path | None = None,
    dest_database: str | Path | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity SQLite→SQLite through stream_database_transfer.

    Dest COUNT is ``SELECT COUNT(*)``. Empty dest is ``ATTACH`` +
    ``INSERT SELECT``, not upsert / ``.dump`` / ``.import``. Same file +
    same table declines. ``:memory:`` declines. Seeds from PostgreSQL→SQLite
    when the source file/table is missing.
    """
    src_table = str(source_table or "bench_pg_sqlite")
    dest = str(dest_table)
    src_db = Path(source_database or (_sqlite_bench_dir() / "src.db"))
    dest_db = Path(dest_database or (_sqlite_bench_dir() / "dest.db"))
    if src_db.resolve() == dest_db.resolve() and src_table.strip().lower() == dest.strip().lower():
        raise AssertionError("SQLite→SQLite bench refuses the same file+table")
    job_store = ensure_memory_job_store_if_mongo_down()
    try:
        have = _sqlite_count(src_db, src_table) if src_db.exists() else 0
    except Exception:
        have = 0
    if have != rows:
        run_pg_sqlite_volume(
            rows=rows,
            dest_table=src_table,
            dest_database=src_db,
            source_table=f"bench_emp_{rows}",
            sync_mode="full_refresh_append",
            keep_dest=False,
            fail_closed=True,
            proof_path=None,
        )
    if not keep_dest:
        _sqlite_drop_table(dest_db, dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    source = EndpointConfig.from_dict("database", _sqlite_cfg(src_db, src_table))
    destination = EndpointConfig.from_dict("database", _sqlite_cfg(dest_db, dest))
    cols = _sqlite_physical_columns()
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in cols
    ]
    schema = dict(cols)
    job_id = f"bench-sqlite-sqlite-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = _sqlite_count(dest_db, dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "sqlite→sqlite",
        "sync_mode": sync_mode,
        "source_table": src_table,
        "dest_table": dest,
        "source_database": str(src_db),
        "dest_database": str(dest_db),
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "sqlite_read": summary.get("sqlite_read"),
        "sqlite_write": summary.get("sqlite_write"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(cols),
        "dest_count_source": "sqlite_count_star",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} sqlite→sqlite [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination COUNT(*): {landed} (source COUNT: {rows})")
    print("Not .dump / .import. Empty dest is INSERT SELECT.")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "attach_insert_select_sqlite"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected SQLite ATTACH + INSERT SELECT, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def run_sqlite_pg_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    source_database: str | Path | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity SQLite→PostgreSQL through stream_database_transfer.

    Dest COUNT is ``SELECT COUNT(*)``. Empty dest is ``COPY FROM STDIN``,
    not upsert / ``.dump``. DATE/BOOLEAN/BLOB on the SQLite source decline
    (affinity would invent a PostgreSQL type). Seeds from PostgreSQL→SQLite
    when the source file/table is missing — hire_date is TEXT after that hop.
    """
    import socket

    try:
        socket.create_connection(("127.0.0.1", 5432), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"PostgreSQL 5432 not reachable: {exc}") from exc

    pg = _pg_local_cfg()
    src_table = str(source_table or "bench_pg_sqlite")
    dest = str(dest_table)
    src_db = Path(source_database or (_sqlite_bench_dir() / "src.db"))
    job_store = ensure_memory_job_store_if_mongo_down()
    try:
        have = _sqlite_count(src_db, src_table) if src_db.exists() else 0
    except Exception:
        have = 0
    if have != rows:
        run_pg_sqlite_volume(
            rows=rows,
            dest_table=src_table,
            dest_database=src_db,
            source_table=f"bench_emp_{rows}",
            sync_mode="full_refresh_append",
            keep_dest=False,
            fail_closed=True,
            proof_path=None,
        )
    if not keep_dest:
        reset_pg_destination(pg, dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    source = EndpointConfig.from_dict("database", _sqlite_cfg(src_db, src_table))
    destination = EndpointConfig.from_dict(
        "database",
        {
            "format": "postgresql",
            "host": pg["host"],
            "port": pg["port"],
            "database": pg["dbname"],
            "username": pg["user"],
            "password": pg["password"],
            "schema": "public",
            "table": dest,
        },
    )
    cols = _sqlite_physical_columns()
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in cols
    ]
    schema = dict(cols)
    job_id = f"bench-sqlite-pg-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = postgres_count(pg, dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "sqlite→postgresql",
        "sync_mode": sync_mode,
        "source_table": src_table,
        "dest_table": dest,
        "source_database": str(src_db),
        "pg_port": 5432,
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "sqlite_read": summary.get("sqlite_read"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(cols),
        "dest_count_source": "postgres_count_star",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} sqlite→postgresql [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination COUNT(*): {landed} (source COUNT: {rows})")
    print("Not .dump. Empty dest is COPY FROM STDIN. DATE source affinity declines.")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "select_sqlite_copy_from_stdin_pg"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected SQLite SELECT + PostgreSQL COPY FROM STDIN, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def _s3_bench_bucket() -> str:
    return os.environ.get("BENCH_S3_BUCKET", "dataflow-bench")


def _s3_local_cfg(bucket: str, key: str) -> dict[str, Any]:
    return {
        "type": "s3",
        "format": "s3",
        "host": os.environ.get("MINIO_HOST", "127.0.0.1"),
        "port": int(os.environ.get("MINIO_PORT", "9000")),
        "database": bucket,
        "table": key,
        "username": os.environ.get("MINIO_USER", "dataflow"),
        "password": os.environ.get("MINIO_SECRET", "dataflowsecret"),
        "ssl": False,
        "path_style": True,
    }


def _s3_count(cfg: dict[str, Any], key: str) -> int:
    from services.dest_precount import destination_row_count

    n = destination_row_count("s3", cfg, schema="", table_name=key)
    if n is None:
        raise ValueError(f"S3 dest COUNT unmeasured for {key}")
    return int(n)


def _s3_delete_key(cfg: dict[str, Any], key: str) -> None:
    from services.copy_s3_common import s3_delete_keys, s3_list_keys

    s3_delete_keys(cfg, s3_list_keys(cfg, key))


def _require_minio() -> None:
    import socket

    host = os.environ.get("MINIO_HOST", "127.0.0.1")
    port = int(os.environ.get("MINIO_PORT", "9000"))
    try:
        socket.create_connection((host, port), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"MinIO {host}:{port} not reachable: {exc}") from exc


def run_pg_s3_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    dest_bucket: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity PostgreSQL→S3 through stream_database_transfer.

    Dest COUNT is object-store artifact COUNT of the CSV (header skipped),
    never ListObjects length, never writer PUT ack. Empty dest is COPY CSV
    + upload_file, not ``aws s3 cp``. Dest key must be ``.csv`` / ``.tsv``.
    Unique dest ``bench_pg_s3.csv`` is not reused from ``bench_1m``.
    """
    import socket

    try:
        socket.create_connection(("127.0.0.1", 5432), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"PostgreSQL 5432 not reachable: {exc}") from exc
    _require_minio()

    pg = _pg_local_cfg()
    src_table = source_table or f"bench_emp_{rows}"
    dest = str(dest_table)
    bucket = dest_bucket or _s3_bench_bucket()
    dest_cfg = _s3_local_cfg(bucket, dest)
    job_store = ensure_memory_job_store_if_mongo_down()
    seed_source(pg, src_table, rows)
    if not keep_dest:
        from services.copy_s3_common import s3_ensure_bucket

        s3_ensure_bucket(dest_cfg)
        _s3_delete_key(dest_cfg, dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    source = EndpointConfig.from_dict(
        "database",
        {
            "format": "postgresql",
            "host": pg["host"],
            "port": pg["port"],
            "database": pg["dbname"],
            "username": pg["user"],
            "password": pg["password"],
            "schema": "public",
            "table": src_table,
        },
    )
    destination = EndpointConfig.from_dict("database", dest_cfg)
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-pg-s3-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = _s3_count(dest_cfg, dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "postgresql→s3",
        "sync_mode": sync_mode,
        "source_table": src_table,
        "dest_table": dest,
        "dest_bucket": bucket,
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "s3_write": summary.get("s3_write"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(COLUMNS),
        "dest_count_source": "s3_artifact_count",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {bucket}/{dest} postgresql→s3 [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination artifact COUNT: {landed} (source snapshot COUNT: {rows})")
    print("Not aws s3 cp. Empty dest is COPY CSV + upload. CSV HEADER is not a dest row.")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "copy_csv_pg_upload_s3"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected PostgreSQL COPY CSV + S3 upload, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def run_s3_s3_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    dest_bucket: str | None = None,
    source_bucket: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity S3→S3 through stream_database_transfer.

    Dest COUNT is object-store artifact COUNT, never ListObjects length.
    Empty dest is server-side CopyObject, not GET+PUT / ``aws s3 cp`` /
    ``aws s3 sync``. Seeds from PostgreSQL→S3 CSV when the source object
    is missing. Unique dest ``bench_s3_clone.csv`` is not reused from
    ``bench_pg_s3.csv`` / ``bench_1m``.
    """
    _require_minio()
    src_key = str(source_table or "bench_pg_s3.csv")
    dest = str(dest_table)
    bucket = dest_bucket or _s3_bench_bucket()
    src_bucket = source_bucket or bucket
    src_cfg = _s3_local_cfg(src_bucket, src_key)
    dest_cfg = _s3_local_cfg(bucket, dest)
    if src_cfg["host"] == dest_cfg["host"] and src_bucket == bucket and src_key == dest:
        raise AssertionError("S3→S3 bench refuses the same endpoint+bucket+key")
    job_store = ensure_memory_job_store_if_mongo_down()
    try:
        have = _s3_count(src_cfg, src_key)
    except Exception:
        have = 0
    if have != rows:
        run_pg_s3_volume(
            rows=rows,
            dest_table=src_key,
            dest_bucket=src_bucket,
            source_table=f"bench_emp_{rows}",
            sync_mode="full_refresh_append",
            keep_dest=False,
            fail_closed=True,
            proof_path=None,
        )
    if not keep_dest:
        from services.copy_s3_common import s3_ensure_bucket

        s3_ensure_bucket(dest_cfg)
        _s3_delete_key(dest_cfg, dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    source = EndpointConfig.from_dict("database", src_cfg)
    destination = EndpointConfig.from_dict("database", dest_cfg)
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-s3-s3-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = _s3_count(dest_cfg, dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "s3→s3",
        "sync_mode": sync_mode,
        "source_table": src_key,
        "dest_table": dest,
        "source_bucket": src_bucket,
        "dest_bucket": bucket,
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "s3_read": summary.get("s3_read"),
        "s3_write": summary.get("s3_write"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(COLUMNS),
        "dest_count_source": "s3_artifact_count",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {bucket}/{dest} s3→s3 [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination artifact COUNT: {landed} (source COUNT: {rows})")
    print("Not aws s3 cp / aws s3 sync / GET+PUT. Empty dest is CopyObject.")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "copy_object_s3_s3"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected S3 CopyObject, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def run_s3_pg_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    source_bucket: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity S3→PostgreSQL through stream_database_transfer.

    Dest COUNT is ``SELECT COUNT(*)``. Empty dest is ``COPY FROM STDIN``
    CSV HEADER, not upsert / ``aws s3 cp``. JSON/JSONL/Parquet decline.
    Seeds from PostgreSQL→S3 CSV when the source object is missing.
    Unique dest ``bench_s3_from_pg`` is not reused from ``bench_1m``.
    """
    import socket

    try:
        socket.create_connection(("127.0.0.1", 5432), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"PostgreSQL 5432 not reachable: {exc}") from exc
    _require_minio()

    pg = _pg_local_cfg()
    src_key = str(source_table or "bench_pg_s3.csv")
    dest = str(dest_table)
    bucket = source_bucket or _s3_bench_bucket()
    src_cfg = _s3_local_cfg(bucket, src_key)
    job_store = ensure_memory_job_store_if_mongo_down()
    try:
        have = _s3_count(src_cfg, src_key)
    except Exception:
        have = 0
    if have != rows:
        run_pg_s3_volume(
            rows=rows,
            dest_table=src_key,
            dest_bucket=bucket,
            source_table=f"bench_emp_{rows}",
            sync_mode="full_refresh_append",
            keep_dest=False,
            fail_closed=True,
            proof_path=None,
        )
    if not keep_dest:
        reset_pg_destination(pg, dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    source = EndpointConfig.from_dict("database", src_cfg)
    destination = EndpointConfig.from_dict(
        "database",
        {
            "format": "postgresql",
            "host": pg["host"],
            "port": pg["port"],
            "database": pg["dbname"],
            "username": pg["user"],
            "password": pg["password"],
            "schema": "public",
            "table": dest,
        },
    )
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-s3-pg-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = postgres_count(pg, dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "s3→postgresql",
        "sync_mode": sync_mode,
        "source_table": src_key,
        "dest_table": dest,
        "source_bucket": bucket,
        "pg_port": 5432,
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "s3_read": summary.get("s3_read"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(COLUMNS),
        "dest_count_source": "postgres_count_star",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} s3→postgresql [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination COUNT(*): {landed} (source artifact COUNT: {rows})")
    print("Not aws s3 cp. Empty dest is COPY FROM STDIN. JSON/JSONL decline.")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "get_csv_s3_copy_from_stdin_pg"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected S3 GET CSV + PostgreSQL COPY FROM STDIN, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def _require_mysql() -> None:
    import socket

    try:
        socket.create_connection(("127.0.0.1", 3306), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"MySQL 3306 not reachable: {exc}") from exc


def run_mysql_s3_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    dest_bucket: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity MySQL→S3 through stream_database_transfer.

    Dest COUNT is object-store artifact COUNT of the CSV (header skipped),
    never ListObjects length, never writer PUT ack. Empty dest is SELECT
    CSV + upload_file, not ``aws s3 cp``. Dest key must be ``.csv`` /
    ``.tsv``. Unique dest ``bench_mysql_s3.csv`` is not reused from
    ``bench_1m`` / ``bench_pg_s3.csv``.
    """
    _require_mysql()
    _require_minio()

    src_table = source_table or "bench_1m"
    dest = str(dest_table)
    bucket = dest_bucket or _s3_bench_bucket()
    dest_cfg = _s3_local_cfg(bucket, dest)
    mysql = _mysql_local_cfg()
    job_store = ensure_memory_job_store_if_mongo_down()
    _ensure_mysql_employee_fixture(rows, src_table)
    if not keep_dest:
        from services.copy_s3_common import s3_ensure_bucket

        s3_ensure_bucket(dest_cfg)
        _s3_delete_key(dest_cfg, dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    source = EndpointConfig.from_dict(
        "database",
        {
            "format": "mysql",
            "host": mysql["host"],
            "port": mysql["port"],
            "database": mysql["database"],
            "username": mysql["username"],
            "password": mysql["password"],
            "table": src_table,
        },
    )
    destination = EndpointConfig.from_dict("database", dest_cfg)
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-mysql-s3-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = _s3_count(dest_cfg, dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "mysql→s3",
        "sync_mode": sync_mode,
        "source_table": src_table,
        "dest_table": dest,
        "dest_bucket": bucket,
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "s3_write": summary.get("s3_write"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(COLUMNS),
        "dest_count_source": "s3_artifact_count",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {bucket}/{dest} mysql→s3 [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination artifact COUNT: {landed} (source snapshot COUNT: {rows})")
    print("Not aws s3 cp. Empty dest is SELECT CSV + upload. CSV HEADER is not a dest row.")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "select_mysql_upload_s3"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected MySQL SELECT CSV + S3 upload, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def run_s3_mysql_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    source_bucket: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity S3→MySQL through stream_database_transfer.

    Dest COUNT is ``SELECT COUNT(*)``. Empty dest is STRICT
    ``LOAD DATA LOCAL INFILE``, not upsert / ``aws s3 cp``.
    JSON/JSONL/Parquet decline. Seeds from MySQL→S3 CSV when the source
    object is missing. Unique dest ``bench_s3_from_mysql`` is not reused
    from ``bench_1m``.
    """
    _require_mysql()
    _require_minio()

    mysql = _mysql_local_cfg()
    src_key = str(source_table or "bench_mysql_s3.csv")
    dest = str(dest_table)
    bucket = source_bucket or _s3_bench_bucket()
    src_cfg = _s3_local_cfg(bucket, src_key)
    job_store = ensure_memory_job_store_if_mongo_down()
    try:
        have = _s3_count(src_cfg, src_key)
    except Exception:
        have = 0
    if have != rows:
        run_mysql_s3_volume(
            rows=rows,
            dest_table=src_key,
            dest_bucket=bucket,
            source_table="bench_1m",
            sync_mode="full_refresh_append",
            keep_dest=False,
            fail_closed=True,
            proof_path=None,
        )
    if not keep_dest:
        reset_destination(mysql, dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    source = EndpointConfig.from_dict("database", src_cfg)
    destination = EndpointConfig.from_dict(
        "database",
        {
            "format": "mysql",
            "host": mysql["host"],
            "port": mysql["port"],
            "database": mysql["database"],
            "username": mysql["username"],
            "password": mysql["password"],
            "table": dest,
        },
    )
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-s3-mysql-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = destination_count(mysql, dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "s3→mysql",
        "sync_mode": sync_mode,
        "source_table": src_key,
        "dest_table": dest,
        "source_bucket": bucket,
        "mysql_port": 3306,
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "s3_read": summary.get("s3_read"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(COLUMNS),
        "dest_count_source": "mysql_count_star",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} s3→mysql [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination COUNT(*): {landed} (source artifact COUNT: {rows})")
    print("Not aws s3 cp. Empty dest is STRICT LOAD DATA. JSON/JSONL decline.")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "get_csv_s3_load_data_mysql"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected S3 GET CSV + MySQL STRICT LOAD DATA, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def run_sqlite_s3_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    source_database: str | Path | None = None,
    dest_bucket: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity SQLite→S3 through stream_database_transfer.

    Dest COUNT is object-store artifact COUNT of the CSV (header skipped),
    never ListObjects length, never writer PUT ack. Empty dest is SELECT
    CSV + upload_file, not ``.dump`` / ``aws s3 cp``. Dest key must be
    ``.csv`` / ``.tsv``. DATE affinity is allowed (stored TEXT). Unique
    dest ``bench_sqlite_s3.csv`` is not reused from ``bench_mysql_s3.csv``
    / ``bench_pg_s3.csv``. Seeds from PostgreSQL→SQLite when the source
    file/table is missing.
    """
    _require_minio()

    src_table = str(source_table or "bench_pg_sqlite")
    dest = str(dest_table)
    src_db = Path(source_database or (_sqlite_bench_dir() / "src.db"))
    bucket = dest_bucket or _s3_bench_bucket()
    dest_cfg = _s3_local_cfg(bucket, dest)
    job_store = ensure_memory_job_store_if_mongo_down()
    try:
        have = _sqlite_count(src_db, src_table) if src_db.exists() else 0
    except Exception:
        have = 0
    if have != rows:
        run_pg_sqlite_volume(
            rows=rows,
            dest_table=src_table,
            dest_database=src_db,
            source_table=f"bench_emp_{rows}",
            sync_mode="full_refresh_append",
            keep_dest=False,
            fail_closed=True,
            proof_path=None,
        )
    if not keep_dest:
        from services.copy_s3_common import s3_ensure_bucket

        s3_ensure_bucket(dest_cfg)
        _s3_delete_key(dest_cfg, dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    source = EndpointConfig.from_dict("database", _sqlite_cfg(src_db, src_table))
    destination = EndpointConfig.from_dict("database", dest_cfg)
    cols = _sqlite_physical_columns()
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in cols
    ]
    schema = dict(cols)
    job_id = f"bench-sqlite-s3-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = _s3_count(dest_cfg, dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "sqlite→s3",
        "sync_mode": sync_mode,
        "source_table": src_table,
        "source_database": str(src_db),
        "dest_table": dest,
        "dest_bucket": bucket,
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "sqlite_read": summary.get("sqlite_read"),
        "s3_write": summary.get("s3_write"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(cols),
        "dest_count_source": "s3_artifact_count",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {bucket}/{dest} sqlite→s3 [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination artifact COUNT: {landed} (source snapshot COUNT: {rows})")
    print("Not aws s3 cp / .dump. Empty dest is SELECT CSV + upload. CSV HEADER is not a dest row.")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "select_sqlite_upload_s3"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected SQLite SELECT CSV + S3 upload, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def run_s3_sqlite_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    source_bucket: str | None = None,
    dest_database: str | Path | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity S3→SQLite through stream_database_transfer.

    Dest COUNT is ``SELECT COUNT(*)``. Empty dest is ``executemany``
    insert, not upsert / sqlite3 ``.import`` / ``aws s3 cp``.
    JSON/JSONL/Parquet decline. Seeds from SQLite→S3 CSV when the source
    object is missing. Unique dest ``bench_s3_from_sqlite`` is not reused
    from ``bench_1m`` / ``bench_pg_sqlite``.
    """
    _require_minio()

    src_key = str(source_table or "bench_sqlite_s3.csv")
    dest = str(dest_table)
    dest_db = Path(dest_database or (_sqlite_bench_dir() / "from_s3.db"))
    bucket = source_bucket or _s3_bench_bucket()
    src_cfg = _s3_local_cfg(bucket, src_key)
    job_store = ensure_memory_job_store_if_mongo_down()
    try:
        have = _s3_count(src_cfg, src_key)
    except Exception:
        have = 0
    if have != rows:
        run_sqlite_s3_volume(
            rows=rows,
            dest_table=src_key,
            dest_bucket=bucket,
            source_table="bench_pg_sqlite",
            sync_mode="full_refresh_append",
            keep_dest=False,
            fail_closed=True,
            proof_path=None,
        )
    if not keep_dest:
        _sqlite_drop_table(dest_db, dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    source = EndpointConfig.from_dict("database", src_cfg)
    destination = EndpointConfig.from_dict("database", _sqlite_cfg(dest_db, dest))
    cols = _sqlite_physical_columns()
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in cols
    ]
    schema = dict(cols)
    job_id = f"bench-s3-sqlite-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = _sqlite_count(dest_db, dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "s3→sqlite",
        "sync_mode": sync_mode,
        "source_table": src_key,
        "source_bucket": bucket,
        "dest_table": dest,
        "dest_database": str(dest_db),
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "s3_read": summary.get("s3_read"),
        "sqlite_write": summary.get("sqlite_write"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(cols),
        "dest_count_source": "sqlite_count_star",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} s3→sqlite [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination COUNT(*): {landed} (source artifact COUNT: {rows})")
    print("Not aws s3 cp / .import. Empty dest is executemany insert. JSON/JSONL decline.")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "get_csv_s3_executemany_sqlite"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected S3 GET CSV + SQLite executemany, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def run_mongo_s3_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    dest_bucket: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity MongoDB→S3 through stream_database_transfer.

    Source COUNT is ``count_documents({})``, never
    ``estimatedDocumentCount``. Dest COUNT is object-store artifact COUNT
    of the CSV (header skipped), never ListObjects length, never writer
    PUT ack. Empty dest is snapshot find CSV + upload_file, not
    ``mongoexport`` / ``aws s3 cp``. Dest key must be ``.csv`` / ``.tsv``.
    Unique dest ``bench_mongo_s3.csv`` is not reused from
    ``bench_mysql_s3.csv`` / ``bench_sqlite_s3.csv``. Seeds from
    MySQL→Mongo when the source collection is missing.
    """
    import socket

    try:
        socket.create_connection(("127.0.0.1", 27017), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"MongoDB 27017 not reachable: {exc}") from exc
    _require_minio()

    mongo_src = str(source_table or "bench_mysql_mongo")
    dest = str(dest_table)
    bucket = dest_bucket or _s3_bench_bucket()
    dest_cfg = _s3_local_cfg(bucket, dest)
    job_store = ensure_memory_job_store_if_mongo_down()
    try:
        have = _mongo_count(mongo_src)
    except Exception:
        have = 0
    if have != rows:
        run_mysql_mongo_volume(
            rows=rows,
            dest_table=mongo_src,
            source_table="bench_1m",
            sync_mode="full_refresh_append",
            keep_dest=False,
            fail_closed=True,
            proof_path=None,
        )
    if not keep_dest:
        from services.copy_s3_common import s3_ensure_bucket

        s3_ensure_bucket(dest_cfg)
        _s3_delete_key(dest_cfg, dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    source = EndpointConfig.from_dict("database", _mongo_cfg(mongo_src))
    destination = EndpointConfig.from_dict("database", dest_cfg)
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-mongo-s3-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = _s3_count(dest_cfg, dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "mongodb→s3",
        "sync_mode": sync_mode,
        "source_table": mongo_src,
        "dest_table": dest,
        "dest_bucket": bucket,
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "mongo_read": summary.get("mongo_read"),
        "s3_write": summary.get("s3_write"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(COLUMNS),
        "dest_count_source": "s3_artifact_count",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {bucket}/{dest} mongodb→s3 [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination artifact COUNT: {landed} (source count_documents: {rows})")
    print(
        "Not mongoexport / aws s3 cp. Empty dest is snapshot find CSV + upload. "
        "CSV HEADER is not a dest row."
    )
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "snapshot_find_mongo_upload_s3"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected Mongo snapshot find CSV + S3 upload, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def run_s3_mongo_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    source_bucket: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity S3→MongoDB through stream_database_transfer.

    Dest COUNT is ``count_documents({})``, never
    ``estimatedDocumentCount``. Empty dest is ``insert_many``, not upsert
    / ``mongoimport`` / ``aws s3 cp``. JSON/JSONL/Parquet decline. Seeds
    from Mongo→S3 CSV when the source object is missing. Unique dest
    ``bench_s3_from_mongo`` is not reused from ``bench_1m`` /
    ``bench_pg_mongo``.
    """
    import socket

    try:
        socket.create_connection(("127.0.0.1", 27017), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"MongoDB 27017 not reachable: {exc}") from exc
    _require_minio()

    src_key = str(source_table or "bench_mongo_s3.csv")
    dest = str(dest_table)
    bucket = source_bucket or _s3_bench_bucket()
    src_cfg = _s3_local_cfg(bucket, src_key)
    job_store = ensure_memory_job_store_if_mongo_down()
    try:
        have = _s3_count(src_cfg, src_key)
    except Exception:
        have = 0
    if have != rows:
        run_mongo_s3_volume(
            rows=rows,
            dest_table=src_key,
            dest_bucket=bucket,
            source_table="bench_mysql_mongo",
            sync_mode="full_refresh_append",
            keep_dest=False,
            fail_closed=True,
            proof_path=None,
        )
    if not keep_dest:
        _mongo_drop(dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    source = EndpointConfig.from_dict("database", src_cfg)
    destination = EndpointConfig.from_dict("database", _mongo_cfg(dest))
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in COLUMNS
    ]
    schema = dict(COLUMNS)
    job_id = f"bench-s3-mongo-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = _mongo_count(dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "s3→mongodb",
        "sync_mode": sync_mode,
        "source_table": src_key,
        "source_bucket": bucket,
        "dest_table": dest,
        "mongo_port": 27017,
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "s3_read": summary.get("s3_read"),
        "mongo_write": summary.get("mongo_write"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(COLUMNS),
        "dest_count_source": "mongodb_count_documents",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} s3→mongodb [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination count_documents: {landed} (source artifact COUNT: {rows})")
    print("Not mongoimport / aws s3 cp. Empty dest is insert_many. JSON/JSONL decline.")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "get_csv_s3_insert_many_mongo"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected S3 GET CSV + Mongo insert_many, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def run_sqlite_mongo_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    source_database: str | Path | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity SQLite→MongoDB through stream_database_transfer.

    Dest COUNT is ``count_documents({})``, never
    ``estimatedDocumentCount``. Empty dest is SELECT ``fetchmany`` +
    ``insert_many``, not upsert / ``mongoimport`` / sqlite3 ``.dump``.
    DATE ISO TEXT stays a string (identity of SQLite TEXT storage after
    PostgreSQL→SQLite). DATETIME / TIMESTAMP / BLOB decline.
    ``:memory:`` declines. Unique dest ``bench_sqlite_mongo`` is not
    reused from ``bench_1m`` / ``bench_pg_mongo`` / ``bench_mysql_mongo``.
    Seeds from PostgreSQL→SQLite when the source file/table is missing.
    """
    import socket

    try:
        socket.create_connection(("127.0.0.1", 27017), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"MongoDB 27017 not reachable: {exc}") from exc

    src_table = str(source_table or "bench_pg_sqlite")
    dest = str(dest_table)
    src_db = Path(source_database or (_sqlite_bench_dir() / "src.db"))
    job_store = ensure_memory_job_store_if_mongo_down()
    try:
        have = _sqlite_count(src_db, src_table) if src_db.exists() else 0
    except Exception:
        have = 0
    if have != rows:
        run_pg_sqlite_volume(
            rows=rows,
            dest_table=src_table,
            dest_database=src_db,
            source_table=f"bench_emp_{rows}",
            sync_mode="full_refresh_append",
            keep_dest=False,
            fail_closed=True,
            proof_path=None,
        )
    if not keep_dest:
        _mongo_drop(dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    source = EndpointConfig.from_dict("database", _sqlite_cfg(src_db, src_table))
    destination = EndpointConfig.from_dict("database", _mongo_cfg(dest))
    cols = _sqlite_physical_columns()
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in cols
    ]
    schema = dict(cols)
    job_id = f"bench-sqlite-mongo-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = _mongo_count(dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "sqlite→mongodb",
        "sync_mode": sync_mode,
        "source_table": src_table,
        "source_database": str(src_db),
        "dest_table": dest,
        "mongo_port": 27017,
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "sqlite_read": summary.get("sqlite_read"),
        "mongo_write": summary.get("mongo_write"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(cols),
        "dest_count_source": "mongodb_count_documents",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} sqlite→mongodb [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination count_documents: {landed} (source COUNT(*): {rows})")
    print("Not mongoimport / .dump. Empty dest is insert_many. DATE ISO TEXT stays a string.")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "select_sqlite_insert_many_mongo"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected SQLite SELECT + Mongo insert_many, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def run_mongo_sqlite_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    dest_database: str | Path | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity MongoDB→SQLite through stream_database_transfer.

    Source COUNT is ``count_documents({})``, never
    ``estimatedDocumentCount``. Dest COUNT is SQLite ``COUNT(*)``.
    Empty dest is snapshot ``find()`` + ``executemany``, not upsert /
    ``mongoexport`` / sqlite3 ``.import``. DATE lands as SQLite TEXT
    (ISO calendar day — no DATE affinity). Nested BSON / binary decline.
    ``:memory:`` declines. Unique dest ``bench_sqlite_from_mongo`` is not
    reused from ``bench_1m`` / ``bench_pg_sqlite``. Seeds from
    SQLite→Mongo when the source collection is missing.
    """
    import socket

    try:
        socket.create_connection(("127.0.0.1", 27017), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"MongoDB 27017 not reachable: {exc}") from exc

    mongo_src = str(source_table or "bench_sqlite_mongo")
    dest = str(dest_table)
    dest_db = Path(dest_database or (_sqlite_bench_dir() / "from_mongo.db"))
    dest_db.parent.mkdir(parents=True, exist_ok=True)
    job_store = ensure_memory_job_store_if_mongo_down()
    try:
        have = _mongo_count(mongo_src)
    except Exception:
        have = 0
    if have != rows:
        run_sqlite_mongo_volume(
            rows=rows,
            dest_table=mongo_src,
            source_table="bench_pg_sqlite",
            sync_mode="full_refresh_append",
            keep_dest=False,
            fail_closed=True,
            proof_path=None,
        )
    if not keep_dest:
        _sqlite_drop_table(dest_db, dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    source = EndpointConfig.from_dict("database", _mongo_cfg(mongo_src))
    destination = EndpointConfig.from_dict("database", _sqlite_cfg(dest_db, dest))
    cols = _sqlite_physical_columns()
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in cols
    ]
    schema = dict(cols)
    job_id = f"bench-mongo-sqlite-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = _sqlite_count(dest_db, dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "mongodb→sqlite",
        "sync_mode": sync_mode,
        "source_table": mongo_src,
        "dest_table": dest,
        "dest_database": str(dest_db),
        "mongo_port": 27017,
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "mongo_read": summary.get("mongo_read"),
        "sqlite_write": summary.get("sqlite_write"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(cols),
        "dest_count_source": "sqlite_count_star",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} mongodb→sqlite [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination COUNT(*): {landed} (source count_documents: {rows})")
    print("Not mongoexport / .import. Empty dest is executemany. DATE lands as TEXT.")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "snapshot_find_mongo_executemany_sqlite"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected Mongo snapshot find + SQLite executemany, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def run_sqlite_mysql_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    source_database: str | Path | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity SQLite→MySQL through stream_database_transfer.

    Dest COUNT is ``SELECT COUNT(*)``. Empty dest is SELECT TSV + STRICT
    ``LOAD DATA LOCAL INFILE``, not upsert / sqlite3 ``.dump`` / sqlldr.
    DATE ISO TEXT stays a string after PostgreSQL→SQLite. DATETIME /
    TIMESTAMP / BLOB / JSON decline. ``:memory:`` declines. Unique dest
    ``bench_sqlite_mysql`` is not reused from ``bench_1m`` /
    ``bench_mysql_mongo``. Seeds from PostgreSQL→SQLite when the source
    file/table is missing.
    """
    _require_mysql()

    src_table = str(source_table or "bench_pg_sqlite")
    dest = str(dest_table)
    src_db = Path(source_database or (_sqlite_bench_dir() / "src.db"))
    mysql = _mysql_local_cfg()
    job_store = ensure_memory_job_store_if_mongo_down()
    try:
        have = _sqlite_count(src_db, src_table) if src_db.exists() else 0
    except Exception:
        have = 0
    if have != rows:
        run_pg_sqlite_volume(
            rows=rows,
            dest_table=src_table,
            dest_database=src_db,
            source_table=f"bench_emp_{rows}",
            sync_mode="full_refresh_append",
            keep_dest=False,
            fail_closed=True,
            proof_path=None,
        )
    if not keep_dest:
        reset_destination(mysql, dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    source = EndpointConfig.from_dict("database", _sqlite_cfg(src_db, src_table))
    destination = EndpointConfig.from_dict(
        "database",
        {
            "format": "mysql",
            "host": mysql["host"],
            "port": mysql["port"],
            "database": mysql["database"],
            "username": mysql["username"],
            "password": mysql["password"],
            "table": dest,
        },
    )
    cols = _sqlite_physical_columns()
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in cols
    ]
    schema = dict(cols)
    job_id = f"bench-sqlite-mysql-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = destination_count(mysql, dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "sqlite→mysql",
        "sync_mode": sync_mode,
        "source_table": src_table,
        "source_database": str(src_db),
        "dest_table": dest,
        "mysql_port": 3306,
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "sqlite_read": summary.get("sqlite_read"),
        "load_data": summary.get("load_data"),
        "mysql_write": summary.get("mysql_write"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(cols),
        "dest_count_source": "mysql_count_star",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} sqlite→mysql [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination COUNT(*): {landed} (source COUNT(*): {rows})")
    print("Not .dump / sqlldr. Empty dest is STRICT LOAD DATA. DATE ISO TEXT stays a string.")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "select_sqlite_load_data_mysql"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected SQLite SELECT TSV + MySQL LOAD DATA, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def run_mysql_sqlite_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    dest_database: str | Path | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity MySQL→SQLite through stream_database_transfer.

    Source COUNT is InnoDB consistent-snapshot ``COUNT(*)``. Dest COUNT
    is SQLite ``COUNT(*)``. Empty dest is snapshot SELECT +
    ``executemany``, not upsert / mysqldump / sqlite3 ``.import``. DATE
    lands as SQLite TEXT (ISO calendar day — no DATE affinity). TIMESTAMP
    / BLOB / JSON decline. ``:memory:`` declines. Unique dest
    ``bench_sqlite_from_mysql`` is not reused from ``bench_1m`` /
    ``bench_pg_sqlite``. Seeds from SQLite→MySQL when the source table is
    missing.
    """
    _require_mysql()

    mysql_src = str(source_table or "bench_sqlite_mysql")
    dest = str(dest_table)
    dest_db = Path(dest_database or (_sqlite_bench_dir() / "from_mysql.db"))
    dest_db.parent.mkdir(parents=True, exist_ok=True)
    mysql = _mysql_local_cfg()
    job_store = ensure_memory_job_store_if_mongo_down()
    try:
        have = destination_count(mysql, mysql_src)
    except Exception:
        have = 0
    if have != rows:
        run_sqlite_mysql_volume(
            rows=rows,
            dest_table=mysql_src,
            source_table="bench_pg_sqlite",
            sync_mode="full_refresh_append",
            keep_dest=False,
            fail_closed=True,
            proof_path=None,
        )
    if not keep_dest:
        _sqlite_drop_table(dest_db, dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    source = EndpointConfig.from_dict(
        "database",
        {
            "format": "mysql",
            "host": mysql["host"],
            "port": mysql["port"],
            "database": mysql["database"],
            "username": mysql["username"],
            "password": mysql["password"],
            "table": mysql_src,
        },
    )
    destination = EndpointConfig.from_dict("database", _sqlite_cfg(dest_db, dest))
    cols = _sqlite_physical_columns()
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in cols
    ]
    schema = dict(cols)
    job_id = f"bench-mysql-sqlite-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = _sqlite_count(dest_db, dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "mysql→sqlite",
        "sync_mode": sync_mode,
        "source_table": mysql_src,
        "dest_table": dest,
        "dest_database": str(dest_db),
        "mysql_port": 3306,
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "mysql_read": summary.get("mysql_read"),
        "sqlite_write": summary.get("sqlite_write"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(cols),
        "dest_count_source": "sqlite_count_star",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} mysql→sqlite [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination COUNT(*): {landed} (source snapshot COUNT: {rows})")
    print("Not mysqldump / .import. Empty dest is executemany. DATE lands as TEXT.")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "select_mysql_executemany_sqlite"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected MySQL snapshot SELECT + SQLite executemany, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def run_sqlite_iceberg_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    source_database: str | Path | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity SQLite→Iceberg through stream_database_transfer.

    Dest COUNT is file footers, never ``scan().count()``. Empty dest is
    SELECT CSV + CoW snapshot append, not upsert / sqlite3 ``.dump`` /
    ``MERGE INTO``. DATE ISO TEXT stays a string after PostgreSQL→SQLite.
    DATETIME / TIMESTAMP / BLOB / JSON decline. ``:memory:`` declines.
    Unique dest ``bench_sqlite_iceberg`` is not reused from ``bench_1m`` /
    ``bench_pg_iceberg`` / ``bench_mysql_ice``. Seeds from PostgreSQL→SQLite
    when the source file/table is missing. Iceberg times are **local**
    warehouse.
    """
    import socket
    from urllib.request import urlopen

    rest = _iceberg_rest_uri()
    try:
        with urlopen(f"{rest}/v1/config", timeout=2) as resp:
            if int(getattr(resp, "status", 0) or 0) != 200:
                raise RuntimeError(f"Iceberg REST {rest}/v1/config not 200")
    except Exception as exc:
        raise RuntimeError(f"Iceberg REST not reachable at {rest}: {exc}") from exc
    try:
        socket.create_connection(("127.0.0.1", 5432), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"PostgreSQL 5432 not reachable: {exc}") from exc

    src_table = str(source_table or "bench_pg_sqlite")
    dest = str(dest_table)
    src_db = Path(source_database or (_sqlite_bench_dir() / "src.db"))
    job_store = ensure_memory_job_store_if_mongo_down()
    try:
        have = _sqlite_count(src_db, src_table) if src_db.exists() else 0
    except Exception:
        have = 0
    if have != rows:
        run_pg_sqlite_volume(
            rows=rows,
            dest_table=src_table,
            dest_database=src_db,
            source_table=f"bench_emp_{rows}",
            sync_mode="full_refresh_append",
            keep_dest=False,
            fail_closed=True,
            proof_path=None,
        )
    if not keep_dest:
        _iceberg_drop(dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    source = EndpointConfig.from_dict("database", _sqlite_cfg(src_db, src_table))
    destination = EndpointConfig.from_dict("database", _iceberg_rest_cfg(dest))
    cols = _sqlite_physical_columns()
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in cols
    ]
    schema = dict(cols)
    job_id = f"bench-sqlite-iceberg-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = _iceberg_count(dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "sqlite→iceberg",
        "sync_mode": sync_mode,
        "source_table": src_table,
        "source_database": str(src_db),
        "dest_table": dest,
        "iceberg_rest": rest,
        "iceberg_warehouse": _iceberg_rest_warehouse(),
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "sqlite_read": summary.get("sqlite_read"),
        "iceberg_write": summary.get("iceberg_write"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(cols),
        "dest_count_source": "iceberg_file_footers",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} sqlite→iceberg [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination COUNT (file footers): {landed} (source COUNT(*): {rows})")
    print("Not .dump / MERGE INTO. Empty dest is CoW snapshot append. DATE ISO TEXT stays a string.")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "select_sqlite_csv_iceberg_snapshot"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected SQLite SELECT CSV + Iceberg snapshot, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def run_iceberg_sqlite_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    dest_database: str | Path | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity Iceberg→SQLite through stream_database_transfer.

    Source COUNT is Iceberg file footers, never ``scan().count()``. Dest
    COUNT is SQLite ``COUNT(*)``. Empty dest is snapshot Parquet +
    ``executemany``, not upsert / sqlite3 ``.import`` / ``MERGE INTO``.
    DATE lands as SQLite TEXT (ISO calendar day — no DATE affinity).
    Nested list/map/struct / MoR / binary decline. ``:memory:`` declines.
    Unique dest ``bench_sqlite_from_iceberg`` is not reused from
    ``bench_1m`` / ``bench_pg_sqlite`` / ``bench_ice_mongo``. Seeds from
    SQLite→Iceberg when the source table is missing.
    """
    import socket
    from urllib.request import urlopen

    rest = _iceberg_rest_uri()
    try:
        with urlopen(f"{rest}/v1/config", timeout=2) as resp:
            if int(getattr(resp, "status", 0) or 0) != 200:
                raise RuntimeError(f"Iceberg REST {rest}/v1/config not 200")
    except Exception as exc:
        raise RuntimeError(f"Iceberg REST not reachable at {rest}: {exc}") from exc
    try:
        socket.create_connection(("127.0.0.1", 5432), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"PostgreSQL 5432 not reachable: {exc}") from exc

    ice_src = str(source_table or "bench_sqlite_iceberg")
    dest = str(dest_table)
    dest_db = Path(dest_database or (_sqlite_bench_dir() / "from_iceberg.db"))
    dest_db.parent.mkdir(parents=True, exist_ok=True)
    job_store = ensure_memory_job_store_if_mongo_down()
    try:
        have = _iceberg_count(ice_src)
    except Exception:
        have = 0
    if have != rows:
        run_sqlite_iceberg_volume(
            rows=rows,
            dest_table=ice_src,
            source_table="bench_pg_sqlite",
            sync_mode="full_refresh_append",
            keep_dest=False,
            fail_closed=True,
            proof_path=None,
        )
    if not keep_dest:
        _sqlite_drop_table(dest_db, dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    source = EndpointConfig.from_dict("database", _iceberg_rest_cfg(ice_src))
    destination = EndpointConfig.from_dict("database", _sqlite_cfg(dest_db, dest))
    cols = _sqlite_physical_columns()
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in cols
    ]
    schema = dict(cols)
    job_id = f"bench-iceberg-sqlite-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = _sqlite_count(dest_db, dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "iceberg→sqlite",
        "sync_mode": sync_mode,
        "source_table": ice_src,
        "dest_table": dest,
        "dest_database": str(dest_db),
        "iceberg_rest": rest,
        "iceberg_warehouse": _iceberg_rest_warehouse(),
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "iceberg_read": summary.get("iceberg_read"),
        "sqlite_write": summary.get("sqlite_write"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(cols),
        "dest_count_source": "sqlite_count_star",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} iceberg→sqlite [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination COUNT(*): {landed} (source footer COUNT: {rows})")
    print("Not .import / MERGE INTO. Empty dest is executemany. DATE lands as TEXT.")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "iceberg_parquet_executemany_sqlite"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected Iceberg snapshot Parquet + SQLite executemany, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def run_sqlite_sqlserver_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    source_database: str | Path | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity SQLite→SQL Server through stream_database_transfer.

    Dest COUNT is ``SELECT COUNT(*)``. Empty dest is SELECT +
    ``fast_executemany``, not upsert / BCP / ``BULK INSERT`` / sqlite3
    ``.dump``. DATE ISO TEXT stays a string after PostgreSQL→SQLite.
    DATETIME / TIMESTAMP / BLOB / JSON decline. ``:memory:`` declines.
    Unique dest ``bench_sqlite_sqlserver`` is not reused from ``bench_1m``
    / ``bench_ss_mongo``. Seeds from PostgreSQL→SQLite when the source
    file/table is missing.
    """
    import socket

    try:
        socket.create_connection(("127.0.0.1", 1433), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"SQL Server 1433 not reachable: {exc}") from exc
    try:
        socket.create_connection(("127.0.0.1", 5432), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"PostgreSQL 5432 not reachable: {exc}") from exc

    src_table = str(source_table or "bench_pg_sqlite")
    dest = str(dest_table)
    src_db = Path(source_database or (_sqlite_bench_dir() / "src.db"))
    job_store = ensure_memory_job_store_if_mongo_down()
    try:
        have = _sqlite_count(src_db, src_table) if src_db.exists() else 0
    except Exception:
        have = 0
    if have != rows:
        run_pg_sqlite_volume(
            rows=rows,
            dest_table=src_table,
            dest_database=src_db,
            source_table=f"bench_emp_{rows}",
            sync_mode="full_refresh_append",
            keep_dest=False,
            fail_closed=True,
            proof_path=None,
        )
    if not keep_dest:
        _sqlserver_drop_table(dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    ss = _sqlserver_cfg()
    source = EndpointConfig.from_dict("database", _sqlite_cfg(src_db, src_table))
    destination = EndpointConfig.from_dict(
        "database", {**ss, "format": "sqlserver", "table": dest}
    )
    cols = _sqlite_physical_columns()
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in cols
    ]
    schema = dict(cols)
    job_id = f"bench-sqlite-sqlserver-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = _sqlserver_dest_count(dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "sqlite→sqlserver",
        "sync_mode": sync_mode,
        "source_table": src_table,
        "source_database": str(src_db),
        "dest_table": dest,
        "sqlserver_port": 1433,
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "sqlite_read": summary.get("sqlite_read"),
        "sqlserver_write": summary.get("sqlserver_write"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(cols),
        "dest_count_source": "sqlserver_count_star",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} sqlite→sqlserver [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination COUNT(*): {landed} (source COUNT(*): {rows})")
    print("Not BCP / BULK INSERT / .dump. Empty dest is fast_executemany. DATE ISO TEXT stays a string.")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "select_sqlite_fast_executemany_sqlserver"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected SQLite SELECT + SQL Server fast_executemany, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def run_sqlserver_sqlite_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    dest_database: str | Path | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity SQL Server→SQLite through stream_database_transfer.

    Source COUNT is HOLDLOCK/SNAPSHOT ``COUNT(*)``. Dest COUNT is SQLite
    ``COUNT(*)``. Empty dest is HOLDLOCK SELECT + ``executemany``, not
    upsert / BCP / sqlite3 ``.import``. DATE lands as SQLite TEXT (ISO
    calendar day — no DATE affinity). DATETIMEOFFSET / varbinary decline.
    ``:memory:`` declines. Unique dest ``bench_sqlite_from_sqlserver`` is
    not reused from ``bench_1m`` / ``bench_pg_sqlite``. Seeds from
    SQLite→SQL Server when the source table is missing.
    """
    import socket

    try:
        socket.create_connection(("127.0.0.1", 1433), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"SQL Server 1433 not reachable: {exc}") from exc
    try:
        socket.create_connection(("127.0.0.1", 5432), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"PostgreSQL 5432 not reachable: {exc}") from exc

    ss_src = str(source_table or "bench_sqlite_sqlserver")
    dest = str(dest_table)
    dest_db = Path(dest_database or (_sqlite_bench_dir() / "from_sqlserver.db"))
    dest_db.parent.mkdir(parents=True, exist_ok=True)
    job_store = ensure_memory_job_store_if_mongo_down()
    try:
        have = _sqlserver_dest_count(ss_src)
    except Exception:
        have = 0
    if have != rows:
        run_sqlite_sqlserver_volume(
            rows=rows,
            dest_table=ss_src,
            source_table="bench_pg_sqlite",
            sync_mode="full_refresh_append",
            keep_dest=False,
            fail_closed=True,
            proof_path=None,
        )
    if not keep_dest:
        _sqlite_drop_table(dest_db, dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    ss = _sqlserver_cfg()
    source = EndpointConfig.from_dict(
        "database", {**ss, "format": "sqlserver", "table": ss_src}
    )
    destination = EndpointConfig.from_dict("database", _sqlite_cfg(dest_db, dest))
    cols = _sqlite_physical_columns()
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in cols
    ]
    schema = dict(cols)
    job_id = f"bench-sqlserver-sqlite-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = _sqlite_count(dest_db, dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "sqlserver→sqlite",
        "sync_mode": sync_mode,
        "source_table": ss_src,
        "dest_table": dest,
        "dest_database": str(dest_db),
        "sqlserver_port": 1433,
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "sqlserver_read": summary.get("sqlserver_read"),
        "sqlite_write": summary.get("sqlite_write"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(cols),
        "dest_count_source": "sqlite_count_star",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} sqlserver→sqlite [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination COUNT(*): {landed} (source snapshot COUNT: {rows})")
    print("Not BCP / .import. Empty dest is executemany. DATE lands as TEXT.")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "select_sqlserver_executemany_sqlite"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected SQL Server HOLDLOCK SELECT + SQLite executemany, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def run_sqlite_oracle_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    source_database: str | Path | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity SQLite→Oracle through stream_database_transfer.

    Dest COUNT is ``SELECT COUNT(*)``. Empty dest is SELECT +
    ``executemany``, not upsert / sqlldr / Data Pump / sqlite3 ``.dump``.
    DATE ISO TEXT stays a string after PostgreSQL→SQLite. DATETIME /
    TIMESTAMP / BLOB / JSON decline. ``:memory:`` declines. VARCHAR2
    stores ``''`` as NULL (engine law, counted in
    ``empty_string_as_null_cells``). Unique dest ``bench_sqlite_oracle``
    is not reused from ``bench_1m`` / ``bench_pg_oracle``. Seeds from
    PostgreSQL→SQLite when the source file/table is missing.
    """
    import socket

    try:
        socket.create_connection(("127.0.0.1", 1521), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"Oracle 1521 not reachable: {exc}") from exc
    try:
        socket.create_connection(("127.0.0.1", 5432), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"PostgreSQL 5432 not reachable: {exc}") from exc

    src_table = str(source_table or "bench_pg_sqlite")
    dest = str(dest_table)
    src_db = Path(source_database or (_sqlite_bench_dir() / "src.db"))
    job_store = ensure_memory_job_store_if_mongo_down()
    try:
        have = _sqlite_count(src_db, src_table) if src_db.exists() else 0
    except Exception:
        have = 0
    if have != rows:
        run_pg_sqlite_volume(
            rows=rows,
            dest_table=src_table,
            dest_database=src_db,
            source_table=f"bench_emp_{rows}",
            sync_mode="full_refresh_append",
            keep_dest=False,
            fail_closed=True,
            proof_path=None,
        )
    if not keep_dest:
        _oracle_drop_table(dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    ora = _oracle_cfg()
    source = EndpointConfig.from_dict("database", _sqlite_cfg(src_db, src_table))
    destination = EndpointConfig.from_dict(
        "database", {**ora, "format": "oracle", "table": dest}
    )
    cols = _sqlite_physical_columns()
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in cols
    ]
    schema = dict(cols)
    job_id = f"bench-sqlite-oracle-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = _oracle_dest_count(dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "sqlite→oracle",
        "sync_mode": sync_mode,
        "source_table": src_table,
        "source_database": str(src_db),
        "dest_table": dest,
        "oracle_port": 1521,
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "sqlite_read": summary.get("sqlite_read"),
        "oracle_write": summary.get("oracle_write"),
        "empty_string_as_null_cells": summary.get("empty_string_as_null_cells") or 0,
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(cols),
        "dest_count_source": "oracle_count_star",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} sqlite→oracle [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination COUNT(*): {landed} (source COUNT(*): {rows})")
    print(
        "Not sqlldr / Data Pump / .dump. Empty dest is executemany. "
        "VARCHAR2 '' IS NULL (engine law)."
    )
    print(
        "empty_string_as_null_cells: "
        f"{summary.get('empty_string_as_null_cells') or 0}"
    )
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "select_sqlite_executemany_oracle"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected SQLite SELECT + Oracle executemany, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def run_oracle_sqlite_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    dest_database: str | Path | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity Oracle→SQLite through stream_database_transfer.

    Source COUNT is SHARE-lock ``COUNT(*)``. Dest COUNT is SQLite
    ``COUNT(*)``. Empty dest is SHARE-lock SELECT + ``executemany``, not
    upsert / sqlldr / sqlite3 ``.import``. DATE lands as SQLite TEXT
    (ISO calendar day — no DATE affinity). BLOB dest DDL declines.
    ``:memory:`` declines. VARCHAR2 empty strings already arrive as
    NULL. Unique dest ``bench_sqlite_from_oracle`` is not reused from
    ``bench_1m`` / ``bench_pg_sqlite``. Seeds from SQLite→Oracle when
    the source table is missing.
    """
    import socket

    try:
        socket.create_connection(("127.0.0.1", 1521), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"Oracle 1521 not reachable: {exc}") from exc
    try:
        socket.create_connection(("127.0.0.1", 5432), timeout=1).close()
    except OSError as exc:
        raise RuntimeError(f"PostgreSQL 5432 not reachable: {exc}") from exc

    ora_src = str(source_table or "bench_sqlite_oracle")
    dest = str(dest_table)
    dest_db = Path(dest_database or (_sqlite_bench_dir() / "from_oracle.db"))
    dest_db.parent.mkdir(parents=True, exist_ok=True)
    job_store = ensure_memory_job_store_if_mongo_down()
    try:
        have = _oracle_dest_count(ora_src)
    except Exception:
        have = 0
    if have != rows:
        run_sqlite_oracle_volume(
            rows=rows,
            dest_table=ora_src,
            source_table="bench_pg_sqlite",
            sync_mode="full_refresh_append",
            keep_dest=False,
            fail_closed=True,
            proof_path=None,
        )
    if not keep_dest:
        _sqlite_drop_table(dest_db, dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    ora = _oracle_cfg()
    source = EndpointConfig.from_dict(
        "database", {**ora, "format": "oracle", "table": ora_src}
    )
    destination = EndpointConfig.from_dict("database", _sqlite_cfg(dest_db, dest))
    cols = _sqlite_physical_columns()
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in cols
    ]
    schema = dict(cols)
    job_id = f"bench-oracle-sqlite-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = _sqlite_count(dest_db, dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "oracle→sqlite",
        "sync_mode": sync_mode,
        "source_table": ora_src,
        "dest_table": dest,
        "dest_database": str(dest_db),
        "oracle_port": 1521,
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "oracle_lock": summary.get("oracle_lock"),
        "oracle_read": summary.get("oracle_read"),
        "sqlite_write": summary.get("sqlite_write"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(cols),
        "dest_count_source": "sqlite_count_star",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} oracle→sqlite [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination COUNT(*): {landed} (source snapshot COUNT: {rows})")
    print("Not sqlldr / .import. Empty dest is executemany. DATE lands as TEXT.")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "select_oracle_executemany_sqlite"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected Oracle SHARE-lock SELECT + SQLite executemany, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def run_s3_iceberg_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    source_bucket: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity S3→Iceberg through stream_database_transfer.

    Source COUNT is object-store artifact COUNT of the CSV (header
    skipped), never ListObjects length. Dest COUNT is file footers, never
    ``scan().count()``. Empty dest is GET CSV + CoW snapshot append, not
    ``aws s3 cp`` / ``MERGE INTO``. JSON/JSONL/Parquet decline. Unique dest
    ``bench_s3_iceberg`` is not reused from ``bench_1m`` /
    ``bench_pg_iceberg`` / ``bench_sqlite_iceberg``. Seeds from SQLite→S3
    when the source object is missing. Iceberg times are **local**
    warehouse.
    """
    from urllib.request import urlopen

    _require_minio()
    rest = _iceberg_rest_uri()
    try:
        with urlopen(f"{rest}/v1/config", timeout=2) as resp:
            if int(getattr(resp, "status", 0) or 0) != 200:
                raise RuntimeError(f"Iceberg REST {rest}/v1/config not 200")
    except Exception as exc:
        raise RuntimeError(f"Iceberg REST not reachable at {rest}: {exc}") from exc

    src_key = str(source_table or "bench_sqlite_s3.csv")
    dest = str(dest_table)
    bucket = source_bucket or _s3_bench_bucket()
    src_cfg = _s3_local_cfg(bucket, src_key)
    job_store = ensure_memory_job_store_if_mongo_down()
    try:
        have = _s3_count(src_cfg, src_key)
    except Exception:
        have = 0
    if have != rows:
        run_sqlite_s3_volume(
            rows=rows,
            dest_table=src_key,
            dest_bucket=bucket,
            source_table="bench_pg_sqlite",
            sync_mode="full_refresh_append",
            keep_dest=False,
            fail_closed=True,
            proof_path=None,
        )
    if not keep_dest:
        _iceberg_drop(dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    source = EndpointConfig.from_dict("database", src_cfg)
    destination = EndpointConfig.from_dict("database", _iceberg_rest_cfg(dest))
    cols = _sqlite_physical_columns()
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in cols
    ]
    schema = dict(cols)
    job_id = f"bench-s3-iceberg-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = _iceberg_count(dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "s3→iceberg",
        "sync_mode": sync_mode,
        "source_table": src_key,
        "source_bucket": bucket,
        "dest_table": dest,
        "iceberg_rest": rest,
        "iceberg_warehouse": _iceberg_rest_warehouse(),
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "s3_read": summary.get("s3_read"),
        "iceberg_write": summary.get("iceberg_write"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(cols),
        "dest_count_source": "iceberg_file_footers",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {dest} s3→iceberg [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination COUNT (file footers): {landed} (source artifact COUNT: {rows})")
    print("Not aws s3 cp / MERGE INTO. Empty dest is GET CSV + CoW snapshot append.")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "get_csv_s3_iceberg_snapshot"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected S3 GET CSV + Iceberg snapshot, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


def run_iceberg_s3_volume(
    *,
    rows: int,
    dest_table: str,
    source_table: str | None = None,
    dest_bucket: str | None = None,
    sync_mode: str = "full_refresh_append",
    keep_dest: bool = False,
    fail_closed: bool = True,
    proof_path: str | Path | None = None,
) -> dict[str, Any]:
    """Identity Iceberg→S3 through stream_database_transfer.

    Source COUNT is Iceberg file footers, never ``scan().count()``. Dest
    COUNT is object-store artifact COUNT of the CSV (header skipped),
    never ListObjects length, never writer PUT ack. Empty dest is snapshot
    Parquet + ``upload_file``, not ``MERGE INTO`` / ``aws s3 cp``. Dest
    key must be ``.csv`` / ``.tsv``. Nested list/map/struct / MoR / JSON
    dest keys decline. Unique dest ``bench_s3_from_iceberg.csv`` is not
    reused from ``bench_1m`` / ``bench_sqlite_s3.csv`` /
    ``bench_s3_clone.csv``. Seeds from S3→Iceberg when the source table
    is missing. Iceberg times are **local** warehouse.
    """
    from urllib.request import urlopen

    _require_minio()
    rest = _iceberg_rest_uri()
    try:
        with urlopen(f"{rest}/v1/config", timeout=2) as resp:
            if int(getattr(resp, "status", 0) or 0) != 200:
                raise RuntimeError(f"Iceberg REST {rest}/v1/config not 200")
    except Exception as exc:
        raise RuntimeError(f"Iceberg REST not reachable at {rest}: {exc}") from exc

    ice_src = str(source_table or "bench_s3_iceberg")
    dest = str(dest_table)
    bucket = dest_bucket or _s3_bench_bucket()
    dest_cfg = _s3_local_cfg(bucket, dest)
    job_store = ensure_memory_job_store_if_mongo_down()
    try:
        have = _iceberg_count(ice_src)
    except Exception:
        have = 0
    if have != rows:
        run_s3_iceberg_volume(
            rows=rows,
            dest_table=ice_src,
            source_table="bench_sqlite_s3.csv",
            source_bucket=bucket,
            sync_mode="full_refresh_append",
            keep_dest=False,
            fail_closed=True,
            proof_path=None,
        )
    if not keep_dest:
        from services.copy_s3_common import s3_ensure_bucket

        s3_ensure_bucket(dest_cfg)
        _s3_delete_key(dest_cfg, dest)

    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    source = EndpointConfig.from_dict("database", _iceberg_rest_cfg(ice_src))
    destination = EndpointConfig.from_dict("database", dest_cfg)
    cols = _sqlite_physical_columns()
    mappings = [
        {"source": name, "target": name, "type": ddl, "transform": "none"}
        for name, ddl in cols
    ]
    schema = dict(cols)
    job_id = f"bench-iceberg-s3-{dest}-{int(time.time())}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})

    started = time.monotonic()
    transferred, _ddl, summary, _columns = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        job_id=job_id,
    )
    elapsed = time.monotonic() - started
    landed = _s3_count(dest_cfg, dest)
    rejected = int(summary.get("rejected_rows") or 0)
    conservation = row_conservation(
        source_rows=rows,
        dest_count=landed,
        rejected_rows=rejected,
    )
    rps = transferred / elapsed if elapsed > 0 else 0.0
    report: dict[str, Any] = {
        "route": "iceberg→s3",
        "sync_mode": sync_mode,
        "source_table": ice_src,
        "dest_table": dest,
        "dest_bucket": bucket,
        "iceberg_rest": rest,
        "iceberg_warehouse": _iceberg_rest_warehouse(),
        "job_store": job_store,
        "job_id": job_id,
        "load_method": summary.get("load_method"),
        "shard_mode": summary.get("shard_mode"),
        "copy_split": summary.get("copy_split"),
        "iceberg_read": summary.get("iceberg_read"),
        "s3_write": summary.get("s3_write"),
        "partition_proof": summary.get("partition_proof"),
        "proof_scope": summary.get("proof_scope"),
        "copy_workers": summary.get("copy_workers"),
        "copy_partitions": summary.get("copy_partitions"),
        "partitions_skipped": summary.get("partitions_skipped"),
        "rows_requested": rows,
        "rows_transferred": transferred,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_sec": round(rps, 1),
        "dest_count": landed,
        "rejected_rows": rejected,
        "conservation": conservation,
        "columns": len(cols),
        "dest_count_source": "s3_artifact_count",
        "summary_scalars": {
            key: summary[key] for key in REPORTED_SUMMARY_KEYS if key in summary
        },
    }
    print(
        f"\n=== {bucket}/{dest} iceberg→s3 [{sync_mode}]: {transferred} rows "
        f"in {elapsed:.1f}s = {rps:,.0f} rows/s"
    )
    for key in REPORTED_SUMMARY_KEYS:
        if key in summary:
            print(f"{key}: {summary[key]}")
    print(f"destination artifact COUNT: {landed} (source footer COUNT: {rows})")
    print("Not aws s3 cp / MERGE INTO. Empty dest is snapshot Parquet + upload. CSV HEADER is not a dest row.")
    print(f"row conservation: {conservation['verdict']}")
    if proof_path:
        write_million_proof(proof_path, report)
    if fail_closed:
        assert_clean_conservation(conservation)
        if transferred != rows:
            raise AssertionError(
                f"engine transferred {transferred}, requested {rows}"
            )
        expected_load = "iceberg_parquet_upload_s3"
        if summary.get("load_method") != expected_load:
            raise AssertionError(
                "expected Iceberg snapshot Parquet + S3 upload, "
                f"got load_method={summary.get('load_method')!r}"
            )
    return report


