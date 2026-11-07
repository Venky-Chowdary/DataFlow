"""COPY upsert SQL + decline capture + worker auto-scale. Live PG/MySQL when up."""

from __future__ import annotations

import os
import socket
import uuid

import pytest

from services.copy_fast_path import (
    FastPathUnavailable,
    begin_copy_decline_capture,
    reset_copy_decline_capture,
)
from services.copy_pg_mysql import pg_mysql_copy_partitions, pg_mysql_copy_workers
from services.copy_upsert import (
    UPSERT_PROOF_SCOPE,
    mysql_upsert_from_staging_sql,
    pg_upsert_from_staging_sql,
    pk_join_count_sql,
    staging_table_name,
)
from services.sku_honesty import (
    classify_sku_route,
    route_is_customer_handover,
    sku_honesty_summary,
)


def test_staging_table_name_stays_within_mysql_ident_cap():
    short = staging_table_name("orders")
    assert short.startswith("_df_stg_")
    assert short == "_df_stg_orders"
    long_name = "x" * 80
    assert len(staging_table_name(long_name)) <= 64


def test_mysql_upsert_sql_updates_non_pk_columns():
    sql = mysql_upsert_from_staging_sql(
        "`dest`", "`_df_stg_dest`", ["id", "name"], "id", lambda c: f"`{c}`"
    )
    assert "ON DUPLICATE KEY UPDATE" in sql
    assert "`name`=VALUES(`name`)" in sql
    assert "VALUES(`id`)" not in sql


def test_pg_upsert_sql_conflicts_on_pk():
    sql = pg_upsert_from_staging_sql(
        "public.dest", "public._df_stg_dest", ["id", "name"], "id", lambda c: f'"{c}"'
    )
    assert 'ON CONFLICT ("id") DO UPDATE SET' in sql
    assert '"name"=EXCLUDED."name"' in sql


def test_sqlite_upsert_sql_conflicts_on_pk():
    from services.copy_upsert import sqlite_upsert_from_staging_sql

    sql = sqlite_upsert_from_staging_sql(
        '"dest"', '"_df_stg_dest"', ["id", "name"], "id", lambda c: f'"{c}"'
    )
    assert 'ON CONFLICT ("id") DO UPDATE SET' in sql
    assert '"name"=excluded."name"' in sql
    assert "WHERE true" in sql
    assert "IGNORE" not in sql


def test_pk_join_count_sql():
    sql = pk_join_count_sql("`dest`", "`stg`", "`id`")
    assert "INNER JOIN" in sql
    assert "d.`id` = s.`id`" in sql


def test_result_with_upsert_proof_stamps_key_census_inserts_and_updates():
    """D33: MERGE pre-count + join count become dest-engine inserts/updates.

    Writer ON DUPLICATE / ON CONFLICT rowcount cannot give that split.
    """
    from services.copy_fast_path import FastPathResult
    from services.copy_upsert import _result_with_upsert_proof
    from services.row_conservation import CENSUS_KEY, KeyCensus

    proven = _result_with_upsert_proof(
        FastPathResult(
            rows_copied=4,
            source_rows=4,
            source_checksum="src",
            target_rows=4,
            target_checksum="dst",
            source_snapshot={},
        ),
        join_count=4,
        dest_count=5,
        staging_table="_df_stg_items",
        dest_table="items",
        dest_preexisting=3,
    )
    census = KeyCensus.from_mapping((proven.source_snapshot or {}).get(CENSUS_KEY))
    assert census is not None
    assert census.unique_batch_keys == 4
    assert census.dest_preexisting == 3
    assert census.inserts == 1
    assert census.updates == 3
    assert census.deletes == 0
    assert census.expected_delta == 1


def test_fast_path_unavailable_records_decline_reason():
    sink: list[str] = []
    token, _ = begin_copy_decline_capture(sink)
    try:
        raise FastPathUnavailable("occupied dest stays on the row path")
    except FastPathUnavailable:
        pass
    finally:
        reset_copy_decline_capture(token)
    assert sink == ["occupied dest stays on the row path"]


def test_copy_workers_scale_at_1m():
    cpus = os.cpu_count() or 4
    assert pg_mysql_copy_workers(1_000) == 1
    assert pg_mysql_copy_workers(50_000) == min(4, cpus)
    assert pg_mysql_copy_workers(1_000_000) == min(8, cpus)
    assert pg_mysql_copy_workers(10_000_000) == min(8, cpus)
    assert pg_mysql_copy_partitions(10_000_000, 8) >= 8


def test_customer_handover_is_relational_file_core_only():
    assert route_is_customer_handover("database", "postgresql", "database", "mysql")
    assert route_is_customer_handover("file", "csv", "database", "sqlite")
    assert not route_is_customer_handover("database", "postgresql", "database", "snowflake")
    assert not route_is_customer_handover("database", "postgresql", "database", "milvus")
    pg_mysql = classify_sku_route(("database", "postgresql", "database", "mysql"))
    assert pg_mysql["customer_handover_eligible"] is True
    snow = classify_sku_route(("database", "postgresql", "database", "snowflake"))
    assert snow["customer_handover_eligible"] is False
    summary = sku_honesty_summary()
    assert summary["customer_handover_sold"] >= 1
    assert summary["customer_handover_sold"] <= summary["production_sku_sold"]
    assert "Customer handover" in summary["note"]


def _pg_mysql_up() -> bool:
    try:
        socket.create_connection(("127.0.0.1", 5432), timeout=1).close()
        socket.create_connection(("127.0.0.1", 3306), timeout=1).close()
        return True
    except OSError:
        return False


@pytest.mark.skipif(not _pg_mysql_up(), reason="PostgreSQL/MySQL not on 5432/3306")
def test_pg_mysql_copy_upsert_updates_occupied_dest():
    psycopg2 = pytest.importorskip("psycopg2")
    pymysql = pytest.importorskip("pymysql")
    from services.copy_upsert import copy_postgres_to_mysql_upsert
    from services.copy_pg_mysql import copy_postgres_to_mysql

    suffix = uuid.uuid4().hex[:8]
    src = f"up_src_{suffix}"
    dst = f"up_dst_{suffix}"
    pg = {
        "host": "127.0.0.1",
        "port": 5432,
        "database": "dataflow",
        "username": "dataflow",
        "password": "dataflow",
    }
    mysql = {
        "host": "127.0.0.1",
        "port": 3306,
        "database": "dataflow",
        "username": "dataflow",
        "password": "dataflow",
    }
    pg_conn = psycopg2.connect(
        host="127.0.0.1", port=5432, dbname="dataflow", user="dataflow", password="dataflow"
    )
    pg_conn.autocommit = True
    my_conn = pymysql.connect(
        host="127.0.0.1",
        port=3306,
        user="dataflow",
        password="dataflow",
        database="dataflow",
        autocommit=True,
    )
    pairs = [("id", "id"), ("name", "name")]
    mysql_ddls = ["VARCHAR(32)", "VARCHAR(64)"]
    try:
        with pg_conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{src}"')
            cur.execute(
                f'CREATE TABLE "{src}" (id varchar(32) PRIMARY KEY, name varchar(64))'
            )
            cur.execute(
                f"INSERT INTO \"{src}\" (id, name) VALUES ('a','one'), ('b','two'), ('c','three')"
            )
        first = copy_postgres_to_mysql(
            source_cfg=pg,
            source_schema="public",
            source_table=src,
            dest_cfg=mysql,
            dest_table=dst,
            pairs=pairs,
            mysql_ddls=mysql_ddls,
            replace_destination=True,
        )
        assert first.source_rows == 3
        with pg_conn.cursor() as cur:
            cur.execute(f"UPDATE \"{src}\" SET name = 'ONE' WHERE id = 'a'")
            cur.execute(f"INSERT INTO \"{src}\" (id, name) VALUES ('d','four')")
        result = copy_postgres_to_mysql_upsert(
            source_cfg=pg,
            source_schema="public",
            source_table=src,
            dest_cfg=mysql,
            dest_table=dst,
            pairs=pairs,
            mysql_ddls=mysql_ddls,
        )
        assert result.proof_scope == UPSERT_PROOF_SCOPE
        assert result.source_rows == 4
        assert result.target_rows == 4
        assert result.source_snapshot["dest_count"] == 4
        from services.row_conservation import CENSUS_KEY, KeyCensus

        census = KeyCensus.from_mapping((result.source_snapshot or {}).get(CENSUS_KEY))
        assert census is not None, result.source_snapshot
        assert census.inserts == 1
        assert census.updates == 3
        assert census.deletes == 0
        with my_conn.cursor() as cur:
            cur.execute(f"SELECT name FROM `{dst}` WHERE id = 'a'")
            assert cur.fetchone()[0] == "ONE"
            cur.execute(f"SELECT COUNT(*) FROM `{dst}`")
            assert int(cur.fetchone()[0]) == 4
            cur.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_name = %s",
                (f"_df_stg_{dst}",),
            )
            assert cur.fetchone() is None
    finally:
        with pg_conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{src}"')
        with my_conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{dst}`")
        pg_conn.close()
        my_conn.close()


def _pg_up() -> bool:
    try:
        socket.create_connection(("127.0.0.1", 5432), timeout=1).close()
        return True
    except OSError:
        return False


@pytest.mark.skipif(not _pg_up(), reason="PostgreSQL not on 5432")
def test_pg_pg_copy_upsert_updates_occupied_dest():
    psycopg2 = pytest.importorskip("psycopg2")
    from services.copy_fast_path import copy_between_postgres
    from services.copy_upsert import copy_between_postgres_upsert

    suffix = uuid.uuid4().hex[:8]
    src = f"up_pgs_{suffix}"
    dst = f"up_pgd_{suffix}"
    cfg = {
        "host": "127.0.0.1",
        "port": 5432,
        "database": "dataflow",
        "username": "dataflow",
        "password": "dataflow",
    }
    conn = psycopg2.connect(
        host="127.0.0.1", port=5432, dbname="dataflow", user="dataflow", password="dataflow"
    )
    conn.autocommit = True
    pairs = [("id", "id"), ("name", "name")]
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{src}", "{dst}"')
            cur.execute(
                f'CREATE TABLE "{src}" (id varchar(32) PRIMARY KEY, name varchar(64))'
            )
            cur.execute(
                f"INSERT INTO \"{src}\" (id, name) VALUES ('a','one'), ('b','two')"
            )
        copy_between_postgres(
            source_cfg=cfg,
            source_schema="public",
            source_table=src,
            dest_cfg=cfg,
            dest_schema="public",
            dest_table=dst,
            pairs=pairs,
            replace_destination=True,
        )
        with conn.cursor() as cur:
            cur.execute(f"UPDATE \"{src}\" SET name = 'ONE' WHERE id = 'a'")
            cur.execute(f"INSERT INTO \"{src}\" (id, name) VALUES ('c','three')")
        result = copy_between_postgres_upsert(
            source_cfg=cfg,
            source_schema="public",
            source_table=src,
            dest_cfg=cfg,
            dest_schema="public",
            dest_table=dst,
            pairs=pairs,
        )
        assert result.proof_scope == UPSERT_PROOF_SCOPE
        assert result.source_rows == 3
        assert result.source_snapshot["dest_count"] == 3
        from services.row_conservation import CENSUS_KEY, KeyCensus

        census = KeyCensus.from_mapping((result.source_snapshot or {}).get(CENSUS_KEY))
        assert census is not None, result.source_snapshot
        assert census.inserts == 1
        assert census.updates == 2
        assert census.deletes == 0
        with conn.cursor() as cur:
            cur.execute(f'SELECT name FROM "{dst}" WHERE id = %s', ("a",))
            assert cur.fetchone()[0] == "ONE"
            cur.execute(f'SELECT COUNT(*) FROM "{dst}"')
            assert int(cur.fetchone()[0]) == 3
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{src}", "{dst}", "{staging_table_name(dst)}"')
        conn.close()


@pytest.mark.skipif(not _pg_up(), reason="PostgreSQL not on 5432")
def test_pg_execute_tracked_copy_upsert_ledger_inserts_updates():
    """D33: execute_tracked COPY MERGE stamps keyed census onto the ledger.

    Dest held 3 keys; batch is 3 updates + 1 insert. COUNT(*) grows by 1;
    writer ack is 4. Inserts/updates/deletes must not stay None.
    """
    os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
    os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    psycopg2 = pytest.importorskip("psycopg2")
    from services.migration_certificate import row_accounting
    from services.row_conservation import CENSUS_KEY
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    suffix = uuid.uuid4().hex[:8]
    src = f"d33_src_{suffix}"
    dst = f"d33_dst_{suffix}"
    maps = [
        {
            "source": "id",
            "target": "id",
            "source_type": "BIGINT",
            "target_type": "BIGINT",
            "approved": True,
            "confidence": 0.99,
        },
        {
            "source": "label",
            "target": "label",
            "source_type": "TEXT",
            "target_type": "TEXT",
            "approved": True,
            "confidence": 0.99,
        },
    ]
    src_ep = EndpointConfig(
        kind="database",
        format="postgresql",
        host="127.0.0.1",
        port=5432,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        schema="public",
        table=src,
    )
    dst_ep = EndpointConfig(
        kind="database",
        format="postgresql",
        host="127.0.0.1",
        port=5432,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        schema="public",
        table=dst,
    )
    conn = psycopg2.connect(
        host="127.0.0.1", port=5432, dbname="dataflow", user="dataflow", password="dataflow"
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{src}", public."{dst}"')
            cur.execute(
                f'CREATE TABLE public."{src}" (id BIGINT PRIMARY KEY, label TEXT NOT NULL)'
            )
            cur.execute(
                f"INSERT INTO public.\"{src}\" (id, label) VALUES (1,'a'),(2,'b'),(3,'c')"
            )
        seed = TransferRequest(
            source=src_ep,
            destination=dst_ep,
            mappings=maps,
            sync_mode="full_refresh_overwrite",
            validation_mode="warn",
            skip_preflight=True,
        )
        seeded = UniversalTransferEngine().execute_tracked(seed, uuid.uuid4().hex[:24])
        assert seeded.success, seeded.error
        with conn.cursor() as cur:
            cur.execute(f"UPDATE public.\"{src}\" SET label = 'A' WHERE id = 1")
            cur.execute(f"INSERT INTO public.\"{src}\" (id, label) VALUES (4,'d')")
        result = UniversalTransferEngine().execute_tracked(
            TransferRequest(
                source=src_ep,
                destination=dst_ep,
                mappings=maps,
                stream_contracts=[
                    {"name": src, "selected": True, "sync_mode": "upsert", "primary_key": "id"}
                ],
                sync_mode="upsert",
                validation_mode="warn",
                skip_preflight=True,
            ),
            uuid.uuid4().hex[:24],
        )
        assert result.success, result.error
        summary = result.destination_summary or {}
        assert summary.get("copy_fast_path") == "used", summary
        census = summary.get(CENSUS_KEY) or {}
        assert census.get("inserts") == 1, census
        assert census.get("updates") == 3, census
        assert census.get("deletes") == 0, census
        stamped = result.row_accounting or {}
        assert stamped.get("inserts") == 1, stamped
        assert stamped.get("updates") == 3, stamped
        assert stamped.get("deletes") == 0, stamped
        assert stamped.get("conservation_kind") == "keyed", stamped
        assert stamped.get("balanced") is True, stamped
        job = {
            "records_processed": result.records_transferred,
            "sync_mode": "upsert",
            "reconciliation": result.reconciliation or {},
            "destination_summary": summary,
        }
        ledger = row_accounting(job)
        assert ledger["inserts"] == 1, ledger
        assert ledger["updates"] == 3, ledger
        assert ledger["deletes"] == 0, ledger
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM public."{dst}"')
            assert int(cur.fetchone()[0]) == 4
            cur.execute(f'SELECT label FROM public."{dst}" WHERE id = 1')
            assert cur.fetchone()[0] == "A"
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{src}", public."{dst}"')
        conn.close()
