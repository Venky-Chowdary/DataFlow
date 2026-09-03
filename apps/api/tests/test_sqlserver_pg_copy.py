"""SQL Server → PostgreSQL SELECT + COPY FROM STDIN — dest COUNT(*), PK-range proof."""

from __future__ import annotations

import socket
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_fast_path import FastPathUnavailable  # noqa: E402
from services.copy_mysql_pg import fast_copy_text_value  # noqa: E402
from services.copy_sqlserver_pg import (  # noqa: E402
    copy_sqlserver_to_postgres,
    sqlserver_type_is_copy_safe,
)


def test_sqlserver_copy_safe_types():
    assert sqlserver_type_is_copy_safe("NVARCHAR(32)") is True
    assert sqlserver_type_is_copy_safe("BIGINT") is True
    assert sqlserver_type_is_copy_safe("DATE") is True
    assert sqlserver_type_is_copy_safe("DATETIME2(7)") is True
    assert sqlserver_type_is_copy_safe("VARBINARY(16)") is False
    assert sqlserver_type_is_copy_safe("XML") is False
    assert sqlserver_type_is_copy_safe("DATETIMEOFFSET") is False
    assert sqlserver_type_is_copy_safe("UNIQUEIDENTIFIER") is False


def test_fast_copy_text_empty_string_is_not_null():
    assert fast_copy_text_value(None) == "\\N"
    assert fast_copy_text_value("") == ""


def _pg_ss_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 5432), timeout=1):
            pass
        with socket.create_connection(("127.0.0.1", 1433), timeout=1):
            pass
    except OSError:
        pytest.skip("PostgreSQL 5432 or SQL Server 1433 not reachable")


def _pg_cfg() -> dict:
    return {
        "host": "127.0.0.1",
        "port": 5432,
        "database": "dataflow",
        "username": "dataflow",
        "password": "dataflow",
        "schema": "public",
    }


def _ss_cfg() -> dict:
    return {
        "host": "127.0.0.1",
        "port": 1433,
        "database": "dataflow",
        "username": "sa",
        "password": "DataFlow_CDC_2022!",
        "schema": "dbo",
        "trust_server_certificate": True,
        "encrypt": "yes",
    }


def _pg_connect():
    _pg_ss_or_skip()
    psycopg2 = pytest.importorskip("psycopg2")
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        user="dataflow",
        password="dataflow",
        dbname="dataflow",
    )
    conn.autocommit = True
    return conn


def _ss_connect():
    _pg_ss_or_skip()
    pymssql = pytest.importorskip("pymssql")
    try:
        conn = pymssql.connect(
            server="127.0.0.1",
            port=1433,
            user="sa",
            password="DataFlow_CDC_2022!",
            database="dataflow",
            login_timeout=3,
            autocommit=True,
        )
    except Exception as exc:
        pytest.skip(f"SQL Server auth failed: {exc}")
    return conn


def _seed_ss(cur, table: str, rows: int) -> None:
    cur.execute(f"IF OBJECT_ID(N'dbo.{table}', 'U') IS NOT NULL DROP TABLE dbo.[{table}]")
    cur.execute(
        f"CREATE TABLE dbo.[{table}] (id BIGINT NOT NULL PRIMARY KEY, label NVARCHAR(32) NULL)"
    )
    cur.execute(
        f"""
        WITH n AS (
          SELECT TOP ({int(rows)}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS seq
          FROM sys.all_objects a CROSS JOIN sys.all_objects b
        )
        INSERT INTO dbo.[{table}] (id, label)
        SELECT seq, CONCAT(N'r', seq) FROM n
        """
    )


def test_live_sqlserver_pg_dest_count(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PG_MYSQL_COPY_WORKERS", "4")
    monkeypatch.delenv("DATAFLOW_SQLSERVER_PG_COPY", raising=False)
    pg = _pg_connect()
    ss = _ss_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ss_pg_src_{tag}"
    dest = f"ss_pg_dst_{tag}"
    try:
        _seed_ss(ss.cursor(), src, 800)
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{dest}"')
        result = copy_sqlserver_to_postgres(
            source_cfg=_ss_cfg(),
            source_table=src,
            dest_cfg=_pg_cfg(),
            dest_schema="public",
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            pg_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("copy_split") == "serial"
        assert result.source_snapshot.get("shard_mode") == "pk"
        parts = result.source_snapshot.get("partition_proof") or []
        assert parts
        assert sum(int(p["source_count"]) for p in parts) == 800
        assert all(int(p["source_count"]) == int(p["dest_count"]) for p in parts)
        with pg.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM public."{dest}"')
            assert int(cur.fetchone()[0]) == 800
            cur.execute(f'SELECT id, label FROM public."{dest}" WHERE id = 1')
            row = cur.fetchone()
            assert int(row[0]) == 1
            assert str(row[1]) == "r1"
    finally:
        ss.cursor().execute(
            f"IF OBJECT_ID(N'dbo.{src}', 'U') IS NOT NULL DROP TABLE dbo.[{src}]"
        )
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{dest}"')
        pg.close()
        ss.close()


def test_live_sqlserver_pg_empty_string_is_not_null():
    pg = _pg_connect()
    ss = _ss_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ss_pg_null_{tag}"
    dest = f"ss_pg_null_d_{tag}"
    try:
        cur = ss.cursor()
        cur.execute(f"IF OBJECT_ID(N'dbo.{src}', 'U') IS NOT NULL DROP TABLE dbo.[{src}]")
        cur.execute(
            f"CREATE TABLE dbo.[{src}] (id BIGINT NOT NULL PRIMARY KEY, label NVARCHAR(32) NULL)"
        )
        cur.execute(
            f"INSERT INTO dbo.[{src}] (id, label) VALUES (1, NULL), (2, N''), (3, N'x')"
        )
        with pg.cursor() as pcur:
            pcur.execute(f'DROP TABLE IF EXISTS public."{dest}"')
        result = copy_sqlserver_to_postgres(
            source_cfg=_ss_cfg(),
            source_table=src,
            dest_cfg=_pg_cfg(),
            dest_schema="public",
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            pg_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=True,
        )
        assert result.target_rows == 3
        with pg.cursor() as pcur:
            pcur.execute(f'SELECT id, label FROM public."{dest}" ORDER BY id')
            rows = list(pcur.fetchall())
        assert int(rows[0][0]) == 1
        assert rows[0][1] is None
        assert int(rows[1][0]) == 2
        assert rows[1][1] == ""
        assert int(rows[2][0]) == 3
        assert str(rows[2][1]) == "x"
    finally:
        ss.cursor().execute(
            f"IF OBJECT_ID(N'dbo.{src}', 'U') IS NOT NULL DROP TABLE dbo.[{src}]"
        )
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{dest}"')
        pg.close()
        ss.close()


def test_live_sqlserver_pg_occupied_without_pk_declines():
    pg = _pg_connect()
    ss = _ss_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ss_pg_nopk_{tag}"
    dest = f"ss_pg_nopk_d_{tag}"
    try:
        cur = ss.cursor()
        cur.execute(f"IF OBJECT_ID(N'dbo.{src}', 'U') IS NOT NULL DROP TABLE dbo.[{src}]")
        cur.execute(
            f"CREATE TABLE dbo.[{src}] (id BIGINT NOT NULL, label NVARCHAR(32) NULL)"
        )
        cur.execute(f"INSERT INTO dbo.[{src}] (id, label) VALUES (1, N'a'), (2, N'b')")
        with pg.cursor() as pcur:
            pcur.execute(f'DROP TABLE IF EXISTS public."{dest}"')
            pcur.execute(
                f'CREATE TABLE public."{dest}" (id bigint NOT NULL, label varchar(32))'
            )
            pcur.execute(f'INSERT INTO public."{dest}" (id, label) VALUES (1, \'old\')')
        with pytest.raises(FastPathUnavailable, match="non-empty"):
            copy_sqlserver_to_postgres(
                source_cfg=_ss_cfg(),
                source_table=src,
                dest_cfg=_pg_cfg(),
                dest_schema="public",
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                pg_ddls=["BIGINT", "VARCHAR(32)"],
                replace_destination=False,
            )
    finally:
        ss.cursor().execute(
            f"IF OBJECT_ID(N'dbo.{src}', 'U') IS NOT NULL DROP TABLE dbo.[{src}]"
        )
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{dest}"')
        pg.close()
        ss.close()


def test_live_sqlserver_pg_resume_skips_complete_range(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PG_MYSQL_COPY_WORKERS", "4")
    monkeypatch.delenv("DATAFLOW_SQLSERVER_PG_COPY", raising=False)
    pg = _pg_connect()
    ss = _ss_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ss_pg_resume_{tag}"
    dest = f"ss_pg_resume_d_{tag}"
    try:
        _seed_ss(ss.cursor(), src, 8000)
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{dest}"')
        first = copy_sqlserver_to_postgres(
            source_cfg=_ss_cfg(),
            source_table=src,
            dest_cfg=_pg_cfg(),
            dest_schema="public",
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            pg_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=True,
        )
        assert first.source_rows == 8000
        parts = first.source_snapshot["partition_proof"]
        assert len(parts) == 4
        victim = parts[2]
        lo = victim["lo"]
        assert lo is not None
        with pg.cursor() as cur:
            cur.execute(f'DELETE FROM public."{dest}" WHERE id = %s', (lo,))
            cur.execute(f'SELECT COUNT(*) FROM public."{dest}"')
            assert int(cur.fetchone()[0]) == 7999
        second = copy_sqlserver_to_postgres(
            source_cfg=_ss_cfg(),
            source_table=src,
            dest_cfg=_pg_cfg(),
            dest_schema="public",
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            pg_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=False,
        )
        assert second.source_rows == 8000
        assert second.target_rows == 8000
        actions = [p["action"] for p in second.source_snapshot["partition_proof"]]
        assert actions.count("skip") == 3
        assert actions.count("reload") == 1
        assert second.source_snapshot.get("partitions_skipped") == 3
        with pg.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM public."{dest}"')
            assert int(cur.fetchone()[0]) == 8000
    finally:
        ss.cursor().execute(
            f"IF OBJECT_ID(N'dbo.{src}', 'U') IS NOT NULL DROP TABLE dbo.[{src}]"
        )
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{dest}"')
        pg.close()
        ss.close()


def test_live_sqlserver_pg_stream_load_method(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PG_MYSQL_COPY_WORKERS", "4")
    monkeypatch.delenv("DATAFLOW_SQLSERVER_PG_COPY", raising=False)
    pg = _pg_connect()
    ss = _ss_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ss_pg_stream_{tag}"
    dest = f"ss_pg_stream_d_{tag}"
    try:
        _seed_ss(ss.cursor(), src, 800)
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{dest}"')
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"ss-pg-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_ss_cfg(), "format": "sqlserver", "table": src}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_pg_cfg(), "format": "postgresql", "table": dest}
        )
        mappings = [
            {"source": "id", "target": "id", "type": "BIGINT", "transform": "none"},
            {
                "source": "label",
                "target": "label",
                "type": "VARCHAR(32)",
                "transform": "none",
            },
        ]
        transferred, ddl_log, summary, _cols = stream_database_transfer(
            source,
            destination,
            mappings,
            {"id": "BIGINT", "label": "VARCHAR(32)"},
            sync_mode="full_refresh_overwrite",
            job_id=job_id,
        )
        assert transferred == 800
        assert summary.get("load_method") == "select_sqlserver_copy_from_stdin_pg"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("COPY FROM STDIN" in line for line in ddl_log)
        with pg.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM public."{dest}"')
            assert int(cur.fetchone()[0]) == 800
    finally:
        ss.cursor().execute(
            f"IF OBJECT_ID(N'dbo.{src}', 'U') IS NOT NULL DROP TABLE dbo.[{src}]"
        )
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{dest}"')
        pg.close()
        ss.close()
