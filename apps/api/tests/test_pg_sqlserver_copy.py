"""PostgreSQL → SQL Server COPY text + fast_executemany — dest COUNT(*), PK-range proof."""

from __future__ import annotations
import os

import socket
import sys
import uuid
from decimal import Decimal
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.postgresql_writer import _copy_text_value  # noqa: E402
from services.copy_fast_path import FastPathUnavailable  # noqa: E402
from services.copy_pg_sqlserver import (  # noqa: E402
    converter_for_ddl,
    copy_postgres_to_sqlserver,
    decode_copy_text_field,
    decode_copy_text_row,
)


def test_decimal_copy_text_binds_as_decimal():
    """DECIMAL(6,2) text is wider than the precision because of the point.

    ODBC Driver 18 fast_executemany sizes a wchar buffer from the precision
    and rejects ``1000.00`` (14 bytes into a 12-byte buffer). The numeric
    bind is the value the column can store.
    """
    convert = converter_for_ddl("DECIMAL(6,2)")
    assert convert("1000.00") == Decimal("1000.00")
    assert convert("2000.50") == Decimal("2000.50")
    assert convert(None) is None
    with pytest.raises(ValueError):
        convert("not-a-decimal")


def test_copy_text_decode_roundtrip_matches_pg_escape():
    samples = [
        None,
        "",
        "hello",
        "a\tb",
        "a\nb",
        "a\rb",
        "back\\slash",
        "mix\\t\n\r",
        "N",
        "\\N",
        "plain EMP0000001",
    ]
    for value in samples:
        encoded = _copy_text_value(value)
        assert decode_copy_text_field(encoded) == value, value


def test_copy_text_decode_null_is_not_empty_string():
    assert decode_copy_text_field("\\N") is None
    assert decode_copy_text_field("") == ""
    row = decode_copy_text_row("1\t\\N\t\tx")
    assert row == ["1", None, "", "x"]


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
        "password": os.environ.get("DATAFLOW_TEST_MSSQL_PASSWORD", ""),
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
            password=os.environ.get("DATAFLOW_TEST_MSSQL_PASSWORD", ""),
            database="dataflow",
            login_timeout=3,
            autocommit=True,
        )
    except Exception as exc:
        pytest.skip(f"SQL Server auth failed: {exc}")
    return conn


def _seed_pg(cur, table: str, rows: int) -> None:
    cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
    cur.execute(
        f'CREATE TABLE public."{table}" (id bigint PRIMARY KEY, label varchar(32))'
    )
    cur.execute(
        f"""
        INSERT INTO public."{table}" (id, label)
        SELECT g, 'r' || g::text FROM generate_series(1, {int(rows)}) AS g
        """
    )


def _drop_ss(cur, table: str) -> None:
    cur.execute(
        f"IF OBJECT_ID(N'dbo.{table}', 'U') IS NOT NULL DROP TABLE dbo.[{table}]"
    )


def test_live_pg_sqlserver_dest_count(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PG_MYSQL_COPY_WORKERS", "4")
    monkeypatch.delenv("DATAFLOW_PG_SQLSERVER_COPY", raising=False)
    pg = _pg_connect()
    ss = _ss_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"pg_ss_src_{tag}"
    dest = f"pg_ss_dst_{tag}"
    try:
        with pg.cursor() as cur:
            _seed_pg(cur, src, 800)
        ss_cur = ss.cursor()
        _drop_ss(ss_cur, dest)
        result = copy_postgres_to_sqlserver(
            source_cfg=_pg_cfg(),
            source_schema="public",
            source_table=src,
            dest_cfg=_ss_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlserver_ddls=["BIGINT", "NVARCHAR(32)"],
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
        ss_cur.execute(f"SELECT COUNT(*) FROM dbo.[{dest}]")
        assert int(ss_cur.fetchone()[0]) == 800
        ss_cur.execute(f"SELECT id, label FROM dbo.[{dest}] WHERE id = 1")
        row = ss_cur.fetchone()
        assert int(row[0]) == 1
        assert str(row[1]) == "r1"
    finally:
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{src}"')
        _drop_ss(ss.cursor(), dest)
        pg.close()
        ss.close()


def test_live_pg_sqlserver_empty_string_is_not_null():
    pg = _pg_connect()
    ss = _ss_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"pg_ss_null_{tag}"
    dest = f"pg_ss_null_d_{tag}"
    try:
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{src}"')
            cur.execute(
                f'CREATE TABLE public."{src}" '
                "(id bigint PRIMARY KEY, label varchar(32))"
            )
            cur.execute(
                f'INSERT INTO public."{src}" (id, label) VALUES '
                "(1, NULL), (2, ''), (3, 'x')"
            )
        _drop_ss(ss.cursor(), dest)
        result = copy_postgres_to_sqlserver(
            source_cfg=_pg_cfg(),
            source_schema="public",
            source_table=src,
            dest_cfg=_ss_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlserver_ddls=["BIGINT", "NVARCHAR(32)"],
            replace_destination=True,
        )
        assert result.target_rows == 3
        cur = ss.cursor()
        cur.execute(f"SELECT id, label FROM dbo.[{dest}] ORDER BY id")
        rows = list(cur.fetchall())
        assert int(rows[0][0]) == 1
        assert rows[0][1] is None
        assert int(rows[1][0]) == 2
        assert rows[1][1] == ""
        assert int(rows[2][0]) == 3
        assert str(rows[2][1]) == "x"
    finally:
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{src}"')
        _drop_ss(ss.cursor(), dest)
        pg.close()
        ss.close()


def test_live_pg_sqlserver_occupied_without_pk_declines():
    pg = _pg_connect()
    ss = _ss_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"pg_ss_nopk_{tag}"
    dest = f"pg_ss_nopk_d_{tag}"
    try:
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{src}"')
            cur.execute(
                f'CREATE TABLE public."{src}" (id bigint NOT NULL, label varchar(32))'
            )
            cur.execute(
                f'INSERT INTO public."{src}" (id, label) VALUES (1, \'a\'), (2, \'b\')'
            )
        cur = ss.cursor()
        _drop_ss(cur, dest)
        cur.execute(
            f"CREATE TABLE dbo.[{dest}] (id BIGINT NOT NULL, label NVARCHAR(32) NULL)"
        )
        cur.execute(f"INSERT INTO dbo.[{dest}] (id, label) VALUES (1, N'old')")
        with pytest.raises(FastPathUnavailable, match="non-empty"):
            copy_postgres_to_sqlserver(
                source_cfg=_pg_cfg(),
                source_schema="public",
                source_table=src,
                dest_cfg=_ss_cfg(),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                sqlserver_ddls=["BIGINT", "NVARCHAR(32)"],
                replace_destination=False,
            )
    finally:
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{src}"')
        _drop_ss(ss.cursor(), dest)
        pg.close()
        ss.close()


def test_live_pg_sqlserver_resume_skips_complete_range(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PG_MYSQL_COPY_WORKERS", "4")
    monkeypatch.delenv("DATAFLOW_PG_SQLSERVER_COPY", raising=False)
    pg = _pg_connect()
    ss = _ss_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"pg_ss_resume_{tag}"
    dest = f"pg_ss_resume_d_{tag}"
    try:
        with pg.cursor() as cur:
            _seed_pg(cur, src, 8000)
        _drop_ss(ss.cursor(), dest)
        first = copy_postgres_to_sqlserver(
            source_cfg=_pg_cfg(),
            source_schema="public",
            source_table=src,
            dest_cfg=_ss_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlserver_ddls=["BIGINT", "NVARCHAR(32)"],
            replace_destination=True,
        )
        assert first.source_rows == 8000
        parts = first.source_snapshot["partition_proof"]
        assert len(parts) == 4
        victim = parts[2]
        lo = victim["lo"]
        assert lo is not None
        cur = ss.cursor()
        cur.execute(f"DELETE FROM dbo.[{dest}] WHERE id = %s", (lo,))
        cur.execute(f"SELECT COUNT(*) FROM dbo.[{dest}]")
        assert int(cur.fetchone()[0]) == 7999
        with pytest.raises(FastPathUnavailable, match="partly"):
            copy_postgres_to_sqlserver(
                source_cfg=_pg_cfg(),
                source_schema="public",
                source_table=src,
                dest_cfg=_ss_cfg(),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                sqlserver_ddls=["BIGINT", "NVARCHAR(32)"],
                replace_destination=False,
            )
        cur.execute(f"SELECT COUNT(*) FROM dbo.[{dest}]")
        assert int(cur.fetchone()[0]) == 7999
    finally:
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{src}"')
        _drop_ss(ss.cursor(), dest)
        pg.close()
        ss.close()


def test_live_pg_sqlserver_stream_load_method(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PG_MYSQL_COPY_WORKERS", "4")
    monkeypatch.delenv("DATAFLOW_PG_SQLSERVER_COPY", raising=False)
    pg = _pg_connect()
    ss = _ss_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"pg_ss_stream_{tag}"
    dest = f"pg_ss_stream_d_{tag}"
    try:
        with pg.cursor() as cur:
            _seed_pg(cur, src, 800)
        _drop_ss(ss.cursor(), dest)
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"pg-ss-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_pg_cfg(), "format": "postgresql", "table": src}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_ss_cfg(), "format": "sqlserver", "table": dest}
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
        assert summary.get("load_method") == "copy_text_pg_to_sqlserver_fast_executemany"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("fast_executemany" in line for line in ddl_log)
        cur = ss.cursor()
        cur.execute(f"SELECT COUNT(*) FROM dbo.[{dest}]")
        assert int(cur.fetchone()[0]) == 800
    finally:
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{src}"')
        _drop_ss(ss.cursor(), dest)
        pg.close()
        ss.close()


def test_live_pg_sqlserver_overwrite_replaces_existing_dest():
    """A staged overwrite loads the staging table and swaps it in.

    The INSERT used to be built before the staging branch rebound
    ``dest_ref``, so rows went into the live table (duplicate key on its PK)
    and the swap then promoted the empty staging table.
    """
    src_conn = _pg_connect()
    ss = _ss_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"pg_ss_ow_src_{tag}"
    dest = f"pg_ss_ow_dst_{tag}"
    try:
        with src_conn.cursor() as cur:
            _seed_pg(cur, src, 800)
        ss_cur = ss.cursor()
        _drop_ss(ss_cur, dest)
        ss_cur.execute(
            f"CREATE TABLE dbo.[{dest}] ("
            "id BIGINT NOT NULL PRIMARY KEY, label NVARCHAR(32) NULL)"
        )
        ss_cur.execute(f"INSERT INTO dbo.[{dest}] (id, label) VALUES (1, N'ghost')")
        result = copy_postgres_to_sqlserver(
            source_cfg=_pg_cfg(),
            source_schema="public",
            source_table=src,
            dest_cfg=_ss_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlserver_ddls=["BIGINT", "NVARCHAR(32)"],
            replace_destination=True,
        )
        assert result.target_rows == 800
        ss_cur.execute(f"SELECT COUNT(*) FROM dbo.[{dest}]")
        assert int(ss_cur.fetchone()[0]) == 800
        ss_cur.execute(f"SELECT label FROM dbo.[{dest}] WHERE id = 1")
        assert str(ss_cur.fetchone()[0]) == "r1"
        ss_cur.execute(
            "SELECT COUNT(*) FROM sys.tables WHERE name LIKE %s OR name LIKE %s",
            (f"df_ow_{dest}%", f"df_trash_{dest}%"),
        )
        assert int(ss_cur.fetchone()[0]) == 0
    finally:
        with src_conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{src}"')
        _drop_ss(ss.cursor(), dest)
        src_conn.close()
        ss.close()
