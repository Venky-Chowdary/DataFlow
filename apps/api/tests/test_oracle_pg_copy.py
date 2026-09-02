"""Oracle → PostgreSQL SELECT + COPY FROM STDIN — dest COUNT(*), PK-range proof."""

from __future__ import annotations

import os
import socket
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_fast_path import FastPathUnavailable  # noqa: E402
from services.copy_oracle_pg import (  # noqa: E402
    copy_oracle_to_postgres,
    oracle_type_is_copy_safe,
)
from services.copy_mysql_pg import fast_copy_text_value  # noqa: E402


def test_oracle_copy_safe_types():
    assert oracle_type_is_copy_safe("VARCHAR2(32)") is True
    assert oracle_type_is_copy_safe("NUMBER") is True
    assert oracle_type_is_copy_safe("DATE") is True
    assert oracle_type_is_copy_safe("TIMESTAMP(6)") is True
    assert oracle_type_is_copy_safe("BLOB") is False
    assert oracle_type_is_copy_safe("RAW(16)") is False
    assert oracle_type_is_copy_safe("XMLTYPE") is False
    assert oracle_type_is_copy_safe("CLOB") is False
    assert oracle_type_is_copy_safe("TIMESTAMP WITH TIME ZONE") is False
    assert oracle_type_is_copy_safe("INTERVAL DAY TO SECOND") is False


def test_fast_copy_text_empty_string_is_not_null():
    assert fast_copy_text_value(None) == "\\N"
    assert fast_copy_text_value("") == ""


def _oracle_password() -> str:
    env = (os.environ.get("DATAFLOW_ORACLE_PASSWORD") or os.environ.get("ORA_PASSWORD") or "").strip()
    if env:
        return env
    path = Path("/tmp/df-desktop-lab/oracle_password")
    if path.is_file():
        return path.read_text().strip()
    return "dataflow"


def _ora_pg_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 5432), timeout=1):
            pass
        with socket.create_connection(("127.0.0.1", 1521), timeout=1):
            pass
    except OSError:
        pytest.skip("PostgreSQL 5432 or Oracle 1521 not reachable")


def _pg_cfg() -> dict:
    return {
        "host": "127.0.0.1",
        "port": 5432,
        "database": "dataflow",
        "username": "dataflow",
        "password": "dataflow",
        "schema": "public",
    }


def _ora_cfg() -> dict:
    return {
        "host": "127.0.0.1",
        "port": 1521,
        "database": "XEPDB1",
        "service_name": "XEPDB1",
        "username": "dataflow",
        "password": _oracle_password(),
        "schema": "DATAFLOW",
    }


def _pg_connect():
    _ora_pg_or_skip()
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


def _ora_connect():
    _ora_pg_or_skip()
    oracledb = pytest.importorskip("oracledb")
    try:
        conn = oracledb.connect(
            user="dataflow",
            password=_oracle_password(),
            dsn="127.0.0.1:1521/XEPDB1",
        )
    except Exception as exc:
        pytest.skip(f"Oracle auth failed: {exc}")
    return conn


def _drop_ora(cur, table: str) -> None:
    cur.execute(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE "
        f"{table} PURGE'; EXCEPTION WHEN OTHERS THEN "
        "IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


def _seed_ora(cur, table: str, rows: int) -> None:
    _drop_ora(cur, table)
    cur.execute(
        f"CREATE TABLE {table} (ID NUMBER NOT NULL PRIMARY KEY, LABEL VARCHAR2(32))"
    )
    cur.execute(
        f"INSERT INTO {table} (ID, LABEL) "
        f"SELECT LEVEL, 'r' || LEVEL FROM dual CONNECT BY LEVEL <= {int(rows)}"
    )


def test_live_oracle_pg_dest_count(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PG_MYSQL_COPY_WORKERS", "4")
    monkeypatch.delenv("DATAFLOW_ORACLE_PG_COPY", raising=False)
    pg = _pg_connect()
    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"ORA_PG_SRC_{tag}"
    dest = f"ora_pg_dst_{tag.lower()}"
    try:
        cur = ora.cursor()
        _seed_ora(cur, src, 800)
        ora.commit()
        with pg.cursor() as pcur:
            pcur.execute(f'DROP TABLE IF EXISTS public."{dest}"')
        result = copy_oracle_to_postgres(
            source_cfg=_ora_cfg(),
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
        with pg.cursor() as pcur:
            pcur.execute(f'SELECT COUNT(*) FROM public."{dest}"')
            assert int(pcur.fetchone()[0]) == 800
            pcur.execute(f'SELECT id, label FROM public."{dest}" WHERE id = 1')
            row = pcur.fetchone()
            assert int(row[0]) == 1
            assert str(row[1]) == "r1"
    finally:
        _drop_ora(ora.cursor(), src)
        ora.commit()
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{dest}"')
        pg.close()
        ora.close()


def test_live_oracle_pg_varchar2_empty_is_null():
    pg = _pg_connect()
    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"ORA_PG_NULL_{tag}"
    dest = f"ora_pg_null_d_{tag.lower()}"
    try:
        cur = ora.cursor()
        _drop_ora(cur, src)
        cur.execute(
            f"CREATE TABLE {src} (ID NUMBER NOT NULL PRIMARY KEY, LABEL VARCHAR2(32))"
        )
        cur.execute(
            f"INSERT INTO {src} (ID, LABEL) VALUES (1, NULL), (2, ''), (3, 'x')"
        )
        ora.commit()
        with pg.cursor() as pcur:
            pcur.execute(f'DROP TABLE IF EXISTS public."{dest}"')
        result = copy_oracle_to_postgres(
            source_cfg=_ora_cfg(),
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
        assert rows[1][1] is None
        assert int(rows[2][0]) == 3
        assert str(rows[2][1]) == "x"
    finally:
        _drop_ora(ora.cursor(), src)
        ora.commit()
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{dest}"')
        pg.close()
        ora.close()


def test_live_oracle_pg_occupied_without_pk_declines():
    pg = _pg_connect()
    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"ORA_PG_NOPK_{tag}"
    dest = f"ora_pg_nopk_d_{tag.lower()}"
    try:
        cur = ora.cursor()
        _drop_ora(cur, src)
        cur.execute(
            f"CREATE TABLE {src} (ID NUMBER NOT NULL, LABEL VARCHAR2(32))"
        )
        cur.execute(f"INSERT INTO {src} (ID, LABEL) VALUES (1, 'a'), (2, 'b')")
        ora.commit()
        with pg.cursor() as pcur:
            pcur.execute(f'DROP TABLE IF EXISTS public."{dest}"')
            pcur.execute(
                f'CREATE TABLE public."{dest}" (id bigint NOT NULL, label varchar(32))'
            )
            pcur.execute(f'INSERT INTO public."{dest}" (id, label) VALUES (1, \'old\')')
        with pytest.raises(FastPathUnavailable, match="non-empty"):
            copy_oracle_to_postgres(
                source_cfg=_ora_cfg(),
                source_table=src,
                dest_cfg=_pg_cfg(),
                dest_schema="public",
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                pg_ddls=["BIGINT", "VARCHAR(32)"],
                replace_destination=False,
            )
    finally:
        _drop_ora(ora.cursor(), src)
        ora.commit()
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{dest}"')
        pg.close()
        ora.close()


def test_live_oracle_pg_resume_skips_complete_range(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PG_MYSQL_COPY_WORKERS", "4")
    monkeypatch.delenv("DATAFLOW_ORACLE_PG_COPY", raising=False)
    pg = _pg_connect()
    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"ORA_PG_RSM_{tag}"
    dest = f"ora_pg_rsm_d_{tag.lower()}"
    try:
        cur = ora.cursor()
        _seed_ora(cur, src, 8000)
        ora.commit()
        with pg.cursor() as pcur:
            pcur.execute(f'DROP TABLE IF EXISTS public."{dest}"')
        first = copy_oracle_to_postgres(
            source_cfg=_ora_cfg(),
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
        with pg.cursor() as pcur:
            pcur.execute(f'DELETE FROM public."{dest}" WHERE id = %s', (lo,))
            pcur.execute(f'SELECT COUNT(*) FROM public."{dest}"')
            assert int(pcur.fetchone()[0]) == 7999
        second = copy_oracle_to_postgres(
            source_cfg=_ora_cfg(),
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
        with pg.cursor() as pcur:
            pcur.execute(f'SELECT COUNT(*) FROM public."{dest}"')
            assert int(pcur.fetchone()[0]) == 8000
    finally:
        _drop_ora(ora.cursor(), src)
        ora.commit()
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{dest}"')
        pg.close()
        ora.close()


def test_live_oracle_pg_stream_load_method(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PG_MYSQL_COPY_WORKERS", "4")
    monkeypatch.delenv("DATAFLOW_ORACLE_PG_COPY", raising=False)
    pg = _pg_connect()
    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"ORA_PG_STR_{tag}"
    dest = f"ora_pg_str_d_{tag.lower()}"
    try:
        cur = ora.cursor()
        _seed_ora(cur, src, 800)
        ora.commit()
        with pg.cursor() as pcur:
            pcur.execute(f'DROP TABLE IF EXISTS public."{dest}"')
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"ora-pg-copy-{tag.lower()}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_ora_cfg(), "format": "oracle", "table": src}
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
        assert summary.get("load_method") == "select_oracle_copy_from_stdin_pg"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("COPY FROM STDIN" in line for line in ddl_log)
        with pg.cursor() as pcur:
            pcur.execute(f'SELECT COUNT(*) FROM public."{dest}"')
            assert int(pcur.fetchone()[0]) == 800
    finally:
        _drop_ora(ora.cursor(), src)
        ora.commit()
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{dest}"')
        pg.close()
        ora.close()
