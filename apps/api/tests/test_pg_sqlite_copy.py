"""PostgreSQL COPY text → SQLite executemany — dest COUNT(*)."""

from __future__ import annotations

import socket
import sqlite3
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_fast_path import FastPathUnavailable  # noqa: E402
from services.copy_pg_sqlite import (  # noqa: E402
    copy_postgres_to_sqlite,
    pg_sqlite_copy_enabled,
    pg_sqlite_type_is_copy_safe,
)
from services.dest_precount import destination_row_count  # noqa: E402


def _pg_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 5432), timeout=1):
            pass
    except OSError:
        pytest.skip("PostgreSQL 5432 not reachable")


def _pg_cfg() -> dict:
    return {
        "host": "127.0.0.1",
        "port": 5432,
        "database": "dataflow",
        "username": "dataflow",
        "password": "dataflow",
        "schema": "public",
    }


def _sqlite_cfg(path: Path | str, table: str) -> dict:
    return {
        "type": "sqlite",
        "format": "sqlite",
        "database": str(path),
        "table": table,
    }


def _pg_connect():
    _pg_or_skip()
    psycopg2 = pytest.importorskip("psycopg2")
    try:
        return psycopg2.connect(
            host="127.0.0.1",
            port=5432,
            user="dataflow",
            password="dataflow",
            dbname="dataflow",
        )
    except Exception as exc:
        pytest.skip(f"PostgreSQL auth failed: {exc}")


def _seed_pg(cur, table: str, rows: int) -> None:
    cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
    cur.execute(
        f'CREATE TABLE public."{table}" ('
        "id BIGINT NOT NULL PRIMARY KEY, label VARCHAR(32) NULL)"
    )
    cur.execute(
        f'INSERT INTO public."{table}" (id, label) '
        f"SELECT g, 'r' || g::text FROM generate_series(1, {int(rows)}) g"
    )


def _drop_pg(cur, table: str) -> None:
    cur.execute(f'DROP TABLE IF EXISTS public."{table}"')


def _dest_count(path: Path | str, table: str) -> int:
    n = destination_row_count(
        "sqlite", _sqlite_cfg(path, table), schema="", table_name=table
    )
    assert n is not None
    return int(n)


def test_pg_sqlite_copy_safe_types():
    assert pg_sqlite_type_is_copy_safe("BIGINT") is True
    assert pg_sqlite_type_is_copy_safe("VARCHAR(32)") is True
    assert pg_sqlite_type_is_copy_safe("DATE") is True
    assert pg_sqlite_type_is_copy_safe("BOOLEAN") is True
    assert pg_sqlite_type_is_copy_safe("JSONB") is False
    assert pg_sqlite_type_is_copy_safe("BYTEA") is False
    assert pg_sqlite_type_is_copy_safe("TIMESTAMPTZ") is False
    assert pg_sqlite_type_is_copy_safe("TIMESTAMP") is False


def test_pg_sqlite_copy_kill_switch(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_PG_SQLITE_COPY", "0")
    assert pg_sqlite_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_postgres_to_sqlite(
            source_cfg=_pg_cfg(),
            source_table="missing",
            dest_cfg=_sqlite_cfg(tmp_path / "nope.db", "nope"),
            dest_table="nope",
            pairs=[("id", "id")],
            sqlite_ddls=["INTEGER"],
            replace_destination=True,
        )


def test_live_pg_sqlite_dest_count(monkeypatch, tmp_path):
    monkeypatch.delenv("DATAFLOW_PG_SQLITE_COPY", raising=False)
    pg = _pg_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"pg_sqlite_src_{tag}"
    dest = tmp_path / "dst.db"
    try:
        with pg.cursor() as cur:
            _seed_pg(cur, src, 800)
        pg.commit()
        result = copy_postgres_to_sqlite(
            source_cfg=_pg_cfg(),
            source_schema="public",
            source_table=src,
            dest_cfg=_sqlite_cfg(dest, "dst_t"),
            dest_table="dst_t",
            pairs=[("id", "id"), ("label", "label")],
            sqlite_ddls=["BIGINT", "TEXT"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("copy_split") == "serial"
        assert result.source_snapshot.get("sqlite_write") == "insert"
        assert _dest_count(dest, "dst_t") == 800
    finally:
        with pg.cursor() as cur:
            _drop_pg(cur, src)
        pg.commit()
        pg.close()


def test_live_pg_sqlite_date_lands_as_text(tmp_path):
    pg = _pg_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"pg_sqlite_date_{tag}"
    dest = tmp_path / "dst.db"
    try:
        with pg.cursor() as cur:
            _drop_pg(cur, src)
            cur.execute(
                f'CREATE TABLE public."{src}" ('
                "id BIGINT NOT NULL PRIMARY KEY, hired DATE NULL)"
            )
            cur.execute(
                f'INSERT INTO public."{src}" (id, hired) VALUES '
                "(1, DATE '2024-03-15'), (2, NULL)"
            )
        pg.commit()
        result = copy_postgres_to_sqlite(
            source_cfg=_pg_cfg(),
            source_schema="public",
            source_table=src,
            dest_cfg=_sqlite_cfg(dest, "dst_t"),
            dest_table="dst_t",
            pairs=[("id", "id"), ("hired", "hired")],
            sqlite_ddls=["BIGINT", "DATE"],
            replace_destination=True,
        )
        assert result.target_rows == 2
        assert _dest_count(dest, "dst_t") == 2
        conn = sqlite3.connect(dest)
        rows = conn.execute("SELECT id, hired, typeof(hired) FROM dst_t ORDER BY id").fetchall()
        conn.close()
        assert rows[0][1] == "2024-03-15"
        assert rows[0][2] == "text"
        assert rows[1][1] is None
    finally:
        with pg.cursor() as cur:
            _drop_pg(cur, src)
        pg.commit()
        pg.close()


def test_live_pg_sqlite_empty_string_and_null_preserved(tmp_path):
    pg = _pg_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"pg_sqlite_null_{tag}"
    dest = tmp_path / "dst.db"
    try:
        with pg.cursor() as cur:
            _drop_pg(cur, src)
            cur.execute(
                f'CREATE TABLE public."{src}" ('
                "id BIGINT NOT NULL PRIMARY KEY, label VARCHAR(32) NULL)"
            )
            cur.execute(
                f'INSERT INTO public."{src}" (id, label) VALUES '
                "(1, NULL), (2, ''), (3, 'x')"
            )
        pg.commit()
        result = copy_postgres_to_sqlite(
            source_cfg=_pg_cfg(),
            source_schema="public",
            source_table=src,
            dest_cfg=_sqlite_cfg(dest, "dst_t"),
            dest_table="dst_t",
            pairs=[("id", "id"), ("label", "label")],
            sqlite_ddls=["BIGINT", "TEXT"],
            replace_destination=True,
        )
        assert result.source_rows == 3
        assert _dest_count(dest, "dst_t") == 3
        conn = sqlite3.connect(dest)
        rows = conn.execute("SELECT id, label FROM dst_t ORDER BY id").fetchall()
        conn.close()
        assert rows[0] == (1, None)
        assert rows[1] == (2, "")
        assert rows[2] == (3, "x")
    finally:
        with pg.cursor() as cur:
            _drop_pg(cur, src)
        pg.commit()
        pg.close()


def test_live_pg_sqlite_skip_when_dest_count_matches(tmp_path):
    pg = _pg_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"pg_sqlite_skip_{tag}"
    dest = tmp_path / "dst.db"
    try:
        with pg.cursor() as cur:
            _seed_pg(cur, src, 800)
        pg.commit()
        first = copy_postgres_to_sqlite(
            source_cfg=_pg_cfg(),
            source_schema="public",
            source_table=src,
            dest_cfg=_sqlite_cfg(dest, "dst_t"),
            dest_table="dst_t",
            pairs=[("id", "id"), ("label", "label")],
            sqlite_ddls=["BIGINT", "TEXT"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_postgres_to_sqlite(
            source_cfg=_pg_cfg(),
            source_schema="public",
            source_table=src,
            dest_cfg=_sqlite_cfg(dest, "dst_t"),
            dest_table="dst_t",
            pairs=[("id", "id"), ("label", "label")],
            sqlite_ddls=["BIGINT", "TEXT"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert second.source_snapshot.get("partitions_skipped") == 1
        assert _dest_count(dest, "dst_t") == 800
    finally:
        with pg.cursor() as cur:
            _drop_pg(cur, src)
        pg.commit()
        pg.close()


def test_live_pg_sqlite_occupied_mismatch_declines(tmp_path):
    pg = _pg_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"pg_sqlite_occ_{tag}"
    dest = tmp_path / "dst.db"
    try:
        with pg.cursor() as cur:
            _seed_pg(cur, src, 800)
        pg.commit()
        conn = sqlite3.connect(dest)
        conn.execute("CREATE TABLE dst_t (id INTEGER NOT NULL PRIMARY KEY, label TEXT)")
        conn.executemany("INSERT INTO dst_t (id, label) VALUES (?, ?)", [(1, "g"), (2, "g")])
        conn.commit()
        conn.close()
        assert _dest_count(dest, "dst_t") == 2
        with pytest.raises(FastPathUnavailable, match="occupied SQLite dest"):
            copy_postgres_to_sqlite(
                source_cfg=_pg_cfg(),
                source_schema="public",
                source_table=src,
                dest_cfg=_sqlite_cfg(dest, "dst_t"),
                dest_table="dst_t",
                pairs=[("id", "id"), ("label", "label")],
                sqlite_ddls=["BIGINT", "TEXT"],
                replace_destination=False,
            )
        assert _dest_count(dest, "dst_t") == 2
    finally:
        with pg.cursor() as cur:
            _drop_pg(cur, src)
        pg.commit()
        pg.close()


def test_live_pg_sqlite_overwrite_replaces_dest(tmp_path):
    pg = _pg_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"pg_sqlite_ow_{tag}"
    dest = tmp_path / "dst.db"
    try:
        with pg.cursor() as cur:
            _seed_pg(cur, src, 800)
        pg.commit()
        conn = sqlite3.connect(dest)
        conn.execute("CREATE TABLE dst_t (id INTEGER NOT NULL PRIMARY KEY, label TEXT)")
        conn.execute("INSERT INTO dst_t (id, label) VALUES (1, 'ghost')")
        conn.commit()
        conn.close()
        result = copy_postgres_to_sqlite(
            source_cfg=_pg_cfg(),
            source_schema="public",
            source_table=src,
            dest_cfg=_sqlite_cfg(dest, "dst_t"),
            dest_table="dst_t",
            pairs=[("id", "id"), ("label", "label")],
            sqlite_ddls=["BIGINT", "TEXT"],
            replace_destination=True,
        )
        assert result.source_snapshot.get("sqlite_write") == "overwrite"
        assert _dest_count(dest, "dst_t") == 800
    finally:
        with pg.cursor() as cur:
            _drop_pg(cur, src)
        pg.commit()
        pg.close()


def test_live_pg_sqlite_stream_load_method(monkeypatch, tmp_path):
    monkeypatch.delenv("DATAFLOW_PG_SQLITE_COPY", raising=False)
    pg = _pg_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"pg_sqlite_str_{tag}"
    dest = tmp_path / "dst.db"
    try:
        with pg.cursor() as cur:
            _seed_pg(cur, src, 800)
        pg.commit()
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"pg-sqlite-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_pg_cfg(), "format": "postgresql", "table": src}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_sqlite_cfg(dest, "dst_t"), "format": "sqlite"}
        )
        mappings = [
            {"source": "id", "target": "id", "type": "BIGINT", "transform": "none"},
            {"source": "label", "target": "label", "type": "TEXT", "transform": "none"},
        ]
        transferred, ddl_log, summary, _cols = stream_database_transfer(
            source,
            destination,
            mappings,
            {"id": "BIGINT", "label": "TEXT"},
            sync_mode="full_refresh_overwrite",
            job_id=job_id,
        )
        assert transferred == 800
        assert summary.get("load_method") == "copy_text_pg_executemany_sqlite"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("SQLite" in line for line in ddl_log)
        assert _dest_count(dest, "dst_t") == 800
    finally:
        with pg.cursor() as cur:
            _drop_pg(cur, src)
        pg.commit()
        pg.close()
