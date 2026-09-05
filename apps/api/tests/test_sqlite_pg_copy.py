"""SQLite SELECT → PostgreSQL COPY FROM STDIN — dest COUNT(*)."""

from __future__ import annotations

import socket
import sqlite3
import sys
import uuid
from datetime import date, datetime
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_fast_path import FastPathUnavailable  # noqa: E402
from services.copy_sqlite_common import (  # noqa: E402
    sqlite_copy_date_value,
    sqlite_copy_naive_datetime_value,
    sqlite_pg_type_is_copy_safe,
)
from services.copy_sqlite_pg import (  # noqa: E402
    copy_sqlite_to_postgres,
    sqlite_pg_copy_enabled,
    sqlite_value_to_pg_copy,
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


def _drop_pg(cur, table: str) -> None:
    cur.execute(f'DROP TABLE IF EXISTS public."{table}"')


def _dest_count(table: str) -> int:
    n = destination_row_count("postgresql", _pg_cfg(), schema="public", table_name=table)
    assert n is not None
    return int(n)


def _seed_sqlite(path: Path, table: str, rows: int) -> None:
    conn = sqlite3.connect(path)
    conn.execute(f'DROP TABLE IF EXISTS "{table}"')
    conn.execute(f'CREATE TABLE "{table}" (id INTEGER NOT NULL PRIMARY KEY, label TEXT)')
    conn.executemany(
        f'INSERT INTO "{table}" (id, label) VALUES (?, ?)',
        [(i, f"r{i}") for i in range(1, rows + 1)],
    )
    conn.commit()
    conn.close()


def test_sqlite_pg_copy_safe_types():
    assert sqlite_pg_type_is_copy_safe("INTEGER") is True
    assert sqlite_pg_type_is_copy_safe("TEXT") is True
    assert sqlite_pg_type_is_copy_safe("VARCHAR(32)") is True
    assert sqlite_pg_type_is_copy_safe("") is True
    assert sqlite_pg_type_is_copy_safe("DATE") is True
    assert sqlite_pg_type_is_copy_safe("DATETIME") is True
    assert sqlite_pg_type_is_copy_safe("TIMESTAMP") is True
    assert sqlite_pg_type_is_copy_safe("BLOB") is False
    assert sqlite_pg_type_is_copy_safe("BOOLEAN") is False
    assert sqlite_pg_type_is_copy_safe("JSON") is False
    assert sqlite_pg_type_is_copy_safe("TIMESTAMPTZ") is False
    assert sqlite_pg_type_is_copy_safe("TIMESTAMP WITH TIME ZONE") is False


def test_sqlite_pg_temporal_cell_proof():
    assert sqlite_copy_date_value("2024-03-15") == date(2024, 3, 15)
    assert sqlite_copy_date_value(date(2024, 3, 15)) == date(2024, 3, 15)
    assert sqlite_copy_date_value(None) is None
    with pytest.raises(FastPathUnavailable, match="DATETIME"):
        sqlite_copy_date_value("2024-03-15 12:00:00")
    with pytest.raises(FastPathUnavailable, match="ISO calendar-day"):
        sqlite_copy_date_value(1710460800)
    parsed = sqlite_copy_naive_datetime_value("2024-10-01 12:00:00")
    assert parsed == datetime(2024, 10, 1, 12, 0, 0)
    assert sqlite_copy_naive_datetime_value("2024-10-01T12:00:00") == datetime(
        2024, 10, 1, 12, 0, 0
    )
    assert sqlite_copy_naive_datetime_value(None) is None
    with pytest.raises(FastPathUnavailable, match="unix"):
        sqlite_copy_naive_datetime_value(1711929600)
    with pytest.raises(FastPathUnavailable, match="tz-aware"):
        sqlite_copy_naive_datetime_value("2024-10-01T12:00:00Z")
    with pytest.raises(FastPathUnavailable, match="tz-aware"):
        sqlite_copy_naive_datetime_value("2024-10-01 12:00:00+00:00")
    with pytest.raises(FastPathUnavailable, match="date-only"):
        sqlite_copy_naive_datetime_value("2024-10-01")
    with pytest.raises(FastPathUnavailable, match="invent 00:00:00"):
        sqlite_copy_naive_datetime_value(date(2024, 10, 1))
    assert sqlite_value_to_pg_copy("2024-03-15", "DATE") == "2024-03-15"
    assert sqlite_value_to_pg_copy("2024-10-01 12:00:00", "TIMESTAMP") == (
        "2024-10-01 12:00:00"
    )
    assert sqlite_value_to_pg_copy(None, "DATETIME") == "\\N"
    with pytest.raises(FastPathUnavailable, match="not PostgreSQL COPY-safe"):
        sqlite_value_to_pg_copy("2024-10-01 12:00:00", "TIMESTAMPTZ")


def test_sqlite_pg_copy_kill_switch(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_SQLITE_PG_COPY", "0")
    assert sqlite_pg_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_sqlite_to_postgres(
            source_cfg=_sqlite_cfg(tmp_path / "src.db", "missing"),
            source_table="missing",
            dest_cfg=_pg_cfg(),
            dest_schema="public",
            dest_table="nope",
            pairs=[("id", "id")],
            pg_ddls=["BIGINT"],
            replace_destination=True,
        )


def test_live_sqlite_pg_dest_count(monkeypatch, tmp_path):
    monkeypatch.delenv("DATAFLOW_SQLITE_PG_COPY", raising=False)
    pg = _pg_connect()
    tag = uuid.uuid4().hex[:8]
    src_path = tmp_path / "src.db"
    dest = f"sqlite_pg_dst_{tag}"
    _seed_sqlite(src_path, "src_t", 800)
    try:
        with pg.cursor() as cur:
            _drop_pg(cur, dest)
        pg.commit()
        result = copy_sqlite_to_postgres(
            source_cfg=_sqlite_cfg(src_path, "src_t"),
            source_table="src_t",
            dest_cfg=_pg_cfg(),
            dest_schema="public",
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            pg_ddls=["BIGINT", "TEXT"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("copy_split") == "serial"
        assert result.source_snapshot.get("sqlite_read") == "select"
        assert _dest_count(dest) == 800
    finally:
        with pg.cursor() as cur:
            _drop_pg(cur, dest)
        pg.commit()
        pg.close()


def test_live_sqlite_pg_date_iso_dest_count(tmp_path):
    pg = _pg_connect()
    tag = uuid.uuid4().hex[:8]
    src_path = tmp_path / "src.db"
    dest = f"sqlite_pg_date_{tag}"
    conn = sqlite3.connect(src_path)
    conn.execute("CREATE TABLE src_t (id INTEGER NOT NULL PRIMARY KEY, hired DATE)")
    conn.execute("INSERT INTO src_t (id, hired) VALUES (1, '2024-03-15'), (2, NULL)")
    conn.commit()
    conn.close()
    try:
        with pg.cursor() as cur:
            _drop_pg(cur, dest)
        pg.commit()
        result = copy_sqlite_to_postgres(
            source_cfg=_sqlite_cfg(src_path, "src_t"),
            source_table="src_t",
            dest_cfg=_pg_cfg(),
            dest_schema="public",
            dest_table=dest,
            pairs=[("id", "id"), ("hired", "hired")],
            pg_ddls=["BIGINT", "DATE"],
            replace_destination=True,
        )
        assert result.source_rows == 2
        assert _dest_count(dest) == 2
        with pg.cursor() as cur:
            cur.execute(f'SELECT id, hired FROM public."{dest}" ORDER BY id')
            rows = cur.fetchall()
        assert rows[0] == (1, date(2024, 3, 15))
        assert rows[1] == (2, None)
    finally:
        with pg.cursor() as cur:
            _drop_pg(cur, dest)
        pg.commit()
        pg.close()


def test_live_sqlite_pg_datetime_iso_dest_count(tmp_path):
    pg = _pg_connect()
    tag = uuid.uuid4().hex[:8]
    src_path = tmp_path / "src.db"
    dest = f"sqlite_pg_dt_{tag}"
    conn = sqlite3.connect(src_path)
    conn.execute(
        "CREATE TABLE src_t (id INTEGER NOT NULL PRIMARY KEY, updated_at DATETIME)"
    )
    conn.execute(
        "INSERT INTO src_t (id, updated_at) VALUES (1, '2024-10-01 12:00:00'), (2, NULL)"
    )
    conn.commit()
    conn.close()
    try:
        with pg.cursor() as cur:
            _drop_pg(cur, dest)
        pg.commit()
        result = copy_sqlite_to_postgres(
            source_cfg=_sqlite_cfg(src_path, "src_t"),
            source_table="src_t",
            dest_cfg=_pg_cfg(),
            dest_schema="public",
            dest_table=dest,
            pairs=[("id", "id"), ("updated_at", "updated_at")],
            pg_ddls=["BIGINT", "TIMESTAMP"],
            replace_destination=True,
        )
        assert result.source_rows == 2
        assert result.source_checksum == "dest_count:2"
        assert _dest_count(dest) == 2
        with pg.cursor() as cur:
            cur.execute(f'SELECT id, updated_at FROM public."{dest}" ORDER BY id')
            rows = cur.fetchall()
        assert rows[0] == (1, datetime(2024, 10, 1, 12, 0, 0))
        assert rows[1] == (2, None)
    finally:
        with pg.cursor() as cur:
            _drop_pg(cur, dest)
        pg.commit()
        pg.close()


def test_live_sqlite_pg_unix_datetime_declines(tmp_path):
    pg = _pg_connect()
    tag = uuid.uuid4().hex[:8]
    src_path = tmp_path / "src.db"
    dest = f"sqlite_pg_unix_{tag}"
    conn = sqlite3.connect(src_path)
    conn.execute(
        "CREATE TABLE src_t (id INTEGER NOT NULL PRIMARY KEY, updated_at DATETIME)"
    )
    conn.execute("INSERT INTO src_t (id, updated_at) VALUES (1, 1711929600)")
    conn.commit()
    conn.close()
    try:
        with pytest.raises(FastPathUnavailable, match="unix"):
            copy_sqlite_to_postgres(
                source_cfg=_sqlite_cfg(src_path, "src_t"),
                source_table="src_t",
                dest_cfg=_pg_cfg(),
                dest_schema="public",
                dest_table=dest,
                pairs=[("id", "id"), ("updated_at", "updated_at")],
                pg_ddls=["BIGINT", "TIMESTAMP"],
                replace_destination=True,
            )
    finally:
        with pg.cursor() as cur:
            _drop_pg(cur, dest)
        pg.commit()
        pg.close()


def test_live_sqlite_pg_empty_string_and_null_preserved(tmp_path):
    pg = _pg_connect()
    tag = uuid.uuid4().hex[:8]
    src_path = tmp_path / "src.db"
    dest = f"sqlite_pg_null_{tag}"
    conn = sqlite3.connect(src_path)
    conn.execute("CREATE TABLE src_t (id INTEGER NOT NULL PRIMARY KEY, label TEXT)")
    conn.executemany(
        "INSERT INTO src_t (id, label) VALUES (?, ?)",
        [(1, None), (2, ""), (3, "x")],
    )
    conn.commit()
    conn.close()
    try:
        with pg.cursor() as cur:
            _drop_pg(cur, dest)
        pg.commit()
        result = copy_sqlite_to_postgres(
            source_cfg=_sqlite_cfg(src_path, "src_t"),
            source_table="src_t",
            dest_cfg=_pg_cfg(),
            dest_schema="public",
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            pg_ddls=["BIGINT", "TEXT"],
            replace_destination=True,
        )
        assert result.source_rows == 3
        assert _dest_count(dest) == 3
        with pg.cursor() as cur:
            cur.execute(f'SELECT id, label FROM public."{dest}" ORDER BY id')
            rows = cur.fetchall()
        assert rows[0] == (1, None)
        assert rows[1] == (2, "")
        assert rows[2] == (3, "x")
    finally:
        with pg.cursor() as cur:
            _drop_pg(cur, dest)
        pg.commit()
        pg.close()


def test_live_sqlite_pg_skip_when_dest_count_matches(tmp_path):
    pg = _pg_connect()
    tag = uuid.uuid4().hex[:8]
    src_path = tmp_path / "src.db"
    dest = f"sqlite_pg_skip_{tag}"
    _seed_sqlite(src_path, "src_t", 800)
    try:
        with pg.cursor() as cur:
            _drop_pg(cur, dest)
        pg.commit()
        first = copy_sqlite_to_postgres(
            source_cfg=_sqlite_cfg(src_path, "src_t"),
            source_table="src_t",
            dest_cfg=_pg_cfg(),
            dest_schema="public",
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            pg_ddls=["BIGINT", "TEXT"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_sqlite_to_postgres(
            source_cfg=_sqlite_cfg(src_path, "src_t"),
            source_table="src_t",
            dest_cfg=_pg_cfg(),
            dest_schema="public",
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            pg_ddls=["BIGINT", "TEXT"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert second.source_snapshot.get("partitions_skipped") == 1
        assert _dest_count(dest) == 800
    finally:
        with pg.cursor() as cur:
            _drop_pg(cur, dest)
        pg.commit()
        pg.close()


def test_live_sqlite_pg_occupied_mismatch_declines(tmp_path):
    pg = _pg_connect()
    tag = uuid.uuid4().hex[:8]
    src_path = tmp_path / "src.db"
    dest = f"sqlite_pg_occ_{tag}"
    _seed_sqlite(src_path, "src_t", 800)
    try:
        with pg.cursor() as cur:
            _drop_pg(cur, dest)
            cur.execute(
                f'CREATE TABLE public."{dest}" ('
                "id BIGINT NOT NULL PRIMARY KEY, label TEXT)"
            )
            cur.execute(f'INSERT INTO public."{dest}" (id, label) VALUES (1, \'g\'), (2, \'g\')')
        pg.commit()
        assert _dest_count(dest) == 2
        with pytest.raises(FastPathUnavailable, match="occupied PostgreSQL dest"):
            copy_sqlite_to_postgres(
                source_cfg=_sqlite_cfg(src_path, "src_t"),
                source_table="src_t",
                dest_cfg=_pg_cfg(),
                dest_schema="public",
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                pg_ddls=["BIGINT", "TEXT"],
                replace_destination=False,
            )
        assert _dest_count(dest) == 2
    finally:
        with pg.cursor() as cur:
            _drop_pg(cur, dest)
        pg.commit()
        pg.close()


def test_live_sqlite_pg_overwrite_replaces_dest(tmp_path):
    pg = _pg_connect()
    tag = uuid.uuid4().hex[:8]
    src_path = tmp_path / "src.db"
    dest = f"sqlite_pg_ow_{tag}"
    _seed_sqlite(src_path, "src_t", 800)
    try:
        with pg.cursor() as cur:
            _drop_pg(cur, dest)
            cur.execute(
                f'CREATE TABLE public."{dest}" ('
                "id BIGINT NOT NULL PRIMARY KEY, label TEXT)"
            )
            cur.execute(f'INSERT INTO public."{dest}" (id, label) VALUES (1, \'ghost\')')
        pg.commit()
        result = copy_sqlite_to_postgres(
            source_cfg=_sqlite_cfg(src_path, "src_t"),
            source_table="src_t",
            dest_cfg=_pg_cfg(),
            dest_schema="public",
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            pg_ddls=["BIGINT", "TEXT"],
            replace_destination=True,
        )
        assert result.target_rows == 800
        assert _dest_count(dest) == 800
    finally:
        with pg.cursor() as cur:
            _drop_pg(cur, dest)
        pg.commit()
        pg.close()


def test_live_sqlite_pg_count_mismatch_rolls_back(monkeypatch, tmp_path):
    """Dest COUNT is checked before commit — a short COPY cannot land."""
    monkeypatch.delenv("DATAFLOW_SQLITE_PG_COPY", raising=False)
    pg = _pg_connect()
    tag = uuid.uuid4().hex[:8]
    src_path = tmp_path / "src.db"
    dest = f"sqlite_pg_rb_{tag}"
    _seed_sqlite(src_path, "src_t", 80)

    class _EmptyReader:
        def readable(self) -> bool:
            return True

        def read(self, size: int = -1) -> bytes:
            return b""

    monkeypatch.setattr(
        "services.copy_sqlite_pg._SqliteCopyReader",
        lambda *_a, **_k: _EmptyReader(),
    )
    try:
        with pg.cursor() as cur:
            _drop_pg(cur, dest)
        pg.commit()
        with pytest.raises(ValueError, match="dest COUNT"):
            copy_sqlite_to_postgres(
                source_cfg=_sqlite_cfg(src_path, "src_t"),
                source_table="src_t",
                dest_cfg=_pg_cfg(),
                dest_schema="public",
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                pg_ddls=["BIGINT", "TEXT"],
                replace_destination=True,
            )
        n = destination_row_count(
            "postgresql", _pg_cfg(), schema="public", table_name=dest
        )
        assert n in {0, None}
    finally:
        with pg.cursor() as cur:
            _drop_pg(cur, dest)
        pg.commit()
        pg.close()


def test_live_sqlite_pg_stream_load_method(monkeypatch, tmp_path):
    monkeypatch.delenv("DATAFLOW_SQLITE_PG_COPY", raising=False)
    pg = _pg_connect()
    tag = uuid.uuid4().hex[:8]
    src_path = tmp_path / "src.db"
    dest = f"sqlite_pg_str_{tag}"
    _seed_sqlite(src_path, "src_t", 800)
    try:
        with pg.cursor() as cur:
            _drop_pg(cur, dest)
        pg.commit()
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"sqlite-pg-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_sqlite_cfg(src_path, "src_t"), "format": "sqlite"}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_pg_cfg(), "format": "postgresql", "table": dest}
        )
        mappings = [
            {"source": "id", "target": "id", "type": "INTEGER", "transform": "none"},
            {"source": "label", "target": "label", "type": "TEXT", "transform": "none"},
        ]
        transferred, ddl_log, summary, _cols = stream_database_transfer(
            source,
            destination,
            mappings,
            {"id": "INTEGER", "label": "TEXT"},
            sync_mode="full_refresh_overwrite",
            job_id=job_id,
        )
        assert transferred == 800
        assert summary.get("load_method") == "select_sqlite_copy_from_stdin_pg"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("PostgreSQL" in line for line in ddl_log)
        assert _dest_count(dest) == 800
    finally:
        with pg.cursor() as cur:
            _drop_pg(cur, dest)
        pg.commit()
        pg.close()
