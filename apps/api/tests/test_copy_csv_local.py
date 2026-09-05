"""Local CSV identity COPY: full refresh + incremental into SQLite / PG / MySQL."""

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

from services.copy_csv_local import (  # noqa: E402
    csv_copy_load_method,
    identity_csv_copy_route,
    try_copy_local_csv,
)
from services.sync_cursor import get_watermark  # noqa: E402
from src.transfer.file_stream import stream_file_to_database  # noqa: E402
from src.transfer.models import EndpointConfig  # noqa: E402


class _FakeCheckpointService:
    def __init__(self) -> None:
        self.checkpoints: dict[str, dict] = {}
        self.failed_saves = 0

    @property
    def has_failed_saves(self) -> bool:
        return self.failed_saves > 0

    def save(self, checkpoint) -> bool:
        self.checkpoints[checkpoint.job_id] = checkpoint.to_dict()
        return True

    def require_save(self, checkpoint) -> None:
        self.save(checkpoint)

    def load(self, job_id: str):
        return self.checkpoints.get(job_id)


def _pg_up() -> bool:
    try:
        socket.create_connection(("127.0.0.1", 5432), timeout=1).close()
        return True
    except OSError:
        return False


def _mysql_up() -> bool:
    try:
        socket.create_connection(("127.0.0.1", 3306), timeout=1).close()
        return True
    except OSError:
        return False


def _csv(rows: list[tuple[int, str, str]]) -> bytes:
    body = "id,name,updated_at\n" + "\n".join(
        f"{i},{name},{ts}" for i, name, ts in rows
    )
    return body.encode("utf-8")


DAY1 = [
    (1, "one", "2024-06-01T00:00:00"),
    (2, "two", "2024-06-01T00:00:00"),
    (3, "three", "2024-06-02T00:00:00"),
]
DAY2 = [
    (1, "ONE", "2024-06-03T00:00:00"),
    (3, "three", "2024-06-02T00:00:00"),
    (4, "four", "2024-06-03T00:00:00"),
]
APPEND_DAY1 = [
    (1, "a", "2024-06-01T00:00:00"),
    (2, "b", "2024-06-01T00:00:00"),
]
APPEND_DAY2 = APPEND_DAY1 + [(3, "c", "2024-06-02T00:00:00")]


def _isolate_cursor_store(monkeypatch, tmp_path) -> None:
    from services import sync_cursor

    monkeypatch.setattr(sync_cursor, "STORE_PATH", tmp_path / "cursors.json")
    monkeypatch.setattr(sync_cursor, "_mongo_cursors", lambda: None)


def _mappings() -> list[dict]:
    return [
        {"source": "id", "target": "id", "type": "INTEGER", "transform": "none"},
        {"source": "name", "target": "name", "type": "TEXT", "transform": "none"},
        {
            "source": "updated_at",
            "target": "updated_at",
            "type": "TEXT",
            "transform": "none",
        },
    ]


def _schema() -> dict[str, str]:
    return {"id": "INTEGER", "name": "TEXT", "updated_at": "TEXT"}


def _contracts(sync_mode: str) -> list[dict]:
    return [
        {
            "selected": True,
            "sync_mode": sync_mode,
            "cursor_field": "updated_at",
            "cursor_semantics": "modification_timestamp",
            "primary_key": "id",
        }
    ]


def _run(
    payload: bytes,
    dest: EndpointConfig,
    cp: _FakeCheckpointService,
    *,
    sync_mode: str,
    job_id: str,
) -> tuple[int, dict]:
    from services.million_row_proof import ensure_memory_job_store_if_mongo_down

    ensure_memory_job_store_if_mongo_down()
    written, _ddl, summary, _ = stream_file_to_database(
        payload,
        "events.csv",
        dest,
        _mappings(),
        _schema(),
        job_id=job_id,
        checkpoint_service=cp,
        sync_mode=sync_mode,
        stream_contracts=_contracts(sync_mode),
    )
    return written, summary


def test_identity_csv_copy_route_sql_core_only():
    assert identity_csv_copy_route("csv", "sqlite")
    assert identity_csv_copy_route("tsv", "postgresql")
    assert identity_csv_copy_route("csv", "mysql")
    assert not identity_csv_copy_route("json", "sqlite")
    assert not identity_csv_copy_route("csv", "snowflake")
    assert not identity_csv_copy_route("yaml", "postgresql")


def test_csv_copy_load_method_tokens():
    assert csv_copy_load_method("sqlite", "incremental_deduped") == (
        "csv_executemany_sqlite_incremental_deduped"
    )
    assert csv_copy_load_method("postgresql", "incremental_append") == (
        "csv_copy_from_stdin_pg_incremental_append"
    )
    assert csv_copy_load_method("mysql", "full_refresh_overwrite") == "csv_load_data_mysql"


def test_try_copy_declines_json_and_transforms(tmp_path):
    dest = EndpointConfig(
        kind="database",
        format="sqlite",
        database=str(tmp_path / "x.db"),
        table="events",
    )
    dest_cfg = {"format": "sqlite", "database": str(tmp_path / "x.db"), "table": "events"}
    declined = try_copy_local_csv(
        content=_csv(DAY1),
        filename="events.json",
        file_type="json",
        dest_type="sqlite",
        dest_cfg=dest_cfg,
        dest_table="events",
        dest_schema="",
        mappings=_mappings(),
        schema=_schema(),
        effective_sync="full_refresh_overwrite",
    )
    assert declined is None
    transformed = try_copy_local_csv(
        content=_csv(DAY1),
        filename="events.csv",
        file_type="csv",
        dest_type="sqlite",
        dest_cfg=dest_cfg,
        dest_table="events",
        dest_schema="",
        mappings=[
            {
                "source": "id",
                "target": "id",
                "type": "INTEGER",
                "transform": "upper",
            }
        ],
        schema={"id": "INTEGER"},
        effective_sync="full_refresh_overwrite",
    )
    assert transformed is None
    del dest


def test_csv_sqlite_overwrite_dest_count_equals_source(tmp_path):
    from services.million_row_proof import ensure_memory_job_store_if_mongo_down

    ensure_memory_job_store_if_mongo_down()
    dest_path = tmp_path / "ow.db"
    dest = EndpointConfig(
        kind="database",
        format="sqlite",
        database=str(dest_path),
        table="events",
    )
    cp = _FakeCheckpointService()
    written, summary = _run(
        _csv(DAY1),
        dest,
        cp,
        sync_mode="full_refresh_overwrite",
        job_id="csv-ow-1",
    )
    assert written == 3
    assert summary.get("copy_fast_path") == "used"
    assert summary.get("load_method") == "csv_executemany_sqlite"
    conn = sqlite3.connect(dest_path)
    try:
        assert int(conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]) == 3
    finally:
        conn.close()
    written2, summary2 = _run(
        _csv(APPEND_DAY1),
        dest,
        cp,
        sync_mode="full_refresh_overwrite",
        job_id="csv-ow-2",
    )
    assert written2 == 2
    assert summary2.get("copy_fast_path") == "used"
    conn = sqlite3.connect(dest_path)
    try:
        dest_count = int(conn.execute("SELECT COUNT(*) FROM events").fetchone()[0])
        names = [r[0] for r in conn.execute("SELECT name FROM events ORDER BY id")]
    finally:
        conn.close()
    assert dest_count == 2
    assert names == ["a", "b"]


def test_csv_sqlite_incremental_deduped_delta_and_noop(monkeypatch, tmp_path):
    _isolate_cursor_store(monkeypatch, tmp_path)
    dest_path = tmp_path / "inc.db"
    dest = EndpointConfig(
        kind="database",
        format="sqlite",
        database=str(dest_path),
        table="events",
    )
    cp = _FakeCheckpointService()
    first, summary1 = _run(
        _csv(DAY1),
        dest,
        cp,
        sync_mode="incremental_deduped",
        job_id="csv-sqlite-a",
    )
    assert first == 3, summary1
    assert summary1.get("copy_fast_path") == "used"
    assert "incremental_deduped" in str(summary1.get("load_method") or "")
    assert "sqlite" in str(summary1.get("load_method") or "")
    conn = sqlite3.connect(dest_path)
    try:
        tick1 = int(conn.execute("SELECT COUNT(*) FROM events").fetchone()[0])
    finally:
        conn.close()
    assert tick1 == 3
    wm1 = str(summary1.get("incremental_watermark") or "")
    assert wm1
    second, summary2 = _run(
        _csv(DAY2),
        dest,
        cp,
        sync_mode="incremental_deduped",
        job_id="csv-sqlite-b",
    )
    assert second == 2, summary2
    conn = sqlite3.connect(dest_path)
    try:
        dest_count = int(conn.execute("SELECT COUNT(*) FROM events").fetchone()[0])
        name = conn.execute("SELECT name FROM events WHERE id = 1").fetchone()[0]
        staging_left = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            ("_df_stg_events",),
        ).fetchone()
    finally:
        conn.close()
    assert dest_count == 4
    assert name == "ONE"
    assert staging_left is None
    assert int((summary2.get("source_snapshot") or {}).get("dest_count") or 0) == 4
    cursor_key = str(summary2.get("cursor_key") or "")
    stored = get_watermark(cursor_key)
    assert stored
    assert stored != wm1
    third, summary3 = _run(
        _csv(DAY2),
        dest,
        cp,
        sync_mode="incremental_deduped",
        job_id="csv-sqlite-c",
    )
    assert third == 0
    assert summary3.get("source_row_count") == 0
    assert get_watermark(cursor_key) == stored
    conn = sqlite3.connect(dest_path)
    try:
        assert int(conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]) == 4
    finally:
        conn.close()


def test_csv_sqlite_incremental_append_delta_and_noop(monkeypatch, tmp_path):
    _isolate_cursor_store(monkeypatch, tmp_path)
    dest_path = tmp_path / "inc_append.db"
    dest = EndpointConfig(
        kind="database",
        format="sqlite",
        database=str(dest_path),
        table="events",
    )
    cp = _FakeCheckpointService()
    first, summary1 = _run(
        _csv(APPEND_DAY1),
        dest,
        cp,
        sync_mode="incremental_append",
        job_id="csv-sqlite-ap-a",
    )
    assert first == 2, summary1
    assert "incremental_append" in str(summary1.get("load_method") or "")
    conn = sqlite3.connect(dest_path)
    try:
        tick1 = int(conn.execute("SELECT COUNT(*) FROM events").fetchone()[0])
    finally:
        conn.close()
    assert tick1 == 2
    second, summary2 = _run(
        _csv(APPEND_DAY2),
        dest,
        cp,
        sync_mode="incremental_append",
        job_id="csv-sqlite-ap-b",
    )
    assert second == 1, summary2
    conn = sqlite3.connect(dest_path)
    try:
        dest_count = int(conn.execute("SELECT COUNT(*) FROM events").fetchone()[0])
        names = [r[0] for r in conn.execute("SELECT name FROM events ORDER BY id")]
    finally:
        conn.close()
    assert dest_count == 3
    assert names == ["a", "b", "c"]
    assert int((summary2.get("source_snapshot") or {}).get("dest_count") or 0) == 3
    stored = get_watermark(str(summary2.get("cursor_key") or ""))
    assert stored
    third, summary3 = _run(
        _csv(APPEND_DAY2),
        dest,
        cp,
        sync_mode="incremental_append",
        job_id="csv-sqlite-ap-c",
    )
    assert third == 0
    assert summary3.get("source_row_count") == 0
    conn = sqlite3.connect(dest_path)
    try:
        assert int(conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]) == 3
    finally:
        conn.close()


def test_csv_incremental_refuses_unbounded_cursor(monkeypatch, tmp_path):
    _isolate_cursor_store(monkeypatch, tmp_path)
    dest = EndpointConfig(
        kind="database",
        format="sqlite",
        database=str(tmp_path / "bad.db"),
        table="events",
    )
    cp = _FakeCheckpointService()
    with pytest.raises(ValueError, match="no value for cursor"):
        _run(
            _csv(DAY1) + b"\n4,blank,\n",
            dest,
            cp,
            sync_mode="incremental_deduped",
            job_id="csv-unbounded",
        )


def test_try_copy_declines_occupied_append_count_mismatch(tmp_path):
    dest_path = tmp_path / "occ.db"
    dest_cfg = {"format": "sqlite", "database": str(dest_path), "table": "events"}
    first = try_copy_local_csv(
        content=_csv(APPEND_DAY1),
        filename="events.csv",
        file_type="csv",
        dest_type="sqlite",
        dest_cfg=dest_cfg,
        dest_table="events",
        dest_schema="",
        mappings=_mappings(),
        schema=_schema(),
        effective_sync="full_refresh_overwrite",
    )
    assert first is not None
    second = try_copy_local_csv(
        content=_csv(DAY1),
        filename="events.csv",
        file_type="csv",
        dest_type="sqlite",
        dest_cfg=dest_cfg,
        dest_table="events",
        dest_schema="",
        mappings=_mappings(),
        schema=_schema(),
        effective_sync="full_refresh_append",
    )
    assert second is None


@pytest.mark.skipif(not _pg_up(), reason="PostgreSQL not on 5432")
def test_csv_pg_incremental_deduped_delta_and_noop(monkeypatch, tmp_path):
    _isolate_cursor_store(monkeypatch, tmp_path)
    psycopg2 = pytest.importorskip("psycopg2")
    suffix = uuid.uuid4().hex[:8]
    table = f"csv_inc_{suffix}"
    dest = EndpointConfig.from_dict(
        "database",
        {
            "format": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "dataflow",
            "schema": "public",
            "username": "dataflow",
            "password": "dataflow",
            "table": table,
        },
    )
    cp = _FakeCheckpointService()
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        user="dataflow",
        password="dataflow",
        dbname="dataflow",
    )
    conn.autocommit = True
    try:
        first, summary1 = _run(
            _csv(DAY1),
            dest,
            cp,
            sync_mode="incremental_deduped",
            job_id=f"csv-pg-a-{suffix}",
        )
        assert first == 3, summary1
        assert summary1.get("copy_fast_path") == "used"
        assert "incremental_deduped" in str(summary1.get("load_method") or "")
        assert "pg" in str(summary1.get("load_method") or "")
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM public."{table}"')
            tick1 = int(cur.fetchone()[0])
        assert tick1 == 3
        second, summary2 = _run(
            _csv(DAY2),
            dest,
            cp,
            sync_mode="incremental_deduped",
            job_id=f"csv-pg-b-{suffix}",
        )
        assert second == 2, summary2
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM public."{table}"')
            dest_count = int(cur.fetchone()[0])
            cur.execute(f'SELECT name FROM public."{table}" WHERE id = 1')
            name = cur.fetchone()[0]
            cur.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = %s",
                (f"_df_stg_{table}",),
            )
            staging_left = cur.fetchone()
        assert dest_count == 4
        assert name == "ONE"
        assert staging_left is None
        stored = get_watermark(str(summary2.get("cursor_key") or ""))
        assert stored
        third, summary3 = _run(
            _csv(DAY2),
            dest,
            cp,
            sync_mode="incremental_deduped",
            job_id=f"csv-pg-c-{suffix}",
        )
        assert third == 0
        assert summary3.get("source_row_count") == 0
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM public."{table}"')
            assert int(cur.fetchone()[0]) == 4
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
        conn.close()


@pytest.mark.skipif(not _mysql_up(), reason="MySQL not on 3306")
def test_csv_mysql_incremental_append_delta_and_noop(monkeypatch, tmp_path):
    _isolate_cursor_store(monkeypatch, tmp_path)
    pymysql = pytest.importorskip("pymysql")
    suffix = uuid.uuid4().hex[:8]
    table = f"csv_inc_{suffix}"
    dest = EndpointConfig.from_dict(
        "database",
        {
            "format": "mysql",
            "host": "127.0.0.1",
            "port": 3306,
            "database": "dataflow",
            "username": "dataflow",
            "password": "dataflow",
            "table": table,
        },
    )
    cp = _FakeCheckpointService()
    conn = pymysql.connect(
        host="127.0.0.1",
        port=3306,
        user="dataflow",
        password="dataflow",
        database="dataflow",
        autocommit=True,
    )
    try:
        first, summary1 = _run(
            _csv(APPEND_DAY1),
            dest,
            cp,
            sync_mode="incremental_append",
            job_id=f"csv-mysql-a-{suffix}",
        )
        assert first == 2, summary1
        assert summary1.get("copy_fast_path") == "used"
        assert "incremental_append" in str(summary1.get("load_method") or "")
        assert "mysql" in str(summary1.get("load_method") or "")
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{table}`")
            tick1 = int(cur.fetchone()[0])
        assert tick1 == 2
        second, summary2 = _run(
            _csv(APPEND_DAY2),
            dest,
            cp,
            sync_mode="incremental_append",
            job_id=f"csv-mysql-b-{suffix}",
        )
        assert second == 1, summary2
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{table}`")
            dest_count = int(cur.fetchone()[0])
            cur.execute(f"SELECT name FROM `{table}` ORDER BY id")
            names = [r[0] for r in cur.fetchall()]
            cur.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_name = %s",
                (f"_df_stg_{table}",),
            )
            staging_left = cur.fetchone()
        assert dest_count == 3
        assert names == ["a", "b", "c"]
        assert staging_left is None
        stored = get_watermark(str(summary2.get("cursor_key") or ""))
        assert stored
        third, summary3 = _run(
            _csv(APPEND_DAY2),
            dest,
            cp,
            sync_mode="incremental_append",
            job_id=f"csv-mysql-c-{suffix}",
        )
        assert third == 0
        assert summary3.get("source_row_count") == 0
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{table}`")
            assert int(cur.fetchone()[0]) == 3
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        conn.close()


def test_file_incremental_duckdb_still_uses_row_path(monkeypatch, tmp_path):
    """json/yaml/duckdb stay on the row path; COPY is CSV→SQL-core only."""
    duckdb = pytest.importorskip(
        "duckdb", reason="requires the optional DuckDB test dependency"
    )
    _isolate_cursor_store(monkeypatch, tmp_path)
    dest = EndpointConfig(
        kind="database",
        format="duckdb",
        database=str(tmp_path / "events.duckdb"),
        table="events",
    )
    cp = _FakeCheckpointService()
    written, summary = _run(
        _csv(APPEND_DAY1),
        dest,
        cp,
        sync_mode="incremental_append",
        job_id="csv-duck-a",
    )
    assert written == 2
    assert summary.get("copy_fast_path") != "used"
