"""json / yaml / fixed-width identity COPY into SQLite, PostgreSQL, MySQL."""

from __future__ import annotations

import json
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
    file_copy_load_method,
    try_copy_local_csv,
)
from services.fixed_width_layout import layout_header_line  # noqa: E402
from services.sync_cursor import get_watermark  # noqa: E402
from src.transfer.file_stream import stream_file_to_database  # noqa: E402
from src.transfer.models import EndpointConfig  # noqa: E402
from tests.test_copy_csv_local import (  # noqa: E402
    APPEND_DAY1,
    APPEND_DAY2,
    DAY1,
    DAY2,
    _FakeCheckpointService,
    _isolate_cursor_store,
    _mappings,
    _schema,
    _contracts,
)


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


def _yaml(rows: list[tuple[int, str, str]]) -> bytes:
    chunks = [
        f'- id: "{i}"\n  name: {name}\n  updated_at: "{ts}"' for i, name, ts in rows
    ]
    return ("\n".join(chunks) + "\n").encode("utf-8")


def _json_docs(rows: list[tuple[int, str, str]]) -> bytes:
    return json.dumps(
        [{"id": i, "name": name, "updated_at": ts} for i, name, ts in rows]
    ).encode("utf-8")


def _fwf(rows: list[tuple[int, str, str]]) -> bytes:
    layout = (("id", 8), ("name", 8), ("updated_at", 20))
    body = layout_header_line(layout) + "\n"
    for i, name, ts in rows:
        body += str(i).ljust(8) + name.ljust(8) + ts.ljust(20) + "\n"
    return body.encode("utf-8")


def _run(
    payload: bytes,
    filename: str,
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
        filename,
        dest,
        _mappings(),
        _schema(),
        job_id=job_id,
        checkpoint_service=cp,
        sync_mode=sync_mode,
        stream_contracts=_contracts(sync_mode),
    )
    return written, summary


def test_file_copy_load_method_tokens():
    assert file_copy_load_method("yaml", "sqlite", "incremental_deduped") == (
        "yaml_records_executemany_sqlite_incremental_deduped"
    )
    assert file_copy_load_method("json", "postgresql", "incremental_deduped") == (
        "json_records_copy_from_stdin_pg_incremental_deduped"
    )
    assert file_copy_load_method("fixed_width", "mysql", "incremental_append") == (
        "fwf_records_load_data_mysql_incremental_append"
    )


def test_nested_json_declines_copy(tmp_path):
    dest_cfg = {"format": "sqlite", "database": str(tmp_path / "n.db"), "table": "events"}
    nested = json.dumps(
        [{"id": 1, "name": {"inner": "x"}, "updated_at": "2024-06-01T00:00:00"}]
    ).encode()
    declined = try_copy_local_csv(
        content=nested,
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


def test_yaml_sqlite_incremental_deduped_delta_and_noop(monkeypatch, tmp_path):
    _isolate_cursor_store(monkeypatch, tmp_path)
    dest_path = tmp_path / "yaml.db"
    dest = EndpointConfig(
        kind="database",
        format="sqlite",
        database=str(dest_path),
        table="events",
    )
    cp = _FakeCheckpointService()
    first, summary1 = _run(
        _yaml(DAY1),
        "events.yaml",
        dest,
        cp,
        sync_mode="incremental_deduped",
        job_id="yaml-sqlite-a",
    )
    assert first == 3, summary1
    assert summary1.get("copy_fast_path") == "used"
    assert summary1.get("load_method") == (
        "yaml_records_executemany_sqlite_incremental_deduped"
    )
    conn = sqlite3.connect(dest_path)
    try:
        tick1 = int(conn.execute("SELECT COUNT(*) FROM events").fetchone()[0])
        flag_like = conn.execute(
            "SELECT name FROM events WHERE id = 1"
        ).fetchone()[0]
    finally:
        conn.close()
    assert tick1 == 3
    assert flag_like == "one"
    second, summary2 = _run(
        _yaml(DAY2),
        "events.yaml",
        dest,
        cp,
        sync_mode="incremental_deduped",
        job_id="yaml-sqlite-b",
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
    cursor_key = str(summary2.get("cursor_key") or "")
    stored = get_watermark(cursor_key)
    assert stored
    third, summary3 = _run(
        _yaml(DAY2),
        "events.yaml",
        dest,
        cp,
        sync_mode="incremental_deduped",
        job_id="yaml-sqlite-c",
    )
    assert third == 0
    assert summary3.get("source_row_count") == 0
    assert get_watermark(cursor_key) == stored
    conn = sqlite3.connect(dest_path)
    try:
        assert int(conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]) == 4
    finally:
        conn.close()


def test_json_sqlite_overwrite_dest_count(tmp_path):
    dest_path = tmp_path / "json.db"
    dest = EndpointConfig(
        kind="database",
        format="sqlite",
        database=str(dest_path),
        table="events",
    )
    cp = _FakeCheckpointService()
    first, summary1 = _run(
        _json_docs(DAY1),
        "events.json",
        dest,
        cp,
        sync_mode="full_refresh_overwrite",
        job_id="json-ow-a",
    )
    assert first == 3, summary1
    assert summary1.get("copy_fast_path") == "used"
    assert summary1.get("load_method") == "json_records_executemany_sqlite"
    conn = sqlite3.connect(dest_path)
    try:
        assert int(conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]) == 3
    finally:
        conn.close()
    second, summary2 = _run(
        _json_docs(APPEND_DAY1),
        "events.json",
        dest,
        cp,
        sync_mode="full_refresh_overwrite",
        job_id="json-ow-b",
    )
    assert second == 2, summary2
    conn = sqlite3.connect(dest_path)
    try:
        dest_count = int(conn.execute("SELECT COUNT(*) FROM events").fetchone()[0])
    finally:
        conn.close()
    assert dest_count == 2


def test_fwf_sqlite_incremental_append_delta_and_noop(monkeypatch, tmp_path):
    _isolate_cursor_store(monkeypatch, tmp_path)
    dest_path = tmp_path / "fwf.db"
    dest = EndpointConfig(
        kind="database",
        format="sqlite",
        database=str(dest_path),
        table="events",
    )
    cp = _FakeCheckpointService()
    first, summary1 = _run(
        _fwf(APPEND_DAY1),
        "events.fwf",
        dest,
        cp,
        sync_mode="incremental_append",
        job_id="fwf-sqlite-a",
    )
    assert first == 2, summary1
    assert summary1.get("copy_fast_path") == "used"
    assert "fwf_records" in str(summary1.get("load_method") or "")
    conn = sqlite3.connect(dest_path)
    try:
        tick1 = int(conn.execute("SELECT COUNT(*) FROM events").fetchone()[0])
    finally:
        conn.close()
    assert tick1 == 2
    second, summary2 = _run(
        _fwf(APPEND_DAY2),
        "events.fwf",
        dest,
        cp,
        sync_mode="incremental_append",
        job_id="fwf-sqlite-b",
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
    stored = get_watermark(str(summary2.get("cursor_key") or ""))
    assert stored
    third, summary3 = _run(
        _fwf(APPEND_DAY2),
        "events.fwf",
        dest,
        cp,
        sync_mode="incremental_append",
        job_id="fwf-sqlite-c",
    )
    assert third == 0
    assert summary3.get("source_row_count") == 0
    conn = sqlite3.connect(dest_path)
    try:
        assert int(conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]) == 3
    finally:
        conn.close()


@pytest.mark.skipif(not _pg_up(), reason="PostgreSQL not on 5432")
def test_json_pg_incremental_deduped_delta_and_noop(monkeypatch, tmp_path):
    _isolate_cursor_store(monkeypatch, tmp_path)
    psycopg2 = pytest.importorskip("psycopg2")
    suffix = uuid.uuid4().hex[:8]
    table = f"json_inc_{suffix}"
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
            _json_docs(DAY1),
            "events.json",
            dest,
            cp,
            sync_mode="incremental_deduped",
            job_id=f"json-pg-a-{suffix}",
        )
        assert first == 3, summary1
        assert summary1.get("copy_fast_path") == "used"
        assert summary1.get("load_method") == (
            "json_records_copy_from_stdin_pg_incremental_deduped"
        )
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM public."{table}"')
            tick1 = int(cur.fetchone()[0])
        assert tick1 == 3
        second, summary2 = _run(
            _json_docs(DAY2),
            "events.json",
            dest,
            cp,
            sync_mode="incremental_deduped",
            job_id=f"json-pg-b-{suffix}",
        )
        assert second == 2, summary2
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM public."{table}"')
            dest_count = int(cur.fetchone()[0])
            cur.execute(f'SELECT name FROM public."{table}" WHERE id = 1')
            name = cur.fetchone()[0]
        assert dest_count == 4
        assert name == "ONE"
        stored = get_watermark(str(summary2.get("cursor_key") or ""))
        assert stored
        third, summary3 = _run(
            _json_docs(DAY2),
            "events.json",
            dest,
            cp,
            sync_mode="incremental_deduped",
            job_id=f"json-pg-c-{suffix}",
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
def test_yaml_mysql_incremental_append_delta_and_noop(monkeypatch, tmp_path):
    _isolate_cursor_store(monkeypatch, tmp_path)
    pymysql = pytest.importorskip("pymysql")
    suffix = uuid.uuid4().hex[:8]
    table = f"yaml_inc_{suffix}"
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
            _yaml(APPEND_DAY1),
            "events.yaml",
            dest,
            cp,
            sync_mode="incremental_append",
            job_id=f"yaml-mysql-a-{suffix}",
        )
        assert first == 2, summary1
        assert summary1.get("copy_fast_path") == "used"
        assert "yaml_records" in str(summary1.get("load_method") or "")
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{table}`")
            tick1 = int(cur.fetchone()[0])
        assert tick1 == 2
        second, summary2 = _run(
            _yaml(APPEND_DAY2),
            "events.yaml",
            dest,
            cp,
            sync_mode="incremental_append",
            job_id=f"yaml-mysql-b-{suffix}",
        )
        assert second == 1, summary2
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{table}`")
            dest_count = int(cur.fetchone()[0])
            cur.execute(f"SELECT name FROM `{table}` ORDER BY id")
            names = [r[0] for r in cur.fetchall()]
        assert dest_count == 3
        assert names == ["a", "b", "c"]
        stored = get_watermark(str(summary2.get("cursor_key") or ""))
        assert stored
        third, summary3 = _run(
            _yaml(APPEND_DAY2),
            "events.yaml",
            dest,
            cp,
            sync_mode="incremental_append",
            job_id=f"yaml-mysql-c-{suffix}",
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
