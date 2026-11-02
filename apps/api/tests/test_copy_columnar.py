"""Parquet / Avro / ORC identity COPY into SQLite, PostgreSQL, MySQL."""

from __future__ import annotations

import gzip
import io
import socket
import sqlite3
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

pytest.importorskip("pyarrow")
pytest.importorskip("fastavro")

from services.copy_csv_local import try_copy_local_csv  # noqa: E402
from services.format_converter import convert_rows  # noqa: E402
from services.sync_cursor import get_watermark  # noqa: E402
from src.transfer.file_stream import stream_file_to_database  # noqa: E402
from src.transfer.models import EndpointConfig  # noqa: E402
from tests.test_copy_csv_local import (  # noqa: E402
    APPEND_DAY1,
    APPEND_DAY2,
    DAY1,
    DAY2,
    _FakeCheckpointService,
    _contracts,
    _isolate_cursor_store,
    _mappings,
    _schema,
)

_KINDS = ("parquet", "avro", "orc")


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


def _columnar(kind: str, rows: list[tuple[int, str, str]]) -> bytes:
    headers = ["id", "name", "updated_at"]
    body = [[str(i), name, ts] for i, name, ts in rows]
    content, _ = convert_rows(headers, body, source_format="csv", target_format=kind)
    return content


def _nested_parquet() -> bytes:
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table(
        {
            "id": [1],
            "name": [{"inner": "x"}],
            "updated_at": ["2024-06-01T00:00:00"],
        }
    )
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


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


@pytest.mark.parametrize("kind", _KINDS)
def test_garbage_columnar_declines_copy(kind, tmp_path):
    dest_cfg = {"format": "sqlite", "database": str(tmp_path / "x.db"), "table": "events"}
    declined = try_copy_local_csv(
        content=b"not-a-columnar-file",
        filename=f"events.{kind}",
        file_type=kind,
        dest_type="sqlite",
        dest_cfg=dest_cfg,
        dest_table="events",
        dest_schema="",
        mappings=_mappings(),
        schema=_schema(),
        effective_sync="full_refresh_overwrite",
    )
    assert declined is None


def test_nested_parquet_declines_copy(tmp_path):
    dest_cfg = {"format": "sqlite", "database": str(tmp_path / "x.db"), "table": "events"}
    declined = try_copy_local_csv(
        content=_nested_parquet(),
        filename="events.parquet",
        file_type="parquet",
        dest_type="sqlite",
        dest_cfg=dest_cfg,
        dest_table="events",
        dest_schema="",
        mappings=_mappings(),
        schema=_schema(),
        effective_sync="full_refresh_overwrite",
    )
    assert declined is None


@pytest.mark.parametrize("kind", _KINDS)
def test_columnar_sqlite_overwrite_dest_count(kind, tmp_path):
    dest_path = tmp_path / f"{kind}.db"
    dest = EndpointConfig(
        kind="database",
        format="sqlite",
        database=str(dest_path),
        table="events",
    )
    cp = _FakeCheckpointService()
    first, summary1 = _run(
        _columnar(kind, DAY1),
        f"events.{kind}",
        dest,
        cp,
        sync_mode="full_refresh_overwrite",
        job_id=f"{kind}-ow-a",
    )
    assert first == 3, summary1
    assert summary1.get("copy_fast_path") == "used"
    assert summary1.get("engine_source_checksum") == "dest_count:3"
    assert summary1.get("engine_target_checksum") == "dest_count:3"
    assert str(summary1.get("load_method") or "").startswith(f"{kind}_records_")
    conn = sqlite3.connect(dest_path)
    try:
        assert int(conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]) == 3
    finally:
        conn.close()
    second, summary2 = _run(
        _columnar(kind, APPEND_DAY1),
        f"events.{kind}",
        dest,
        cp,
        sync_mode="full_refresh_overwrite",
        job_id=f"{kind}-ow-b",
    )
    assert second == 2, summary2
    conn = sqlite3.connect(dest_path)
    try:
        dest_count = int(conn.execute("SELECT COUNT(*) FROM events").fetchone()[0])
    finally:
        conn.close()
    assert dest_count == 2


@pytest.mark.parametrize("kind", _KINDS)
def test_columnar_sqlite_incremental_deduped_delta_and_noop(kind, monkeypatch, tmp_path):
    _isolate_cursor_store(monkeypatch, tmp_path)
    dest_path = tmp_path / f"{kind}-inc.db"
    dest = EndpointConfig(
        kind="database",
        format="sqlite",
        database=str(dest_path),
        table="events",
    )
    cp = _FakeCheckpointService()
    first, summary1 = _run(
        _columnar(kind, DAY1),
        f"events.{kind}",
        dest,
        cp,
        sync_mode="incremental_deduped",
        job_id=f"{kind}-sqlite-a",
    )
    assert first == 3, summary1
    assert summary1.get("copy_fast_path") == "used"
    conn = sqlite3.connect(dest_path)
    try:
        tick1 = int(conn.execute("SELECT COUNT(*) FROM events").fetchone()[0])
    finally:
        conn.close()
    assert tick1 == 3
    second, summary2 = _run(
        _columnar(kind, DAY2),
        f"events.{kind}",
        dest,
        cp,
        sync_mode="incremental_deduped",
        job_id=f"{kind}-sqlite-b",
    )
    assert second == 2, summary2
    conn = sqlite3.connect(dest_path)
    try:
        dest_count = int(conn.execute("SELECT COUNT(*) FROM events").fetchone()[0])
        name = conn.execute("SELECT name FROM events WHERE id = 1").fetchone()[0]
    finally:
        conn.close()
    assert dest_count == 4
    assert name == "ONE"
    stored = get_watermark(str(summary2.get("cursor_key") or ""))
    assert stored
    third, summary3 = _run(
        _columnar(kind, DAY2),
        f"events.{kind}",
        dest,
        cp,
        sync_mode="incremental_deduped",
        job_id=f"{kind}-sqlite-c",
    )
    assert third == 0
    assert summary3.get("source_row_count") == 0
    conn = sqlite3.connect(dest_path)
    try:
        assert int(conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]) == 4
    finally:
        conn.close()


def test_gzip_parquet_sqlite_overwrite(tmp_path):
    dest_path = tmp_path / "gz.db"
    dest_cfg = {
        "format": "sqlite",
        "database": str(dest_path),
        "table": "events",
    }
    used = try_copy_local_csv(
        content=gzip.compress(_columnar("parquet", APPEND_DAY1)),
        filename="events.parquet",
        file_type="parquet",
        dest_type="sqlite",
        dest_cfg=dest_cfg,
        dest_table="events",
        dest_schema="",
        mappings=_mappings(),
        schema=_schema(),
        effective_sync="full_refresh_overwrite",
    )
    assert used is not None
    written, _ddl, summary, _cols = used
    assert written == 2, summary
    assert summary.get("copy_fast_path") == "used"
    conn = sqlite3.connect(dest_path)
    try:
        assert int(conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]) == 2
    finally:
        conn.close()


@pytest.mark.parametrize("kind", _KINDS)
def test_columnar_to_sqlite_execute_tracked_gate8(kind, tmp_path):
    from services.million_row_proof import ensure_memory_job_store_if_mongo_down
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import TransferRequest

    ensure_memory_job_store_if_mongo_down()
    db = tmp_path / f"{kind}-ledger.db"
    request = TransferRequest(
        source=EndpointConfig(kind="file", format=kind),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(db), table="ledger"
        ),
        source_content=_columnar(
            kind, [(1, "a", "2024-06-01T00:00:00"), (2, "b", "2024-06-02T00:00:00")]
        ),
        source_filename=f"ledger.{kind}",
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        mappings=_mappings(),
    )
    result = UniversalTransferEngine().execute_tracked(request, f"{kind}-sqlite")
    assert result.success, getattr(result, "error", result)
    summary = result.destination_summary or {}
    assert summary.get("copy_fast_path") == "used"
    assert summary.get("engine_source_checksum") == "dest_count:2"
    recon = result.reconciliation or {}
    assert recon.get("passed") is True, recon
    conn = sqlite3.connect(str(db))
    try:
        n = conn.execute("SELECT COUNT(*) FROM ledger").fetchone()[0]
    finally:
        conn.close()
    assert n == 2


@pytest.mark.skipif(not _pg_up(), reason="PostgreSQL not on 5432")
def test_parquet_pg_incremental_deduped_delta_and_noop(monkeypatch, tmp_path):
    _isolate_cursor_store(monkeypatch, tmp_path)
    psycopg2 = pytest.importorskip("psycopg2")
    suffix = uuid.uuid4().hex[:8]
    table = f"parquet_inc_{suffix}"
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
            _columnar("parquet", DAY1),
            "events.parquet",
            dest,
            cp,
            sync_mode="incremental_deduped",
            job_id=f"parquet-pg-a-{suffix}",
        )
        assert first == 3, summary1
        assert summary1.get("copy_fast_path") == "used"
        assert summary1.get("load_method") == (
            "parquet_records_copy_from_stdin_pg_incremental_deduped"
        )
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM public."{table}"')
            tick1 = int(cur.fetchone()[0])
        assert tick1 == 3
        second, summary2 = _run(
            _columnar("parquet", DAY2),
            "events.parquet",
            dest,
            cp,
            sync_mode="incremental_deduped",
            job_id=f"parquet-pg-b-{suffix}",
        )
        assert second == 2, summary2
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM public."{table}"')
            dest_count = int(cur.fetchone()[0])
            cur.execute(f'SELECT name FROM public."{table}" WHERE id = 1')
            name = cur.fetchone()[0]
        assert dest_count == 4
        assert name == "ONE"
        third, summary3 = _run(
            _columnar("parquet", DAY2),
            "events.parquet",
            dest,
            cp,
            sync_mode="incremental_deduped",
            job_id=f"parquet-pg-c-{suffix}",
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
def test_avro_mysql_incremental_append_delta_and_noop(monkeypatch, tmp_path):
    _isolate_cursor_store(monkeypatch, tmp_path)
    pymysql = pytest.importorskip("pymysql")
    suffix = uuid.uuid4().hex[:8]
    table = f"avro_inc_{suffix}"
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
            _columnar("avro", APPEND_DAY1),
            "events.avro",
            dest,
            cp,
            sync_mode="incremental_append",
            job_id=f"avro-mysql-a-{suffix}",
        )
        assert first == 2, summary1
        assert summary1.get("copy_fast_path") == "used"
        assert "avro_records" in str(summary1.get("load_method") or "")
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{table}`")
            tick1 = int(cur.fetchone()[0])
        assert tick1 == 2
        second, summary2 = _run(
            _columnar("avro", APPEND_DAY2),
            "events.avro",
            dest,
            cp,
            sync_mode="incremental_append",
            job_id=f"avro-mysql-b-{suffix}",
        )
        assert second == 1, summary2
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{table}`")
            dest_count = int(cur.fetchone()[0])
            cur.execute(f"SELECT name FROM `{table}` ORDER BY id")
            names = [r[0] for r in cur.fetchall()]
        assert dest_count == 3
        assert names == ["a", "b", "c"]
        third, summary3 = _run(
            _columnar("avro", APPEND_DAY2),
            "events.avro",
            dest,
            cp,
            sync_mode="incremental_append",
            job_id=f"avro-mysql-c-{suffix}",
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


@pytest.mark.skipif(not _mysql_up(), reason="MySQL not on 3306")
def test_orc_mysql_overwrite_dest_count(tmp_path):
    pymysql = pytest.importorskip("pymysql")
    suffix = uuid.uuid4().hex[:8]
    table = f"orc_ow_{suffix}"
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
            _columnar("orc", DAY1),
            "events.orc",
            dest,
            cp,
            sync_mode="full_refresh_overwrite",
            job_id=f"orc-mysql-a-{suffix}",
        )
        assert first == 3, summary1
        assert summary1.get("copy_fast_path") == "used"
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{table}`")
            tick1 = int(cur.fetchone()[0])
        assert tick1 == 3
        second, summary2 = _run(
            _columnar("orc", APPEND_DAY1),
            "events.orc",
            dest,
            cp,
            sync_mode="full_refresh_overwrite",
            job_id=f"orc-mysql-b-{suffix}",
        )
        assert second == 2, summary2
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{table}`")
            dest_count = int(cur.fetchone()[0])
        assert dest_count == 2
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        conn.close()
