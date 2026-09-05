"""XML identity COPY into SQLite, PostgreSQL, MySQL."""

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

from services.copy_csv_local import try_copy_local_csv  # noqa: E402
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


def _xml(rows: list[tuple[int, str, str]]) -> bytes:
    chunks = ["<records>"]
    for i, name, ts in rows:
        chunks.append(
            f"<record><id>{i}</id><name>{name}</name>"
            f"<updated_at>{ts}</updated_at></record>"
        )
    chunks.append("</records>")
    return "".join(chunks).encode("utf-8")


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
        "events.xml",
        dest,
        _mappings(),
        _schema(),
        job_id=job_id,
        checkpoint_service=cp,
        sync_mode=sync_mode,
        stream_contracts=_contracts(sync_mode),
    )
    return written, summary


def test_document_xml_declines_copy(tmp_path):
    dest_cfg = {"format": "sqlite", "database": str(tmp_path / "x.db"), "table": "events"}
    declined = try_copy_local_csv(
        content=b"<root>hello</root>",
        filename="note.xml",
        file_type="xml",
        dest_type="sqlite",
        dest_cfg=dest_cfg,
        dest_table="events",
        dest_schema="",
        mappings=_mappings(),
        schema=_schema(),
        effective_sync="full_refresh_overwrite",
    )
    assert declined is None


def test_xxe_xml_declines_copy(tmp_path):
    dest_cfg = {"format": "sqlite", "database": str(tmp_path / "x.db"), "table": "events"}
    xxe = (
        b'<?xml version="1.0"?>'
        b'<!DOCTYPE r [<!ENTITY e SYSTEM "file:///etc/passwd">]>'
        b"<records><record>&e;</record></records>"
    )
    declined = try_copy_local_csv(
        content=xxe,
        filename="xxe.xml",
        file_type="xml",
        dest_type="sqlite",
        dest_cfg=dest_cfg,
        dest_table="events",
        dest_schema="",
        mappings=_mappings(),
        schema=_schema(),
        effective_sync="full_refresh_overwrite",
    )
    assert declined is None


def test_xml_sqlite_overwrite_dest_count(tmp_path):
    dest_path = tmp_path / "xml.db"
    dest = EndpointConfig(
        kind="database",
        format="sqlite",
        database=str(dest_path),
        table="events",
    )
    cp = _FakeCheckpointService()
    first, summary1 = _run(
        _xml(DAY1),
        dest,
        cp,
        sync_mode="full_refresh_overwrite",
        job_id="xml-ow-a",
    )
    assert first == 3, summary1
    assert summary1.get("copy_fast_path") == "used"
    assert summary1.get("load_method") == "xml_records_executemany_sqlite"
    assert summary1.get("engine_source_checksum") == "dest_count:3"
    assert summary1.get("engine_target_checksum") == "dest_count:3"
    conn = sqlite3.connect(dest_path)
    try:
        assert int(conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]) == 3
    finally:
        conn.close()
    second, summary2 = _run(
        _xml(APPEND_DAY1),
        dest,
        cp,
        sync_mode="full_refresh_overwrite",
        job_id="xml-ow-b",
    )
    assert second == 2, summary2
    conn = sqlite3.connect(dest_path)
    try:
        dest_count = int(conn.execute("SELECT COUNT(*) FROM events").fetchone()[0])
    finally:
        conn.close()
    assert dest_count == 2


def test_xml_sqlite_incremental_deduped_delta_and_noop(monkeypatch, tmp_path):
    _isolate_cursor_store(monkeypatch, tmp_path)
    dest_path = tmp_path / "xml-inc.db"
    dest = EndpointConfig(
        kind="database",
        format="sqlite",
        database=str(dest_path),
        table="events",
    )
    cp = _FakeCheckpointService()
    first, summary1 = _run(
        _xml(DAY1),
        dest,
        cp,
        sync_mode="incremental_deduped",
        job_id="xml-sqlite-a",
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
        _xml(DAY2),
        dest,
        cp,
        sync_mode="incremental_deduped",
        job_id="xml-sqlite-b",
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
        _xml(DAY2),
        dest,
        cp,
        sync_mode="incremental_deduped",
        job_id="xml-sqlite-c",
    )
    assert third == 0
    assert summary3.get("source_row_count") == 0
    conn = sqlite3.connect(dest_path)
    try:
        assert int(conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]) == 4
    finally:
        conn.close()


def test_xml_to_sqlite_execute_tracked_gate8(tmp_path):
    from services.million_row_proof import ensure_memory_job_store_if_mongo_down
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import TransferRequest

    ensure_memory_job_store_if_mongo_down()
    db = tmp_path / "ledger.db"
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="xml"),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(db), table="ledger"
        ),
        source_content=_xml(
            [(1, "a", "2024-06-01T00:00:00"), (2, "b", "2024-06-02T00:00:00")]
        ),
        source_filename="ledger.xml",
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        mappings=_mappings(),
    )
    result = UniversalTransferEngine().execute_tracked(request, "xml-sqlite")
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
def test_xml_pg_incremental_deduped_delta_and_noop(monkeypatch, tmp_path):
    _isolate_cursor_store(monkeypatch, tmp_path)
    psycopg2 = pytest.importorskip("psycopg2")
    suffix = uuid.uuid4().hex[:8]
    table = f"xml_inc_{suffix}"
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
            _xml(DAY1),
            dest,
            cp,
            sync_mode="incremental_deduped",
            job_id=f"xml-pg-a-{suffix}",
        )
        assert first == 3, summary1
        assert summary1.get("copy_fast_path") == "used"
        assert summary1.get("load_method") == (
            "xml_records_copy_from_stdin_pg_incremental_deduped"
        )
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM public."{table}"')
            tick1 = int(cur.fetchone()[0])
        assert tick1 == 3
        second, summary2 = _run(
            _xml(DAY2),
            dest,
            cp,
            sync_mode="incremental_deduped",
            job_id=f"xml-pg-b-{suffix}",
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
            _xml(DAY2),
            dest,
            cp,
            sync_mode="incremental_deduped",
            job_id=f"xml-pg-c-{suffix}",
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
def test_xml_mysql_incremental_append_delta_and_noop(monkeypatch, tmp_path):
    _isolate_cursor_store(monkeypatch, tmp_path)
    pymysql = pytest.importorskip("pymysql")
    suffix = uuid.uuid4().hex[:8]
    table = f"xml_inc_{suffix}"
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
            _xml(APPEND_DAY1),
            dest,
            cp,
            sync_mode="incremental_append",
            job_id=f"xml-mysql-a-{suffix}",
        )
        assert first == 2, summary1
        assert summary1.get("copy_fast_path") == "used"
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{table}`")
            tick1 = int(cur.fetchone()[0])
        assert tick1 == 2
        second, summary2 = _run(
            _xml(APPEND_DAY2),
            dest,
            cp,
            sync_mode="incremental_append",
            job_id=f"xml-mysql-b-{suffix}",
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
            _xml(APPEND_DAY2),
            dest,
            cp,
            sync_mode="incremental_append",
            job_id=f"xml-mysql-c-{suffix}",
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
