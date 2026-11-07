"""Resume without a keyset bookmark continues one scan, not OFFSET pages.

A mid-run crash often persists offset/chunk_index but no cursor_value — the
write loop used to OFFSET-page the remainder (O(n²), concurrent-insert drift).
The shared owner is ``align_snapshot_resume``: drain the held SELECT past the
committed prefix, then fetchmany the tail. A PK bookmark is still persisted so
the *next* resume can seek.
"""

from __future__ import annotations

import socket
import uuid
from typing import Any

import pytest

from connectors.sql_snapshot_scan import align_snapshot_resume, drop_batch_prefix
from services.checkpoint_service import Checkpoint, CheckpointService
from src.transfer.models import EndpointConfig


class _Page:
    def __init__(self, rows: list[list[Any]], *, offset: int = 0) -> None:
        self.headers = ["id", "v"]
        self.rows = rows
        self.offset = offset
        self.total_rows = None
        self.raw_page_rows = len(rows)
        self.raw_page_cursor = ""
        self.raw_page_keyset = ""
        self.raw_page_filtered = 0


class _MemoryJobs:
    def __init__(self) -> None:
        self.jobs: dict[str, dict[str, Any]] = {}

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        return self.jobs.get(job_id)

    def update_job_status(self, job_id: str, status: str, **fields: Any) -> bool:
        doc = self.jobs.setdefault(job_id, {"_id": job_id})
        doc["status"] = status
        doc.update(fields)
        return True


def test_drop_batch_prefix_clears_the_raw_page_mark() -> None:
    page = _Page([[1, "a"], [2, "b"], [3, "c"]], offset=10)
    out = drop_batch_prefix(page, 2)
    assert out.rows == [[3, "c"]]
    assert out.offset == 12
    assert out.raw_page_rows is None


def test_align_snapshot_resume_discards_whole_pages_then_slices() -> None:
    pages = [
        _Page([[i, i] for i in range(1, 5)]),
        _Page([[i, i] for i in range(5, 9)]),
        _Page([[i, i] for i in range(9, 13)]),
    ]
    calls = {"n": 0}

    def read_next() -> _Page:
        calls["n"] += 1
        return pages[calls["n"]]

    aligned = align_snapshot_resume(pages[0], skip_rows=6, read_next=read_next)
    assert [row[0] for row in aligned.rows] == [7, 8]
    assert calls["n"] == 1


def test_align_snapshot_resume_is_a_no_op_when_nothing_was_committed() -> None:
    page = _Page([[1, "a"]])
    assert align_snapshot_resume(page, 0, read_next=lambda: page) is page


def _reachable(port: int) -> bool:
    try:
        socket.create_connection(("127.0.0.1", port), timeout=1).close()
        return True
    except OSError:
        return False


def _count_offset_reads(monkeypatch: pytest.MonkeyPatch, reader_mod: str) -> list[int]:
    """Count LIMIT/OFFSET pager calls — resume must not use this path."""
    import importlib

    mod = importlib.import_module(reader_mod)
    original = mod.read_table_batch
    seen: list[int] = []

    def wrapped(*args: Any, **kwargs: Any):
        seen.append(int(kwargs.get("offset") or 0))
        return original(*args, **kwargs)

    monkeypatch.setattr(mod, "read_table_batch", wrapped)
    return seen


def _resume_upsert(
    *,
    fmt: str,
    source: EndpointConfig,
    destination: EndpointConfig,
    committed: int,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[int, dict[str, Any], Checkpoint]:
    from src.transfer import stream as stream_mod
    from src.transfer.stream import stream_database_transfer

    monkeypatch.setattr(stream_mod, "CHUNK_SIZE", 400)
    job_id = uuid.uuid4().hex[:24]
    checkpoint = Checkpoint(
        job_id=job_id,
        chunk_index=2,
        offset=committed,
        rows_processed=committed,
        write_mode="upsert",
        conflict_columns=["id"],
    )
    written, _ddl, summary, _cols = stream_database_transfer(
        source,
        destination,
        [{"source": "id", "target": "id"}, {"source": "v", "target": "v"}],
        {"id": "INTEGER", "v": "INTEGER"},
        sync_mode="upsert",
        stream_contracts=[
            {
                "name": source.table,
                "sync_mode": "upsert",
                "primary_key": "id",
                "selected": True,
            }
        ],
        job_id=job_id,
        checkpoint=checkpoint,
        checkpoint_service=CheckpointService(_MemoryJobs()),
        skip_preflight=True,
    )
    return written, summary, checkpoint


@pytest.mark.skipif(not _reachable(5432), reason="PostgreSQL not reachable on 127.0.0.1:5432")
def test_pg_resume_without_bookmark_lands_the_population(monkeypatch: pytest.MonkeyPatch) -> None:
    psycopg2 = pytest.importorskip("psycopg2")
    table = "resume_scan_" + uuid.uuid4().hex[:8]
    dest = table + "_dst"
    total = 2000
    committed = 800
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        dbname="dataflow",
        user="dataflow",
        password="dataflow",
    )
    conn.autocommit = True
    offset_calls = _count_offset_reads(monkeypatch, "connectors.postgresql_reader")
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'CREATE TABLE "{table}" (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)'
            )
            cur.execute(
                f'INSERT INTO "{table}" (id, v) SELECT g, g * 10 FROM generate_series(1, %s) g',
                (total,),
            )
            cur.execute(
                f'CREATE TABLE "{dest}" (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)'
            )
            cur.execute(
                f'INSERT INTO "{dest}" (id, v) SELECT id, v FROM "{table}" WHERE id <= %s',
                (committed,),
            )
        source = EndpointConfig(
            kind="database",
            format="postgresql",
            host="127.0.0.1",
            port=5432,
            database="dataflow",
            username="dataflow",
            password="dataflow",
            schema="public",
            table=table,
        )
        destination = EndpointConfig(
            kind="database",
            format="postgresql",
            host="127.0.0.1",
            port=5432,
            database="dataflow",
            username="dataflow",
            password="dataflow",
            schema="public",
            table=dest,
        )
        written, summary, checkpoint = _resume_upsert(
            fmt="postgresql",
            source=source,
            destination=destination,
            committed=committed,
            monkeypatch=monkeypatch,
        )
        with conn.cursor() as cur:
            cur.execute(
                f'SELECT count(*), count(DISTINCT id), coalesce(sum(v),0) FROM "{dest}"'
            )
            count, distinct, dest_sum = cur.fetchone()
            cur.execute(f'SELECT coalesce(sum(v),0) FROM "{table}"')
            src_sum = cur.fetchone()[0]
        assert written == total
        assert count == total == distinct
        assert dest_sum == src_sum
        assert summary.get("pagination_mode") == "scan"
        assert summary.get("resume_pagination") == "snapshot_prefix"
        # A first-page OFFSET 0 (introspect / Gate-8 sample) is not the cliff.
        # OFFSET past the committed prefix is the failure this test forbids.
        assert not any(off > 0 for off in offset_calls)
        assert checkpoint.cursor_value not in (None, "")
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
            cur.execute(f'DROP TABLE IF EXISTS "{dest}"')
        conn.close()


@pytest.mark.skipif(not _reachable(3306), reason="MySQL not reachable on 127.0.0.1:3306")
def test_mysql_resume_without_bookmark_lands_the_population(monkeypatch: pytest.MonkeyPatch) -> None:
    pymysql = pytest.importorskip("pymysql")
    table = "resume_scan_" + uuid.uuid4().hex[:8]
    dest = table + "_dst"
    total = 2000
    committed = 800
    conn = pymysql.connect(
        host="127.0.0.1",
        port=3306,
        user="dataflow",
        password="dataflow",
        database="dataflow",
        autocommit=True,
    )
    offset_calls = _count_offset_reads(monkeypatch, "connectors.mysql_reader")
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE `{table}` (id INT PRIMARY KEY, v INT NOT NULL)")
            cur.execute(
                f"INSERT INTO `{table}` (id, v) "
                "SELECT n, n * 10 FROM ("
                "SELECT 1 + units.i + tens.i * 10 + hundreds.i * 100 + thousands.i * 1000 AS n "
                "FROM (SELECT 0 i UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 "
                "UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) units "
                "CROSS JOIN (SELECT 0 i UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 "
                "UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) tens "
                "CROSS JOIN (SELECT 0 i UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 "
                "UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) hundreds "
                "CROSS JOIN (SELECT 0 i UNION SELECT 1 UNION SELECT 2) thousands"
                f") seq WHERE n BETWEEN 1 AND {total}"
            )
            cur.execute(f"CREATE TABLE `{dest}` (id INT PRIMARY KEY, v INT NOT NULL)")
            cur.execute(
                f"INSERT INTO `{dest}` (id, v) SELECT id, v FROM `{table}` WHERE id <= {committed}"
            )
        source = EndpointConfig(
            kind="database",
            format="mysql",
            host="127.0.0.1",
            port=3306,
            database="dataflow",
            username="dataflow",
            password="dataflow",
            schema="dataflow",
            table=table,
        )
        destination = EndpointConfig(
            kind="database",
            format="mysql",
            host="127.0.0.1",
            port=3306,
            database="dataflow",
            username="dataflow",
            password="dataflow",
            schema="dataflow",
            table=dest,
        )
        written, summary, checkpoint = _resume_upsert(
            fmt="mysql",
            source=source,
            destination=destination,
            committed=committed,
            monkeypatch=monkeypatch,
        )
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT count(*), count(DISTINCT id), coalesce(sum(v),0) FROM `{dest}`"
            )
            count, distinct, dest_sum = cur.fetchone()
            cur.execute(f"SELECT coalesce(sum(v),0) FROM `{table}`")
            src_sum = cur.fetchone()[0]
        assert written == total
        assert count == total == distinct
        assert dest_sum == src_sum
        assert summary.get("pagination_mode") == "scan"
        assert summary.get("resume_pagination") == "snapshot_prefix"
        assert not any(off > 0 for off in offset_calls)
        assert checkpoint.cursor_value not in (None, "")
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
            cur.execute(f"DROP TABLE IF EXISTS `{dest}`")
        conn.close()
