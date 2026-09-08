"""Snapshot scan → keyset seek handoff must agree on the read order.

The stream engine lands page one from a held snapshot scan and then seeks past
it on the keyset columns. A SQLite scan ordered by ``rowid`` while the seek ran
on a ``BIGINT PRIMARY KEY`` handed the engine ids 2501..22500 first and then
sought ``id > 22500`` — the 2,500 keys the heap stored last were never read,
and a PostgreSQL→SQLite mirror landed 97,000 of 99,500 staged rows (updates on
the low keys never reached the target). Two owners now defend this:

* every snapshot reader publishes the ORDER BY it actually opened with, and the
  SQLite reader orders keyed tables by their declared primary key;
* the engine refuses the seek when the published order is missing or is not
  led by the keyset columns, and keeps paging the held scan instead.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))
_SRC = _API_ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

import connectors.sqlite_reader as sqlite_reader_mod  # noqa: E402
from connectors.sql_snapshot_scan import (  # noqa: E402
    publish_scan_order,
    scan_order_supports_seek,
)
from connectors.sqlite_reader import read_table_scan_batch  # noqa: E402
from services.checkpoint_service import CheckpointService  # noqa: E402
from src.transfer.models import EndpointConfig  # noqa: E402
from src.transfer.stream import stream_database_transfer  # noqa: E402

# --------------------------------------------------------------------------
# Owner 1: the handoff predicate
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("scan_order", "keyset", "expected"),
    [
        (["id"], ["id"], True),
        (["ID"], ["id"], True),
        (["id", "rowid"], ["id"], True),
        (["tenant", "id"], ["tenant", "id"], True),
        (["tenant", "id"], ["tenant"], True),
        (["rowid"], ["id"], False),
        (["id"], ["tenant", "id"], False),
        (["tenant", "id"], ["id"], False),
        (None, ["id"], False),
        ([], ["id"], False),
        (["id"], [], False),
    ],
)
def test_scan_order_supports_seek(scan_order, keyset, expected):
    assert scan_order_supports_seek(scan_order, keyset) is expected


def test_publish_scan_order_records_strings_and_drops_blanks():
    state: dict = {}
    publish_scan_order(state, ["id", "", "updated_seq"])
    assert state["order_cols"] == ["id", "updated_seq"]


# --------------------------------------------------------------------------
# Owner 2: the SQLite snapshot reader orders keyed tables by their key
# --------------------------------------------------------------------------


def _scan_pages(db: Path, table: str, page: int, **filter_kw) -> tuple[list, dict]:
    state: dict = {}
    rows: list = []
    offset = 0
    order_seen: dict = {}
    while True:
        batch = read_table_scan_batch(
            host="",
            port=0,
            database=str(db),
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            table=table,
            offset=offset,
            limit=page,
            scan_state=state,
            **filter_kw,
        )
        if "order_cols" in state:
            order_seen["order_cols"] = list(state["order_cols"])
        if not batch.rows:
            break
        rows.extend(batch.rows)
        offset += len(batch.rows)
    return rows, order_seen


def _rotated_ids(n: int, pivot: int) -> list[int]:
    """Physical insert order pivot+1..n, then 1..pivot — rowid order != key order."""
    return list(range(pivot + 1, n + 1)) + list(range(1, pivot + 1))


def test_sqlite_scan_orders_bigint_primary_key_table_by_key(tmp_path: Path):
    db = tmp_path / "keyed.db"
    with sqlite3.connect(db) as conn:
        conn.execute('CREATE TABLE "t" ("id" BIGINT NOT NULL, "name" VARCHAR, PRIMARY KEY ("id"))')
        conn.executemany(
            'INSERT INTO "t" VALUES (?, ?)', [(i, f"name-{i}") for i in _rotated_ids(40, 10)]
        )
        assert conn.execute('SELECT "id" FROM "t" ORDER BY rowid LIMIT 1').fetchone()[0] == 11

    rows, state = _scan_pages(db, "t", page=7)
    assert [int(r[0]) for r in rows] == list(range(1, 41))
    assert state["order_cols"] == ["id"]
    assert scan_order_supports_seek(state["order_cols"], ["id"])


def test_sqlite_scan_orders_composite_key_in_declared_key_order(tmp_path: Path):
    db = tmp_path / "composite.db"
    with sqlite3.connect(db) as conn:
        conn.execute(
            'CREATE TABLE "t" ("region" TEXT, "seq" INTEGER, "v" TEXT, PRIMARY KEY ("seq", "region"))'
        )
        conn.executemany(
            'INSERT INTO "t" VALUES (?, ?, ?)',
            [(r, s, f"{r}{s}") for s in (3, 1, 2) for r in ("b", "a")],
        )

    rows, state = _scan_pages(db, "t", page=4)
    assert state["order_cols"] == ["seq", "region"]
    assert [(int(r[1]), r[0]) for r in rows] == sorted(
        (s, r) for s in (1, 2, 3) for r in ("a", "b")
    )


def test_sqlite_scan_without_rowid_table_orders_by_key(tmp_path: Path):
    db = tmp_path / "norowid.db"
    with sqlite3.connect(db) as conn:
        conn.execute('CREATE TABLE "t" ("id" INTEGER PRIMARY KEY, "v" TEXT) WITHOUT ROWID')
        conn.executemany(
            'INSERT INTO "t" VALUES (?, ?)', [(i, str(i)) for i in _rotated_ids(12, 5)]
        )

    rows, state = _scan_pages(db, "t", page=5)
    assert [int(r[0]) for r in rows] == list(range(1, 13))
    assert state["order_cols"] == ["id"]


def test_sqlite_scan_unkeyed_table_publishes_rowid_order(tmp_path: Path):
    db = tmp_path / "heap.db"
    with sqlite3.connect(db) as conn:
        conn.execute('CREATE TABLE "t" ("v" TEXT)')
        conn.executemany('INSERT INTO "t" VALUES (?)', [(f"r{i}",) for i in range(9)])

    rows, state = _scan_pages(db, "t", page=4)
    assert [r[0] for r in rows] == [f"r{i}" for i in range(9)]
    assert state["order_cols"] == ["rowid"]
    assert not scan_order_supports_seek(state["order_cols"], ["id"])


def test_sqlite_filtered_scan_publishes_cursor_first_then_key(tmp_path: Path):
    db = tmp_path / "filtered.db"
    with sqlite3.connect(db) as conn:
        conn.execute(
            'CREATE TABLE "t" ("id" INTEGER NOT NULL, "updated_seq" INTEGER, PRIMARY KEY ("id"))'
        )
        conn.executemany(
            'INSERT INTO "t" VALUES (?, ?)', [(i, 1 + i // 10) for i in _rotated_ids(40, 10)]
        )

    rows, state = _scan_pages(db, "t", page=7, filter_column="updated_seq", filter_after="1")
    assert state["order_cols"] == ["updated_seq", "id"]
    assert sorted(int(r[0]) for r in rows) == list(range(10, 41))
    assert [(int(r[1]), int(r[0])) for r in rows] == sorted((int(r[1]), int(r[0])) for r in rows)


# --------------------------------------------------------------------------
# End to end: the stream engine lands every key across the scan→seek handoff
# --------------------------------------------------------------------------


def _keyed_source(tmp_path: Path, n: int, pivot: int) -> Path:
    db = tmp_path / "src.db"
    with sqlite3.connect(db) as conn:
        conn.execute(
            'CREATE TABLE "orders" ("id" BIGINT NOT NULL, "name" VARCHAR, "amount" TEXT, PRIMARY KEY ("id"))'
        )
        conn.executemany(
            'INSERT INTO "orders" VALUES (?, ?, ?)',
            [(i, f"name-{i}", f"{i}.50") for i in _rotated_ids(n, pivot)],
        )
    return db


def _dest_ids(db: Path) -> list[int]:
    with sqlite3.connect(db) as conn:
        return [r[0] for r in conn.execute('SELECT "id" FROM "orders_out" ORDER BY "id"')]


def _run_upsert(tmp_path: Path, src: Path, job_id: str) -> tuple[int, dict, Path]:
    dst = tmp_path / "dst.db"
    source = EndpointConfig(kind="database", format="sqlite", database=str(src), table="orders")
    destination = EndpointConfig(
        kind="database", format="sqlite", database=str(dst), table="orders_out"
    )
    mappings = [
        {"source": "id", "target": "id"},
        {"source": "name", "target": "name"},
        {"source": "amount", "target": "amount"},
    ]
    schema = {"id": "integer", "name": "string", "amount": "decimal"}
    rows_written, _ddl, summary, _cols = stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode="upsert",
        stream_contracts=[{"selected": True, "sync_mode": "upsert", "primary_key": "id"}],
        job_id=job_id,
        checkpoint_service=CheckpointService(_FakeMongo()),
    )
    return rows_written, summary, dst


class _FakeMongo:
    def __init__(self):
        self.jobs: dict[str, dict] = {}

    def get_job(self, job_id: str) -> dict | None:
        return self.jobs.get(job_id)

    def update_job_status(self, job_id: str, status: str, **kwargs) -> bool:
        self.jobs.setdefault(job_id, {})
        self.jobs[job_id].update(kwargs)
        self.jobs[job_id]["status"] = status
        return True


@pytest.fixture
def small_chunks(monkeypatch):
    import src.transfer.stream as stream_mod

    monkeypatch.setenv("DATAFLOW_SQLITE_SQLITE_COPY", "0")
    monkeypatch.setattr(stream_mod, "CHUNK_SIZE", 40)


def test_upsert_lands_every_key_when_heap_order_differs_from_key_order(
    tmp_path: Path, small_chunks
):
    """213 keys, rowid order rotated so keys 1..60 are stored last: the page-one
    scan and the later seeks must read the same order or 60 keys vanish."""
    src = _keyed_source(tmp_path, 213, 60)

    rows_written, summary, dst = _run_upsert(tmp_path, src, "0000000000000000000000a1")

    assert rows_written == 213
    assert summary.get("batches") and summary["batches"] >= 2
    assert _dest_ids(dst) == list(range(1, 214))


def test_engine_refuses_seek_when_scan_order_is_not_the_keyset_order(
    tmp_path: Path, small_chunks, monkeypatch, caplog
):
    """A reader that publishes a heap order must not be seeked past: the engine
    keeps paging the held scan and still lands every key."""
    src = _keyed_source(tmp_path, 213, 60)

    def _publish_heap_order(scan_state: dict, order_cols) -> None:
        scan_state["order_cols"] = ["rowid"]

    monkeypatch.setattr(sqlite_reader_mod, "publish_scan_order", _publish_heap_order)

    with caplog.at_level("WARNING", logger="src.transfer.stream"):
        rows_written, _summary, dst = _run_upsert(tmp_path, src, "0000000000000000000000a2")

    assert rows_written == 213
    assert _dest_ids(dst) == list(range(1, 214))
    assert any("continuing the held scan" in r.getMessage() for r in caplog.records)


def test_engine_refuses_seek_when_scan_order_is_unpublished(
    tmp_path: Path, small_chunks, monkeypatch
):
    src = _keyed_source(tmp_path, 213, 60)
    monkeypatch.setattr(sqlite_reader_mod, "publish_scan_order", lambda *_a, **_k: None)

    rows_written, _summary, dst = _run_upsert(tmp_path, src, "0000000000000000000000a3")

    assert rows_written == 213
    assert _dest_ids(dst) == list(range(1, 214))
