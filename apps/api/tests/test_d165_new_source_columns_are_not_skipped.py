"""D165 — a new source column must move when the setting says to add it.

Two causes, one per route family:

* A database source read the table through a metadata cache that is only
  coherent with this process's own DDL (5 minute TTL). A column added upstream
  inside that window was read with the old column list and silently never
  moved. A run now starts from the table as it is.
* A schedule replays the mappings stored when the table was created. They
  predate the column, so under a propagate policy (or ``backfill_new_fields``)
  the run was refused for an unmapped source column instead of adding it.

Every route below runs through the engine with preflight ON. Without approval
the column still blocks and the destination is untouched.
"""

from __future__ import annotations

import os
import socket
import sqlite3
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

PG_PASSWORD = os.environ.get("PGPASSWORD") or "dataflow"

APPROVALS = {
    "propagate": {"schema_policy": "propagate_columns"},
    "backfill": {"backfill_new_fields": True},
}


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _run(fake, request: TransferRequest):
    job_id = "d165" + uuid.uuid4().hex[:15]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _stream(table: str) -> list[dict]:
    return [
        {
            "name": table,
            "cursor_field": "id",
            "cursor_semantics": "monotonic_sequence",
            "sync_mode": "incremental_append",
            "selected": True,
        }
    ]


def _stored_maps(*names: str) -> list[dict]:
    """What run 1 stored: every column stamped create_new (the table was absent)."""
    return [
        {
            "source": n,
            "target": n,
            "confidence": 1.0,
            "create_new": True,
            "assignment_strategy": "create_compatible_new",
        }
        for n in names
    ]


# ── unit ──────────────────────────────────────────────────────────────────


def test_a_run_forgets_cached_shapes_of_the_tables_it_moves():
    from services import reflection_cache

    reflection_cache.reset_for_tests()
    reflection_cache.get_or_load_by_identity("db", "public", "orders", "reflect", lambda: ["id"])
    reflection_cache.get_or_load_by_identity("db", "public", "other", "reflect", lambda: ["id"])

    assert reflection_cache.invalidate_named_tables("ORDERS", None) == 1
    seen = []
    reflection_cache.get_or_load_by_identity(
        "db", "public", "orders", "reflect", lambda: seen.append(1) or ["id", "new"]
    )
    reflection_cache.get_or_load_by_identity(
        "db", "public", "other", "reflect", lambda: seen.append(2) or []
    )
    assert seen == [1], "only the named table is reloaded"
    reflection_cache.reset_for_tests()


@pytest.mark.parametrize("backfill", [True, False])
def test_schedule_guard_treats_backfill_as_propagate(backfill):
    from services.schedule_runner import _unmapped_added_columns
    from services.source_schema_memory import REVIEW

    sched = SimpleNamespace(schema_policy="manual_review", backfill_new_fields=backfill)
    request = SimpleNamespace(mappings=_stored_maps("id"))
    verdict = SimpleNamespace(verdict=REVIEW, additive=[{"kind": "add_column", "column": "extra"}])

    assert _unmapped_added_columns(sched, request, verdict) == ([] if backfill else ["extra"])


# ── SQLite streaming route (cached reflection) ───────────────────────────


def _sqlite_source(tmp_path: Path):
    src = tmp_path / "src.sqlite"
    conn = sqlite3.connect(src)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
    conn.executemany("INSERT INTO t VALUES (?,?)", [(1, "a"), (2, "b")])
    conn.commit()
    return conn, src


def _sqlite_request(src: Path, dst: Path, **extra):
    return TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        sync_mode="incremental_append",
        stream_contracts=_stream("t"),
        validation_mode="strict",
        **extra,
    )


def _sqlite_dest(dst: Path):
    conn = sqlite3.connect(dst)
    try:
        return (
            [r[1] for r in conn.execute("PRAGMA table_info(t)")],
            conn.execute("SELECT * FROM t ORDER BY id").fetchall(),
        )
    finally:
        conn.close()


def _grow_sqlite_source(conn):
    conn.execute("ALTER TABLE t ADD COLUMN extra TEXT")
    conn.executemany("INSERT INTO t VALUES (?,?,?)", [(3, "c", "x3"), (4, "d", "x4")])
    conn.commit()


@pytest.mark.parametrize("approval", sorted(APPROVALS))
def test_stream_reads_a_column_added_upstream_inside_the_cache_window(tmp_path, isolated, approval):
    conn, src = _sqlite_source(tmp_path)
    dst = tmp_path / "dst.sqlite"
    assert _run(isolated, _sqlite_request(src, dst)).success
    _grow_sqlite_source(conn)

    result = _run(isolated, _sqlite_request(src, dst, **APPROVALS[approval]))

    assert result.success, result.error
    cols, rows = _sqlite_dest(dst)
    assert cols == ["id", "name", "extra"]
    assert rows == [(1, "a", None), (2, "b", None), (3, "c", "x3"), (4, "d", "x4")]


@pytest.mark.parametrize("approval", sorted(APPROVALS))
def test_scheduled_replay_of_stored_mappings_adds_the_new_column(tmp_path, isolated, approval):
    conn, src = _sqlite_source(tmp_path)
    dst = tmp_path / "dst.sqlite"
    stored = _stored_maps("id", "name")
    assert _run(isolated, _sqlite_request(src, dst, mappings=stored)).success
    _grow_sqlite_source(conn)

    result = _run(isolated, _sqlite_request(src, dst, mappings=stored, **APPROVALS[approval]))

    assert result.success, result.error
    cols, rows = _sqlite_dest(dst)
    assert cols == ["id", "name", "extra"]
    assert [r[2] for r in rows] == [None, None, "x3", "x4"]


def test_scheduled_replay_without_approval_still_blocks_and_leaves_the_table(tmp_path, isolated):
    conn, src = _sqlite_source(tmp_path)
    dst = tmp_path / "dst.sqlite"
    stored = _stored_maps("id", "name")
    assert _run(isolated, _sqlite_request(src, dst, mappings=stored)).success
    _grow_sqlite_source(conn)

    result = _run(isolated, _sqlite_request(src, dst, mappings=stored))

    assert not result.success
    assert _sqlite_dest(dst)[0] == ["id", "name"]


# ── file source ──────────────────────────────────────────────────────────


def _csv_request(dst: Path, content: bytes, **extra):
    return TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        source_filename="t.csv",
        source_content=content,
        sync_mode="full_refresh_append",
        validation_mode="strict",
        **extra,
    )


@pytest.mark.parametrize("approval", sorted(APPROVALS))
def test_file_upload_with_a_new_column_lands_it_when_approved(tmp_path, isolated, approval):
    dst = tmp_path / "dst.sqlite"
    assert _run(isolated, _csv_request(dst, b"id,name\n1,a\n2,b\n")).success

    result = _run(
        isolated, _csv_request(dst, b"id,name,extra\n3,c,x3\n4,d,x4\n", **APPROVALS[approval])
    )

    assert result.success, result.error
    cols, rows = _sqlite_dest(dst)
    assert cols == ["id", "name", "extra"]
    assert rows[-2:] == [(3, "c", "x3"), (4, "d", "x4")]


def test_file_upload_with_a_new_column_blocks_without_approval(tmp_path, isolated):
    dst = tmp_path / "dst.sqlite"
    assert _run(isolated, _csv_request(dst, b"id,name\n1,a\n")).success

    result = _run(isolated, _csv_request(dst, b"id,name,extra\n2,b,x2\n"))

    assert not result.success
    assert _sqlite_dest(dst)[0] == ["id", "name"]


# ── PostgreSQL streaming route, stored mappings ──────────────────────────


def test_pg_scheduled_replay_adds_the_new_column_under_propagate(tmp_path, isolated):
    try:
        socket.create_connection(("127.0.0.1", 5432), timeout=1).close()
    except OSError:
        pytest.skip("PostgreSQL not reachable on :5432")
    import psycopg2

    try:
        conn = psycopg2.connect(
            host="127.0.0.1", user="dataflow", password=PG_PASSWORD, dbname="dataflow"
        )
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL credentials rejected: {exc}")
    conn.autocommit = True
    cur = conn.cursor()
    suffix = uuid.uuid4().hex[:8]
    src_t, dst_t = f"d165s_{suffix}", f"d165d_{suffix}"

    def endpoint(table):
        return EndpointConfig(
            kind="database", format="postgresql", host="127.0.0.1", port=5432,
            database="dataflow", username="dataflow", password=PG_PASSWORD,
            schema="public", table=table,
        )

    def request(**extra):
        return TransferRequest(
            source=endpoint(src_t), destination=endpoint(dst_t),
            sync_mode="incremental_append", stream_contracts=_stream(src_t),
            validation_mode="strict", mappings=_stored_maps("id", "name"), **extra,
        )

    try:
        cur.execute(f"CREATE TABLE {src_t} (id INT PRIMARY KEY, name TEXT)")
        cur.executemany(f"INSERT INTO {src_t} VALUES (%s,%s)", [(1, "a"), (2, "b")])
        assert _run(isolated, request()).success
        cur.execute(f"ALTER TABLE {src_t} ADD COLUMN extra TEXT")
        cur.executemany(f"INSERT INTO {src_t} VALUES (%s,%s,%s)", [(3, "c", "x3"), (4, "d", "x4")])

        blocked = _run(isolated, request())
        assert not blocked.success
        cur.execute(f"SELECT count(*) FROM information_schema.columns WHERE table_name='{dst_t}'")
        assert cur.fetchone()[0] == 2

        result = _run(isolated, request(schema_policy="propagate_columns"))

        assert result.success, result.error
        cur.execute(f"SELECT id, extra FROM {dst_t} ORDER BY id")
        assert cur.fetchall() == [(1, None), (2, None), (3, "x3"), (4, "x4")]
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {src_t}")
        cur.execute(f"DROP TABLE IF EXISTS {dst_t}")
        conn.close()
