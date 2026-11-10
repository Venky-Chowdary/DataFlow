"""D335 — an additive source column must not re-append the watermark row.

incremental_append (key ``id``, cursor ``updated_at``) with
``schema_policy=propagate_columns``. Run 1 loads six rows, run 2 appends ids
7 and 8 (watermark = id 8's ``updated_at``). The source then gains
``tier INTEGER DEFAULT 1`` and two new ids after the watermark. Run 3 re-reads
the watermark's tie group (``cursor >= watermark``), so id 8 comes back with
``tier = 1``.

Before the fix the boundary dedupe kept any re-read peer whose *new* column
carried a value, even on a keyed append whose identity is (cursor, key): id 8
was appended a second time, Gate-8 failed on the double append and the
duplicate stayed in the destination. The same (cursor, key) is the same row
version; a value in a column the destination never had is not a new version.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


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
    job_id = "d335" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _request(src: Path, dst: Path) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        sync_mode="incremental_append",
        stream_contracts=[
            {
                "name": "t",
                "cursor_field": "updated_at",
                "cursor_semantics": "modification_timestamp",
                "primary_key": ["id"],
                "sync_mode": "incremental_append",
                "selected": True,
            }
        ],
        schema_policy="propagate_columns",
        validation_mode="strict",
    )


def _ts(i: int) -> str:
    # ISO with ``T``: the SQLite writer stores a timestamp-shaped TEXT cursor
    # in this spelling, so the boundary lookup matches it.
    return f"2026-11-10T12:{i:02d}:00"


def _dest(dst: Path):
    conn = sqlite3.connect(dst)
    try:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(t)")]
        ids = [r[0] for r in conn.execute("SELECT id FROM t ORDER BY id")]
    finally:
        conn.close()
    return cols, ids


@pytest.mark.parametrize("dest_has_key", [True, False], ids=["dest-pk", "dest-no-pk"])
def test_add_column_does_not_reappend_the_watermark_row(tmp_path, isolated, dest_has_key):
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    if not dest_has_key:
        # A pre-existing destination without a unique key (the QA MySQL
        # table): a re-appended row is not refused by the database, it lands.
        pre = sqlite3.connect(dst)
        pre.execute("CREATE TABLE t (id INTEGER, name TEXT, updated_at TEXT)")
        pre.commit()
        pre.close()
    conn = sqlite3.connect(src)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, updated_at TEXT)")
    conn.executemany("INSERT INTO t VALUES (?,?,?)", [(i, f"n{i}", _ts(i)) for i in range(1, 7)])
    conn.commit()

    first = _run(isolated, _request(src, dst))
    assert first.success, first.error
    conn.executemany("INSERT INTO t VALUES (?,?,?)", [(i, f"n{i}", _ts(i)) for i in (7, 8)])
    conn.commit()
    second = _run(isolated, _request(src, dst))
    assert second.success, second.error
    assert _dest(dst)[1] == list(range(1, 9))

    conn.execute("ALTER TABLE t ADD COLUMN tier INTEGER DEFAULT 1")
    conn.executemany(
        "INSERT INTO t (id, name, updated_at, tier) VALUES (?,?,?,?)",
        [(i, f"n{i}", _ts(i), 1) for i in (9, 10)],
    )
    conn.commit()
    conn.close()

    third = _run(isolated, _request(src, dst))

    cols, ids = _dest(dst)
    assert ids == list(range(1, 11)), f"boundary id re-appended: {ids}"
    assert third.success, third.error
    assert "tier" in cols
    conn = sqlite3.connect(dst)
    try:
        tiers = dict(conn.execute("SELECT id, tier FROM t WHERE id IN (8, 9, 10)").fetchall())
    finally:
        conn.close()
    assert tiers == {8: None, 9: 1, 10: 1}


def test_window_duplicate_verdict_names_the_duplicated_key():
    from services.incremental_drift import fail_on_window_duplicates, measure_window_duplicates

    window = [{"id": i, "updated_at": _ts(i)} for i in (8, 9, 10)]
    dest = [window[0], *window]  # id 8 landed twice
    dup = measure_window_duplicates(
        source_rows=window, target_rows=dest,
        match_cols=["updated_at", "id"], dest_db_type="sqlite",
    )
    out = fail_on_window_duplicates({"passed": True, "message": ""}, dup)

    assert out["passed"] is False
    assert dup["rows"] == 1
    assert "id=8 (2 copies, source holds 1)" in out["message"]
    assert "id=9" not in out["message"] and "id=10" not in out["message"]


# ── live: PostgreSQL → MySQL, the QA route ────────────────────────────────


def _pg_conn():
    import psycopg2

    from tests.helpers.live_env import pg_creds

    c = pg_creds()
    conn = psycopg2.connect(
        host=c["host"], port=int(c["port"]), dbname=c["database"],
        user=c["username"], password=c["password"], connect_timeout=5,
    )
    conn.autocommit = True
    return conn


def _my_conn():
    import pymysql

    from tests.helpers.live_env import mysql_creds

    c = mysql_creds()
    return pymysql.connect(
        host=c["host"], port=int(c["port"]), user=c["username"],
        password=c["password"], database=c["database"], autocommit=True,
    )


def _live_request(src_table: str, dst_table: str) -> TransferRequest:
    from tests.helpers.live_env import mysql_creds, pg_creds

    p, m = pg_creds(), mysql_creds()
    return TransferRequest(
        source=EndpointConfig(
            kind="database", format="postgresql", host=str(p["host"]), port=int(p["port"]),
            database=str(p["database"]), schema="public", table=src_table,
            username=str(p["username"]), password=str(p["password"]), ssl=False,
        ),
        destination=EndpointConfig(
            kind="database", format="mysql", host=str(m["host"]), port=int(m["port"]),
            database=str(m["database"]), table=dst_table,
            username=str(m["username"]), password=str(m["password"]), ssl=False,
        ),
        sync_mode="incremental_append",
        stream_contracts=[
            {
                "name": src_table,
                "cursor_field": "updated_at",
                "cursor_semantics": "modification_timestamp",
                "primary_key": ["id"],
                "sync_mode": "incremental_append",
                "selected": True,
            }
        ],
        schema_policy="propagate_columns",
        validation_mode="strict",
    )


@pytest.mark.parametrize("dest_has_key", [True, False], ids=["dest-pk", "dest-no-pk"])
def test_pg_to_mysql_add_column_does_not_reappend_the_watermark_row(isolated, dest_has_key):
    from tests.helpers.live_env import mysql_up, pg_up

    if not (pg_up() and mysql_up()):
        pytest.skip("live PostgreSQL and MySQL not available")
    suffix = uuid.uuid4().hex[:8]
    src_table, dst_table = f"d335s_{suffix}", f"d335d_{suffix}"
    pg, my = _pg_conn(), _my_conn()
    try:
        with pg.cursor() as cur:
            cur.execute(
                f"CREATE TABLE public.{src_table} (id INT PRIMARY KEY, name TEXT, "
                "updated_at TIMESTAMP NOT NULL)"
            )
            cur.executemany(
                f"INSERT INTO public.{src_table} VALUES (%s,%s,%s)",
                [(i, f"n{i}", _ts(i)) for i in range(1, 7)],
            )
        if not dest_has_key:
            with my.cursor() as cur:
                cur.execute(
                    f"CREATE TABLE `{dst_table}` (id BIGINT, name VARCHAR(40), updated_at DATETIME)"
                )
        assert _run(isolated, _live_request(src_table, dst_table)).success
        with pg.cursor() as cur:
            cur.executemany(
                f"INSERT INTO public.{src_table} VALUES (%s,%s,%s)",
                [(i, f"n{i}", _ts(i)) for i in (7, 8)],
            )
        second = _run(isolated, _live_request(src_table, dst_table))
        assert second.success, second.error
        with pg.cursor() as cur:
            cur.execute(f"ALTER TABLE public.{src_table} ADD COLUMN tier INT DEFAULT 1")
            cur.executemany(
                f"INSERT INTO public.{src_table} (id, name, updated_at, tier) VALUES (%s,%s,%s,%s)",
                [(i, f"n{i}", _ts(i), 1) for i in (9, 10)],
            )

        third = _run(isolated, _live_request(src_table, dst_table))

        with my.cursor() as cur:
            cur.execute(f"SELECT id, tier FROM `{dst_table}` ORDER BY id")
            got = [tuple(r) for r in cur.fetchall()]
        assert [r[0] for r in got] == list(range(1, 11)), f"boundary id re-appended: {got}"
        assert third.success, third.error
        assert dict(got)[8] is None and dict(got)[9] == 1 and dict(got)[10] == 1
    finally:
        with pg.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS public.{src_table}")
        with my.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{dst_table}`")
        pg.close()
        my.close()
