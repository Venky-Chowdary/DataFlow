"""D124 / D60 / D98 — rows tying the stored watermark below the last key read.

An incremental run stores the composite watermark ``(cursor, last key)``. A row
updated to exactly that cursor value, or a brand-new row inserted at it, whose
key sorts *below* the last key read was silently skipped by every later run:
the read resumed strictly after ``(watermark, last key)`` instead of re-reading
the whole tie group at the watermark.

Real routes through the engine with preflight ON — SQLite → SQLite in process,
PostgreSQL → PostgreSQL and MySQL → PostgreSQL live — both incremental modes,
exact destination contents after each run and after a quiet poll.
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
from tests.helpers.live_env import mysql_creds, mysql_up, pg_creds, pg_up  # noqa: E402


class _Sqlite:
    def __init__(self, path: Path):
        self.path = path

    def execute(self, sql: str) -> None:
        conn = sqlite3.connect(str(self.path))
        try:
            with conn:
                conn.execute(sql)
        finally:
            conn.close()

    def rows(self, table: str) -> list[tuple]:
        conn = sqlite3.connect(str(self.path))
        try:
            return conn.execute(f'SELECT id, seq, v FROM "{table}" ORDER BY id, v').fetchall()
        finally:
            conn.close()

    def endpoint(self, table: str) -> EndpointConfig:
        return EndpointConfig(kind="database", format="sqlite", database=str(self.path), table=table)

    def drop(self, table: str) -> None:
        self.execute(f'DROP TABLE IF EXISTS "{table}"')


class _Postgres:
    def _connect(self):
        import psycopg2

        c = pg_creds()
        return psycopg2.connect(
            host=c["host"], port=int(c["port"]), dbname=c["database"],
            user=c["username"], password=c["password"], connect_timeout=5,
        )

    def execute(self, sql: str) -> None:
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()
        finally:
            conn.close()

    def rows(self, table: str) -> list[tuple]:
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f'SELECT id, seq, v FROM public."{table}" ORDER BY id, v')
                return [tuple(r) for r in cur.fetchall()]
        finally:
            conn.close()

    def endpoint(self, table: str) -> EndpointConfig:
        c = pg_creds()
        return EndpointConfig(
            kind="database", format="postgresql", host=str(c["host"]), port=int(c["port"]),
            database=str(c["database"]), schema="public", table=table,
            username=str(c["username"]), password=str(c["password"]), ssl=False,
        )

    def drop(self, table: str) -> None:
        self.execute(f'DROP TABLE IF EXISTS public."{table}"')


class _Mysql:
    def _connect(self):
        import pymysql

        c = mysql_creds()
        return pymysql.connect(
            host=c["host"], port=int(c["port"]), user=c["username"],
            password=c["password"], database=c["database"], autocommit=True,
        )

    def execute(self, sql: str) -> None:
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(sql.replace('"', "`").replace("TEXT", "VARCHAR(40)"))
        finally:
            conn.close()

    def endpoint(self, table: str) -> EndpointConfig:
        c = mysql_creds()
        return EndpointConfig(
            kind="database", format="mysql", host=str(c["host"]), port=int(c["port"]),
            database=str(c["database"]), table=table,
            username=str(c["username"]), password=str(c["password"]), ssl=False,
        )

    def drop(self, table: str) -> None:
        self.execute(f'DROP TABLE IF EXISTS "{table}"')


# (availability probe, source, destination, force the row path). A
# pass-through source filter declines the COPY fast path, so the row-path
# seek is exercised on the live engines too.
_ROUTES = {
    "sqlite_sqlite": (None, "sqlite", "sqlite", False),
    "sqlite_sqlite_rowpath": (None, "sqlite", "sqlite", True),
    "pg_pg": (pg_up, "pg", "pg", False),
    "pg_pg_rowpath": (pg_up, "pg", "pg", True),
    "mysql_pg": (lambda: pg_up() and mysql_up(), "mysql", "pg", False),
    "mysql_pg_rowpath": (lambda: pg_up() and mysql_up(), "mysql", "pg", True),
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


def _backend(kind: str, tmp_path: Path, role: str):
    if kind == "sqlite":
        return _Sqlite(tmp_path / f"{role}.sqlite")
    return _Postgres() if kind == "pg" else _Mysql()


def _run(fake, src, dst, src_table: str, dst_table: str, mode: str, row_path: bool):
    job_id = "d124" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    request = TransferRequest(
        source=src.endpoint(src_table),
        destination=dst.endpoint(dst_table),
        sync_mode=mode,
        stream_contracts=[
            {
                "name": src_table,
                "cursor_field": "seq",
                "cursor_semantics": "monotonic_sequence",
                "primary_key": ["id"],
                "sync_mode": mode,
                "selected": True,
            }
        ],
        validation_mode="strict",
        source_filter={"column": "id", "op": "gt", "value": 0} if row_path else {},
    )
    return UniversalTransferEngine().execute_tracked(request, job_id)


@pytest.mark.parametrize("mode", ["incremental_deduped", "incremental_append"])
@pytest.mark.parametrize("route", sorted(_ROUTES))
def test_tie_at_watermark_below_last_key_lands(tmp_path, isolated, route, mode):
    probe, src_kind, dst_kind, row_path = _ROUTES[route]
    if probe is not None and not probe():
        pytest.skip(f"{route}: live database not available")
    src = _backend(src_kind, tmp_path, "src")
    dst = _backend(dst_kind, tmp_path, "dst")
    suffix = uuid.uuid4().hex[:8]
    src_table, dst_table = f"d124s_{suffix}", f"d124d_{suffix}"
    src.execute(f'CREATE TABLE "{src_table}" (id INTEGER PRIMARY KEY, seq INTEGER NOT NULL, v TEXT)')
    try:
        # Run 1 ends on the tie group seq=5 → stored watermark (5, 10).
        src.execute(
            f'INSERT INTO "{src_table}" VALUES '
            + ", ".join(f"({i}, {min(i, 5)}, 'v{i}')" for i in range(4, 11))
        )
        first = _run(isolated, src, dst, src_table, dst_table, mode, row_path)
        assert first.success, first.error
        assert len(dst.rows(dst_table)) == 7

        # Cursor exactly at the watermark, key below the last key read (10):
        # a brand-new insert, and (upsert) an update restamped to the watermark.
        src.execute(f"INSERT INTO \"{src_table}\" VALUES (2, 5, 'new2')")
        if mode == "incremental_deduped":
            # Restamped onto the watermark, and edited inside the tie group.
            src.execute(f"UPDATE \"{src_table}\" SET seq = 5, v = 'upd4' WHERE id = 4")
            src.execute(f"UPDATE \"{src_table}\" SET v = 'upd7' WHERE id = 7")
        second = _run(isolated, src, dst, src_table, dst_table, mode, row_path)
        assert second.success, second.error
        got = dst.rows(dst_table)
        expected = [(2, 5, "new2")]
        expected += [(4, 5, "upd4")] if mode == "incremental_deduped" else [(4, 4, "v4")]
        expected += [(i, 5, f"v{i}") for i in range(5, 11)]
        if mode == "incremental_deduped":
            expected[expected.index((7, 5, "v7"))] = (7, 5, "upd7")
        # The tie peers run 1 delivered are never appended twice.
        assert got == expected, got

        third = _run(isolated, src, dst, src_table, dst_table, mode, row_path)  # quiet poll
        assert third.success, third.error
        assert dst.rows(dst_table) == expected
    finally:
        src.drop(src_table)
        dst.drop(dst_table)


def test_unkeyed_append_tie_insert_lands_once(tmp_path, isolated):
    """No key anywhere: the re-read tie peers are deduped by full-row content."""
    src, dst = _Sqlite(tmp_path / "src.sqlite"), _Sqlite(tmp_path / "dst.sqlite")
    src.execute('CREATE TABLE "t" (id INTEGER, seq INTEGER NOT NULL, v TEXT)')
    src.execute(
        'INSERT INTO "t" VALUES ' + ", ".join(f"({i}, {min(i, 5)}, 'v{i}')" for i in range(4, 11))
    )

    def run():
        job_id = "d124u" + uuid.uuid4().hex[:16]
        isolated.update_job_status(job_id, "pending", transfer_request={})
        request = TransferRequest(
            source=src.endpoint("t"),
            destination=dst.endpoint("t"),
            sync_mode="incremental_append",
            stream_contracts=[
                {
                    "name": "t",
                    "cursor_field": "seq",
                    "cursor_semantics": "monotonic_sequence",
                    "sync_mode": "incremental_append",
                    "selected": True,
                }
            ],
            validation_mode="strict",
        )
        return UniversalTransferEngine().execute_tracked(request, job_id)

    first = run()
    assert first.success, first.error
    src.execute("INSERT INTO \"t\" VALUES (2, 5, 'new2')")
    second = run()
    assert second.success, second.error
    expected = [(2, 5, "new2"), (4, 4, "v4")] + [(i, 5, f"v{i}") for i in range(5, 11)]
    assert dst.rows("t") == expected
    third = run()
    assert third.success, third.error
    assert dst.rows("t") == expected
