"""D165 — ``propagate_columns`` adds a new source column, and says so.

SQLite -> SQLite in-process, preflight ON: run 1 creates the destination, the
source gains a column, run 2 must land it. The job names the column it added
(or the column it left out), and a failed ADD COLUMN fails the job loudly,
naming the column. The schedule and file routes live in
``test_d165_new_source_columns_are_not_skipped.py``.
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
def env(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)

    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    conn = sqlite3.connect(src)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
    conn.executemany("INSERT INTO t VALUES (?,?)", [(i, f"n{i}") for i in range(1, 6)])
    conn.commit()

    def run(**extra):
        request = TransferRequest(
            source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
            destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
            sync_mode="incremental_append",
            stream_contracts=[
                {
                    "name": "t",
                    "cursor_field": "id",
                    "cursor_semantics": "monotonic_sequence",
                    "sync_mode": "incremental_append",
                    "selected": True,
                }
            ],
            validation_mode="strict",
            **extra,
        )
        job_id = "d165" + uuid.uuid4().hex[:15]
        fake.update_job_status(job_id, "pending", transfer_request={})
        return UniversalTransferEngine().execute_tracked(request, job_id), fake.get_job(job_id)

    def dest():
        d = sqlite3.connect(dst)
        try:
            return (
                [r[1] for r in d.execute("PRAGMA table_info(t)")],
                d.execute("SELECT * FROM t ORDER BY id").fetchall(),
            )
        finally:
            d.close()

    return SimpleNamespaceLike(conn=conn, run=run, dest=dest)


class SimpleNamespaceLike:
    def __init__(self, **kw):
        self.__dict__.update(kw)


def _grow(conn):
    conn.execute("ALTER TABLE t ADD COLUMN extra TEXT")
    conn.execute("INSERT INTO t VALUES (6, 'n6', 'x6')")
    conn.commit()


def test_propagate_columns_adds_the_column_and_names_it(env):
    first, _ = env.run()
    assert first.success, first.error
    _grow(env.conn)

    result, job = env.run(schema_policy="propagate_columns")

    assert result.success, result.error
    cols, rows = env.dest()
    assert cols == ["id", "name", "extra"]
    assert rows[-1] == (6, "n6", "x6")
    assert job["schema_propagation"]["added_columns"] == ["extra"]


def test_a_failed_add_column_fails_the_job_naming_the_column(env, monkeypatch):
    import sqlite3 as real_sqlite3
    import types

    import connectors.sqlite_writer as writer

    first, _ = env.run()
    assert first.success, first.error
    _grow(env.conn)

    class Cursor:
        def __init__(self, inner):
            self._inner = inner

        def execute(self, sql, *args):
            if "ADD COLUMN" in str(sql):
                raise real_sqlite3.OperationalError("attempt to write a readonly database")
            return self._inner.execute(sql, *args)

        def __getattr__(self, name):
            return getattr(self._inner, name)

        def __iter__(self):
            return iter(self._inner)

    class Connection:
        def __init__(self, inner):
            self._inner = inner

        def cursor(self, *a, **k):
            return Cursor(self._inner.cursor(*a, **k))

        def __enter__(self):
            self._inner.__enter__()
            return self

        def __exit__(self, *exc):
            return self._inner.__exit__(*exc)

        def __getattr__(self, name):
            return getattr(self._inner, name)

    proxy = types.SimpleNamespace(**vars(real_sqlite3))
    proxy.connect = lambda *a, **k: Connection(real_sqlite3.connect(*a, **k))
    monkeypatch.setattr(writer, "sqlite3", proxy)

    result, _job = env.run(schema_policy="propagate_columns")

    assert not result.success
    assert "extra" in result.error and "ADD COLUMN" in result.error
    assert env.dest()[0] == ["id", "name"]


def test_a_column_nothing_maps_is_named_when_the_run_skips_preflight(env):
    first, _ = env.run()
    assert first.success, first.error
    _grow(env.conn)

    result, job = env.run(
        skip_preflight=True,
        mappings=[
            {"source": "id", "target": "id", "confidence": 1.0},
            {"source": "name", "target": "name", "confidence": 1.0},
        ],
    )

    assert job["schema_propagation"]["skipped_columns"] == ["extra"]
    assert env.dest()[0] == ["id", "name"]
    del result
