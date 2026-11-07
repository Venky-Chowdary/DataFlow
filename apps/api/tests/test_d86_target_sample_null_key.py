"""D86 — NULL sentinel must never reach target-sample SQL.

Gate-8's keyed destination sample bound the ``__DF_SQL_NULL__`` sentinel into
``IN (...)`` — Postgres died with ``invalid input syntax for type bigint`` and
a destination NULL key could never match a source NULL key.

Expected: NULL keys (None, "", the sentinel string) never bind into IN (...);
a destination NULL key matches a source NULL key via an explicit IS NULL
branch; key binds stay typed.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from services.target_sample import read_target_sample  # noqa: E402
from src.transfer.reconcile_step import _source_key_values  # noqa: E402


class _FakeCur:
    def __init__(self, conn):
        self._conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def close(self):
        pass

    def execute(self, sql, params=None):
        self._conn.captured.append((str(sql), tuple(params or ())))
        self._rows = self._conn.rows
        return self._rows

    @property
    def description(self):
        return [(c,) for c in self._conn.cols]

    def fetchall(self):
        return self._rows


class _FakeConn:
    """Captures executed SQL + binds; returns one row keyed NULL."""

    def __init__(self, captured, rows, cols):
        self.captured = captured
        self.rows = rows
        self.cols = cols

    def cursor(self):
        return _FakeCur(self)

    def execute(self, sql, params=None):  # sqlite-style shorthand
        self.captured.append((str(sql), tuple(params or ())))
        cur = _FakeCur(self)
        cur._rows = self.rows
        return cur

    def close(self):
        pass


def _dest() -> dict:
    return {
        "host": "h",
        "port": 1,
        "database": "d",
        "username": "u",
        "password": "p",
        "connection_string": "",
        "ssl": False,
    }


def _sentinel_keys():
    return [10, "__DF_SQL_NULL__", None, "", 20]


def _patch_conn(monkeypatch, target, captured, rows):
    fake = _FakeConn(captured, rows, ["id", "v"])
    if target == "pymssql":
        import sys
        import types

        mod = types.ModuleType("pymssql")
        mod.connect = lambda **_kw: fake
        monkeypatch.setitem(sys.modules, "pymssql", mod)
        return
    import importlib

    mod = importlib.import_module(target)
    monkeypatch.setattr(mod, "get_connection", lambda **_kw: fake)
    if target == "connectors.snowflake_conn":
        monkeypatch.setattr(
            mod, "resolve_or_fold_snowflake_table", lambda cur, sch, t: t
        )


@pytest.mark.parametrize(
    "db_type,conn_module",
    [
        ("postgresql", "connectors.postgresql_conn"),
        ("mysql", "connectors.mysql_conn"),
        ("sqlserver", "pymssql"),
        ("snowflake", "connectors.snowflake_conn"),
    ],
)
def test_target_sample_null_key_includes_is_null(
    monkeypatch, db_type, conn_module
):
    captured: list[tuple] = []
    rows = [(None, "nullrow"), (10, "a"), (20, "b")]
    _patch_conn(monkeypatch, conn_module, captured, rows)
    try:
        out = read_target_sample(
            db_type,
            _dest(),
            schema="public",
            table_name="t",
            columns=["id", "v"],
            sort_key="id",
            key_values=_sentinel_keys(),
        )
    except Exception as exc:  # engine driver may need real kwargs shapes
        pytest.fail(f"{db_type} keyed read failed: {exc}")
    assert captured, "no keyed read executed"
    sql, params = captured[-1][0], captured[-1][1]
    assert "__DF_SQL_NULL__" not in str(params), f"sentinel bound: {params}"
    assert "IS NULL" in sql.upper(), f"no NULL branch: {sql}"
    assert [r["id"] for r in out] == [None, 10, 20]


def test_target_sample_never_binds_null_sentinel_sqlite(tmp_path: Path):
    db = tmp_path / "dst.db"
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE t (id INTEGER, v TEXT)")
    conn.executemany(
        "INSERT INTO t (id, v) VALUES (?, ?)",
        [(None, "nullrow"), (10, "a"), (20, "b"), (30, "c")],
    )
    conn.commit()
    conn.close()
    out = read_target_sample(
        "sqlite",
        {"database": str(db), "connection_string": ""},
        schema="",
        table_name="t",
        columns=["id", "v"],
        sort_key="id",
        key_values=_sentinel_keys(),
    )
    keys = [r["id"] for r in out]
    assert None in keys, "destination NULL key must match a source NULL key"
    assert set(keys) == {None, 10, 20}


def test_source_key_values_never_emits_sentinel():
    recs = [
        {"id": "__DF_SQL_NULL__", "v": "x"},
        {"id": None, "v": "y"},
        {"id": 5, "v": "a"},
        {"id": "", "v": "z"},
        {"id": 6, "v": "b"},
    ]
    out = _source_key_values(recs, sort_key="id", mappings=[])
    assert "__DF_SQL_NULL__" not in out
    assert None in out  # real NULL keys propagate as the IS NULL flag
    assert 5 in out and 6 in out
    assert "" not in out
