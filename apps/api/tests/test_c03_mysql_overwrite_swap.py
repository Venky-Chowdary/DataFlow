"""C03 / X-OW2 — transactional destination boundary per run (MySQL dest).

MySQL auto-commits DDL, so the old overwrite path ran ``DROP TABLE dest``
first and committed it: a reader mid-run saw no destination table at all,
and a failed copy left nothing to roll back to. Every ``*_mysql`` COPY path
now copies into a run-scoped staging table and finishes with one atomic
``RENAME TABLE dest → trash, staging → dest`` plus a trash drop.
"""

from __future__ import annotations

import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from services.mysql_dest_swap import (
    mysql_overwrite_trash_name,
    mysql_staged_overwrite_name,
    mysql_swap_staged_dest,
)


class _RecCur:
    """Minimal cursor recording executes; returns canned counts."""

    def __init__(self, sink: list, *, exists: bool = True, count: int = 0):
        self.sink = sink
        self.exists = exists
        self.count = count
        self.rowcount = 0

    def execute(self, sql, params=None):
        self.sink.append(str(sql))
        return self

    def fetchone(self):
        sql = self.sink[-1]
        if "information_schema.tables" in sql:
            return (1,) if self.exists else None
        if "COUNT(*)" in sql:
            return (self.count,)
        return None

    def fetchall(self):
        return []

    def close(self):
        return None


def test_staging_and_trash_names_are_run_scoped_and_legal():
    a = mysql_staged_overwrite_name("orders")
    b = mysql_staged_overwrite_name("orders")
    assert a != b and a.startswith("df_ow_orders_") and len(a) <= 64
    t = mysql_overwrite_trash_name("orders")
    assert t.startswith("df_trash_orders_") and len(t) <= 64


def test_rename_swap_is_one_atomic_statement_then_drop():
    cur = _RecCur([])
    mysql_swap_staged_dest(
        cur, dest_q="`orders`", staging_q="`df_ow_orders_x`", trash_q="`df_trash_orders_y`"
    )
    assert cur.sink == [
        "RENAME TABLE `orders` TO `df_trash_orders_y`, `df_ow_orders_x` TO `orders`",
        "DROP TABLE `df_trash_orders_y`",
    ]
    # The swap is one statement — readers never see the destination absent.


def _sqlite_source(tmp_path):
    db = tmp_path / "src.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
    conn.executemany("INSERT INTO t VALUES (?, ?)", [(1, "a"), (2, "b"), (3, "c")])
    conn.commit()
    return conn


def _run_copy(tmp_path, monkeypatch, *, boom=None, sink=None):
    import services.copy_sqlite_mysql as mod

    if sink is None:
        sink = []
    source_conn = _sqlite_source(tmp_path)
    cur = _RecCur(sink, exists=True, count=3)
    dest_conn = MagicMock()
    dest_conn.cursor.return_value = cur

    monkeypatch.setattr(mod, "sqlite_connect", lambda cfg: source_conn)
    monkeypatch.setattr(mod, "sqlite_resolved_path", lambda cfg: ":memory:")
    monkeypatch.setattr(mod, "_mysql_connect", lambda cfg: dest_conn)
    monkeypatch.setattr(mod, "settle_fast_path_create_on", lambda *a, **k: None)
    monkeypatch.setattr(
        "connectors.mysql_load_data.mysql_load_data_session_ready",
        lambda *a, **k: (True, ""),
    )
    monkeypatch.setattr(
        mod, "_load_tsv_into_mysql", boom if boom else (lambda *a, **k: None)
    )

    mod.copy_sqlite_to_mysql(
        source_cfg={"filename": ":memory:"},
        source_table="t",
        dest_cfg={},
        dest_table="orders",
        pairs=[("id", "id"), ("name", "name")],
        mysql_ddls=["INT", "TEXT"],
        replace_destination=True,
    )
    return sink


def test_overwrite_stages_then_atomic_swaps(tmp_path, monkeypatch):
    """replace_destination must never DROP the live dest before the swap."""
    sink = _run_copy(tmp_path, monkeypatch)
    sql = "\n".join(sink)
    assert "DROP TABLE IF EXISTS `orders`" not in sql
    assert "df_ow_orders_" in sql, sql
    assert "RENAME TABLE `orders` TO `df_trash_orders_" in sql
    assert "TO `orders`" in sql
    assert sql.index("RENAME TABLE") < sql.index("DROP TABLE `df_trash_orders_")


def test_failed_copy_leaves_dest_table_untouched(tmp_path, monkeypatch):
    """A mid-run failure drops only staging — never the real destination."""
    def _boom(*a, **k):
        raise RuntimeError("disk full mid-copy")

    sink: list[str] = []
    with pytest.raises(RuntimeError):
        _run_copy(tmp_path, monkeypatch, boom=_boom, sink=sink)
    sql = "\n".join(sink)
    assert "DROP TABLE IF EXISTS `orders`" not in sql
    assert "RENAME TABLE" not in sql
    # The only cleanup drop targets the staging table.
    assert "DROP TABLE IF EXISTS `df_ow_orders_" in sql
