"""C03 / X-OW2 — transactional destination boundary per run (MySQL dest).

MySQL auto-commits DDL, so the old overwrite path ran ``DROP TABLE dest``
first and committed it: a reader mid-run saw no destination table at all,
and a failed copy left nothing to roll back to. Every ``*_mysql`` COPY path
now copies into a run-scoped staging table. Publishing keeps the live table
object (D195: a rename swap replaced it with DataFlow's DDL and lost every
CHECK / FK / DEFAULT / index): the rows move with ``DELETE`` + ``INSERT ...
SELECT`` inside one transaction, then the staging table is dropped.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from services.dest_overwrite_swap import (
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
        self.description = None

    def execute(self, sql, params=None):
        self.sink.append(str(sql))
        text = str(sql)
        if "WHERE 1=0" in text:
            self.description = [("id",), ("name",)]
        elif text.lstrip().upper().startswith(("SELECT", "SHOW")):
            self.description = [("x",)]
        else:
            self.description = None
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


def _live_untouched(sink: list[str], live: str) -> None:
    joined = "\n".join(sink)
    assert "RENAME" not in joined.upper(), joined
    assert "sp_rename" not in joined, joined
    assert "SWAP WITH" not in joined.upper(), joined
    assert f"DROP TABLE {live}" not in joined, joined


def test_mysql_publish_moves_rows_in_one_transaction_and_keeps_live_table():
    cur = _RecCur([])
    mysql_swap_staged_dest(
        cur, dest_q="`orders`", staging_q="`df_ow_orders_x`", trash_q="`df_trash_orders_y`"
    )
    _live_untouched(cur.sink, "`orders`")
    dml = [q for q in cur.sink if not q.lstrip().upper().startswith(("SELECT", "SHOW"))]
    assert dml == [
        "START TRANSACTION",
        "DELETE FROM `orders`",
        "INSERT INTO `orders` (`id`, `name`) SELECT `id`, `name` FROM `df_ow_orders_x`",
        "COMMIT",
        "DROP TABLE `df_ow_orders_x`",
    ]


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
    _live_untouched(sink, "`orders`")
    assert "DELETE FROM `orders`" in sql
    assert "INSERT INTO `orders` (`id`, `name`) SELECT" in sql
    assert sql.index("DELETE FROM `orders`") < sql.index("DROP TABLE `df_ow_orders_")


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


def test_pg_publish_truncates_and_inserts_inside_the_callers_transaction():
    from services.dest_overwrite_swap import pg_swap_staged_dest

    cur = _RecCur([])
    pg_swap_staged_dest(
        cur,
        schema="public",
        dest_table="orders",
        staging_table="df_ow_orders_x",
        trash_table="df_trash_orders_y",
        quote=lambda n: f'"{n}"',
    )
    _live_untouched(cur.sink, '"public"."orders"')
    dml = [q for q in cur.sink if not q.lstrip().upper().startswith("SELECT")]
    assert dml == [
        "SAVEPOINT df_overwrite_publish",
        'TRUNCATE TABLE "public"."orders"',
        "RELEASE SAVEPOINT df_overwrite_publish",
        'INSERT INTO "public"."orders" ("id", "name") SELECT "id", "name" '
        'FROM "public"."df_ow_orders_x"',
        'DROP TABLE "public"."df_ow_orders_x"',
    ]
    assert "COMMIT" not in cur.sink  # the caller commits once


def test_sqlserver_publish_truncates_and_inserts_inside_the_callers_transaction():
    from services.dest_overwrite_swap import sqlserver_swap_staged_dest

    cur = _RecCur([])
    sqlserver_swap_staged_dest(
        cur,
        schema="dbo",
        dest_table="orders",
        staging_table="df_ow_orders_x",
        trash_table="df_trash_orders_y",
        quote=lambda n: f"[{n}]",
    )
    _live_untouched(cur.sink, "[dbo].[orders]")
    dml = [q for q in cur.sink if not q.lstrip().upper().startswith("SELECT")]
    assert dml == [
        "TRUNCATE TABLE [dbo].[orders]",
        "INSERT INTO [dbo].[orders] ([id], [name]) SELECT [id], [name] FROM [dbo].[df_ow_orders_x]",
        "DROP TABLE [dbo].[df_ow_orders_x]",
    ]


def test_snowflake_publish_keeps_the_live_table():
    from services.dest_overwrite_swap import snowflake_swap_staged_dest

    cur = _RecCur([])
    snowflake_swap_staged_dest(
        cur, dest_ref='"DB"."PUBLIC"."ORDERS"', staging_ref='"DB"."PUBLIC"."DF_OW_X"'
    )
    _live_untouched(cur.sink, '"DB"."PUBLIC"."ORDERS"')
    dml = [q for q in cur.sink if not q.lstrip().upper().startswith("SELECT")]
    assert dml == [
        "BEGIN",
        'DELETE FROM "DB"."PUBLIC"."ORDERS"',
        'INSERT INTO "DB"."PUBLIC"."ORDERS" ("id", "name") SELECT "id", "name" '
        'FROM "DB"."PUBLIC"."DF_OW_X"',
        "COMMIT",
        'DROP TABLE IF EXISTS "DB"."PUBLIC"."DF_OW_X"',
    ]


def test_oracle_publish_is_a_dml_boundary_then_purge():
    from services.dest_overwrite_swap import oracle_swap_staged_dest

    cur = _RecCur([])
    oracle_swap_staged_dest(
        cur,
        dest_ref='"HR"."ORDERS"',
        staging_ref='"HR"."DF_OW_ORDERS_X"',
        dest_table="ORDERS",
        trash_table="DF_TRASH_ORDERS_Y",
    )
    _live_untouched(cur.sink, '"HR"."ORDERS"')
    dml = [q for q in cur.sink if not q.lstrip().upper().startswith("SELECT")]
    assert dml == [
        'DELETE FROM "HR"."ORDERS"',
        'INSERT INTO "HR"."ORDERS" ("id", "name") SELECT "id", "name" FROM "HR"."DF_OW_ORDERS_X"',
        "COMMIT",
        'DROP TABLE "HR"."DF_OW_ORDERS_X" PURGE',
    ]


def test_publish_declines_when_staging_adds_a_column():
    from services.copy_fast_path import FastPathUnavailable
    from services.dest_overwrite_swap import pg_swap_staged_dest

    class _Wider(_RecCur):
        def execute(self, sql, params=None):
            super().execute(sql, params)
            if "WHERE 1=0" in str(sql) and "df_ow" in str(sql):
                self.description = [("id",), ("name",), ("added",)]
            return self

    cur = _Wider([])
    with pytest.raises(FastPathUnavailable, match="added"):
        pg_swap_staged_dest(
            cur, schema="public", dest_table="orders", staging_table="df_ow_orders_x",
            quote=lambda n: f'"{n}"',
        )
    assert not any(q.startswith(("TRUNCATE", "DELETE", "INSERT")) for q in cur.sink)


def test_no_copy_path_drops_and_commits_dest_before_copy():
    """C03: no overwrite path may commit the drop before the copy finishes.

    Transactional-DDL engines (PG, SQL Server, SQLite, DuckDB) may keep the
    DROP inside the run's transaction — it rolls back with the run. The
    forbidden shape is DROP + early COMMIT (dest absent for the whole run),
    and every overwrite on MySQL/Oracle/Snowflake/BigQuery must stage + swap.
    """
    import re

    api = Path(__file__).resolve().parents[1]
    offenders = []
    for src in (api / "services").glob("copy_*_*.py"):
        text = src.read_text()
        for m in re.finditer(r"if replace_destination and exists:", text):
            tail = text[m.end():m.end() + 700]
            head = tail.split("exists = False")[0]
            if ".commit()" in head:
                offenders.append(f"{src.name}@{m.start()} early-commit")
            # MySQL/Oracle DDL always auto-commits — an unstaged DROP on a
            # *destination* of those engines is the same defect even without
            # an explicit commit().
            auto_commit_ddl = src.stem.rsplit("_", 1)[-1] in {"mysql", "oracle"}
            if auto_commit_ddl and "DROP TABLE" in head \
                    and "staged_overwrite_name" not in tail \
                    and "mysql_staged_overwrite_name" not in tail:
                offenders.append(f"{src.name}@{m.start()} unstaged-drop")
    assert offenders == []
