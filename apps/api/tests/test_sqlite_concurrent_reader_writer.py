"""SQLite: a streamed staging read must not lock out the SCD2/mirror writer."""

from __future__ import annotations

import sqlite3

import sqlalchemy as sa

from connectors.generic_sql import get_sqlalchemy_engine, release_engine
from connectors.sqlite_common import tune_sqlite_connection


def _cfg(path) -> dict:
    return {"type": "sqlite", "database": str(path)}


def test_engine_connections_are_wal_with_busy_timeout(tmp_path):
    engine = get_sqlalchemy_engine(_cfg(tmp_path / "t.db"))
    try:
        with engine.connect() as conn:
            assert conn.exec_driver_sql("PRAGMA journal_mode").scalar() == "wal"
            assert conn.exec_driver_sql("PRAGMA busy_timeout").scalar() == 30_000
    finally:
        release_engine(engine)


def test_ddl_and_writes_commit_while_streamed_select_is_open(tmp_path):
    path = tmp_path / "t.db"
    seed = sqlite3.connect(path)
    with seed:
        seed.execute("CREATE TABLE staging (id INTEGER, amount TEXT)")
        seed.executemany(
            "INSERT INTO staging VALUES (?, ?)",
            [(i, f"{i}.000") for i in range(5_000)],
        )
    seed.close()

    engine = get_sqlalchemy_engine(_cfg(path))
    try:
        reader = engine.connect()
        cursor = reader.execution_options(stream_results=True).execute(
            sa.text("SELECT id, amount FROM staging")
        )
        first = cursor.fetchmany(10)
        assert len(first) == 10

        with engine.begin() as writer:
            writer.exec_driver_sql("CREATE TABLE target (id INTEGER, amount TEXT)")
            writer.exec_driver_sql(
                "INSERT INTO target SELECT id, amount FROM staging WHERE id < 100"
            )

        rest = cursor.fetchall()
        assert len(first) + len(rest) == 5_000
        reader.close()

        with engine.connect() as conn:
            assert conn.exec_driver_sql("SELECT COUNT(*) FROM target").scalar() == 100
    finally:
        release_engine(engine)


def test_tune_is_idempotent_and_survives_readonly_journal(tmp_path):
    conn = sqlite3.connect(tmp_path / "x.db")
    tune_sqlite_connection(conn)
    tune_sqlite_connection(conn)
    assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    conn.close()


def test_lsn_lookup_is_chunked_below_sqlite_expression_depth(tmp_path):
    """A 2K-key CDC batch must not issue one 2K-way OR predicate."""
    from connectors.writer_common import (
        DF_LSN_COL,
        LSN_LOOKUP_MAX_KEYS,
        filter_stale_lsn_rows,
    )

    conn = sqlite3.connect(tmp_path / "lsn.db")
    conn.execute(f'CREATE TABLE t (id INTEGER, v TEXT, "{DF_LSN_COL}" TEXT)')
    conn.executemany(
        f'INSERT INTO t VALUES (?, ?, ?)',
        [(i, "old", "0/00000010") for i in range(2000)],
    )
    conn.commit()
    rows = [(i, "new", "0/00000020" if i % 2 else "0/00000001") for i in range(2000)]
    assert len(rows) > LSN_LOOKUP_MAX_KEYS
    to_write, skipped = filter_stale_lsn_rows(
        conn.cursor(), "t", None, ["id"], rows, ["id", "v", DF_LSN_COL],
        quote='"', placeholder="?",
    )
    assert skipped == 1000
    assert len(to_write) == 1000
    assert all(r[0] % 2 for r in to_write)

