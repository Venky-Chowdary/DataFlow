"""The UPDATE-then-INSERT upsert fallback lets the database decide key equality.

Review H1: the fallback found stored keys with a SQL ``IN`` / ``=`` (which
honours the column collation) but then decided "already stored" by comparing
Python tuples exactly. On a case- or trailing-space-insensitive collation the
stored ``'ABC'`` matched incoming ``'abc'`` in SQL, missed in Python, and the
row was INSERTed: a duplicate key without a unique index, IntegrityError 1062
with one. The DELETE+INSERT fallback it replaced kept one row there.

Now each row is ``UPDATE … WHERE key = :k`` then ``INSERT … SELECT … WHERE NOT
EXISTS (… key = :k)``; the stored row keeps its own key spelling and every
unmapped column.
"""

from __future__ import annotations

import os
import socket
import sqlite3
import time
import uuid

import pytest
import sqlalchemy as sa

from connectors.generic_sql import _upsert_batch

_MAP = [{"source": "k", "target": "k"}, {"source": "name", "target": "name"}]


def _sqlite_write(db, newk: str, *, rows=None):
    from connectors.sqlite_writer import write_mapped_rows

    return write_mapped_rows(
        host="", port=0, database=str(db), username="", password="", schema="",
        connection_string="", ssl=False, table_name="t", headers=["k", "name"],
        data_rows=rows if rows is not None else [[newk, "new"]], mappings=_MAP,
        column_types={"k": "VARCHAR", "name": "VARCHAR"}, create_table=False,
        write_mode="upsert", conflict_columns=["k"],
    )


def _rows(db):
    c = sqlite3.connect(db)
    try:
        return list(c.execute("select k, name, notes from t order by rowid"))
    finally:
        c.close()


@pytest.mark.parametrize(
    ("collation", "stored", "incoming"),
    [("nocase", "ABC", "abc"), ("rtrim", "ab  ", "ab")],
)
def test_sqlite_writer_collation_equal_key_updates_the_stored_row(
    tmp_path, collation, stored, incoming
):
    db = tmp_path / "ws_coll.db"
    c = sqlite3.connect(db)
    # No PRIMARY KEY / UNIQUE: ON CONFLICT cannot run, the fallback does.
    c.execute(f"create table t(k text collate {collation}, name text, notes text)")
    c.execute("insert into t values (?, 'old', 'VIP')", (stored,))
    c.commit()
    c.close()
    r = _sqlite_write(db, incoming)
    assert r.ok, r.error
    assert _rows(db) == [(stored, "new", "VIP")]


def test_generic_fallback_collation_equal_key_updates_the_stored_row(tmp_path):
    """The MERGE dialects' "native failed" branch, driven over SQLite."""
    db = tmp_path / "ws_gen.db"
    c = sqlite3.connect(db)
    c.execute("create table t(k text collate nocase, name text, notes text)")
    c.execute("insert into t values ('ABC', 'old', 'VIP')")
    c.commit()
    c.close()
    eng = sa.create_engine(f"sqlite:///{db}")
    tbl = sa.Table("t", sa.MetaData(), sa.Column("k", sa.Text), sa.Column("name", sa.Text))
    try:
        with eng.connect() as conn:
            n = _upsert_batch(
                conn, tbl, [{"k": "abc", "name": "new"}, {"k": "xyz", "name": "fresh"}],
                ["k"], ["k", "name"], "mssql",
            )
            conn.commit()
    finally:
        eng.dispose()
    assert n == 2
    assert _rows(db) == [("ABC", "new", "VIP"), ("xyz", "fresh", None)]


def test_generic_fallback_5k_rows_stays_batched(tmp_path):
    """Perf sanity: two executemany statements, not a round trip per row."""
    db = tmp_path / "ws_perf.db"
    c = sqlite3.connect(db)
    c.execute("create table t(k text, name text, notes text)")
    c.executemany("insert into t values (?, 'old', 'VIP')", [(f"k{i}",) for i in range(0, 5000, 2)])
    c.commit()
    c.close()
    eng = sa.create_engine(f"sqlite:///{db}")
    tbl = sa.Table("t", sa.MetaData(), sa.Column("k", sa.Text), sa.Column("name", sa.Text))
    statements: list[str] = []
    sa.event.listen(eng, "before_cursor_execute", lambda *a: statements.append(a[2]))
    try:
        with eng.connect() as conn:
            started = time.perf_counter()
            n = _upsert_batch(
                conn, tbl, [{"k": f"k{i}", "name": "new"} for i in range(5000)],
                ["k"], ["k", "name"], "mssql",
            )
            conn.commit()
            elapsed = time.perf_counter() - started
    finally:
        eng.dispose()
    assert n == 5000
    writes = [s for s in statements if s.lstrip().upper().startswith(("UPDATE", "INSERT"))]
    assert len(writes) <= 4, writes[:6]
    assert elapsed < 30, elapsed
    c = sqlite3.connect(db)
    assert c.execute("select count(*), count(distinct k), sum(notes = 'VIP') from t").fetchone() == (
        5000, 5000, 2500,
    )
    c.close()


# ------------------------------------------------------------- live MySQL CI
def _mysql_url():
    host = os.environ.get("DATAFLOW_TEST_MYSQL_HOST", "127.0.0.1")
    port = int(os.environ.get("DATAFLOW_TEST_MYSQL_PORT", "3306"))
    try:
        socket.create_connection((host, port), timeout=1).close()
    except OSError:
        return None
    if not os.environ.get("DATAFLOW_TEST_MYSQL_PASSWORD"):
        return None
    return sa.engine.URL.create(
        "mysql+pymysql", username=os.environ.get("DATAFLOW_TEST_MYSQL_USER", "dataflow"),
        password=os.environ["DATAFLOW_TEST_MYSQL_PASSWORD"], host=host, port=port,
        database=os.environ.get("DATAFLOW_TEST_MYSQL_DB", "dataflow"),
    )


@pytest.fixture
def mysql_engine():
    url = _mysql_url()
    if url is None:
        pytest.skip("MySQL not configured (DATAFLOW_TEST_MYSQL_*)")
    eng = sa.create_engine(url)
    yield eng
    eng.dispose()


@pytest.mark.parametrize("unique", [False, True], ids=["no_unique_key", "primary_key"])
def test_mysql_ci_collation_key_with_lsn_updates_the_stored_row(mysql_engine, unique):
    t = f"ws_ci_{uuid.uuid4().hex[:8]}"
    pk = "primary key" if unique else ""
    with mysql_engine.begin() as c:
        c.execute(sa.text(
            f"create table {t} (k varchar(20) collate utf8mb4_0900_ai_ci {pk}, "
            "name varchar(20), notes varchar(20), _df_lsn bigint)"
        ))
        c.execute(sa.text(f"insert into {t} values ('ABC', 'old', 'VIP', 1)"))
    tbl = sa.Table(
        t, sa.MetaData(), sa.Column("k", sa.String(20)), sa.Column("name", sa.String(20)),
        sa.Column("_df_lsn", sa.BigInteger),
    )
    cols = ["k", "name", "_df_lsn"]

    def write(rows):
        with mysql_engine.connect() as conn:
            n = _upsert_batch(conn, tbl, rows, ["k"], cols, "mysql")
            conn.commit()
        with mysql_engine.connect() as c:
            return n, c.execute(sa.text(f"select k, name, notes, _df_lsn from {t}")).fetchall()

    try:
        n, rows = write([{"k": "abc", "name": "new", "_df_lsn": 2}])
        assert (n, [tuple(r) for r in rows]) == (1, [("ABC", "new", "VIP", 2)])
        # Stale redelivery under another spelling is still refused.
        n, rows = write([{"k": "Abc", "name": "stale", "_df_lsn": 1}])
        assert [tuple(r) for r in rows] == [("ABC", "new", "VIP", 2)]
        # Identical values: MySQL reports 0 changed rows; still no insert.
        n, rows = write([{"k": "abc", "name": "new", "_df_lsn": 3}])
        n, rows = write([{"k": "aBC", "name": "new", "_df_lsn": 4}])
        assert [tuple(r) for r in rows] == [("ABC", "new", "VIP", 4)]
    finally:
        with mysql_engine.begin() as c:
            c.execute(sa.text(f"drop table if exists {t}"))


def test_mysql_identical_values_without_lsn_do_not_insert(mysql_engine):
    """UPDATE rowcount is "changed", not "matched", rows on MySQL: never trust it."""
    t = f"ws_ci_{uuid.uuid4().hex[:8]}"
    with mysql_engine.begin() as c:
        c.execute(sa.text(f"create table {t} (k varchar(20), name varchar(20), notes varchar(20))"))
        c.execute(sa.text(f"insert into {t} values ('ABC', 'same', 'VIP')"))
    tbl = sa.Table(t, sa.MetaData(), sa.Column("k", sa.String(20)), sa.Column("name", sa.String(20)))
    try:
        from connectors.merge_dialects import update_insert_upsert

        with mysql_engine.connect() as conn:
            n = update_insert_upsert(
                conn, tbl, [{"k": "abc", "name": "same"}, {"k": "new", "name": "x"}],
                ["k"], ["k", "name"],
            )
            conn.commit()
        with mysql_engine.connect() as c:
            rows = sorted(tuple(r) for r in c.execute(sa.text(f"select k, name, notes from {t}")))
        assert n == 2
        assert rows == [("ABC", "same", "VIP"), ("new", "x", None)]
    finally:
        with mysql_engine.begin() as c:
            c.execute(sa.text(f"drop table if exists {t}"))


# ------------------------------------------- stale CDC images (re-review F1)
def _lsn_sqlite(tmp_path, rows):
    from connectors.sqlite_writer import write_mapped_rows

    db = tmp_path / "ws_lsn.db"
    c = sqlite3.connect(db)
    c.execute("create table t(k text collate nocase, name text, _df_lsn integer)")
    c.execute("insert into t values ('ABC', 'newer', 10)")
    c.commit()
    c.close()
    r = write_mapped_rows(
        host="", port=0, database=str(db), username="", password="", schema="",
        connection_string="", ssl=False, table_name="t", headers=["k", "name", "_df_lsn"],
        data_rows=rows, mappings=[{"source": x, "target": x} for x in ("k", "name", "_df_lsn")],
        column_types={"k": "VARCHAR", "name": "VARCHAR", "_df_lsn": "BIGINT"},
        create_table=False, write_mode="upsert", conflict_columns=["k"],
    )
    c = sqlite3.connect(db)
    try:
        return r, list(c.execute("select k, name, _df_lsn from t order by rowid"))
    finally:
        c.close()


def test_sqlite_fallback_refuses_a_stale_image_under_another_spelling(tmp_path):
    r, rows = _lsn_sqlite(tmp_path, [["abc", "STALE", 5]])
    assert r.ok, r.error
    assert rows == [("ABC", "newer", 10)]
    assert (r.rows_written, r.rows_skipped) == (0, 1)


def test_sqlite_fallback_applies_a_newer_image_under_another_spelling(tmp_path):
    r, rows = _lsn_sqlite(tmp_path, [["abc", "NEWEST", 11]])
    assert r.ok, r.error
    assert rows == [("ABC", "NEWEST", 11)]


def test_generic_fallback_refuses_a_stale_image_under_another_spelling(tmp_path):
    db = tmp_path / "ws_gen_lsn.db"
    c = sqlite3.connect(db)
    c.execute("create table t(k text collate nocase, name text, _df_lsn integer)")
    c.execute("insert into t values ('ABC', 'newer', 10)")
    c.commit()
    c.close()
    eng = sa.create_engine(f"sqlite:///{db}")
    tbl = sa.Table(
        "t", sa.MetaData(), sa.Column("k", sa.Text), sa.Column("name", sa.Text),
        sa.Column("_df_lsn", sa.Integer),
    )
    try:
        with eng.connect() as conn:
            n = _upsert_batch(
                conn, tbl, [{"k": "abc", "name": "STALE", "_df_lsn": 5}], ["k"],
                ["k", "name", "_df_lsn"], "mssql",
            )
            conn.commit()
    finally:
        eng.dispose()
    c = sqlite3.connect(db)
    assert (n, list(c.execute("select * from t"))) == (0, [("ABC", "newer", 10)])
    c.close()


def _lsn_batch(tmp_path, stored, batch):
    from connectors.sqlite_writer import write_mapped_rows

    db = tmp_path / "ws_lsn_batch.db"
    c = sqlite3.connect(db)
    c.execute("create table t(k text collate nocase, name text, _df_lsn integer)")
    if stored:
        c.execute("insert into t values (?, ?, ?)", stored)
    c.commit()
    c.close()
    r = write_mapped_rows(
        host="", port=0, database=str(db), username="", password="", schema="",
        connection_string="", ssl=False, table_name="t", headers=["k", "name", "_df_lsn"],
        data_rows=batch, mappings=[{"source": x, "target": x} for x in ("k", "name", "_df_lsn")],
        column_types={"k": "VARCHAR", "name": "VARCHAR", "_df_lsn": "BIGINT"},
        create_table=False, write_mode="upsert", conflict_columns=["k"],
    )
    c = sqlite3.connect(db)
    try:
        return r, list(c.execute("select k, name, _df_lsn from t order by rowid"))
    finally:
        c.close()


@pytest.mark.parametrize(
    ("stored", "batch", "expected"),
    [
        (("ABC", "old", 3), [["abc", "seven", 7], ["Abc", "five", 5]], [("ABC", "seven", 7)]),
        (None, [["abc", "seven", 7], ["Abc", "five", 5]], [("abc", "seven", 7)]),
        (None, [["abc", "five", 5], ["Abc", "seven", 7]], [("abc", "seven", 7)]),
    ],
    ids=["stored_newer_then_older", "new_newer_then_older", "new_older_then_newer"],
)
def test_sqlite_fallback_keeps_the_newest_image_of_spellings_in_one_batch(
    tmp_path, stored, batch, expected
):
    """Re-check Low 1: two spellings equal only under NOCASE, in one batch."""
    r, rows = _lsn_batch(tmp_path, stored, batch)
    assert r.ok, r.error
    assert rows == expected
    assert r.rows_written + r.rows_skipped == 2, r
    assert r.rows_skipped >= 1
