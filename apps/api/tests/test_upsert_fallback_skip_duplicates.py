"""skip_duplicates through the generic SQL UPDATE-then-INSERT fallback keeps stored keys.

Re-review 2: ``update_insert_upsert`` recomputed its SET columns from the
target columns, ignoring the empty list ``upsert_set_columns`` gives
``_upsert_batch`` under skip, so the fallback UPDATEd the stored row; and it
returned ``len(rows)``, so a kept key counted as written. generic_sql, SQL
Server and Oracle are on the skip allow-list, so the gate let it through.
"""

from __future__ import annotations

import os
import socket
import sqlite3
import uuid

import pytest
import sqlalchemy as sa

from connectors.generic_sql import _upsert_batch


@pytest.fixture
def skip():
    from services.duplicate_policy import reset_match_action, set_match_action

    token = set_match_action("skip")
    yield
    reset_match_action(token)


def _rows(db):
    c = sqlite3.connect(db)
    try:
        return list(c.execute("select * from t order by rowid"))
    finally:
        c.close()


def test_generic_fallback_under_skip_leaves_the_stored_row(tmp_path, skip):
    from connectors.merge_dialects import pop_skip_kept

    db = tmp_path / "ws_skip.db"
    c = sqlite3.connect(db)
    c.execute("create table t(id integer, name text)")
    c.execute("insert into t values (1, 'stored')")
    c.commit()
    c.close()
    eng = sa.create_engine(f"sqlite:///{db}")
    tbl = sa.Table("t", sa.MetaData(), sa.Column("id", sa.Integer), sa.Column("name", sa.Text))
    try:
        with eng.connect() as conn:
            n = _upsert_batch(
                conn, tbl, [{"id": 1, "name": "incoming"}, {"id": 2, "name": "new"}],
                ["id"], ["id", "name"], "mssql",
            )
            conn.commit()
    finally:
        eng.dispose()
    assert n == 1
    assert pop_skip_kept(tbl) == ["1"]
    assert _rows(db) == [(1, "stored"), (2, "new")]


def test_generic_writer_under_skip_counts_a_collation_equal_kept_key(tmp_path, skip):
    """End to end: the exact-key hold-back misses 'abc' vs stored 'ABC' (NOCASE);
    the fallback keeps it, and the writer reports it skipped with a marker."""
    from connectors.generic_sql import write_mapped_rows

    db = tmp_path / "ws_skip_ci.db"
    c = sqlite3.connect(db)
    c.execute("create table t(k text collate nocase, name text)")
    c.execute("insert into t values ('ABC', 'stored')")
    c.commit()
    c.close()
    result = write_mapped_rows(
        host="", port=0, database=str(db), username="", password="", schema="",
        connection_string=f"sqlite:///{db}", ssl=False, table_name="t",
        headers=["k", "name"], data_rows=[["abc", "incoming"], ["xyz", "new"]],
        mappings=[{"source": "k", "target": "k"}, {"source": "name", "target": "name"}],
        column_types={"k": "VARCHAR", "name": "VARCHAR"}, create_table=False,
        write_mode="upsert", conflict_columns=["k"], type="sqlite",
    )
    assert result.ok, result.error
    assert (result.rows_written, result.rows_skipped) == (1, 1), result
    assert result.meta.get("skipped_identity_keys") == ["abc"], result.meta
    assert _rows(db) == [("ABC", "stored"), ("xyz", "new")]


# ------------------------------------------------------------------ live engines
def _up(host: str, port: int) -> bool:
    try:
        socket.create_connection((host, port), timeout=1).close()
        return True
    except OSError:
        return False


def _mysql_url():
    if not os.environ.get("DATAFLOW_TEST_MYSQL_PASSWORD") or not _up("127.0.0.1", 3306):
        return None
    return sa.engine.URL.create(
        "mysql+pymysql", username=os.environ.get("DATAFLOW_TEST_MYSQL_USER", "dataflow"),
        password=os.environ["DATAFLOW_TEST_MYSQL_PASSWORD"],
        host=os.environ.get("DATAFLOW_TEST_MYSQL_HOST", "127.0.0.1"),
        port=int(os.environ.get("DATAFLOW_TEST_MYSQL_PORT", "3306")),
        database=os.environ.get("DATAFLOW_TEST_MYSQL_DB", "dataflow"),
    )


def _pg_url():
    if not os.environ.get("PGPASSWORD") or not _up(os.environ.get("PGHOST", "127.0.0.1"), 5432):
        return None
    return sa.engine.URL.create(
        "postgresql+psycopg2", username=os.environ.get("PGUSER", "dataflow"),
        password=os.environ["PGPASSWORD"], host=os.environ.get("PGHOST", "127.0.0.1"),
        port=int(os.environ.get("PGPORT", "5432")), database=os.environ.get("PGDATABASE", "dataflow"),
    )


def test_mysql_lsn_fallback_under_skip_keeps_the_stored_row(skip):
    """MySQL always takes the fallback when ``_df_lsn`` is mapped."""
    url = _mysql_url()
    if url is None:
        pytest.skip("MySQL not configured")
    eng = sa.create_engine(url)
    t = f"ws_skip_{uuid.uuid4().hex[:8]}"
    try:
        with eng.begin() as c:
            c.execute(sa.text(f"create table {t} (k varchar(20), name varchar(20), _df_lsn bigint)"))
            c.execute(sa.text(f"insert into {t} values ('ABC', 'stored', 1)"))
        tbl = sa.Table(
            t, sa.MetaData(), sa.Column("k", sa.String(20)), sa.Column("name", sa.String(20)),
            sa.Column("_df_lsn", sa.BigInteger),
        )
        with eng.connect() as conn:
            n = _upsert_batch(
                conn, tbl,
                [{"k": "abc", "name": "incoming", "_df_lsn": 2}, {"k": "n2", "name": "new", "_df_lsn": 2}],
                ["k"], ["k", "name", "_df_lsn"], "mysql",
            )
            conn.commit()
        with eng.connect() as c:
            rows = sorted(tuple(r) for r in c.execute(sa.text(f"select k, name, _df_lsn from {t}")))
        assert n == 1
        assert rows == [("ABC", "stored", 1), ("n2", "new", 2)]
    finally:
        with eng.begin() as c:
            c.execute(sa.text(f"drop table if exists {t}"))
        eng.dispose()


def test_postgresql_fallback_under_skip_keeps_the_stored_row(skip):
    """No unique constraint: native ON CONFLICT fails and the fallback runs."""
    url = _pg_url()
    if url is None:
        pytest.skip("PostgreSQL not configured")
    eng = sa.create_engine(url)
    t = f"ws_skip_{uuid.uuid4().hex[:8]}"
    try:
        with eng.begin() as c:
            c.execute(sa.text(f"create table {t} (k text, name text)"))
            c.execute(sa.text(f"insert into {t} values ('a', 'stored')"))
        tbl = sa.Table(t, sa.MetaData(), sa.Column("k", sa.Text), sa.Column("name", sa.Text))
        with eng.connect() as conn:
            n = _upsert_batch(
                conn, tbl, [{"k": "a", "name": "incoming"}, {"k": "b", "name": "new"}],
                ["k"], ["k", "name"], "postgresql",
            )
            conn.commit()
        with eng.connect() as c:
            rows = sorted(tuple(r) for r in c.execute(sa.text(f"select k, name from {t}")))
        assert n == 1
        assert rows == [("a", "stored"), ("b", "new")]
    finally:
        with eng.begin() as c:
            c.execute(sa.text(f"drop table if exists {t}"))
        eng.dispose()


def test_salvage_path_counts_a_kept_key_once_and_drops_a_rolled_back_chunks_markers(
    tmp_path, skip, monkeypatch
):
    """Re-check Low 2: the chunk's fallback stashes a kept marker, then the chunk
    fails and rolls back; the per-row salvage path must pop its own markers, and
    the rolled-back chunk's marker must not be recorded (nor leak onward)."""
    import connectors.generic_sql as gs

    db = tmp_path / "ws_salvage.db"
    c = sqlite3.connect(db)
    c.execute("create table t(k text collate nocase, name text)")
    c.execute("insert into t values ('ABC', 'stored')")
    c.commit()
    c.close()
    real = gs._upsert_batch

    def chunk_fails_after_fallback(conn, table_obj, batch, *a, **k):
        n = real(conn, table_obj, batch, *a, **k)
        if len(batch) > 1:
            raise sa.exc.IntegrityError("INSERT", {}, Exception("value out of range"))
        return n

    monkeypatch.setattr(gs, "_upsert_batch", chunk_fails_after_fallback)
    result = gs.write_mapped_rows(
        host="", port=0, database=str(db), username="", password="", schema="",
        connection_string=f"sqlite:///{db}", ssl=False, table_name="t",
        headers=["k", "name"], data_rows=[["abc", "incoming"], ["xyz", "new"]],
        mappings=[{"source": "k", "target": "k"}, {"source": "name", "target": "name"}],
        column_types={"k": "VARCHAR", "name": "VARCHAR"}, create_table=False,
        write_mode="upsert", conflict_columns=["k"], type="sqlite", error_policy="quarantine",
    )
    assert result.ok, result.error
    assert (result.rows_written, result.rows_skipped) == (1, 1), result
    assert result.meta.get("skipped_identity_keys") == ["abc"], result.meta
    assert _rows(db) == [("ABC", "stored"), ("xyz", "new")]


def test_sqlite_writer_under_skip_marks_a_collation_equal_kept_key(tmp_path, skip):
    """Re-check Low 3: the kept key got rows_skipped but no Gate-8 marker, so
    Gate-8 graded the source row against the stored one. Both spellings are
    marked: the source key and the stored key SQLite matched."""
    from connectors.sqlite_writer import write_mapped_rows

    db = tmp_path / "ws_sqlite_skip.db"
    c = sqlite3.connect(db)
    c.execute("create table t(k text collate nocase, name text)")
    c.execute("insert into t values ('ABC', 'stored')")
    c.commit()
    c.close()
    r = write_mapped_rows(
        host="", port=0, database=str(db), username="", password="", schema="",
        connection_string="", ssl=False, table_name="t", headers=["k", "name"],
        data_rows=[["abc", "incoming"], ["xyz", "new"]],
        mappings=[{"source": "k", "target": "k"}, {"source": "name", "target": "name"}],
        column_types={"k": "VARCHAR", "name": "VARCHAR"}, create_table=False,
        write_mode="upsert", conflict_columns=["k"],
    )
    assert r.ok, r.error
    assert (r.rows_written, r.rows_skipped) == (1, 1), r
    assert sorted(r.meta.get("skipped_identity_keys") or []) == ["ABC", "abc"], r.meta
    assert _rows(db) == [("ABC", "stored"), ("xyz", "new")]
