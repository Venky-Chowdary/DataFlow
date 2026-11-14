"""Which statements the UPDATE-then-INSERT fallback sends, per dialect (re-review F2).

``INSERT … SELECT :v, … WHERE NOT EXISTS (…)`` is verified on PostgreSQL,
MySQL / MariaDB, SQLite and SQL Server only. DB2, Firebird and Informix reject
untyped parameter markers in a SELECT list, DB2 / HANA / Firebird need a dummy
FROM, and Oracle binds text over 4000 bytes in ``SELECT :v FROM DUAL`` as
VARCHAR2 (ORA-01461 / ORA-01489). Every other dialect runs UPDATE by key, then
``SELECT COUNT(*) … WHERE key = :k`` (the database decides equality), then a
plain ``INSERT … VALUES`` when the count is 0 — in the caller's transaction.
"""

from __future__ import annotations

import sqlite3

import pytest
import sqlalchemy as sa
from sqlalchemy.engine.default import DefaultDialect

import connectors.merge_dialects as md


@pytest.mark.parametrize("name", ["postgresql", "mysql", "mariadb", "sqlite", "mssql"])
def test_insert_select_only_on_verified_dialects(name):
    assert md._absent_insert_by_select(name)


@pytest.mark.parametrize(
    "name", ["oracle", "ibm_db_sa", "firebird", "informix", "hana", "sybase", "duckdb", "vertica"]
)
def test_other_dialects_count_then_insert_values(name):
    assert not md._absent_insert_by_select(name)


def _dialect(name: str):
    if name == "oracle":
        from sqlalchemy.dialects import oracle

        return oracle.dialect()
    d = DefaultDialect()
    d.name = name  # DB2 / Firebird drivers are not installed here
    return d


class _Recorder:
    """A connection that compiles what it is sent and answers COUNT(*) = 0."""

    def __init__(self, dialect):
        self.dialect = dialect
        self.sql: list[str] = []

    def execute(self, stmt, params=None):
        self.sql.append(" ".join(str(stmt.compile(dialect=self.dialect)).split()))

        class _R:
            rowcount = 1

            @staticmethod
            def scalar():
                return 0

        return _R()


@pytest.mark.parametrize("name", ["oracle", "ibm_db_sa", "firebird"])
def test_compiled_fallback_sql_has_no_parameter_in_a_select_list(name):
    t = sa.Table(
        "t", sa.MetaData(), sa.Column("k", sa.String(10)), sa.Column("n", sa.Integer),
        sa.Column("doc", sa.Text),
    )
    conn = _Recorder(_dialect(name))
    n = md.update_insert_upsert(
        conn, t, [{"k": "a", "n": 1, "doc": "x" * 5000}], ["k"], ["k", "n", "doc"]
    )
    assert n == 1
    kinds = [s.split()[0] for s in conn.sql]
    assert kinds == ["UPDATE", "SELECT", "INSERT"], conn.sql
    select, insert = conn.sql[1], conn.sql[2]
    assert select.upper().startswith("SELECT COUNT(*)") and "FROM t WHERE t.k =" in select
    assert " VALUES (" in insert and "SELECT" not in insert.upper()
    assert "DUAL" not in " ".join(conn.sql).upper()


def _seed(db, rows=(("ABC", "old", "VIP"),)):
    c = sqlite3.connect(db)
    c.execute("create table t(k text collate nocase, name text, notes text)")
    c.executemany("insert into t values (?, ?, ?)", rows)
    c.commit()
    c.close()


def _run(db, rows, *, skip=False):
    from services.duplicate_policy import reset_match_action, set_match_action

    eng = sa.create_engine(f"sqlite:///{db}")
    t = sa.Table("t", sa.MetaData(), sa.Column("k", sa.Text), sa.Column("name", sa.Text))
    token = set_match_action("skip" if skip else "update")
    try:
        with eng.connect() as conn:
            n = md.update_insert_upsert(conn, t, rows, ["k"], ["k", "name"])
            conn.commit()
    finally:
        reset_match_action(token)
        eng.dispose()
    c = sqlite3.connect(db)
    try:
        return n, list(c.execute("select k, name, notes from t order by rowid")), md.pop_skip_kept(t)
    finally:
        c.close()


@pytest.fixture
def count_path(monkeypatch):
    """Drive the COUNT-then-INSERT path on SQLite (which decides NOCASE equality)."""
    monkeypatch.setattr(md, "_INSERT_SELECT_DIALECTS", frozenset())


def test_count_path_updates_a_collation_equal_key(tmp_path, count_path):
    db = tmp_path / "ws_cnt.db"
    _seed(db)
    n, rows, _ = _run(db, [{"k": "abc", "name": "new"}, {"k": "Q", "name": "q1"}, {"k": "q", "name": "q2"}])
    assert n == 3
    # 'q' is collation-equal to the 'Q' this batch inserted: inserted once.
    assert rows == [("ABC", "new", "VIP"), ("Q", "q2", None)] or rows == [
        ("ABC", "new", "VIP"), ("Q", "q1", None)
    ]
    assert len(rows) == 2


def test_count_path_under_skip_keeps_the_stored_row(tmp_path, count_path):
    db = tmp_path / "ws_cnt_skip.db"
    _seed(db)
    n, rows, kept = _run(db, [{"k": "abc", "name": "incoming"}, {"k": "xyz", "name": "new"}], skip=True)
    assert n == 1
    assert kept == ["abc"]
    assert rows == [("ABC", "old", "VIP"), ("xyz", "new", None)]
