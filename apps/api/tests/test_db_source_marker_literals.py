"""A database source's text that spells a wire marker lands as that text (reviewer probes, PR 427).

The review's probes moved PostgreSQL rows whose text spells a marker
(``__DF_SQL_NULL__``, ``__DF_ESC__x``, a near-spelling, ``__DF_MISSING__``)
through the engine into each SQL writer. They are kept here as suite tests:

* PostgreSQL stores every literal byte for byte (the generic SQL writer is
  proven at the writer in test_generic_sql_wire_literal.py: through the engine
  its run fails Gate-8 on the missing sample reader for ``generic_sql``, a
  pre-existing gap, and the run's undo empties the table);
* MySQL stores every literal except ``__DF_MISSING__``, and SQLite stores the
  rows but fails Gate-8: both are pre-existing and filed apart, pinned here as
  strict xfails so the fix flips them.
"""

from __future__ import annotations

import uuid

import pytest

from tests.c3_live_helpers import mysql_endpoint, mysql_sql, pg_endpoint, pg_sql
from tests.test_cr_d125_file_quoted_empty_fidelity import (  # noqa: F401
    _isolated,
    run_transfer,
    sqlite_dest,
    sqlite_rows,
)

VALUES = [
    (1, "__DF_SQL_NULL__"),
    (2, "__DF_ESC__x"),
    (3, "__df_sql_null__"),
    (4, "__DF_MISSING__"),
    (5, None),
    (6, "__DF_SQL_NULL__ "),
    (7, "plain"),
]
MISSING_ID = 4


@pytest.fixture()
def pg_src():
    from tests.helpers.live_env import pg_up

    if not pg_up():
        pytest.skip("live PostgreSQL not reachable")
    t = f"lit_{uuid.uuid4().hex[:8]}"
    pg_sql(f'CREATE TABLE public."{t}" (id INT PRIMARY KEY, name TEXT)')
    for i, v in VALUES:
        pg_sql(f'INSERT INTO public."{t}" VALUES (%s, %s)', (i, v))
    try:
        yield t
    finally:
        pg_sql(f'DROP TABLE IF EXISTS public."{t}"')


def _pg_rows(table):
    return [tuple(r) for r in pg_sql(f'SELECT id, name FROM public."{table}" ORDER BY id', fetch=True)]


def test_postgresql_destination_stores_every_literal(pg_src):
    t = f"litd_{uuid.uuid4().hex[:8]}"
    try:
        result = run_transfer(pg_endpoint(pg_src), pg_endpoint(t))
        assert result.success is True, result.error
        assert _pg_rows(t) == VALUES
    finally:
        pg_sql(f'DROP TABLE IF EXISTS public."{t}"')


@pytest.fixture()
def mysql_rows(pg_src):
    from tests.helpers.live_env import mysql_up

    if not mysql_up():
        pytest.skip("live MySQL not reachable")
    t = f"litm_{uuid.uuid4().hex[:8]}"
    try:
        result = run_transfer(pg_endpoint(pg_src), mysql_endpoint(t))
        assert result.success is True, result.error
        # MySQL's PAD SPACE text column drops the trailing space on compare and
        # the driver returns it trimmed; the stored value is not under test.
        rows = [tuple(r) for r in mysql_sql(f"SELECT id, name FROM `{t}` ORDER BY id", fetch=True)]
        yield {i: (v.rstrip() if isinstance(v, str) else v) for i, v in rows}
    finally:
        mysql_sql(f"DROP TABLE IF EXISTS `{t}`")


def test_mysql_destination_stores_the_literals(mysql_rows):
    expected = {i: (v.rstrip() if isinstance(v, str) else v) for i, v in VALUES if i != MISSING_ID}
    assert {i: v for i, v in mysql_rows.items() if i != MISSING_ID} == expected


@pytest.mark.xfail(strict=True, reason="pre-existing: '__DF_MISSING__' text lands as '' on MySQL (filed apart)")
def test_mysql_destination_stores_the_missing_marker_text(mysql_rows):
    assert mysql_rows[MISSING_ID] == "__DF_MISSING__"


@pytest.mark.xfail(strict=True, reason="pre-existing: PG -> SQLite marker literals land exact but Gate-8 digests mismatch (filed apart)")
def test_sqlite_destination_passes_gate8_on_the_literals(pg_src, tmp_path):
    result = run_transfer(pg_endpoint(pg_src), sqlite_dest(tmp_path))
    assert result.success is True, result.error
    assert [tuple(r) for r in sqlite_rows(tmp_path)] == VALUES
