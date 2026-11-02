"""D60/D98 — watermark tie group must be re-read (cursor >= watermark).

The run-seed read scope re-reads the whole tie group at the watermark so a
row inserted or restamped inside the tie lands on every later incremental
run; page edges inside the run still seek with `>` (never by key order
inside a tie); keyed appends/upserts dedupe by key, unkeyed appends dedupe
by full-row content.
"""

from __future__ import annotations

import inspect
import sys
import types

import pytest

from services.copy_incremental import (
    mysql_cursor_predicate_sql,
    pg_cursor_predicate_sql,
    sqlite_cursor_predicate_sql,
)


class _FakeCur:
    def __init__(self, capture, rows=None):
        self.capture = capture
        self._rows = rows or []
        self.description = []

    def execute(self, q, params=None):
        self.capture.append((q, tuple(params or ())))

    def fetchall(self):
        return list(self._rows)

    def fetchmany(self, n):
        return list(self._rows)[: max(1, int(n))]

    def fetchone(self):
        return self._rows[0] if self._rows else (0,)

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def mogrify(self, clause, params):
        return (clause % tuple(repr(p) for p in params)).encode()


class _FakeConn:
    def __init__(self, capture, rows=None):
        self._capture = capture
        self._rows = rows

    def cursor(self):
        return _FakeCur(self._capture, self._rows)

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


def _render_psycopg2(q):
    from psycopg2 import sql as psql

    if isinstance(q, str):
        return q
    out = []
    for p in getattr(q, "seq", [q]):
        if isinstance(p, psql.Composed):
            out.append(_render_psycopg2(p))
            continue
        w = p._wrapped
        if isinstance(p, psql.SQL):
            out.append(w)
        elif isinstance(p, psql.Identifier):
            out.append('"' + ".".join(w) + '"')
        elif isinstance(p, psql.Literal):
            out.append(repr(w))
    return "".join(out)


# --- copy-path predicates (always run-seed) ---------------------------------


@pytest.mark.parametrize("pk", ["", "id"])
def test_pg_copy_predicate_rereads_tie_group(pk):
    pred = pg_cursor_predicate_sql(
        _FakeCur([]), cursor_column="ts", watermark="5|42", pk_column=pk
    )
    assert '"ts" >= ' in pred
    assert '"ts" > ' not in pred


@pytest.mark.parametrize("pk", ["", "id"])
def test_mysql_copy_predicate_rereads_tie_group(pk):
    pred = mysql_cursor_predicate_sql(
        _FakeCur([]), cursor_column="ts", watermark="5|42", pk_column=pk
    )
    assert "`ts` >= '5'" in pred
    assert "`ts` > " not in pred


@pytest.mark.parametrize("pk", ["", "id"])
def test_sqlite_copy_predicate_rereads_tie_group(pk):
    pred = sqlite_cursor_predicate_sql(
        cursor_column="ts", watermark="9", pk_column=pk
    )
    assert ">=" in pred
    assert " > " not in pred


def test_pg_copy_predicate_drops_tiebreak_position():
    # Key order inside a tie must never decide the seed: the composite
    # bookmark's pk part is discarded, the whole cursor=w group is re-read.
    pred = pg_cursor_predicate_sql(
        _FakeCur([]), cursor_column="ts", watermark="5|42", pk_column="id"
    )
    assert "42" not in pred


# --- filtered snapshot scans (run-seed reads) -------------------------------


def test_pg_filtered_scan_includes_watermark(monkeypatch):
    import services.source_snapshot as ss
    from connectors import postgresql_reader

    capture = []
    monkeypatch.setattr(
        ss, "get_source_snapshot_conn", lambda *a, **k: _FakeConn(capture)
    )
    try:
        postgresql_reader.read_table_scan_batch(
            host="h",
            port=5432,
            database="d",
            username="u",
            password="p",
            schema="public",
            connection_string="",
            ssl=False,
            table="t",
            filter_column="ts",
            filter_after="7",
            scan_state={},
        )
    except TypeError:
        # psycopg2 Identifier.as_string requires a real cursor late in the
        # call; the WHERE predicate is already captured by then.
        pass
    assert capture
    assert any(
        ">= %s" in _render_psycopg2(q) and "> %s" not in _render_psycopg2(q)
        for q, _p in capture
    ), capture


def test_mysql_filtered_scan_includes_watermark(monkeypatch):
    import services.source_snapshot as ss
    from connectors import mysql_reader

    capture = []
    monkeypatch.setattr(
        ss, "get_source_snapshot_conn", lambda *a, **k: _FakeConn(capture)
    )
    mysql_reader.read_table_scan_batch(
        host="h",
        port=3306,
        database="d",
        username="u",
        password="p",
        schema="d",
        connection_string="",
        ssl=False,
        table="t",
        filter_column="ts",
        filter_after="7",
        scan_state={},
        conn=_FakeConn(capture),
    )
    assert capture
    assert any(
        ">= %s" in str(q) and " > %s" not in str(q) for q, _p in capture
    ), capture


# --- keyset row readers: run-seed inclusive, page-edge exclusive ------------


def _pg_cursor_sql(capture):
    return [
        (_render_psycopg2(q), p) for q, p in capture if "WHERE" in _render_psycopg2(q)
    ]


@pytest.mark.parametrize("inclusive,op", [(True, ">="), (False, ">")])
def test_pg_keyset_seed_inclusive_vs_edge(inclusive, op):
    from connectors import postgresql_reader

    capture = []
    postgresql_reader.read_table_cursor_batch(
        host="h",
        port=5432,
        database="d",
        username="u",
        password="p",
        schema="public",
        connection_string="",
        ssl=False,
        table="t",
        cursor_column="ts",
        cursor_after="5|42",
        limit=10,
        cursor_primary_key="id",
        conn=_FakeConn(capture),
        cursor_inclusive=inclusive,
    )
    # only the data SELECT (one that binds the cursor column) matters
    sqls = [(t, p) for t, p in _pg_cursor_sql(capture) if '"ts"' in t]
    assert sqls, capture
    for text, _p in sqls:
        if inclusive:
            assert 'WHERE "ts" >= %s' in text, text
            assert "42" not in str(_p)
        else:
            # composite page-edge seek: (cursor, pk) > (v, pk)
            assert '("ts", "id") > (%s, %s)' in text, text


@pytest.mark.parametrize("inclusive,op", [(True, ">="), (False, ">")])
def test_mysql_keyset_seed_inclusive_vs_edge(inclusive, op):
    from connectors import mysql_reader

    capture = []
    mysql_reader.read_table_cursor_batch(
        host="h",
        port=3306,
        database="d",
        username="u",
        password="p",
        schema="d",
        connection_string="",
        ssl=False,
        table="t",
        cursor_column="ts",
        cursor_after="5|42",
        limit=10,
        cursor_primary_key="id",
        conn=_FakeConn(capture),
        cursor_inclusive=inclusive,
    )
    sqls = [str(q) for q, _p in capture if "WHERE" in str(q) and "`ts`" in str(q)]
    assert sqls, capture
    for text in sqls:
        if inclusive:
            assert "`ts` >= %s" in text, text
        else:
            assert "(`ts`, `id`) > (%s, %s)" in text, text


@pytest.mark.parametrize("inclusive,op", [(True, ">="), (False, ">")])
def test_snowflake_keyset_seed_inclusive_vs_edge(monkeypatch, inclusive, op):
    import connectors.snowflake_reader as sr

    capture = []
    monkeypatch.setattr(sr, "get_connection", lambda **kw: _FakeConn(capture))
    monkeypatch.setattr(sr, "_use_warehouse", lambda *a, **k: None)
    sr.read_table_cursor_batch(
        host="acct",
        port=443,
        database="d",
        username="u",
        password="p",
        schema="s",
        connection_string="",
        warehouse="w",
        table="t",
        cursor_column="ts",
        cursor_after="5|42",
        limit=10,
        cursor_primary_key="id",
        cursor_inclusive=inclusive,
    )
    sqls = [
        str(q)
        for q, _p in capture
        if "WHERE" in str(q) and '"TS"' in str(q).upper() and 'FROM "S"."T"' in str(q).upper()
    ]
    assert sqls, capture
    if inclusive:
        assert any(">= %s" in s for s in sqls), sqls
        assert not any("(%s, %s)" in s for s in sqls)
    else:
        # Exclusive (cursor, pk) edge seek, written out (QA r12 Snowflake D4:
        # a text-bound row value does not cast onto (TIMESTAMP, NUMBER)).
        assert any(
            '"ts" > %s OR ("ts" = %s AND "id" > %s)' in s and ">=" not in s for s in sqls
        ), sqls


# --- append boundary dedupe -------------------------------------------------


def _fps_of(rows, cols):
    from connectors.writer_common import row_fingerprints

    return {fp for _k, fp in row_fingerprints(rows, cols)}


def _dedupe(headers, rows, dest_fps, match_cols=None):
    """Indices of ``rows`` (the page's boundary rows) already at the destination.

    Page-edge preservation is ``_filter_batch``'s job: it marks the page as the
    source handed it over before any rewrite, so dropped tie rows still advance
    the seek (see test_d124_watermark_tie_group_e2e).
    """
    from src.transfer.stream import _boundary_append_dedupe

    return _boundary_append_dedupe(
        list(headers),
        [list(r) for r in rows],
        dest_fps,
        match_cols=list(match_cols or headers),
        mappings=[{"source": c, "target": c} for c in headers],
        target_cols=list(headers),
        column_types={},
        stream_error_policy=None,
        digest_dest_types={},
        dest_bind_dialect="",
    )


def test_append_boundary_no_dupes():
    headers = ["id", "ts", "name"]
    # dest already holds (id=1, ts=5, name='a') from run 1.
    dest_fps = _fps_of([("1", "5", "a")], headers)
    rows = [
        ["1", "5", "a"],  # identical boundary row -> dropped
        ["2", "5", "CHANGED"],  # same cursor, new content -> kept
    ]
    assert _dedupe(headers, rows, dest_fps) == {0}


def test_append_boundary_keyed_matches_cursor_and_key():
    headers = ["id", "ts", "name"]
    # Keyed route: the destination holds key 1 at the watermark cursor.
    dest_fps = _fps_of([("5", "1")], ["ts", "id"])
    rows = [
        ["1", "5", "a"],  # delivered peer -> dropped
        ["2", "5", "new"],  # new key inside the tie -> kept
    ]
    assert _dedupe(headers, rows, dest_fps, match_cols=["ts", "id"]) == {0}


def test_append_boundary_full_tie_group_dropped():
    headers = ["id", "ts"]
    dest_fps = _fps_of([("1", "5"), ("2", "5")], headers)
    assert _dedupe(headers, [["1", "5"], ["2", "5"]], dest_fps) == {0, 1}


def test_append_boundary_missing_fp_match_keeps_row():
    headers = ["id", "ts"]
    dest_fps = _fps_of([("9", "5")], headers)
    assert _dedupe(headers, [["1", "5"]], dest_fps) == set()


def test_append_boundary_empty_dest_set_noop():
    headers = ["id", "ts"]
    assert _dedupe(headers, [["1", "5"]], set()) == set()


# --- plumbing ----------------------------------------------------------------


def test_readers_accept_cursor_inclusive():
    import connectors.bigquery_reader as bqr
    import connectors.generic_sql as gs
    import connectors.mysql_reader as mr
    import connectors.postgresql_reader as pgr
    import connectors.snowflake_reader as sfr
    from src.transfer import batch_readers

    for fn in (
        pgr.read_table_cursor_batch,
        mr.read_table_cursor_batch,
        gs.read_table_cursor_batch,
        sfr.read_table_cursor_batch,
        bqr.read_table_cursor_batch,
        # _read_batch_impl is a thin egress-scope wrapper (*args/**kwargs);
        # the real signature lives on the unscoped reader it forwards to.
        batch_readers._read_batch_unscoped,
    ):
        assert "cursor_inclusive" in inspect.signature(fn).parameters, fn
