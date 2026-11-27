"""PostgreSQL → PostgreSQL COPY route: the in-transaction destination checks.

* The merge (incremental / upsert) is an ``INSERT ... SELECT`` from a staging
  table of the source's types, so a destination column widened in a way the
  assignment cast cannot change a value (int2→int4→int8, varchar(n)→varchar(m≥n)
  / text, numeric(p,s)→numeric(p'≥p,s), timestamp(p)→timestamp(p'≥p)) keeps the
  COPY route. Cast-sensitive pairs still need the exact type.
* A destination that is not an ordinary table (a foreign table: ``LOCK TABLE``
  errors on it) declines to the row path instead of failing the job.
* Append writes binary COPY straight into the destination, so its drift check
  keeps exact types and exact column names (``"Doc"`` and ``"doc"`` are two).
"""

from __future__ import annotations

import os
import uuid

import pytest

from services.copy_fast_path import FastPathUnavailable, pg_assignment_safe

_WIDENING = [
    # (source format_type, live destination format_type, assignment-safe?)
    ("integer", "integer", True),
    ("smallint", "integer", True),
    ("smallint", "bigint", True),
    ("integer", "bigint", True),
    ("bigint", "integer", False),
    ("integer", "smallint", False),
    ("character varying(40)", "character varying(80)", True),
    ("character varying(40)", "character varying(40)", True),
    ("character varying(40)", "character varying", True),
    ("character varying(40)", "text", True),
    ("character varying", "text", True),
    ("character varying(80)", "character varying(40)", False),
    ("character varying", "character varying(40)", False),
    ("text", "character varying", False),
    ("text", "character varying(80)", False),
    ("character(4)", "character(8)", False),
    ("numeric(10,2)", "numeric(12,2)", True),
    ("numeric(10,2)", "numeric", True),
    ("numeric(10,2)", "numeric(12,3)", False),
    ("numeric(10,2)", "numeric(9,2)", False),
    ("numeric", "numeric(38,10)", False),
    ("integer", "numeric(12,2)", False),
    ("timestamp(3) without time zone", "timestamp without time zone", True),
    ("timestamp(3) without time zone", "timestamp(6) without time zone", True),
    ("timestamp without time zone", "timestamp(3) without time zone", False),
    ("timestamp without time zone", "timestamp with time zone", False),
    ("timestamp with time zone", "timestamp without time zone", False),
    ("time(2) without time zone", "time without time zone", True),
    ("date", "timestamp without time zone", False),
    ("json", "jsonb", False),
    ("jsonb", "json", False),
    ("jsonb", "text", False),
    ("integer[]", "bigint[]", False),
    ("text[]", "text", False),
    ("mood", "text", False),
    ("text", "citext", False),
    ("posint", "integer", False),
]


@pytest.mark.parametrize("source,live,safe", _WIDENING)
def test_assignment_safe_widenings_only(source, live, safe):
    assert pg_assignment_safe(source, live) is safe


class _Cur:
    """Answers the relkind probe, LOCK TABLE and the pg_attribute read."""

    def __init__(self, relkind, live):
        self.relkind, self.live, self.sql, self._rows = relkind, live, [], []

    def execute(self, query, params=None):
        text = query if isinstance(query, str) else repr(query)
        self.sql.append(text)
        if "relkind" in text:
            self._rows = [(self.relkind,)] if self.relkind else []
        elif "pg_attribute" in text:
            self._rows = list(self.live.items())
        elif "LOCK TABLE" in text and self.relkind == "f":
            raise RuntimeError('"ft" is not a table')  # what PG 16 says
        else:
            self._rows = [(None,)]

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


def test_merge_check_accepts_widened_columns_and_locks_first():
    from services.copy_fast_path import pg_lock_and_check_live_types

    cur = _Cur("r", {"id": "bigint", "name": "character varying(80)",
                     "amt": "numeric(12,2)", "ts": "timestamp(6) without time zone"})
    pg_lock_and_check_live_types(cur, "public", "t", {
        "id": "integer", "name": "character varying(40)",
        "amt": "numeric(10,2)", "ts": "timestamp(3) without time zone",
    })
    assert "LOCK TABLE" in cur.sql[1] and "pg_attribute" in cur.sql[2]


def test_merge_check_still_declines_cast_sensitive_and_narrowed_columns():
    from services.copy_fast_path import pg_lock_and_check_live_types

    for live in ({"id": "integer", "doc": "text"}, {"id": "smallint", "doc": "jsonb"}):
        with pytest.raises(FastPathUnavailable):
            pg_lock_and_check_live_types(_Cur("r", live), "public", "t",
                                         {"id": "integer", "doc": "jsonb"})


@pytest.mark.parametrize("relkind", ["f", "v", "m", None])
def test_non_ordinary_destination_declines_before_lock(relkind):
    from services.copy_fast_path import pg_lock_and_check_live_types

    cur = _Cur(relkind, {"id": "integer"})
    with pytest.raises(FastPathUnavailable):
        pg_lock_and_check_live_types(cur, "public", "ft", {"id": "integer"})
    assert not any("LOCK TABLE" in q for q in cur.sql)


def test_append_drift_check_matches_exact_column_names():
    """Live "doc" text + "Doc" jsonb; the append writes target "Doc" (jsonb).
    A case-folded lookup could read "doc" (text) and decline, or — the other way
    round — accept a jsonb COPY into the text column."""
    from services.dest_overwrite_swap import pg_decline_live_type_drift

    class _Rows:
        description = [("attname",), ("format_type",)]

        def __init__(self):
            self.rows = []

        def execute(self, query, params=None):
            self.rows = [("id", "integer"), ("doc", "text"), ("Doc", "jsonb")]

        def fetchall(self):
            return list(self.rows)

        def fetchone(self):
            return self.rows[0] if self.rows else None

    pg_decline_live_type_drift(_Rows(), '"public"."t"', {"id": "integer", "Doc": "jsonb"},
                               action="append")
    with pytest.raises(FastPathUnavailable):
        pg_decline_live_type_drift(_Rows(), '"public"."t"', {"id": "integer", "doc": "jsonb"},
                                   action="append")


def test_jsonb_carry_reads_the_exact_target_column():
    """Destination "Doc" jsonb + "doc" text: only the mapping into "Doc" may drop the
    json cast; the mapping into "doc" keeps it (the text column stores the cast)."""
    from src.transfer.copy_route import _jsonb_carry_mappings

    declared = {"id": "integer", "doc": "jsonb"}
    dest_types = {"id": "integer", "Doc": "jsonb", "doc": "text"}
    into_lower = [{"source": "id", "target": "id", "transform": "integer"},
                  {"source": "doc", "target": "doc", "transform": "json"}]
    into_upper = [{"source": "id", "target": "id", "transform": "integer"},
                  {"source": "doc", "target": "Doc", "transform": "json"}]
    assert _jsonb_carry_mappings(into_lower, declared, dest_types)[1]["transform"] == "json"
    assert _jsonb_carry_mappings(into_upper, declared, dest_types)[1]["transform"] == ""


# ---------------------------------------------------------------- live checks


def _live():
    from tests.helpers.live_env import pg_up

    if not pg_up():
        pytest.skip("PostgreSQL not reachable")
    os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
    os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    from tests.test_pg_jsonb_identity_copy import _pg_conn

    return _pg_conn()


def _summary_method(c, src, dst, mode):
    from tests.test_pg_jsonb_identity_copy import _run

    return _run(c, src, dst, mode)


@pytest.mark.parametrize("mode", ["incremental_deduped", "upsert"])
def test_widened_destination_keeps_binary_copy_and_stores_the_same_rows(mode, monkeypatch):
    conn, c = _live()
    sfx = uuid.uuid4().hex[:8]
    src, dst, dst_row = f"wid_s_{sfx}", f"wid_d_{sfx}", f"wid_r_{sfx}"
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE TABLE "{src}" (id integer PRIMARY KEY, n smallint, '
                        "name varchar(40), amt numeric(10,2), ts timestamp(3))")
            cur.execute(f'INSERT INTO "{src}" SELECT g, (g % 300)::smallint, '
                        "'name ' || g, g * 1.25, timestamp '2026-01-01 00:00:00.123' "
                        "+ g * interval '1 minute' FROM generate_series(1, 60) g")
            for t in (dst, dst_row):
                cur.execute(f'CREATE TABLE "{t}" (id bigint PRIMARY KEY, n integer, '
                            "name varchar(80), amt numeric(12,2), ts timestamp(6))")
        method = _summary_method(c, src, dst, mode)
        with monkeypatch.context() as m:
            m.setenv("DATAWRAP_PG_PG_COPY", "0")
            row_method = _summary_method(c, src, dst_row, mode)
        assert "copy_binary" in method, method
        assert "copy" not in row_method, row_method
        with conn.cursor() as cur:
            cur.execute(f'SELECT * FROM "{dst}" ORDER BY id')
            got = cur.fetchall()
            cur.execute(f'SELECT * FROM "{dst_row}" ORDER BY id')
            want = cur.fetchall()
        assert len(got) == 60 and got == want, (got[:2], want[:2])
    finally:
        with conn.cursor() as cur:
            for t in (src, dst, dst_row):
                cur.execute(f'DROP TABLE IF EXISTS "{t}"')
        conn.close()


@pytest.mark.parametrize("mode", ["incremental_append", "full_refresh_append"])
def test_foreign_table_destination_takes_the_row_path(mode):
    """postgres_fdw loopback foreign table as the destination: ``LOCK TABLE`` on it
    errors (PG 16), so the COPY route must decline before locking, not fail the job.
    (Keyed merges — incremental_deduped, upsert — need a unique index, which a
    foreign table cannot carry, so they stop at the index check on every path.)"""
    conn, c = _live()
    sfx = uuid.uuid4().hex[:8]
    src, base, ft, srv = f"fdw_s_{sfx}", f"fdw_b_{sfx}", f"fdw_f_{sfx}", f"fdw_srv_{sfx}"
    try:
        with conn.cursor() as cur:
            try:
                cur.execute("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
            except Exception as exc:  # noqa: BLE001 - not installable here
                pytest.skip(f"postgres_fdw unavailable: {exc}")
            cur.execute(
                f'CREATE SERVER "{srv}" FOREIGN DATA WRAPPER postgres_fdw '
                "OPTIONS (host %s, port %s, dbname %s)",
                (str(c["host"]), str(c["port"]), str(c["database"])),
            )
            cur.execute(f'CREATE USER MAPPING FOR CURRENT_USER SERVER "{srv}" '
                        "OPTIONS (user %s, password %s)",
                        (str(c["username"]), str(c["password"])))
            cur.execute(f'CREATE TABLE "{src}" (id bigint PRIMARY KEY, name text)')
            cur.execute(f'INSERT INTO "{src}" SELECT g, \'n\' || g FROM generate_series(1, 25) g')
            cur.execute(f'CREATE TABLE "{base}" (id bigint PRIMARY KEY, name text)')
            cur.execute(f'CREATE FOREIGN TABLE "{ft}" (id bigint, name text) SERVER "{srv}" '
                        "OPTIONS (schema_name 'public', table_name %s)", (base,))
        method = _summary_method(c, src, ft, mode)
        assert "copy" not in method, method
        with conn.cursor() as cur:
            cur.execute(f'SELECT id, name FROM "{base}" ORDER BY id')
            rows = cur.fetchall()
        assert rows == [(i, f"n{i}") for i in range(1, 26)]
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP SERVER IF EXISTS "{srv}" CASCADE')
            for t in (src, base):
                cur.execute(f'DROP TABLE IF EXISTS "{t}"')
        conn.close()


def test_overwrite_holds_the_lock_from_type_check_to_copy(monkeypatch):
    """An ALTER tried right after the overwrite's type check (a hook on the check)
    must wait for the job's lock — it cannot change the column the binary COPY
    is about to load."""
    import psycopg2

    import services.dest_overwrite_swap as dos

    conn, c = _live()
    sfx = uuid.uuid4().hex[:8]
    src, dst = f"ovl_s_{sfx}", f"ovl_d_{sfx}"
    docs = ['{"a": 1}', '{"b": [1, 2]}', "[]"]
    real_check = dos.pg_decline_live_type_drift
    tried = []

    def check_then_alter(cur, live_ref, column_types, *, action):
        real_check(cur, live_ref, column_types, action=action)
        if action == "overwrite" and dst in live_ref and not tried:
            other = psycopg2.connect(host=c["host"], port=c["port"], user=c["username"],
                                     password=c["password"], dbname=c["database"])
            other.autocommit = True
            try:
                with other.cursor() as oc:
                    oc.execute("SET lock_timeout = '500ms'")
                    oc.execute(f'ALTER TABLE "{dst}" ALTER COLUMN doc TYPE text USING doc::text')
                tried.append("altered")
            except psycopg2.errors.LockNotAvailable:
                tried.append("blocked")
            finally:
                other.close()

    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE TABLE "{src}" (id bigint PRIMARY KEY, doc jsonb)')
            for i in range(1, 31):
                cur.execute(f'INSERT INTO "{src}" VALUES (%s, %s::jsonb)', (i, docs[i % 3]))
            cur.execute(f'CREATE TABLE "{dst}" (id bigint PRIMARY KEY, doc jsonb)')
            cur.execute(f'INSERT INTO "{dst}" VALUES (999, \'{{"old": 1}}\')')
        monkeypatch.setattr(dos, "pg_decline_live_type_drift", check_then_alter)
        method = _summary_method(c, src, dst, "full_refresh_overwrite")
        assert tried == ["blocked"], tried
        assert "copy_binary" in method, method
        with conn.cursor() as cur:
            cur.execute(f'SELECT id, doc::text FROM "{dst}" ORDER BY id')
            got = cur.fetchall()
            cur.execute(f'SELECT id, doc::text FROM "{src}" ORDER BY id')
            want = cur.fetchall()
        assert got == want
    finally:
        with conn.cursor() as cur:
            for t in (src, dst):
                cur.execute(f'DROP TABLE IF EXISTS "{t}"')
        conn.close()


@pytest.mark.parametrize("referenced", [False, True])
def test_overwrite_type_mismatch_releases_the_lock_and_takes_the_row_path(referenced):
    """The lock is taken before the type check; a mismatch must roll it back so
    the row path (a new connection) can write the table."""
    conn, c = _live()
    sfx = uuid.uuid4().hex[:8]
    src, dst, child = f"ovm_s_{sfx}", f"ovm_d_{sfx}", f"ovm_c_{sfx}"
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE TABLE "{src}" (id bigint PRIMARY KEY, name text)')
            cur.execute(f'INSERT INTO "{src}" SELECT g, \'n\' || g FROM generate_series(1, 20) g')
            cur.execute(f'CREATE TABLE "{dst}" (id bigint PRIMARY KEY, name varchar(10))')
            if referenced:
                cur.execute(f'CREATE TABLE "{child}" (pid bigint REFERENCES "{dst}"(id))')
        method = _summary_method(c, src, dst, "full_refresh_overwrite")
        assert "copy" not in method, method
        with conn.cursor() as cur:
            cur.execute(f'SELECT id, name FROM "{dst}" ORDER BY id')
            assert cur.fetchall() == [(i, f"n{i}") for i in range(1, 21)]
    finally:
        with conn.cursor() as cur:
            for t in (child, src, dst):
                cur.execute(f'DROP TABLE IF EXISTS "{t}"')
        conn.close()
