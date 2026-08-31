"""A destination key census is judged in the destination column's own domain.

The census bound the *source* spelling of a key straight into
``WHERE key IN (…)``. A strict engine refuses to compare across domains, so a
Mongo/JSONL integer key against a ``text`` destination column aborted the whole
probe with ``operator does not exist: text = integer`` (Track C open defect),
and a junk key against an ``integer`` column aborted it with ``invalid input
syntax for type integer``. An aborted probe returns ``None``, which leaves
keyed conservation unproven for a run that was otherwise correct — an upsert
then has no independent update/insert split to reconcile against.

Two rules are asserted here, on the pure coercion owner and on live engines:

* the key is coerced into the column's domain, so ``1`` finds the row spelled
  ``'1'`` in a ``text`` column and ``'2'`` finds the row stored as ``2`` in an
  ``integer`` column;
* a value the column cannot represent (``'abc'`` into ``integer``) is a proven
  **miss** — the remaining keys are still censused instead of the probe failing.
"""

from __future__ import annotations

import sys
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.dest_key_typing import (  # noqa: E402
    BOOL_DOMAIN,
    INT_DOMAIN,
    NUMERIC_DOMAIN,
    OPAQUE_DOMAIN,
    TEXT_DOMAIN,
    coerce_key_tuples,
    coerce_key_value,
    key_domain,
)
from services.dest_precount import destination_key_hits  # noqa: E402
from tests.helpers.live_env import (  # noqa: E402
    mysql_creds,
    mysql_up,
    pg_creds,
    pg_up,
)


@pytest.mark.parametrize(
    ("declared", "expected"),
    [
        ("integer", INT_DOMAIN),
        ("BIGINT", INT_DOMAIN),
        ("smallint", INT_DOMAIN),
        ("int unsigned", INT_DOMAIN),
        ("serial", INT_DOMAIN),
        ("numeric(12,2)", NUMERIC_DOMAIN),
        ("DECIMAL(9, 4)", NUMERIC_DOMAIN),
        ("double precision", NUMERIC_DOMAIN),
        ("text", TEXT_DOMAIN),
        ("character varying(64)", TEXT_DOMAIN),
        ("NVARCHAR(36)", TEXT_DOMAIN),
        ("uuid", TEXT_DOMAIN),
        ("boolean", BOOL_DOMAIN),
        # Left alone on purpose: the engine already compares these correctly
        # and inventing a coercion for them would be a guess.
        ("timestamp with time zone", OPAQUE_DOMAIN),
        ("interval", OPAQUE_DOMAIN),
        ("bytea", OPAQUE_DOMAIN),
        ("jsonb", OPAQUE_DOMAIN),
        ("", OPAQUE_DOMAIN),
        ("some_exotic_udt", OPAQUE_DOMAIN),
    ],
)
def test_declared_type_decides_the_comparison_domain(declared: str, expected: str) -> None:
    assert key_domain(declared) == expected


def test_a_key_is_coerced_into_the_columns_domain() -> None:
    assert coerce_key_value(7, TEXT_DOMAIN) == ("7", True)
    assert coerce_key_value("7", INT_DOMAIN) == (7, True)
    assert coerce_key_value(Decimal("7"), INT_DOMAIN) == (7, True)
    assert coerce_key_value(" 42 ", INT_DOMAIN) == (42, True)
    assert coerce_key_value("t", BOOL_DOMAIN) == (True, True)
    assert coerce_key_value(0, BOOL_DOMAIN) == (False, True)


def test_a_value_the_column_cannot_hold_is_a_miss_not_an_error() -> None:
    """No integer-keyed row can be keyed 'abc' or 22.4, so both are misses."""
    assert coerce_key_value("abc", INT_DOMAIN) == (None, False)
    assert coerce_key_value("22.4", INT_DOMAIN) == (None, False)
    assert coerce_key_value("maybe", BOOL_DOMAIN) == (None, False)
    assert coerce_key_value("not-a-number", NUMERIC_DOMAIN) == (None, False)


def test_unrepresentable_keys_are_dropped_and_the_rest_still_censused() -> None:
    keys, dropped = coerce_key_tuples(
        [(1,), ("abc",), ("3",)], ["id"], {"id": "integer"}
    )
    assert keys == [(1,), (3,)]
    assert dropped == 1


def test_unknown_column_types_leave_the_source_spelling_untouched() -> None:
    raw = [(1,), ("abc",)]
    keys, dropped = coerce_key_tuples(raw, ["id"], {})
    assert keys == [(1,), ("abc",)]
    assert dropped == 0


def test_composite_keys_coerce_per_column() -> None:
    keys, dropped = coerce_key_tuples(
        [(1, "5"), ("2", 6), ("x", 7)],
        ["code", "seq"],
        {"code": "varchar(8)", "seq": "bigint"},
    )
    assert keys == [("1", 5), ("2", 6), ("x", 7)]
    assert dropped == 0


# --------------------------------------------------------------------------
# Live engines: the failure was only visible against a real server.
# --------------------------------------------------------------------------

# Credentials resolve through the one shared live resolver (libpq / MYSQL_* env,
# then the local compose default), so this suite targets the same servers as
# every other live matrix instead of carrying a third spelling of them.
_PG = pg_creds("KEYCENSUS")
_MYSQL = mysql_creds("KEYCENSUS")


def _pg_conn() -> Any:
    psycopg2 = pytest.importorskip("psycopg2")
    if not pg_up("KEYCENSUS"):
        pytest.skip("PostgreSQL not reachable/authenticating for the live key census")
    return psycopg2.connect(
        host=_PG["host"],
        port=_PG["port"],
        dbname=_PG["database"],
        user=_PG["username"],
        password=_PG["password"],
        connect_timeout=4,
    )


def _mysql_conn() -> Any:
    pymysql = pytest.importorskip("pymysql")
    if not mysql_up("KEYCENSUS"):
        pytest.skip("MySQL not reachable/authenticating for the live key census")
    return pymysql.connect(
        host=_MYSQL["host"],
        port=int(_MYSQL["port"]),
        database=_MYSQL["database"],
        user=_MYSQL["username"],
        password=_MYSQL["password"],
        connect_timeout=4,
    )


def _seed(conn: Any, table: str, coltype: str, values: list[Any], quote: str) -> None:
    cur = conn.cursor()
    q = f"{quote}{table}{quote}"
    cur.execute(f"DROP TABLE IF EXISTS {q}")
    cur.execute(f"CREATE TABLE {q} (id {coltype} PRIMARY KEY, payload varchar(8))")
    cur.executemany(f"INSERT INTO {q} (id, payload) VALUES (%s, 'x')", [(v,) for v in values])
    conn.commit()
    cur.close()


def _drop(conn: Any, table: str, quote: str) -> None:
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {quote}{table}{quote}")
    conn.commit()
    cur.close()


def test_live_postgres_text_key_column_censuses_integer_source_keys() -> None:
    """The reported defect: ``operator does not exist: text = integer``."""
    conn = _pg_conn()
    table = f"key_census_txt_{uuid.uuid4().hex[:8]}"
    try:
        _seed(conn, table, "text", ["1", "2", "3"], '"')
        hits = destination_key_hits(
            "postgresql",
            dict(_PG),
            schema="public",
            table_name=table,
            key_columns=["id"],
            keys=[(1,), (2,), (9,)],
        )
        assert hits == 2, "two of the three integer-spelled keys exist as text rows"
    finally:
        _drop(conn, table, '"')
        conn.close()


def test_live_postgres_integer_key_column_treats_junk_key_as_a_miss() -> None:
    conn = _pg_conn()
    table = f"key_census_int_{uuid.uuid4().hex[:8]}"
    try:
        _seed(conn, table, "integer", [1, 2, 3], '"')
        hits = destination_key_hits(
            "postgresql",
            dict(_PG),
            schema="public",
            table_name=table,
            key_columns=["id"],
            keys=[("abc",), (1,), ("22.4",)],
        )
        assert hits == 1, "junk and fractional keys are misses, not a failed probe"
    finally:
        _drop(conn, table, '"')
        conn.close()


def test_live_mysql_text_key_column_censuses_integer_source_keys() -> None:
    conn = _mysql_conn()
    table = f"key_census_txt_{uuid.uuid4().hex[:8]}"
    try:
        _seed(conn, table, "varchar(16)", ["1", "2", "3"], "`")
        hits = destination_key_hits(
            "mysql",
            dict(_MYSQL),
            schema=str(_MYSQL["database"]),
            table_name=table,
            key_columns=["id"],
            keys=[(1,), (2,), (9,)],
        )
        assert hits == 2
    finally:
        _drop(conn, table, "`")
        conn.close()


def test_live_mysql_integer_key_column_treats_junk_key_as_a_miss() -> None:
    conn = _mysql_conn()
    table = f"key_census_int_{uuid.uuid4().hex[:8]}"
    try:
        _seed(conn, table, "int", [1, 2, 3], "`")
        hits = destination_key_hits(
            "mysql",
            dict(_MYSQL),
            schema=str(_MYSQL["database"]),
            table_name=table,
            key_columns=["id"],
            keys=[("abc",), (2,)],
        )
        assert hits == 1
    finally:
        _drop(conn, table, "`")
        conn.close()
