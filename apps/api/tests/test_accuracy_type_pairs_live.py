"""Accuracy Track A item 6, live writer proofs (PostgreSQL 16 <-> MySQL/MariaDB).

Where a create-new carrier could lose values on write, the real engine path
(Validate on) either lands every value exactly or refuses before writing:

* MySQL ``TINYINT(1)`` holding 2 and -1 is not squeezed into a boolean;
* PostgreSQL dates and timestamps outside MySQL's 1000-9999 range (year 500,
  year 12000, 44 BC) are refused, nothing is written;
* MySQL zero dates (``0000-00-00``, month 0, day 0) are refused, never written
  as NULL or as a different date;
* PostgreSQL unbounded ``numeric`` lands exactly in ``DECIMAL(38,15)`` when the
  values fit and is refused when a value has more fraction or integer digits;
* PostgreSQL ``timestamptz`` with non-UTC offsets lands in MariaDB
  ``DATETIME(6)`` / ``TIMESTAMP(6)`` and a created ``timestamptz`` as the same
  instant, and an existing wall-clock ``TIMESTAMP(6)`` refuses it (finding 8).

SQL Server and Oracle pairs: tests/test_c3_lossy_pairs_sqlserver_live.py and
tests/test_c3_lossy_pairs_oracle_live.py (CI live jobs); grading in
tests/test_accuracy_type_pairs.py.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from tests.c3_live_helpers import (
    mysql_endpoint, mysql_sql, pg_endpoint, pg_sql, refused_at_validate, run_transfer, token,
)
from tests.test_commit_safe_review_round2 import isolated  # noqa: F401 — fixture


@pytest.fixture(autouse=True)
def _engines():
    from tests.helpers.live_env import mysql_up, pg_up

    if not (mysql_up() and pg_up()):
        pytest.skip("live MySQL and PostgreSQL not reachable")


def _drop(table: str) -> None:
    mysql_sql(f"DROP TABLE IF EXISTS {table}")
    mysql_sql(f"DROP TABLE IF EXISTS {table}_df_quarantine")
    pg_sql(f'DROP TABLE IF EXISTS public."{table}"')
    pg_sql(f'DROP TABLE IF EXISTS public."{table}_df_quarantine"')


def _mysql_has(table: str) -> bool:
    return bool(mysql_sql(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = %s",
        (table,), fetch=True,
    ))


def _pg_rows(table: str) -> int:
    exists = pg_sql("SELECT to_regclass(%s)", (f'public."{table}"',), fetch=True)[0][0]
    return pg_sql(f'SELECT count(*) FROM public."{table}"', fetch=True)[0][0] if exists else 0


def test_live_tinyint1_with_non_boolean_values_keeps_every_value(isolated):  # noqa: F811
    table = f"acc6_t1_{token()}"
    try:
        mysql_sql(f"CREATE TABLE {table} (id INT PRIMARY KEY, flag TINYINT(1))")
        mysql_sql(f"INSERT INTO {table} VALUES (1,0),(2,1),(3,2),(4,-1),(5,NULL)")
        result = run_transfer(isolated, mysql_endpoint(table), pg_endpoint(table))
        assert result.success, result.error
        kind = pg_sql(
            "SELECT data_type FROM information_schema.columns WHERE table_schema='public' "
            "AND table_name=%s AND column_name='flag'", (table,), fetch=True)
        assert kind and kind[0][0] != "boolean", kind
        landed = pg_sql(f'SELECT id, flag FROM public."{table}" ORDER BY id', fetch=True)
        assert [(i, None if v is None else int(v)) for i, v in landed] == [
            (1, 0), (2, 1), (3, 2), (4, -1), (5, None)]
    finally:
        _drop(table)


def test_live_pg_dates_outside_mysql_range_are_refused_not_written(isolated):  # noqa: F811
    table = f"acc6_d_{token()}"
    try:
        pg_sql(f'CREATE TABLE public."{table}" (id INT PRIMARY KEY, d DATE, ts TIMESTAMP(3))')
        pg_sql(
            f"INSERT INTO public.\"{table}\" VALUES (1,'2024-01-01','2024-01-01 10:00:00.123'),"
            "(2,'0500-06-01','0500-06-01 00:00:00'),(3,'12000-01-01','12000-01-01 00:00:00'),"
            "(4,'0044-03-15 BC','0044-03-15 00:00:00 BC')"
        )
        result = run_transfer(isolated, pg_endpoint(table), mysql_endpoint(table))
        assert not result.success
        assert "outside mysql DATE range" in str(result.error), result.error
        assert not _mysql_has(table) or not mysql_sql(f"SELECT count(*) FROM {table}", fetch=True)[0][0]
    finally:
        _drop(table)


def test_live_mysql_zero_dates_are_refused_never_nulled(isolated):  # noqa: F811
    import pymysql

    from tests.helpers.live_env import mysql_creds

    table = f"acc6_z_{token()}"
    c = mysql_creds()
    try:
        mysql_sql(f"CREATE TABLE {table} (id INT PRIMARY KEY, d DATETIME NULL, dd DATE NULL)")
        conn = pymysql.connect(host=c["host"], port=int(c["port"]), user=c["username"],
                               password=c["password"], database=c["database"], autocommit=True)
        try:
            with conn.cursor() as cur:
                cur.execute("SET SESSION sql_mode=''")
                cur.execute(
                    f"INSERT INTO {table} VALUES (1,'2024-01-01 10:00:00','2024-01-01'),"
                    "(2,'0000-00-00 00:00:00','0000-00-00'),(3,'2024-00-10 00:00:00','2024-02-00')"
                )
        finally:
            conn.close()
        result = run_transfer(isolated, mysql_endpoint(table), pg_endpoint(table))
        assert not result.success
        assert "not a real calendar date" in str(result.error), result.error
        assert _pg_rows(table) == 0
    finally:
        _drop(table)


@pytest.mark.parametrize(
    "values,lands",
    [
        (["12.5", "-3", "0.000000000000001"], True),
        (["1.12345678901234567890", "2"], False),      # 20 fraction digits > 15
        (["123456789012345678901234567890", "2"], False),  # 30 integer digits > 23
    ],
)
def test_live_unbounded_numeric_lands_exactly_or_is_refused(isolated, values, lands):  # noqa: F811
    table = f"acc6_n_{token()}"
    try:
        pg_sql(f'CREATE TABLE public."{table}" (id INT PRIMARY KEY, v NUMERIC)')
        for i, v in enumerate(values, 1):
            pg_sql(f'INSERT INTO public."{table}" VALUES (%s, %s)', (i, v))
        result = run_transfer(isolated, pg_endpoint(table), mysql_endpoint(table))
        if not lands:
            assert not result.success
            assert "v" in str(result.error)
            assert not _mysql_has(table) or not mysql_sql(f"SELECT count(*) FROM {table}", fetch=True)[0][0]
            return
        assert result.success, result.error
        got = mysql_sql(f"SELECT id, v FROM {table} ORDER BY id", fetch=True)
        assert [(i, Decimal(v)) for i, v in got] == [(i, Decimal(v)) for i, v in enumerate(values, 1)]
    finally:
        _drop(table)


# Accuracy review finding 8: what each writer does with an offset-bearing instant.
# PostgreSQL timestamptz values written with non-UTC offsets; every route that
# lands must hold the same instant, and the wall-clock route is refused.
_TZ_ROWS = [(1, "2024-01-02 03:04:05.123+05:30"), (2, "2024-07-01 23:30:00+02:00")]
_UTC_DIGITS = [(1, "2024-01-01 21:34:05.123000"), (2, "2024-07-01 21:30:00.000000")]


def _pg_tz_source(table: str) -> list[str]:
    pg_sql(f'CREATE TABLE public."{table}" (id INT PRIMARY KEY, at TIMESTAMPTZ(6))')
    for i, v in _TZ_ROWS:
        pg_sql(f'INSERT INTO public."{table}" VALUES (%s, %s)', (i, v))
    return [str(r[0]) for r in pg_sql(
        f'SELECT extract(epoch FROM at) FROM public."{table}" ORDER BY id', fetch=True)]


@pytest.mark.parametrize("existing", [None, "DATETIME(6)", "TIMESTAMP(6)"])
def test_live_timestamptz_lands_in_mariadb_as_the_utc_instant(isolated, existing):  # noqa: F811
    table = f"acc8_tz_{token()}"
    try:
        _pg_tz_source(table)
        if existing:
            mysql_sql(f"CREATE TABLE {table} (id INT PRIMARY KEY, at {existing})")
        result = run_transfer(isolated, pg_endpoint(table), mysql_endpoint(table))
        assert result.success, result.error
        got = mysql_sql(f"SELECT id, CAST(at AS CHAR) FROM {table} ORDER BY id", fetch=True)
        assert [(i, s) for i, s in got] == _UTC_DIGITS
    finally:
        _drop(table)


def test_live_timestamptz_create_new_in_postgres_keeps_the_instant(isolated):  # noqa: F811
    src, dst = f"acc8_ts_{token()}", f"acc8_td_{token()}"
    try:
        epochs = _pg_tz_source(src)
        dest = pg_endpoint(dst)
        result = run_transfer(isolated, pg_endpoint(src), dest)
        assert result.success, result.error
        got = pg_sql(f'SELECT extract(epoch FROM at), pg_typeof(at)::text FROM public."{dst}" ORDER BY id',
                     fetch=True)
        assert [str(e) for e, _ in got] == epochs
        assert {t for _, t in got} == {"timestamp with time zone"}
    finally:
        _drop(src)
        _drop(dst)


def test_live_timestamptz_into_a_wall_clock_column_is_refused(isolated):  # noqa: F811
    src, dst = f"acc8_ws_{token()}", f"acc8_wd_{token()}"
    try:
        _pg_tz_source(src)
        pg_sql(f'CREATE TABLE public."{dst}" (id INT PRIMARY KEY, at TIMESTAMP(6))')
        result = run_transfer(isolated, pg_endpoint(src), pg_endpoint(dst))
        assert refused_at_validate(result, "at")
        assert _pg_rows(dst) == 0
    finally:
        _drop(src)
        _drop(dst)
