"""C3 live proof on local engines: MySQL ``TIMESTAMP`` → PostgreSQL, real engine path.

* Into an existing PostgreSQL ``TIMESTAMP`` (a zoneless wall clock) the run is
  refused at Validate: a MySQL ``TIMESTAMP`` is an instant, and the zone would
  be dropped. Before C3 this pair was graded identity and the run wrote.
* Create-new now lands ``timestamp with time zone`` and keeps each instant, so
  the product's own second run into that table is not refused.

SQL Server and Oracle pairs: ``test_c3_lossy_pairs_sqlserver_live.py`` and
``test_c3_lossy_pairs_oracle_live.py`` (CI ``cdc-sqlserver`` / ``cdc-oracle``).
"""

from __future__ import annotations

import pytest

from tests.c3_live_helpers import (
    mysql_endpoint,
    mysql_sql,
    pg_endpoint,
    pg_sql,
    refused_at_validate,
    run_transfer,
    token,
)
from tests.test_commit_safe_review_round2 import isolated  # noqa: F401 — fixture


@pytest.fixture()
def mysql_timestamps():
    from tests.helpers.live_env import mysql_up, pg_up

    if not (mysql_up() and pg_up()):
        pytest.skip("live MySQL and PostgreSQL not reachable")
    table = f"c3ts_{token()}"
    mysql_sql(f"CREATE TABLE {table} (id INT PRIMARY KEY, at TIMESTAMP NULL)")
    mysql_sql(
        f"INSERT INTO {table} VALUES (1, '2024-03-10 01:30:00'), (2, '2024-07-01 12:00:00')"
    )
    try:
        yield table
    finally:
        mysql_sql(f"DROP TABLE IF EXISTS {table}")
        pg_sql(f'DROP TABLE IF EXISTS public."{table}"')
        pg_sql(f'DROP TABLE IF EXISTS public."{table}_df_quarantine"')


def test_live_mysql_timestamp_into_existing_pg_timestamp_is_refused_at_validate(
    isolated, mysql_timestamps  # noqa: F811
):
    table = mysql_timestamps
    pg_sql(f'CREATE TABLE public."{table}" (id INT PRIMARY KEY, at TIMESTAMP)')
    result = run_transfer(isolated, mysql_endpoint(table), pg_endpoint(table))
    assert refused_at_validate(result, "at"), result.error
    assert "Blocking issues detected" in str(result.error), result.error
    assert pg_sql(f'SELECT count(*) FROM public."{table}"', fetch=True)[0][0] == 0


def test_live_mysql_timestamp_create_new_keeps_the_instant(isolated, mysql_timestamps):  # noqa: F811
    table = mysql_timestamps
    session_zone = mysql_sql("SELECT @@session.time_zone, @@system_time_zone", fetch=True)[0]
    expected = mysql_sql(
        f"SELECT id, UNIX_TIMESTAMP(at) FROM {table} ORDER BY id", fetch=True
    )
    result = run_transfer(isolated, mysql_endpoint(table), pg_endpoint(table))
    assert result.success, result.error
    kind = pg_sql(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = %s AND column_name = 'at'",
        (table,), fetch=True,
    )
    assert kind == [("timestamp with time zone",)], kind
    landed = pg_sql(
        f'SELECT id, extract(epoch FROM at)::bigint FROM public."{table}" ORDER BY id', fetch=True
    )
    assert [(i, int(e)) for i, e in landed] == [(i, int(e)) for i, e in expected], session_zone

    # The product's own second run into the table it created is not refused.
    again = run_transfer(isolated, mysql_endpoint(table), pg_endpoint(table))
    assert again.success, again.error
