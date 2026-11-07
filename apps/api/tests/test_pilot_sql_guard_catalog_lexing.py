"""Pilot catalog guard: lexed secret scan and qualified read-only catalog views."""

from __future__ import annotations

import pytest

from services.copilot_sql_guard import assert_identifiers_allowed, secret_catalog_references

_ALLOWED = {"id", "name", "orders", "user", "customers"}


@pytest.mark.parametrize("sql", [
    "SELECT '--' AS a, rolpassword FROM pg_catalog.pg_authid",
    "SELECT '/*' AS a, passwd FROM pg_shadow -- */",
    "SELECT * FROM `mysql`/* ' */.`user` -- '",
    'SELECT * FROM "pg_catalog"."pg_authid"',
    "SELECT * FROM [sys].[sql_logins]",
    "SELECT 1 -- '\n, pg_authid.rolpassword FROM pg_authid -- '",
])
def test_secret_catalog_names_survive_comment_and_quote_tricks(sql: str) -> None:
    assert secret_catalog_references(sql)


@pytest.mark.parametrize("sql", [
    "SELECT * FROM orders -- mysql.user",
    "SELECT 'mysql.user' AS note FROM orders",
    "SELECT user FROM orders",
    "SELECT * FROM public.orders o JOIN customers c ON c.id = o.id",
])
def test_secret_scan_ignores_comments_and_literals(sql: str) -> None:
    assert secret_catalog_references(sql) == set()


@pytest.mark.parametrize("sql", [
    "SELECT name FROM sys.objects",
    "SELECT name FROM sys.databases",
    "SELECT * FROM sys.schemas",
])
def test_qualified_read_only_catalog_views_are_allowed(sql: str) -> None:
    assert_identifiers_allowed(sql, allowed=_ALLOWED)


@pytest.mark.parametrize("sql", [
    "SELECT * FROM sys.sql_logins",
    "SELECT * FROM mysql.user",
    "SELECT * FROM pg_catalog.pg_authid",
    "SELECT * FROM sys.not_a_known_view",
    "SELECT * FROM mysql.innodb_table_stats",
    "SELECT * FROM pg_catalog.pg_description",
])
def test_secret_and_unlisted_catalog_relations_stay_refused(sql: str) -> None:
    with pytest.raises(ValueError):
        assert_identifiers_allowed(sql, allowed=_ALLOWED)
