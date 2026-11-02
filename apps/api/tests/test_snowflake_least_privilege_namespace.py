"""Least-privilege Snowflake namespace setup and census name resolution.

Snowflake checks CREATE DATABASE (an account privilege) before IF NOT EXISTS,
so a role that owns only its database failed every load while issuing
``CREATE DATABASE IF NOT EXISTS`` for a database that already existed (found
on a live trial account with a dedicated DATAWRAP_CERT role).
"""

from __future__ import annotations

import pytest

from connectors.snowflake_conn import use_snowflake_namespace


class _Cur:
    def __init__(self, missing=(), deny_create=False):
        self.sql: list[str] = []
        self.missing = set(missing)
        self.deny_create = deny_create

    def execute(self, sql):
        self.sql.append(sql)
        kind, ident = sql.split()[1], sql.split()[-1]
        if sql.startswith("USE") and (kind, ident) in self.missing:
            raise RuntimeError("002043: Object does not exist or not authorized")
        if sql.startswith("CREATE"):
            if self.deny_create:
                raise RuntimeError("003001: Insufficient privileges")
            self.missing.discard((kind, ident))


def test_existing_namespace_never_issues_create():
    cur = _Cur()
    use_snowflake_namespace(cur, database_q='"DB"', schema_q='"PUBLIC"', create=True)
    assert cur.sql == ['USE DATABASE "DB"', 'USE SCHEMA "PUBLIC"']


def test_missing_schema_is_created_only_when_allowed():
    cur = _Cur(missing={("SCHEMA", '"S1"')})
    use_snowflake_namespace(cur, database_q='"DB"', schema_q='"S1"', create=True)
    assert cur.sql == [
        'USE DATABASE "DB"',
        'USE SCHEMA "S1"',
        'CREATE SCHEMA IF NOT EXISTS "S1"',
        'USE SCHEMA "S1"',
    ]


def test_missing_namespace_without_create_raises_the_use_error():
    cur = _Cur(missing={("DATABASE", '"DB"')})
    with pytest.raises(RuntimeError, match="does not exist"):
        use_snowflake_namespace(cur, database_q='"DB"', schema_q='"S"', create=False)
    assert not any(s.startswith("CREATE") for s in cur.sql)


def test_missing_database_without_privilege_surfaces_privilege_error():
    cur = _Cur(missing={("DATABASE", '"DB"')}, deny_create=True)
    with pytest.raises(RuntimeError, match="Insufficient privileges"):
        use_snowflake_namespace(cur, database_q='"DB"', schema_q='"S"', create=True)


def test_empty_database_uses_schema_only():
    cur = _Cur()
    use_snowflake_namespace(cur, database_q="", schema_q='"PUBLIC"', create=True)
    assert cur.sql == ['USE SCHEMA "PUBLIC"']


def test_census_select_resolves_quoted_lowercase_table_and_columns(monkeypatch):
    import connectors.snowflake_conn as sc
    from src.transfer import reconcile_step

    monkeypatch.setattr(sc, "resolve_snowflake_table_name", lambda cur, s, t: "orders")
    monkeypatch.setattr(
        sc, "snowflake_physical_name_map", lambda cur, s, t: {"ID": "id", "AMOUNT": "AMOUNT"}
    )

    class _Conn:
        def cursor(self):
            class _C:
                def close(self):
                    pass
            return _C()

    sql = reconcile_step._snowflake_census_select(
        _Conn(), {"schema": "public"}, "orders", ["id", "amount"]
    )
    assert sql == 'SELECT "id", "AMOUNT" FROM "PUBLIC"."orders"'
