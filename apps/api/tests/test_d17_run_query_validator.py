"""D17/M19/M20/M22/M23 — run_query validator + result honesty.

Repros from prod c6c16128 hard-QA:
- D17: valid SQL rejected — datediff(day,…), VARIANT doc:k, extract(epoch…),
  AT TIME ZONE, E'\\n', information_schema.columns, pg_class,
  epoch_microsecond.
- M19: a 100+-column ``||`` concat rejected "Maximum grouping depth exceeded".
- M22: single-row aggregate returned truncated=true; limit=500 returned
  25-of-200 with truncated=false.
- M23: catalog queries (sqlite_master, information_schema.columns) refused.
- M20/M11: column_schema guessed from values (TEXT c126 -> DECIMAL(8,4)).
"""

from __future__ import annotations

import sqlite3
from types import SimpleNamespace

import pytest


def _allowed() -> set[str]:
    return {
        "id", "name", "doc", "amount", "created_at", "orders", "hq1608_w",
        "a", "b", "c1", "c2",
    }


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT datediff(day, a, b) FROM orders",
        "SELECT doc:k FROM orders",
        "SELECT extract(epoch from created_at) FROM orders",
        "SELECT created_at AT TIME ZONE 'UTC' FROM orders",
        "SELECT E'x\\ny' FROM orders",
        "SELECT column_name FROM information_schema.columns WHERE table_name='orders'",
        "SELECT * FROM pg_class",
        "SELECT relname FROM pg_catalog.pg_class WHERE relkind='r'",
        "SELECT epoch_microsecond(created_at) FROM orders",
        "SELECT name FROM sqlite_master",
        "SELECT to_varchar(amount) FROM orders",
        "SELECT * FROM pragma_table_info('orders')",
    ],
)
def test_run_query_allows_valid_sql(sql):
    from services.copilot_sql_guard import assert_identifiers_allowed

    assert_identifiers_allowed(sql, allowed=_allowed())


def test_run_query_still_refuses_invented_identifier():
    from services.copilot_sql_guard import assert_identifiers_allowed

    with pytest.raises(ValueError, match="identifiers not in introspected"):
        assert_identifiers_allowed(
            "SELECT secret_col FROM orders", allowed=_allowed()
        )
    with pytest.raises(ValueError):
        assert_identifiers_allowed(
            "SELECT name FROM other_table", allowed=_allowed()
        )


def test_run_query_long_concat_chain_sqlite(tmp_path):
    """M19: 250-column || concat must validate and execute."""
    from src.routers.query_router import _is_safe_sql, _run_sql_query, QueryExecuteRequest

    cols = "||','||".join(f"c{i}" for i in range(1, 250))
    sql = f"SELECT {cols}||','||c250 AS wide FROM hq1608_w"
    assert _is_safe_sql(sql) is True

    db = tmp_path / "t.sqlite"
    conn = sqlite3.connect(db)
    conn.execute(
        "CREATE TABLE hq1608_w (" + ",".join(f"c{i} TEXT" for i in range(1, 251)) + ")"
    )
    conn.execute(
        "INSERT INTO hq1608_w VALUES (" + ",".join("'v'" for _ in range(250)) + ")"
    )
    conn.commit()
    conn.close()
    connector = SimpleNamespace(
        type="sqlite", host="", port=0, database=str(db), username="",
        password="", connection_string="", schema="", warehouse="", ssl=False,
    )
    body = QueryExecuteRequest(connector_id="c", query=sql, limit=5)
    rows, columns, schema, truncated = _run_sql_query(connector, body)
    assert len(rows) == 1
    assert rows[0]["wide"].count("v") == 250


def test_run_query_truncated_flag_exact(tmp_path):
    """M22: truncated only when rows were actually withheld."""
    from src.routers.query_router import _run_sql_query, QueryExecuteRequest

    db = tmp_path / "t.sqlite"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE hq1608_w (id INTEGER)")
    conn.executemany("INSERT INTO hq1608_w VALUES (?)", [(i,) for i in range(200)])
    conn.commit()
    conn.close()
    connector = SimpleNamespace(
        type="sqlite", host="", port=0, database=str(db), username="",
        password="", connection_string="", schema="", warehouse="", ssl=False,
    )
    # one-row aggregate → NOT truncated
    body = QueryExecuteRequest(
        connector_id="c", query="SELECT count(*) AS n FROM hq1608_w", limit=1000
    )
    rows, _c, _s, truncated = _run_sql_query(connector, body)
    assert len(rows) == 1 and truncated is False
    # exact-limit result → NOT truncated (nothing withheld)
    body = QueryExecuteRequest(
        connector_id="c", query="SELECT id FROM hq1608_w", limit=200
    )
    rows, _c, _s, truncated = _run_sql_query(connector, body)
    assert len(rows) == 200 and truncated is False
    # over the cap → truncated
    body = QueryExecuteRequest(
        connector_id="c", query="SELECT id FROM hq1608_w", limit=50
    )
    rows, _c, _s, truncated = _run_sql_query(connector, body)
    assert len(rows) == 50 and truncated is True


def test_run_query_allows_catalog_views_sqlite(tmp_path):
    """M23: sqlite_master / pragma catalog reads execute like any SELECT."""
    from src.routers.query_router import _run_sql_query, QueryExecuteRequest

    db = tmp_path / "t.sqlite"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE hq1608_w (id INTEGER)")
    conn.commit()
    conn.close()
    connector = SimpleNamespace(
        type="sqlite", host="", port=0, database=str(db), username="",
        password="", connection_string="", schema="", warehouse="", ssl=False,
    )
    body = QueryExecuteRequest(
        connector_id="c", query="SELECT name FROM sqlite_master", limit=50
    )
    rows, columns, _s, _t = _run_sql_query(connector, body)
    assert {"name": "hq1608_w"} in rows


def test_run_query_column_schema_uses_declared_types(tmp_path):
    """M20: a TEXT column whose values look numeric reports TEXT, and a
    boolean column reports its declared type — not the value-guess."""
    from src.routers.query_router import _run_sql_query, QueryExecuteRequest

    db = tmp_path / "t.sqlite"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE hq1608_w (c126 TEXT, flag BOOLEAN)")
    conn.execute("INSERT INTO hq1608_w VALUES ('12.3456', 1)")
    conn.commit()
    conn.close()
    connector = SimpleNamespace(
        type="sqlite", host="", port=0, database=str(db), username="",
        password="", connection_string="", schema="", warehouse="", ssl=False,
    )
    body = QueryExecuteRequest(
        connector_id="c", query="SELECT c126, flag FROM hq1608_w", limit=5
    )
    rows, columns, schema, _t = _run_sql_query(connector, body)
    assert schema["c126"].upper() == "TEXT"
    assert schema["flag"].upper() == "BOOLEAN"


def _allowed_with_user_tables() -> set[str]:
    # A workspace whose own schema has ``user`` / ``db`` tables must not
    # unlock the same-named MySQL grant tables.
    return _allowed() | {"user", "db", "authentication_string", "rolpassword"}


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT rolpassword FROM pg_catalog.pg_authid",
        'SELECT rolpassword FROM "pg_catalog"."pg_authid"',
        "SELECT rolpassword FROM pg_catalog . /* x */ pg_authid",
        "SELECT rolpassword FROM pg_authid",
        "SELECT rolpassword FROM pg_authid AS pg_authid",
        "SELECT usename, passwd FROM pg_catalog.pg_shadow",
        "SELECT * FROM pg_catalog.pg_auth_members",
        "SELECT query FROM pg_stat_activity",
        "SELECT data FROM pg_catalog.pg_largeobject",
        "SELECT umoptions FROM pg_catalog.pg_user_mappings",
        "SELECT * FROM mysql.user",
        "SELECT user, authentication_string FROM `mysql`.`user`",
        "SELECT * FROM mysql.db",
        "SELECT * FROM mysql.global_priv",
        "SELECT * FROM orders o JOIN mysql.user u ON u.user = o.name",
        "SELECT * FROM orders WHERE name IN (SELECT user FROM mysql.user)",
        "WITH x AS (SELECT * FROM mysql.user) SELECT * FROM x",
        "SELECT name, password_hash FROM master.sys.sql_logins",
        "SELECT * FROM performance_schema.events_statements_history",
        "SELECT * FROM information_schema.processlist",
        "SELECT * FROM pg_catalog.pg_description",
        "SELECT * FROM pg_catalog.pg_read_file('/etc/passwd')",
    ],
)
def test_run_query_refuses_secret_and_unlisted_catalog_relations(sql):
    from services.copilot_sql_guard import assert_identifiers_allowed

    with pytest.raises(ValueError, match="identifiers not in introspected schema"):
        assert_identifiers_allowed(sql, allowed=_allowed_with_user_tables())


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT rolpassword FROM pg_catalog.pg_authid",
        "SELECT * FROM mysql.user",
    ],
)
def test_run_query_refuses_secret_catalog_on_regex_fallback(sql, monkeypatch):
    from services import copilot_sql_guard

    monkeypatch.setattr(copilot_sql_guard, "_sqlglot_identifiers", lambda *_a: None)
    with pytest.raises(ValueError, match="identifiers not in introspected schema"):
        copilot_sql_guard.assert_identifiers_allowed(
            sql, allowed=_allowed_with_user_tables()
        )


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT count(*) FROM pg_catalog.pg_tables",
        "SELECT * FROM information_schema.tables",
        "SELECT relname FROM pg_catalog.pg_class",
        "SELECT * FROM user",
        "SELECT * FROM db",
        "SELECT name FROM orders WHERE name = 'pg_authid'",
        "SELECT name FROM orders -- mysql.user\n",
    ],
)
def test_run_query_still_allows_safe_catalog_and_user_tables(sql):
    from services.copilot_sql_guard import assert_identifiers_allowed

    assert_identifiers_allowed(sql, allowed=_allowed_with_user_tables())


def test_run_query_refuses_self_aliased_unlisted_relation():
    from services.copilot_sql_guard import assert_identifiers_allowed

    with pytest.raises(ValueError, match="identifiers not in introspected schema"):
        assert_identifiers_allowed(
            "SELECT * FROM other_table AS other_table", allowed=_allowed()
        )


def test_run_query_qualified_catalog_relation_can_be_allowlisted():
    from services.copilot_sql_guard import assert_identifiers_allowed

    allowed = _allowed() | {"pg_catalog.pg_description"}
    assert_identifiers_allowed("SELECT * FROM pg_catalog.pg_description", allowed=allowed)
