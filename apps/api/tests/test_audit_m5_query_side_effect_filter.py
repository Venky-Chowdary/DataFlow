"""Audit M-5: package-qualified calls no longer slip past the query playground filter.

The side-effect scan read only the identifier directly before ``(``. For
``utl_http.request(`` that is ``request``, so the ``utl_`` / ``dbms_`` prefixes
never matched an Oracle package: a ``query.use`` holder could make a customer's
Oracle database issue HTTP requests (``utl_http.request``) or run dynamic SQL
(``dbms_xmlgen.getxml``) with the connector's privileges. PostgreSQL's
``query_to_xml('...')`` hid a whole statement from the scan the same way.

The scan now reads the whole dotted name (quoted or not), and Oracle sessions
are opened READ ONLY like PostgreSQL / MySQL / SQLite already were.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from tenancy_world import tenancy_world  # noqa: E402,F401

REFUSED = [
    # Oracle packages: network, files, dynamic SQL, scheduler, locks.
    "select utl_http.request('http://169.254.169.254/') from dual",
    "select UTL_HTTP.REQUEST('http://169.254.169.254/') from dual",
    "select sys.utl_http.request('http://169.254.169.254/') from dual",
    'select "UTL_HTTP"."REQUEST"(\'http://169.254.169.254/\') from dual',
    'select "SYS"."UTL_HTTP".request(\'http://169.254.169.254/\') from dual',
    "select utl_http . request ('http://169.254.169.254/') from dual",
    "select utl_http/**/./**/request('http://169.254.169.254/') from dual",
    "select utl_inaddr.get_host_address('internal.corp') from dual",
    "select dbms_xmlgen.getxml('select 1 from dual') from dual",
    "select sys.dbms_xmlgen.getxmltype('delete from t') from dual",
    "select dbms_ldap.init('10.0.0.1', 389) from dual",
    "select dbms_lock.sleep(100) from dual",
    "select dbms_java.runjava('x') from dual",
    "select httpuritype('http://169.254.169.254/').getclob() from dual",
    "select sys.httpuritype('http://169.254.169.254/').getclob() from dual",
    "select dburitype('/SYS/USER$').getclob() from dual",
    "select utl_file.fopen('DIR', 'f', 'r') from dual",
    "select utl_http.request@remote_db('http://169.254.169.254/') from dual",
    "select dbms_xmlgen.getxml @ remote.example.com ('select 1 from dual') from dual",
    # PostgreSQL: a statement hidden in a string that the server executes.
    "select query_to_xml('delete from t', true, true, '')",
    "select pg_catalog.query_to_xml('select pg_read_file(''/etc/passwd'')', true, true, '')",
    "select query_to_xml_and_xmlschema('select 1', true, true, '')",
    "select query_to_xmlschema('select 1', true, true, '')",
    "select * from xpath_table('id', 'doc', 't', '/a', 'true') as x(id int, a text)",
    "select * from ts_stat('select pg_sleep(10)')",
    "select * from crosstab('select set_config(''x'',''y'',false)') as c(a text, b text)",
    "select public.dblink_exec('host=10.0.0.1', 'drop table t')",
    "select pg_catalog.set_config('transaction_read_only', 'off', true)",
    "select pg_catalog.pg_read_file('/etc/passwd')",
    'select "pg_catalog"."lo_import"(\'/etc/passwd\')',
    # SQL Server: extended procedures, linked servers, server file readers.
    "select * from openquery(LINKED, 'exec master..xp_cmdshell ''dir''')",
    "select * from master.dbo.xp_cmdshell('dir')",
    "select * from master..xp_cmdshell('dir')",
    "select * from [master]..[xp_cmdshell]('dir')",
    "select * from [master].[dbo].[xp_dirtree]('c:\\')",
    "select * from sys.xp_subdirs('c:\\')",
    "select * from sys.fn_xe_file_target_read_file('x*.xel', null, null, null)",
    "select * from fn_trace_gettable('c:\\trace.trc', default)",
    "select * from sys.dm_exec_sql_text(master.dbo.sp_executesql('x'))",
    # Warehouses: account-level control functions, federated pass-through.
    "select system$cancel_query('01a2b3')",
    "select SYSTEM$ABORT_SESSION(1234)",
    "select * from external_query('proj.us.conn', 'delete from t')",
    # MySQL user-defined shell functions.
    "select mysql.sys_exec('id')",
]

ALLOWED = [
    "select 1",
    "select count(*) from orders",
    "select o.id, upper(o.name), coalesce(o.total, 0) from sales.orders o",
    "select sales.order_total(o.id) from sales.orders o",
    "select dbo.fn_net(o.total) from dbo.orders o",
    "select json_extract(doc, '$.a') from t",
    "select date_trunc('day', created_at), sum(total) from orders group by 1",
    "select sys_context('userenv', 'current_schema') from dual",
    "select sum(total) over (partition by region order by day) from orders",
    "select * from information_schema.tables where table_schema = 'public'",
    "select name, setting, reset_val from pg_settings",
    "select o.request, o.use_count from orders o",
    "select 'utl_http.request(x)' as note from dual",
    "select t.\"request\" from t where t.id in (select max(id) from t)",
    "select xmlagg(xmlelement(name r, id)) from t",
    "select current_setting('server_version')",
    "select schema_name(schema_id), object_name(object_id) from sys.objects",
    "select o.id from orders@reporting o where o.total > (select avg(total) from orders@reporting)",
    "select a.user@example.com, length(a.note) from accounts a",
]


@pytest.mark.parametrize("sql", REFUSED)
def test_qualified_side_effect_call_is_refused(sql):
    from src.routers.query_router import _has_side_effect_functions, _is_safe_sql

    assert _has_side_effect_functions(sql) is True, sql
    assert _is_safe_sql(sql) is False, sql


@pytest.mark.parametrize("sql", ALLOWED)
def test_ordinary_reads_are_still_allowed(sql):
    from src.routers.query_router import _has_side_effect_functions, _is_safe_sql

    assert _has_side_effect_functions(sql) is False, sql
    assert _is_safe_sql(sql) is True, sql


def test_fallback_gate_without_sqlparse_refuses_them_too():
    from src.routers.query_router import _is_safe_sql_fallback

    assert _is_safe_sql_fallback("select utl_http.request('http://169.254.169.254/') from dual") is False
    assert _is_safe_sql_fallback("select dbms_xmlgen.getxml('select 1 from dual') from dual") is False
    assert _is_safe_sql_fallback("select count(*) from orders") is True


class _RecordingConnection:
    def __init__(self, fail: bool = False):
        self.statements: list[str] = []
        self.fail = fail

    def execute(self, statement, *_args, **_kwargs):
        if self.fail:
            raise RuntimeError("ORA-01453: SET TRANSACTION must be first statement of transaction")
        self.statements.append(str(statement))


@pytest.mark.parametrize("db_type", ["oracle", "Oracle", "oracle_adb", "oracle-autonomous"])
def test_oracle_sessions_open_read_only(db_type):
    from src.routers.query_router import _apply_read_only_session

    conn = _RecordingConnection()
    _apply_read_only_session(conn, db_type)
    assert conn.statements == ["SET TRANSACTION READ ONLY"]
    # Best effort, like every other dialect: a refusal does not fail the query.
    _apply_read_only_session(_RecordingConnection(fail=True), db_type)


@pytest.mark.parametrize("db_type", ["sqlserver", "mssql", "azure_sql", "synapse"])
def test_sql_server_has_no_read_only_mode_and_no_session_state_is_left(db_type):
    """T-SQL has no read-only transaction. Nothing session-scoped is set on the
    pooled connection (a later writer would inherit it); the playground's
    transaction is never committed, and the filter above is the guard."""
    from src.routers.query_router import _apply_read_only_session

    conn = _RecordingConnection()
    _apply_read_only_session(conn, db_type)
    assert conn.statements == []


def test_existing_read_only_dialects_are_unchanged():
    from src.routers.query_router import _apply_read_only_session

    for db_type, expected in (
        ("postgresql", "SET TRANSACTION READ ONLY"),
        ("mysql", "SET TRANSACTION READ ONLY"),
        ("sqlite", "PRAGMA query_only = ON"),
    ):
        conn = _RecordingConnection()
        _apply_read_only_session(conn, db_type)
        assert conn.statements == [expected]


def test_query_route_refuses_a_package_qualified_call(tenancy_world):  # noqa: F811
    """Through POST /query/execute, with a real saved connector."""
    from services.connector_store import create_connector

    (tenancy_world.data_dir / "sqlite").mkdir()
    db = tenancy_world.data_dir / "sqlite" / "q.sqlite"
    with sqlite3.connect(str(db)) as conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("INSERT INTO t VALUES (1, 'a')")
    connector = create_connector({
        "name": "m5-sqlite", "type": "sqlite", "role": "source",
        "workspace_id": tenancy_world.workspace_a, "database": str(db),
    })

    ok = tenancy_world.a.post(
        "/api/v1/query/execute", json={"connector_id": connector.id, "query": "select count(*) as n from t"}
    )
    assert ok.status_code == 200, ok.text
    assert ok.json()["rows"] == [{"n": 1}]

    for sql in (
        "select utl_http.request('http://169.254.169.254/') from t",
        "select dbms_xmlgen.getxml('select 1 from t') from t",
        "select query_to_xml('delete from t', true, true, '')",
    ):
        refused = tenancy_world.a.post(
            "/api/v1/query/execute", json={"connector_id": connector.id, "query": sql}
        )
        assert refused.status_code == 400, (sql, refused.text)
        assert refused.json() == {
            "detail": "Only safe read/metadata queries are allowed in the playground"
        }, refused.text
