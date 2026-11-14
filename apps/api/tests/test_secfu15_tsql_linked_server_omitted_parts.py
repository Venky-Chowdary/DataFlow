"""Follow-up review 1, L1: a linked-server name with omitted parts is refused too.

Security follow-up 9 refused four-part names (``server.database.schema.table``)
in the SQL Server playground by counting sqlglot's table parts. T-SQL lets a
name omit middle parts (``srv..dbo.t``, ``srv...t``, ``srv.db..t``), and
sqlglot drops the empty parts, so those linked-server names read as two or
three parts and passed. The playground now counts the dots of each dotted
name, empty parts included: three dots name a linked server. ``db..t`` (this
server, another database, default schema) stays allowed.
"""

from __future__ import annotations

import pytest

from src.routers.query_router import _is_safe_sql


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM srv..dbo.t",
        "SELECT * FROM srv...t",
        "SELECT * FROM srv.db..t",
        "SELECT * FROM [srv]...[t]",
        "select o.id from [srv]..[dbo].[orders] o",
        "select * from t join srv...orders o on o.id = t.id",
        "select * from srv . . dbo . t",
    ],
)
def test_linked_server_names_with_omitted_parts_are_refused(sql):
    assert _is_safe_sql(sql, "sqlserver") is False


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM db..t",
        "select t.id from [db]..[t] t",
        "select 'srv...t' as label from dbo.t",
        "select 1.5 as n, t.a from dbo.t",
        "select sales.dbo.orders.id from sales.dbo.orders",
    ],
)
def test_omitted_parts_on_this_server_and_dots_in_literals_are_allowed(sql):
    assert _is_safe_sql(sql, "sqlserver") is True
