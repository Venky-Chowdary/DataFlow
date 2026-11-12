"""SQL Server playground queries: exactly one SELECT, parsed (security review round 3, M2).

SQL Server runs a second statement after a ``SELECT`` with no separator
(``select 1 use master``, ``select 1 receive top(1) * from q``,
``select 1 commit``). Two rounds of deny-lists of statement words each missed
some and refused ordinary column names (``open``, ``begin``). For SQL Server
the playground now requires, with ``sqlglot``'s T-SQL parser (pinned in
``requirements.txt``):

* the text parses as exactly one statement, and that statement is a
  ``SELECT`` (optionally ``WITH … SELECT``, or a ``UNION`` / ``INTERSECT`` /
  ``EXCEPT`` of them) with no ``INTO``;
* nothing in it is a raw command the parser did not understand;
* no unquoted identifier (column, alias, table) is a T-SQL reserved keyword.
  SQL Server never reads a reserved keyword as a name, so where the parser
  accepted one as an alias (``select 1 commit``), the server would start a
  new statement there. A reserved name must be quoted (``[open]``), as SQL
  Server itself requires.

A text the parser cannot read is refused. Other dialects never come here.
"""

from __future__ import annotations

#: Transact-SQL reserved keywords (SQL Server documentation, "Reserved Keywords").
TSQL_RESERVED = frozenset("""
add all alter and any as asc authorization backup begin between break browse bulk by cascade case
check checkpoint close clustered coalesce collate column commit compute constraint contains
containstable continue convert create cross current current_date current_time current_timestamp
current_user cursor database dbcc deallocate declare default delete deny desc disk distinct
distributed double drop dump else end errlvl escape except exec execute exists exit external fetch
file fillfactor for foreign freetext freetexttable from full function goto grant group having
holdlock identity identity_insert identitycol if in index inner insert intersect into is join key
kill left like lineno load merge national nocheck nonclustered not null nullif of off offsets on
open opendatasource openquery openrowset openxml option or order outer over percent pivot plan
precision primary print proc procedure public raiserror read readtext reconfigure references
replication restore restrict return revert revoke right rollback rowcount rowguidcol rule save
schema securityaudit select semantickeyphrasetable semanticsimilaritydetailstable
semanticsimilaritytable session_user set setuser shutdown some statistics system_user table
tablesample textsize then to top tran transaction trigger truncate try_convert tsequal union unique
unpivot update updatetext use user values varying view waitfor when where while with within
writetext
""".split())


def tsql_single_select(sql: str) -> bool:
    """True when ``sql`` is exactly one read-only SELECT statement in T-SQL."""
    try:
        import sqlglot
        from sqlglot import exp
        from sqlglot.errors import SqlglotError
    except ImportError:  # pragma: no cover - a declared dependency; refuse rather than guess
        return False
    text = str(sql or "").strip()
    while text.endswith(";"):
        text = text[:-1].rstrip()
    if not text:
        return False
    try:
        statements = sqlglot.parse(text, read="tsql")
    except (SqlglotError, ValueError, RecursionError):
        return False
    statements = [s for s in statements if s is not None]
    if len(statements) != 1:
        return False
    root = statements[0]
    if not isinstance(root, (exp.Select, exp.Union, exp.Intersect, exp.Except)):
        return False
    if any(True for _ in root.find_all(exp.Command, exp.Into)):
        return False
    for node in root.find_all(exp.Identifier):
        if not node.quoted and str(node.this or "").lower() in TSQL_RESERVED:
            return False
    return True


__all__ = ["TSQL_RESERVED", "tsql_single_select"]
