"""The quoted DDL builders in connectors.sql_identifiers escape every name.

They exist so callers never interpolate a raw name into DDL (the f-string
execute ratchet, tests/test_sql_fstring_ratchet.py).
"""
from connectors.sql_identifiers import drop_table_if_exists_sql, rename_table_sql, use_warehouse_sql


def test_rename_quotes_and_escapes_both_names():
    assert rename_table_sql('a"b', "c") == 'ALTER TABLE "a""b" RENAME TO "c"'


def test_rename_with_backticks():
    assert rename_table_sql("x`y", "z", "`") == "ALTER TABLE `x``y` RENAME TO `z`"


def test_drop_if_exists_escapes_the_name():
    assert drop_table_if_exists_sql('t"; DROP TABLE users; --') == 'DROP TABLE IF EXISTS "t""; DROP TABLE users; --"'


def test_drop_with_tsql_brackets():
    assert drop_table_if_exists_sql("a]b", "[") == "DROP TABLE IF EXISTS [a]]b]"


def test_use_warehouse_escapes_the_name():
    assert use_warehouse_sql('WH"X') == 'USE WAREHOUSE "WH""X"'
