"""DuckDB reader guard lexes comments/literals; it must not be spoofed by them.

Pure-function checks (no duckdb install needed). The connect-time
``enable_external_access=false`` backstop is covered in
``test_query_duckdb_external_access.py``.
"""

from __future__ import annotations

import pytest

from src.routers.query_router import _duckdb_reads_external


@pytest.mark.parametrize("sql", [
    # a quoted comment opener must not swallow the rest of the line
    "SELECT '--' AS a, * FROM read_csv('/etc/passwd')",
    "SELECT '/*' AS a, * FROM read_text('/etc/hostname') -- */",
    # comma joins and later FROM-list entries
    "SELECT * FROM users, '/etc/passwd'",
    "SELECT * FROM users u,\n'/etc/passwd'",
    "SELECT * FROM (SELECT 1) t, \"/etc/passwd\"",
    "SELECT * FROM users /* c */ , -- x\n '/etc/passwd'",
    "SELECT * FROM users WHERE id IN (SELECT y FROM z, '/etc/passwd')",
    "WITH a AS (SELECT 1) SELECT * FROM a, '/etc/passwd'",
    "SELECT trim(' ' FROM a), * FROM '/etc/passwd'",
    # function forms
    "SELECT * FROM \"read_csv\"('/x')",
    "SELECT * FROM main.read_csv /* c */ ('/x')",
    "SELECT * FROM users, GLOB('/*')",
    "SELECT 1 FROM (SELECT * FROM read_csv('x'))",
])
def test_reader_forms_are_refused(sql: str) -> None:
    assert _duckdb_reads_external(sql) is True


@pytest.mark.parametrize("sql", [
    "SELECT * FROM users WHERE a IS DISTINCT FROM 'x'",
    "SELECT extract(year FROM '2020-01-01'::date) AS y",
    "SELECT trim(BOTH ' ' FROM name) FROM users",
    "SELECT * FROM users WHERE note = 'FROM ''x'''",
    "SELECT * FROM users WHERE name LIKE 'x%' -- note: FROM 'z'",
    'SELECT a, "Order Items" FROM t',
    "SELECT a, 'x', concat(b, 'y') FROM t1 JOIN t2 ON t1.a = t2.a WHERE t1.b IN ('p', 'q')",
    "SELECT * FROM t1, t2 WHERE t1.a = 'x' ORDER BY 1",
    "SELECT * FROM generate_series(1, 5), t",
    "SELECT 'a' FROM users u, \"orders\" o",
])
def test_ordinary_reads_are_allowed(sql: str) -> None:
    assert _duckdb_reads_external(sql) is False
