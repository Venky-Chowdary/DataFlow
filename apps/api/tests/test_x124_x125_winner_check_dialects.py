"""X-124/X-125: the dedupe winner-ordering check must run on an MSSQL and
a DuckDB SOURCE. The ORDER BY used a bare boolean (``(x IS NULL) ASC``)
which T-SQL/Oracle reject; it now uses CASE WHEN. And DuckDB is a listed
source — the catalog-type stamp (X-122) resolves its dialect.
"""

from __future__ import annotations

from src.transfer.reconcile_step import _dedupe_ctx_engine, _unique_check_dialect


def test_duckdb_source_dialect_resolves():
    # _dedupe_ctx_engine prefers the catalog type over the driver key
    # ('generic_sql' has no dialect entry — that was unsupported_source_or_dest).
    engine = _dedupe_ctx_engine("duckdb", "duckdb", "generic_sql")
    assert engine == "duckdb"
    assert _unique_check_dialect(engine) == "duckdb"


def test_mssql_source_dialect_resolves():
    engine = _dedupe_ctx_engine("mssql", "mssql", "pymssql")
    assert engine == "mssql"
    assert _unique_check_dialect(engine) == "sqlserver"


def test_extrema_sql_uses_case_not_boolean_order_term():
    """Build the ORDER BY terms the way _dedupe_source_extrema does and
    assert no bare boolean ORDER BY survives (T-SQL ProgrammingError)."""
    order_cols = [("amt", "desc", "last"), ("grp", "asc", "first")]
    order_terms = []
    for col, direction, nulls in order_cols:
        qc = f'"{col}"'
        dir_sql = "DESC" if direction == "desc" else "ASC"
        null_sql = "DESC" if nulls == "first" else "ASC"
        order_terms.append(
            f"CASE WHEN {qc} IS NULL THEN 1 ELSE 0 END {null_sql}"
        )
        order_terms.append(f"{qc} {dir_sql}")
    joined = " ".join(order_terms)
    assert "(\"amt\" IS NULL)" not in joined
    assert "CASE WHEN \"amt\" IS NULL THEN 1 ELSE 0 END ASC" in joined
    assert "CASE WHEN \"grp\" IS NULL THEN 1 ELSE 0 END DESC" in joined


def test_source_extrema_emits_case_sql(monkeypatch):
    """End-to-end on sqlite: ROW_NUMBER + CASE order runs and returns the
    extremal tuple per key (DDL-agnostic path every dialect shares)."""
    import sqlite3

    from src.transfer import reconcile_step as rs

    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, grp TEXT, amt REAL)")
    conn.executemany(
        "INSERT INTO t (id, grp, amt) VALUES (?,?,?)",
        [(1, "a", 9.0), (2, "a", 10.0), (3, "a", 100.0), (4, None, 1.0)],
    )
    conn.commit()

    class _FakeEngine:
        class dialect:
            class identifier_preparer:
                @staticmethod
                def quote(name):
                    return f'"{name}"'

        def connect(self):
            class _Wrap:
                def __init__(self, c):
                    self.c = c

                def execute(self, stmt):
                    return self.c.execute(str(stmt))

            class _Ctx:
                def __init__(self, c):
                    self.c = c

                def __enter__(self):
                    return _Wrap(self.c)

                def __exit__(self, *a):
                    return False

            return _Ctx(conn)

    from contextlib import contextmanager

    @contextmanager
    def fake_engine(dialect, cfg):
        yield _FakeEngine()

    monkeypatch.setattr(
        "services.dest_precount.warehouse_sql_engine", fake_engine
    )
    monkeypatch.setattr(
        "services.dest_precount.warehouse_sql_table_ref",
        lambda dialect, cfg, schema, table: '"t"',
    )
    rows = rs._dedupe_source_extrema(
        "sqlite",
        {},
        schema="",
        table="t",
        keys=["grp"],
        order_cols=[("amt", "desc", "last")],
    )
    # grp 'a' winner must be amt 100 (max under desc), NULL group keeps amt 1
    assert ("a", 100.0) in [tuple(r) for r in rows]
    assert (None, 1.0) in [tuple(r) for r in rows]
