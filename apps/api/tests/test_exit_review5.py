"""Review 5 of the exit rehearsal's value proof: unit level, no database needed.

* C-1: a connector saved under a hosted or aliased type (``supabase``,
  ``amazon_rds_postgresql``, ``azure_sql_database``, ``amazon_rds_oracle``,
  ``amazon_rds_mysql``, ``mariadb``) is the same engine on the wire, so every
  engine-specific rule (the driver folds, the exact tokens, the server-text
  read) answers exactly as it does for the canonical name.
* C-2: the driver folds are classified on what introspection reports.
* H-3: a SQLite column holding more than one storage class (``'1'`` next to
  ``1``) is named as not provable: the extract writes both alike.
"""

from __future__ import annotations

from tests.exit_background_guard import _no_exit_background_work_outlives_a_test  # noqa: F401

import pytest

from services.exit_completeness import digest_plan, profile_rows, server_text_columns
from tests.test_exit_rehearsal_e2e import _run, world  # noqa: F401

ALIASES = {
    "postgresql": ["postgres", "supabase", "neon", "amazon_rds_postgresql", "alloydb"],
    "sqlserver": ["mssql", "sql_server", "azure_sql_database", "amazon_rds_sqlserver"],
    "oracle": ["amazon_rds_oracle"],
    "mysql": ["amazon_rds_mysql", "azure_database_for_mysql", "mariadb"],
}
#: canonical engine -> declared types whose verdict is engine-specific (a fold, a token, a server-text read).
SHAPES = {
    "postgresql": ["INTEGER[]", "TIME", "TIMETZ", "INTERVAL", "JSONB", "JSON", "NUMERIC", "TIMESTAMPTZ",
                   "TIMESTAMP"],
    "sqlserver": ["DATETIME2", "DATETIME2(7)", "TIME", "DATETIMEOFFSET(7)", "SQL_VARIANT", "DATETIME2(6)"],
    "oracle": ["TIMESTAMP(9)", "TIMESTAMP(6) WITH TIME ZONE", "INTERVAL DAY(2) TO SECOND(6)", "DATE", "NUMBER"],
    "mysql": ["FLOAT", "DOUBLE", "DATETIME(6)"],
}


def _verdicts(engine: str, ddls: list[str]) -> dict[str, tuple]:
    from services.exit_value_fidelity import adapter_fold, server_text_kind
    from services.exit_value_tokens import token_kind

    types = {"id": "BIGINT", **{f"c{i}": d for i, d in enumerate(ddls)}}
    cols = list(types)
    prof = profile_rows([{c: None for c in cols}], cols, ["id"], None, types=types, engine=engine)
    exact, typed, uncompared = digest_plan(cols, types, engine)
    return {d: (adapter_fold(d, engine), server_text_kind(d, engine), token_kind(d, engine),
                prof.uncompared.get(f"c{i}"), f"c{i}" in exact, f"c{i}" in uncompared,
                f"c{i}" in server_text_columns(cols, types, engine))
            for i, d in enumerate(ddls)}


@pytest.mark.parametrize("canonical, alias", [(c, a) for c, names in ALIASES.items() for a in names])
def test_c1_an_aliased_connector_type_gets_the_canonical_engines_verdicts(canonical, alias):
    expected = _verdicts(canonical, SHAPES[canonical])
    assert any(v[0] or v[1] or v[2] for v in expected.values()), "the shapes must exercise an engine rule"
    assert _verdicts(alias, SHAPES[canonical]) == expected


def test_c1_the_rehearsal_reads_the_source_engine_through_the_family_resolver():
    """verify_table and the declared-types read name the engine by its family, never by the raw tile id."""
    import ast
    from pathlib import Path

    tree = ast.parse((Path(__file__).resolve().parents[1] / "services/exit_rehearsal.py").read_text("utf-8"))
    for fn in (n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)
               and n.name in {"verify_table", "_declared_types"}):
        text = ast.unparse(fn)
        assert "family_or_kind" in text, fn.name


# ------------------------------------- C-2: the driver folds, classified on what introspection reports

#: (engine, the catalog's own spelling): introspection hands back a logical carrier (MySQL ``float`` reads
#: as ``FLOAT32``, SQL Server ``datetime2(7)`` as ``TIMESTAMP_NTZ(7)``); each must still be named.
FOLDING_CATALOG = [
    ("mysql", "float"), ("mysql", "float(7,3)"), ("mariadb", "float"),
    ("sqlserver", "datetime2(7)"), ("sqlserver", "datetimeoffset(7)"), ("sqlserver", "time(7)"),
    ("sqlserver", "sql_variant"), ("azure_sql_database", "datetime2(7)"),
    ("oracle", "TIMESTAMP(6) WITH TIME ZONE"), ("oracle", "TIMESTAMP(6) WITH LOCAL TIME ZONE"),
    ("oracle", "TIMESTAMP(9)"), ("oracle", "INTERVAL DAY(2) TO SECOND(6)"), ("oracle", "INTERVAL YEAR(2) TO MONTH"),
    ("amazon_rds_oracle", "TIMESTAMP(6) WITH TIME ZONE"),
]
#: Catalog spellings whose driver reads one-to-one: never named.
EXACT_CATALOG = [
    ("mysql", "double"), ("mysql", "datetime(6)"), ("mysql", "decimal(10,2)"), ("mysql", "time"),
    ("sqlserver", "datetime2(6)"), ("sqlserver", "datetime"), ("sqlserver", "time(6)"),
    ("sqlserver", "datetimeoffset(6)"), ("sqlserver", "float"), ("sqlserver", "decimal(12,2)"),
    ("oracle", "TIMESTAMP(6)"), ("oracle", "DATE"), ("oracle", "NUMBER(10,2)"), ("oracle", "BINARY_DOUBLE"),
    ("postgresql", "real"), ("postgresql", "timestamp(6) without time zone"),
]


@pytest.mark.parametrize("engine, physical", FOLDING_CATALOG)
def test_c2_a_folding_type_is_named_as_introspection_reports_it(engine, physical):
    from services.schema_introspect import catalog_read_back

    reported = catalog_read_back(engine, physical)
    types = {"id": "BIGINT", "c": reported}
    src = profile_rows([{"id": 1, "c": None}], list(types), ["id"], None, types=types, engine=engine)
    assert "not provable" in src.uncompared.get("c", ""), (engine, physical, reported, src.uncompared)


@pytest.mark.parametrize("engine, physical", EXACT_CATALOG)
def test_c2_a_one_to_one_type_is_not_named_as_introspection_reports_it(engine, physical):
    from services.schema_introspect import catalog_read_back

    reported = catalog_read_back(engine, physical)
    types = {"id": "BIGINT", "c": reported}
    src = profile_rows([{"id": 1, "c": None}], list(types), ["id"], None, types=types, engine=engine)
    assert "c" not in src.uncompared, (engine, physical, reported, src.uncompared)


def test_c2_the_mapper_outputs_themselves_are_named():
    """The three introspection mappers' own outputs, fed straight in (no DDL spelling anywhere)."""
    from services.exit_value_fidelity import adapter_fold
    from services.schema_introspect import _mysql_to_logical
    from services.schema_introspect_enterprise import _oracle_to_logical, _sqlserver_to_logical

    assert "single-precision FLOAT" in (adapter_fold(_mysql_to_logical("float"), "mysql") or "")
    assert "DATETIME2(7)" in (adapter_fold(_sqlserver_to_logical("datetime2(7)"), "sqlserver") or "")
    assert "DATETIMEOFFSET(7)" in (adapter_fold(_sqlserver_to_logical("datetimeoffset(7)"), "sqlserver") or "")
    assert "TIME ZONE" in (adapter_fold(_oracle_to_logical("TIMESTAMP(6) WITH LOCAL TIME ZONE"), "oracle") or "")
    assert adapter_fold(_sqlserver_to_logical("datetime"), "sqlserver") is None


# ------------------------------------- H-3: SQLite storage classes

#: table -> (columns, rows): a column holding more than one storage class is named, never proven.
SQLITE_MIXED = {
    "untyped": ("id INTEGER PRIMARY KEY, v", [(1, 1), (2, "1")]),
    "untyped_n": ("id INTEGER PRIMARY KEY, v", [(1, "7"), (2, 7.0), (3, None)]),
}
#: One storage class per column (NULL is no class): still proven.
SQLITE_ONE_CLASS = {
    "untyped_int": ("id INTEGER PRIMARY KEY, v", [(1, 1), (2, 2), (3, None)]),
    "num_any": ("id INTEGER PRIMARY KEY, v ANY", [(1, 1), (2, "1")]),  # NUMERIC affinity stores '1' as 1
    # integer and real are one class (a number): NUMERIC(12,2) stores 5.00 as 5 and 1.25 as a real.
    "numeric": ("id INTEGER PRIMARY KEY, v NUMERIC(12,2)", [(1, "5.00"), (2, "1.25"), (3, None)]),
    "txt": ("id INTEGER PRIMARY KEY, v TEXT", [(1, "007"), (2, "1e3"), (3, None)]),
}


@pytest.mark.parametrize("fmt", ["jsonl", "csv"])
def test_h3_a_sqlite_column_holding_more_than_one_storage_class_is_named_not_provable(world, fmt):  # noqa: F811
    import sqlite3

    from services.exit_inventory import declare

    # A CSV cell cannot keep TEXT '007' (the extract's read-back refuses it): one-class text is checked in JSONL.
    shapes = {**SQLITE_MIXED, **{k: v for k, v in SQLITE_ONE_CLASS.items() if fmt == "jsonl" or k != "txt"}}
    conn = sqlite3.connect(str(world["src"]))
    with conn:
        for name, (ddl, rows) in shapes.items():
            conn.execute(f"CREATE TABLE {name} ({ddl})")  # nosec B608 - test table
            conn.executemany(f"INSERT INTO {name} VALUES (?, ?)", rows)  # nosec B608
    conn.close()
    system = declare({"name": f"review 5 {fmt}", "source": {"connector_id": "c1"}, "extract_format": fmt,
                      "tables": [{"name": t} for t in shapes], "exit_window_hours": 24},
                     workspace_id="ws1", actor="owner@example.com")
    tables = {t["name"]: t for t in _run(system)["tables"]}
    for name in SQLITE_MIXED:
        assert tables[name]["status"] == "not_proven", (name, tables[name]["status"], tables[name]["reason"])
        assert "v holds values of more than one SQLite storage class" in tables[name]["reason"], tables[name]
    for name in shapes.keys() - SQLITE_MIXED.keys():
        assert tables[name]["status"] == "proven", (name, tables[name]["status"], tables[name]["reason"])
