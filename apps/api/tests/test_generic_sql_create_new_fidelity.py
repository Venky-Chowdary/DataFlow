"""generic_sql create-new must emit the planner's constraints, not its own.

MySQL, SQL Server and Oracle destinations build CREATE TABLE through
``_build_table_for_write``. Before this, that function knew only about upsert
conflict columns, so a source PK/NOT NULL/DEFAULT/UNIQUE/CHECK reached those
destinations as bare column types.
"""

from __future__ import annotations

import sqlalchemy as sa

from connectors.generic_sql import _build_table_for_write, _fidelity_dialect
from services.schema_fidelity import resolve_create_fidelity_plan

COLUMNS = ["id", "qty", "status"]
TYPES = {"id": "INTEGER", "qty": "INTEGER", "status": "VARCHAR(10)"}

CATALOG = {
    "dialect": "postgresql",
    "columns": COLUMNS,
    "column_types": TYPES,
    "nullable": {"id": False, "qty": False, "status": True},
    "defaults": {"status": "'a'"},
    "primary_key": ["id"],
    "unique_keys": [["status"]],
    "check_constraints_meta": {
        "dialect": "postgresql",
        "status": "measured",
        "items": [{"name": "ck_qty", "predicate": "qty > 0", "columns": ["qty"]}],
    },
}


def _ddl(dest: str, catalog: dict | None = CATALOG) -> str:
    engine = sa.create_engine("sqlite://")
    plan = resolve_create_fidelity_plan(
        source_schema_catalog=catalog,
        mappings=[{"source": c, "target": c} for c in COLUMNS],
        target_columns=list(COLUMNS),
        target_types=[TYPES[c] for c in COLUMNS],
        dest_dialect=dest,
        table_already_exists=False,
    )
    table = _build_table_for_write(
        engine, "t", None, list(COLUMNS), TYPES, db_type="", fidelity_plan=plan
    )
    return str(sa.schema.CreateTable(table).compile(engine))


def test_check_primary_key_unique_and_not_null_reach_the_ddl():
    ddl = _ddl("mysql")
    assert "CHECK" in ddl and "qty" in ddl
    assert "PRIMARY KEY" in ddl
    assert "UNIQUE" in ddl
    assert "NOT NULL" in ddl


def test_default_is_emitted_as_a_server_default():
    assert "DEFAULT" in _ddl("sqlserver")


def test_no_catalog_means_no_invented_constraints():
    ddl = _ddl("oracle", catalog=None)
    assert "CHECK" not in ddl
    assert "UNIQUE" not in ddl


def test_unportable_check_is_not_emitted():
    catalog = dict(CATALOG)
    catalog["check_constraints_meta"] = {
        "dialect": "postgresql",
        "status": "measured",
        "items": [
            {"name": "ck_sub", "predicate": "qty > (SELECT max(x) FROM other)", "columns": []}
        ],
    }
    assert "CHECK" not in _ddl("mysql", catalog=catalog)


def test_unreadable_catalog_is_not_treated_as_no_checks():
    catalog = dict(CATALOG)
    catalog["check_constraints_meta"] = {
        "dialect": "oracle",
        "status": "unavailable",
        "detail": "no privilege on ALL_CONSTRAINTS",
        "items": [],
    }
    engine = sa.create_engine("sqlite://")
    plan = resolve_create_fidelity_plan(
        source_schema_catalog=catalog,
        mappings=[{"source": c, "target": c} for c in COLUMNS],
        target_columns=list(COLUMNS),
        target_types=[TYPES[c] for c in COLUMNS],
        dest_dialect="mysql",
        table_already_exists=False,
    )
    checks = [i for i in plan.report.items if i.aspect == "check"]
    assert [i.status for i in checks] == ["unknown"]
    table = _build_table_for_write(
        engine, "t", None, list(COLUMNS), TYPES, db_type="", fidelity_plan=plan
    )
    assert "CHECK" not in str(sa.schema.CreateTable(table).compile(engine))


def test_existing_table_does_not_claim_carry():
    plan = resolve_create_fidelity_plan(
        source_schema_catalog=CATALOG,
        mappings=[{"source": c, "target": c} for c in COLUMNS],
        target_columns=list(COLUMNS),
        target_types=[TYPES[c] for c in COLUMNS],
        dest_dialect="mysql",
        table_already_exists=True,
    )
    assert plan.check_predicates == []
    assert plan.primary_key == []
    assert not any(i.status == "carried" for i in plan.report.items)


def test_driver_names_resolve_to_planner_dialects():
    assert _fidelity_dialect("azure_sql", "mssql") == "sqlserver"
    assert _fidelity_dialect("", "mssql") == "sqlserver"
    assert _fidelity_dialect("mariadb", "mysql") == "mysql"
    assert _fidelity_dialect("cockroachdb", "") == "postgresql"
