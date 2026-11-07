"""Uppercase source columns must keep PRIMARY KEY / NOT NULL on create-new.

Oracle and most SQL Server catalogs report column names in upper case. The
fidelity planner used to case-fold the destination names it planned ("ID" →
"id") while every writer creates them verbatim, so the writer's exact-match
lookup found no such column and dropped the PRIMARY KEY, the NOT NULL and the
UNIQUE constraints — while the certificate still read "carried". A foreign key
pointed at that parent then failed with ORA-02270 on a live database, which is
how it was found; these tests keep it found.
"""

from __future__ import annotations

import sqlalchemy as sa

from connectors.generic_sql import _build_table_for_write
from services.schema_fidelity import resolve_create_fidelity_plan

COLUMNS = ["ID", "NAME"]
COLUMN_TYPES = {"ID": "integer", "NAME": "VARCHAR(50)"}
CATALOG = {
    "dialect": "oracle",
    "columns": COLUMNS,
    "column_types": dict(COLUMN_TYPES),
    "nullable": {"ID": False, "NAME": True},
    "primary_key": ["ID"],
    "unique_keys": [["NAME"]],
}
MAPPINGS = [{"source": c, "target": c} for c in COLUMNS]


def _plan():
    return resolve_create_fidelity_plan(
        source_schema_catalog=CATALOG,
        mappings=MAPPINGS,
        target_columns=COLUMNS,
        target_types=["integer", "VARCHAR(50)"],
        dest_dialect="oracle",
    )


def _table(plan) -> sa.Table:
    engine = sa.create_engine("oracle+oracledb://u:p@h:1521/?service_name=s")
    return _build_table_for_write(
        engine,
        "T",
        "SYSTEM",
        COLUMNS,
        dict(COLUMN_TYPES),
        db_type="oracle",
        fidelity_plan=plan,
    )


def test_plan_keeps_the_source_spelling_of_key_columns():
    plan = _plan()
    assert plan.primary_key == ["ID"]
    assert set(plan.not_null_columns) == {"ID"}
    assert [list(u) for u in plan.unique_constraints] == [["NAME"]]


def test_created_table_carries_the_key_the_certificate_claims():
    table = _table(_plan())
    assert [c.name for c in table.primary_key.columns] == ["ID"]
    assert table.c["ID"].nullable is False


def test_a_plan_name_that_differs_only_by_case_still_resolves():
    """The writer must not drop a constraint over a spelling difference."""
    plan = _plan()
    plan.primary_key = ["id"]
    plan.not_null_columns = ["id"]
    table = _table(plan)
    assert [c.name for c in table.primary_key.columns] == ["ID"]
    assert table.c["ID"].nullable is False


def test_a_plan_name_absent_from_the_table_does_not_invent_a_key():
    plan = _plan()
    plan.primary_key = ["MISSING"]
    table = _table(plan)
    assert list(table.primary_key.columns) == []
