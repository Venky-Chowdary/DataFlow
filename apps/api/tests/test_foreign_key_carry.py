"""FOREIGN KEY carry: plan, refuse with a reason, order, and prove.

A reference cannot be certified from the DDL we emitted — only from the
destination catalog after the ALTER. These tests pin what is reproduced, what
is refused and why, and that an orphan rejection stays distinguishable from a
dialect that cannot express the key.
"""

from __future__ import annotations

import os

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from services.foreign_key_carry import (
    apply_foreign_keys,
    order_tables_by_dependency,
    plan_foreign_keys,
    verify_foreign_keys,
)
from services.foreign_key_metadata import ForeignKey, ForeignKeys

MEASURED = {
    "status": "measured",
    "dialect": "postgresql",
    "items": [
        {
            "name": "orders_customer_fk",
            "columns": ["customer_id"],
            "referenced_schema": "public",
            "referenced_table": "customers",
            "referenced_columns": ["id"],
            "on_delete": "CASCADE",
            "on_update": "",
        }
    ],
}


def _plan(**over):
    kwargs = {
        "source_foreign_keys": MEASURED,
        "dest_dialect": "postgresql",
        "dest_schema": "public",
        "dest_table": "orders",
        "dest_columns": ["id", "customer_id"],
        "column_map": {"customer_id": "customer_id"},
        "table_map": {"customers": "customers"},
        "dest_existing_tables": {"customers", "orders"},
    }
    kwargs.update(over)
    return plan_foreign_keys(**kwargs)


def _only(plan):
    assert len(plan.decisions) == 1, plan.decisions
    return plan.decisions[0]


def test_reference_is_planned_as_a_post_load_alter():
    """The constraint is added after the load so the engine validates the rows."""
    plan = _plan()
    decision = _only(plan)
    assert decision.status == "planned"
    assert plan.statements == [decision.dest_ddl]
    assert 'ALTER TABLE "public"."orders" ADD CONSTRAINT' in decision.dest_ddl
    assert 'FOREIGN KEY ("customer_id")' in decision.dest_ddl
    assert 'REFERENCES "public"."customers" ("id")' in decision.dest_ddl
    assert decision.dest_ddl.endswith("ON DELETE CASCADE")


def test_unmeasured_source_catalog_is_unknown_never_no_foreign_keys():
    plan = _plan(source_foreign_keys={"status": "unavailable", "detail": "no grant"})
    decision = _only(plan)
    assert decision.status == "unknown"
    assert "unmeasured, not absent" in decision.reason


def test_measured_table_without_references_is_skipped():
    plan = _plan(source_foreign_keys={"status": "measured", "items": []})
    decision = _only(plan)
    assert decision.status == "skipped"
    assert "no foreign keys" in decision.reason


def test_unmapped_key_column_refuses_instead_of_referencing_the_wrong_column():
    plan = _plan(dest_columns=["id"], column_map={})
    decision = _only(plan)
    assert decision.status == "unsupported"
    assert "customer_id" in decision.reason
    assert plan.statements == []


def test_parent_outside_the_job_and_absent_on_destination_is_refused_by_name():
    plan = _plan(table_map={}, dest_existing_tables={"orders"})
    decision = _only(plan)
    assert decision.status == "unsupported"
    assert "customers" in decision.reason
    assert "stream selection" in decision.reason


def test_parent_outside_the_job_with_unreadable_catalog_is_unknown():
    """"Cannot list destination tables" must not read as "the parent is missing"."""
    plan = _plan(table_map={}, dest_existing_tables=None)
    decision = _only(plan)
    assert decision.status == "unknown"
    assert plan.statements == []


def test_parent_renamed_by_the_job_is_referenced_under_its_destination_name():
    plan = _plan(table_map={"customers": "dim_customer"}, dest_existing_tables=None)
    decision = _only(plan)
    assert decision.status == "planned"
    assert '"public"."dim_customer"' in decision.dest_ddl


def test_sqlite_cannot_add_a_reference_after_the_fact():
    plan = _plan(dest_dialect="sqlite")
    decision = _only(plan)
    assert decision.status == "unsupported"
    assert "rebuild" in decision.reason


def test_on_delete_rule_the_destination_would_not_enforce_is_refused():
    """MySQL parses SET DEFAULT and ignores it — a weaker key is not the key."""
    source = {
        "status": "measured",
        "items": [
            {
                **MEASURED["items"][0],
                "on_delete": "SET DEFAULT",
            }
        ],
    }
    plan = _plan(source_foreign_keys=source, dest_dialect="mysql")
    decision = _only(plan)
    assert decision.status == "unsupported"
    assert "SET DEFAULT" in decision.reason


def test_oracle_has_no_on_update_clause_so_the_rule_is_not_faked():
    source = {
        "status": "measured",
        "items": [{**MEASURED["items"][0], "on_delete": "", "on_update": "CASCADE"}],
    }
    plan = _plan(source_foreign_keys=source, dest_dialect="oracle")
    decision = _only(plan)
    assert decision.status == "unsupported"
    assert "ON UPDATE" in decision.reason


def test_composite_reference_keeps_column_order():
    source = {
        "status": "measured",
        "items": [
            {
                "name": "line_fk",
                "columns": ["order_id", "line_no"],
                "referenced_table": "order_lines",
                "referenced_columns": ["order_id", "line_no"],
            }
        ],
    }
    plan = _plan(
        source_foreign_keys=source,
        dest_columns=["order_id", "line_no"],
        column_map={"order_id": "order_id", "line_no": "line_no"},
        table_map={"order_lines": "order_lines"},
    )
    decision = _only(plan)
    assert '("order_id", "line_no")' in decision.dest_ddl


def test_orphan_rejection_is_a_data_finding_not_a_capability_gap():
    plan = _plan()

    def execute(_sql: str) -> None:
        raise RuntimeError(
            'insert or update on table "orders" violates foreign key constraint'
        )

    settled = apply_foreign_keys(plan, execute)
    assert settled[0].status == "unsupported"
    assert settled[0].integrity_violation is True
    assert "orphan child rows" in settled[0].reason


def test_one_failing_constraint_does_not_cancel_the_clean_ones():
    source = {
        "status": "measured",
        "items": [
            MEASURED["items"][0],
            {
                "name": "orders_region_fk",
                "columns": ["region_id"],
                "referenced_table": "regions",
                "referenced_columns": ["id"],
            },
        ],
    }
    plan = _plan(
        source_foreign_keys=source,
        dest_columns=["id", "customer_id", "region_id"],
        column_map={"customer_id": "customer_id", "region_id": "region_id"},
        table_map={"customers": "customers", "regions": "regions"},
    )
    seen: list[str] = []

    def execute(sql: str) -> None:
        seen.append(sql)
        if "regions" in sql:
            raise RuntimeError("42P01: relation does not exist")

    settled = apply_foreign_keys(plan, execute)
    assert len(seen) == 2
    assert [d.status for d in settled] == ["planned", "unsupported"]
    assert settled[1].integrity_violation is False


def test_carry_is_claimed_only_after_the_destination_catalog_agrees():
    plan = _plan()
    dest = ForeignKeys(
        dialect="postgresql",
        status="measured",
        items=[
            ForeignKey(
                name="orders_customer_fk_1",  # engine renamed it
                columns=["customer_id"],
                referenced_schema="public",
                referenced_table="customers",
                referenced_columns=["id"],
            )
        ],
    )
    settled = verify_foreign_keys(plan.decisions, dest)
    assert settled[0].status == "carried"


def test_destination_without_the_reference_after_the_alter_is_not_carried():
    plan = _plan()
    dest = ForeignKeys(dialect="postgresql", status="measured", items=[])
    assert verify_foreign_keys(plan.decisions, dest)[0].status == "unsupported"


def test_unreadable_destination_catalog_leaves_the_carry_unverified():
    plan = _plan()
    dest = ForeignKeys(dialect="postgresql", status="unavailable", detail="no grant")
    settled = verify_foreign_keys(plan.decisions, dest)
    assert settled[0].status == "unknown"
    assert "emitted DDL is not proof" in settled[0].reason


def test_dependency_order_loads_parents_before_children():
    ordered, cycle = order_tables_by_dependency(
        ["order_lines", "orders", "customers"],
        {"orders": {"customers"}, "order_lines": {"orders"}},
    )
    assert ordered == ["customers", "orders", "order_lines"]
    assert cycle == []


def test_mutual_references_are_reported_rather_than_given_a_fake_order():
    ordered, cycle = order_tables_by_dependency(
        ["a", "b", "c"], {"a": {"b"}, "b": {"a"}}
    )
    assert set(ordered) == {"a", "b", "c"}
    assert ordered[0] == "c"
    assert cycle == ["a", "b"]


def test_references_outside_the_job_do_not_affect_ordering():
    ordered, cycle = order_tables_by_dependency(
        ["orders"], {"orders": {"customers"}}
    )
    assert ordered == ["orders"]
    assert cycle == []


def test_constraint_name_is_derived_from_the_destination_not_the_source():
    """Names are unique per schema on MySQL/SQL Server/Oracle.

    Copying the source name failed the ALTER (MySQL errno 1826, SQL Server msg
    2714) on every same-schema migration, which live runs on all three engines
    reproduced before this was derived from the destination table.
    """
    decision = _only(_plan())
    assert decision.name == "fk_orders_customer_id"
    assert MEASURED["items"][0]["name"] not in decision.dest_ddl
    assert MEASURED["items"][0]["name"] in decision.source_detail


def test_a_name_too_long_for_oracle_is_shortened_without_colliding():
    long_col = "customer_reference_identifier_column"
    plan = plan_foreign_keys(
        source_foreign_keys={
            "status": "measured",
            "dialect": "postgresql",
            "items": [
                {
                    "name": "fk",
                    "columns": [long_col],
                    "referenced_schema": "public",
                    "referenced_table": "customers",
                    "referenced_columns": ["id"],
                }
            ],
        },
        dest_dialect="oracle",
        dest_schema="APP",
        dest_table="ORDERS_FACT_TABLE",
        dest_columns=[long_col],
        column_map={long_col: long_col},
        table_map={"customers": "CUSTOMERS"},
        dest_existing_tables={"CUSTOMERS"},
    )
    name = _only(plan).name
    assert len(name) <= 30
    assert name.startswith("fk_ORDERS_FACT_TABLE")
