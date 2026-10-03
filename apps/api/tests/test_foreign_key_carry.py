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
    classify_cycle_resolution,
    order_tables_by_dependency,
    plan_foreign_keys,
    verify_foreign_keys,
)
from services.foreign_key_orchestration import dependency_order
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
                on_delete="CASCADE",
                validated=True,
            )
        ],
    )
    settled = verify_foreign_keys(plan.decisions, dest)
    assert settled[0].status == "carried"


def test_not_valid_catalog_bit_is_not_a_carried_foreign_key():
    """The relationship matches. The catalog says existing rows were not checked."""
    plan = _plan()
    dest = ForeignKeys(
        dialect="postgresql",
        status="measured",
        items=[
            ForeignKey(
                name="orders_customer_fk",
                columns=["customer_id"],
                referenced_schema="public",
                referenced_table="customers",
                referenced_columns=["id"],
                on_delete="CASCADE",
                validated=False,
            )
        ],
    )
    settled = verify_foreign_keys(plan.decisions, dest)
    assert settled[0].status == "unsupported"
    assert "existing rows were not checked" in settled[0].reason


def test_a_different_referential_action_is_not_the_source_rule():
    """NO ACTION on the same columns is not the CASCADE rule the source declared."""
    plan = _plan()
    weaker = ForeignKeys(
        dialect="postgresql",
        status="measured",
        items=[
            ForeignKey(
                name="orders_customer_fk_old",
                columns=["customer_id"],
                referenced_schema="public",
                referenced_table="customers",
                referenced_columns=["id"],
                on_delete="NO ACTION",
            )
        ],
    )
    settled = verify_foreign_keys(plan.decisions, weaker)
    assert settled[0].status == "unsupported"
    assert "ON DELETE NO ACTION" in settled[0].reason
    assert "ON DELETE CASCADE" in settled[0].reason
    stronger = ForeignKeys(
        dialect="postgresql",
        status="measured",
        items=[
            ForeignKey(
                name="orders_customer_fk_strict",
                columns=["customer_id"],
                referenced_schema="public",
                referenced_table="customers",
                referenced_columns=["id"],
                on_delete="SET NULL",
            )
        ],
    )
    drifted = verify_foreign_keys(plan.decisions, stronger)
    assert drifted[0].status == "unsupported"
    assert "SET NULL" in drifted[0].reason


def test_unreported_action_matches_only_the_engine_default():
    source = {
        "status": "measured",
        "items": [
            {
                "name": "orders_customer_fk",
                "columns": ["customer_id"],
                "referenced_schema": "public",
                "referenced_table": "customers",
                "referenced_columns": ["id"],
            }
        ],
    }
    plan = _plan(source_foreign_keys=source)
    dest = ForeignKeys(
        dialect="postgresql",
        status="measured",
        items=[
            ForeignKey(
                name="orders_customer_fk",
                columns=["customer_id"],
                referenced_schema="public",
                referenced_table="customers",
                referenced_columns=["id"],
                validated=True,
            )
        ],
    )
    assert verify_foreign_keys(plan.decisions, dest)[0].status == "carried"


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


def test_qualified_stream_in_the_job_is_not_an_outside_parent():
    """archive.customers selected in this job is that stream, not a missing leaf."""
    plan = _plan(
        source_foreign_keys={
            "status": "measured",
            "items": [
                {
                    "name": "orders_customer_fk",
                    "columns": ["customer_id"],
                    "referenced_schema": "archive",
                    "referenced_table": "customers",
                    "referenced_columns": ["id"],
                }
            ],
        },
        dest_schema="sales",
        dest_table="orders",
        source_table="sales.orders",
        source_schema="sales",
        table_map={"archive.customers": "dim_customer", "sales.orders": "orders"},
        dest_existing_tables=set(),
    )
    decision = _only(plan)
    assert decision.status == "planned"
    assert decision.referenced_stream == "archive.customers"
    assert 'REFERENCES "sales"."dim_customer"' in decision.dest_ddl


def test_cross_schema_key_is_not_the_cycle_edge():
    """archive.customers must not make the local customers↔orders cycle look carried."""
    plan = _plan(
        source_foreign_keys={
            "status": "measured",
            "items": [
                {
                    "name": "orders_archive_fk",
                    "columns": ["customer_id"],
                    "referenced_schema": "archive",
                    "referenced_table": "customers",
                    "referenced_columns": ["id"],
                }
            ],
        },
        source_schema="public",
        source_table="orders",
        table_map={"orders": "orders", "customers": "customers"},
        cycle_tables=["orders", "customers"],
        dest_existing_tables={"customers", "orders"},
    )
    decision = _only(plan)
    assert decision.referenced_stream == ""
    assert "DEFERRABLE" not in decision.dest_ddl
    missed = classify_cycle_resolution(["orders", "customers"], [decision.__dict__])
    assert missed["edge_count"] == 0
    assert missed["resolved"] is False
    carried = classify_cycle_resolution(
        ["orders", "customers"],
        [
            {
                "source_table": "orders",
                "dest_table": "orders",
                "referenced_table": "customers",
                "referenced_stream": "customers",
                "status": "carried",
            },
            decision.__dict__,
        ],
    )
    assert carried["resolved"] is True
    assert carried["edge_count"] == 1


def test_qualified_cycle_edges_resolve_by_stream_name():
    resolution = classify_cycle_resolution(
        ["archive.customers", "sales.orders"],
        [
            {
                "source_table": "sales.orders",
                "dest_table": "orders",
                "referenced_stream": "archive.customers",
                "referenced_table": "dim_customer",
                "status": "carried",
            },
            {
                "source_table": "archive.customers",
                "dest_table": "dim_customer",
                "referenced_stream": "sales.orders",
                "referenced_table": "orders",
                "status": "carried",
            },
        ],
    )
    assert resolution["resolved"] is True
    assert resolution["edge_count"] == 2


def test_cycle_edge_on_postgres_is_deferrable():
    plan = _plan(cycle_tables=["orders", "customers"])
    decision = _only(plan)
    assert decision.status == "planned"
    assert "DEFERRABLE INITIALLY DEFERRED" in decision.dest_ddl


def test_acyclic_postgres_edge_is_not_deferrable():
    plan = _plan()
    assert "DEFERRABLE" not in _only(plan).dest_ddl


def test_mysql_cycle_uses_post_load_alter_not_deferrable():
    plan = _plan(dest_dialect="mysql", dest_schema="", cycle_tables=["orders", "customers"])
    assert "DEFERRABLE" not in _only(plan).dest_ddl
    assert "ADD CONSTRAINT" in _only(plan).dest_ddl


def test_self_ref_is_a_cycle_edge_even_without_cycle_list():
    source = {
        "status": "measured",
        "items": [
            {
                "name": "emp_mgr",
                "columns": ["mgr_id"],
                "referenced_table": "emp",
                "referenced_columns": ["id"],
            }
        ],
    }
    plan = _plan(
        source_foreign_keys=source,
        dest_table="emp",
        dest_columns=["id", "mgr_id"],
        column_map={"mgr_id": "mgr_id"},
        table_map={"emp": "emp"},
        dest_existing_tables={"emp"},
    )
    assert "DEFERRABLE INITIALLY DEFERRED" in _only(plan).dest_ddl


def test_classify_cycle_resolved_when_every_edge_carried():
    resolution = classify_cycle_resolution(
        ["orders", "customers"],
        [
            {"dest_table": "orders", "referenced_table": "customers", "status": "carried"},
            {"dest_table": "customers", "referenced_table": "orders", "status": "carried"},
        ],
    )
    assert resolution["resolved"] is True
    assert resolution["strategy"] == "post_load_alter"
    assert resolution["edge_count"] == 2


def test_classify_cycle_unresolved_when_an_edge_failed():
    resolution = classify_cycle_resolution(
        ["a", "b"],
        [
            {"dest_table": "a", "referenced_table": "b", "status": "carried"},
            {"dest_table": "b", "referenced_table": "a", "status": "unsupported"},
        ],
    )
    assert resolution["resolved"] is False
    assert resolution["unresolved"][0]["dest_table"] == "b"


def test_classify_cycle_unresolved_when_detected_but_no_edges():
    resolution = classify_cycle_resolution(["a", "b"], [])
    assert resolution["resolved"] is False
    assert resolution["edge_count"] == 0


def _measured(table: str, items: list[ForeignKey], schema: str = "public") -> ForeignKeys:
    return ForeignKeys(
        dialect="postgresql", status="measured", schema=schema, table=table, items=items
    )


def _fk(schema: str, table: str, *, child: str = "customer_id", parent: str = "id") -> ForeignKey:
    return ForeignKey(
        name="fk",
        columns=[child],
        referenced_schema=schema,
        referenced_table=table,
        referenced_columns=[parent],
    )


def test_unqualified_parent_still_loads_first():
    tables = ["order_lines", "orders", "customers"]
    ordered, cycle = dependency_order(
        tables,
        {
            "orders": _measured("orders", [_fk("public", "customers")]),
            "order_lines": _measured("order_lines", [_fk("", "orders", child="order_id")]),
        },
    )
    assert ordered == ["customers", "orders", "order_lines"]
    assert cycle == []


def test_qualified_parent_in_the_job_loads_first():
    """archive.customers is the parent even though the catalog leaf is customers."""
    tables = ["orders", "archive.customers"]
    ordered, cycle = dependency_order(
        tables,
        {
            "orders": _measured("orders", [_fk("archive", "customers")]),
            "archive.customers": _measured("archive.customers", []),
        },
    )
    assert ordered == ["archive.customers", "orders"]
    assert cycle == []


def test_archive_parent_is_not_the_local_customers_table():
    """A local customers→orders edge must not become a cycle with archive.customers."""
    tables = ["orders", "customers"]
    ordered, cycle = dependency_order(
        tables,
        {
            "orders": _measured("orders", [_fk("archive", "customers")]),
            "customers": _measured("customers", [_fk("public", "orders", child="order_id")]),
        },
    )
    assert cycle == []
    assert ordered.index("orders") < ordered.index("customers")


def test_ambiguous_leaf_does_not_invent_a_parent():
    tables = ["orders", "sales.customers", "archive.customers"]
    ordered, cycle = dependency_order(
        tables,
        {"orders": _measured("orders", [_fk("", "customers")])},
    )
    assert ordered == tables
    assert cycle == []


def test_qualified_self_reference_is_not_an_edge_to_another_table():
    tables = ["public.emp", "emp"]
    ordered, cycle = dependency_order(
        tables,
        {"public.emp": _measured("public.emp", [_fk("public", "emp", child="mgr_id")])},
    )
    assert ordered == ["public.emp", "emp"]
    assert cycle == []


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


def test_empty_mapping_document_is_identity_not_unmapped():
    """Create-new with no Map stamps still wrote source column names."""
    plan = _plan(dest_columns=[], column_map={})
    decision = _only(plan)
    assert decision.status == "planned"
    assert '("customer_id")' in decision.dest_ddl
    assert '("id")' in decision.dest_ddl


def test_renamed_parent_key_is_referenced_under_the_destination_name():
    plan = _plan(
        referenced_column_maps={"customers": {"id": "customer_pk"}},
    )
    decision = _only(plan)
    assert decision.status == "planned"
    assert '("customer_pk")' in decision.dest_ddl
    assert decision.referenced_columns == ("customer_pk",)


def test_parent_key_dropped_from_the_parent_map_is_refused():
    plan = _plan(
        referenced_column_maps={"customers": {"name": "name"}},
    )
    decision = _only(plan)
    assert decision.status == "unsupported"
    assert "id" in decision.reason
    assert plan.statements == []


def test_duplicate_alter_stays_planned_so_the_catalog_can_certify():
    """Resume and nested single-table carry re-issue the same ALTER."""
    plan = _plan()

    def execute(_sql: str) -> None:
        raise RuntimeError('constraint "fk_orders_customer_id" already exists')

    settled = apply_foreign_keys(plan, execute)
    assert settled[0].status == "planned"
    assert settled[0].integrity_violation is False


def test_mariadb_duplicate_fk_index_is_already_present_not_a_failure():
    """InnoDB reports errno 121 when the nested single-table carry already added it."""
    plan = _plan(dest_dialect="mysql", dest_schema="")

    def execute(_sql: str) -> None:
        raise RuntimeError(
            '(1005, \'Can\\\'t create table `shop`.`orders` '
            '(errno: 121 "Duplicate key on write or update")\')'
        )

    settled = apply_foreign_keys(plan, execute)
    assert settled[0].status == "planned"
    assert settled[0].integrity_violation is False


def _archive_customer():
    return {
        "status": "measured",
        "items": [
            {
                "name": "orders_customer_fk",
                "columns": ["customer_id"],
                "referenced_schema": "archive",
                "referenced_table": "customers",
                "referenced_columns": ["id"],
            }
        ],
    }


def test_parent_in_another_schema_is_not_the_local_table():
    """sales.customers is not archive.customers, even when the leaf matches."""
    plan = _plan(
        source_foreign_keys=_archive_customer(),
        dest_schema="sales",
        table_map={},
        dest_existing_tables={"customers", "orders"},
    )
    decision = _only(plan)
    assert decision.status == "unknown"
    assert plan.statements == []
    assert "archive.customers" in decision.reason
    assert "sales" in decision.reason


def test_parent_in_another_schema_is_referenced_there_when_listed():
    plan = _plan(
        source_foreign_keys=_archive_customer(),
        dest_schema="sales",
        table_map={},
        dest_existing_tables={"customers", "orders"},
        dest_tables_by_schema={"Archive": {"Customers"}},
    )
    decision = _only(plan)
    assert decision.status == "planned"
    assert 'REFERENCES "archive"."customers" ("id")' in decision.dest_ddl
    assert decision.referenced_schema == "archive"


def test_listed_other_schema_without_the_parent_is_refused():
    plan = _plan(
        source_foreign_keys=_archive_customer(),
        dest_schema="sales",
        table_map={},
        dest_existing_tables={"customers"},
        dest_tables_by_schema={"archive": set()},
    )
    decision = _only(plan)
    assert decision.status == "unsupported"
    assert "archive" in decision.reason
    assert plan.statements == []


def test_parent_moved_by_the_job_lands_in_the_job_schema():
    plan = _plan(
        source_foreign_keys=_archive_customer(),
        dest_schema="sales",
        table_map={"customers": "customers"},
    )
    decision = _only(plan)
    assert decision.status == "planned"
    assert 'REFERENCES "sales"."customers"' in decision.dest_ddl
    assert decision.referenced_schema == "sales"


def test_catalog_reread_rejects_a_different_parent_schema():
    plan = _plan()
    dest = ForeignKeys(
        dialect="postgresql",
        status="measured",
        items=[
            ForeignKey(
                name="orders_customer_fk",
                columns=["customer_id"],
                referenced_schema="archive",
                referenced_table="customers",
                referenced_columns=["id"],
            )
        ],
    )
    settled = verify_foreign_keys(plan.decisions, dest)
    assert settled[0].status == "unsupported"


def test_catalog_reread_accepts_reversed_composite_pairs():
    source = {
        "status": "measured",
        "items": [
            {
                "name": "line_fk",
                "columns": ["a", "b"],
                "referenced_schema": "public",
                "referenced_table": "parent",
                "referenced_columns": ["x", "y"],
            }
        ],
    }
    plan = _plan(
        source_foreign_keys=source,
        dest_columns=["a", "b"],
        column_map={"a": "a", "b": "b"},
        table_map={"parent": "parent"},
    )
    dest = ForeignKeys(
        dialect="postgresql",
        status="measured",
        items=[
            ForeignKey(
                name="line_fk_dest",
                columns=["b", "a"],
                referenced_schema="public",
                referenced_table="parent",
                referenced_columns=["y", "x"],
                validated=True,
            )
        ],
    )
    settled = verify_foreign_keys(plan.decisions, dest)
    assert settled[0].status == "carried"


def test_catalog_reread_rejects_a_different_parent_column():
    plan = _plan()
    dest = ForeignKeys(
        dialect="postgresql",
        status="measured",
        items=[
            ForeignKey(
                name="orders_customer_fk",
                columns=["customer_id"],
                referenced_schema="public",
                referenced_table="customers",
                referenced_columns=["legacy_id"],
            )
        ],
    )
    settled = verify_foreign_keys(plan.decisions, dest)
    assert settled[0].status == "unsupported"


def test_catalog_reread_matches_an_unqualified_parent_name():
    """Engines that omit the default schema still match a qualified plan."""
    plan = _plan()
    dest = ForeignKeys(
        dialect="postgresql",
        status="measured",
        items=[
            ForeignKey(
                name="orders_customer_fk",
                columns=["customer_id"],
                referenced_schema="",
                referenced_table="customers",
                referenced_columns=["id"],
                on_delete="CASCADE",
                validated=True,
            )
        ],
    )
    settled = verify_foreign_keys(plan.decisions, dest)
    assert settled[0].status == "carried"


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
