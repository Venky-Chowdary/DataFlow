"""Multi-table transforms and per-stream stored procedures.

The compiler stamps each step with its source table. Execute must apply that
step only to that table, and a CALL on one stream must not be replayed onto
the others.
"""

from __future__ import annotations

from services.multi_stream_plan import (
    adopt_inherited_mappings,
    approved_recipe_refusal,
    contract_for_stream,
    design_source_patch,
    endpoint_session_hooks,
    full_recipe_hash,
    partition_shape_steps,
    patches_for_stream,
    review_stream_procedures,
    strip_session_hooks,
)
from services.shape_models import ShapeRecipe


def _trim(column: str, table: str = "") -> dict:
    step = {"op": "trim", "column": column, "enabled": True}
    if table:
        step["source_table"] = table
    return step


def test_partition_keeps_each_tables_steps() -> None:
    grouped, refusal = partition_shape_steps(
        {"steps": [_trim("email", "customers"), _trim("sku", "orders")]},
        ["customers", "orders"],
    )
    assert refusal == ""
    assert [step["column"] for step in grouped["customers"]] == ["email"]
    assert [step["column"] for step in grouped["orders"]] == ["sku"]
    assert "source_table" not in grouped["customers"][0]


def test_untagged_step_among_several_tables_is_refused() -> None:
    grouped, refusal = partition_shape_steps(
        {"steps": [_trim("email", "customers"), _trim("sku")]},
        ["customers", "orders"],
    )
    assert grouped == {}
    assert "no source table" in refusal
    assert "sku" in refusal
    assert "customers" in refusal and "orders" in refusal


def test_step_for_an_unselected_table_is_refused() -> None:
    _grouped, refusal = partition_shape_steps(
        {"steps": [_trim("name", "ghost")]},
        ["customers", "orders"],
    )
    assert "ghost" in refusal
    assert "not among" in refusal


def test_single_stream_accepts_an_untagged_step() -> None:
    grouped, refusal = partition_shape_steps(
        {"steps": [_trim("email")]},
        ["customers"],
    )
    assert refusal == ""
    assert grouped["customers"][0]["column"] == "email"


def test_table_stamp_does_not_change_the_approved_hash() -> None:
    stamped = {"steps": [_trim("email", "customers"), _trim("sku", "orders")]}
    plain = {"steps": [_trim("email"), _trim("sku")]}
    assert full_recipe_hash(stamped) == full_recipe_hash(plain)
    assert full_recipe_hash(stamped) == ShapeRecipe.parse(plain).recipe_hash
    assert approved_recipe_refusal(stamped, full_recipe_hash(plain)) == ""
    assert "not the one approved" in approved_recipe_refusal(stamped, "deadbeef")


def test_endpoint_procedure_is_not_replayed_onto_every_stream() -> None:
    source = {
        "type": "postgresql",
        "extra": {
            "source_read_mode": "procedure",
            "procedure_call": "CALL public.get_orders()",
        },
    }
    destination = {"type": "postgresql", "extra": {}}
    contracts = [
        {"name": "customers", "selected": True},
        {"name": "orders", "selected": True},
    ]
    refusal = review_stream_procedures(
        source, destination, contracts, ["customers", "orders"], sync_mode="full_refresh_append"
    )
    assert "get_orders" in refusal
    assert "customers" in refusal
    assert "Replaying" in refusal


def test_per_stream_call_is_that_streams_extract_only() -> None:
    source = {"type": "postgresql", "extra": {}}
    destination = {"type": "postgresql", "extra": {}}
    contracts = [
        {
            "name": "customers",
            "selected": True,
            "source_read_mode": "procedure",
            "procedure_call": "CALL public.get_customers()",
            "dest_write_mode": "procedure",
            "dest_procedure_call": "CALL public.land_customer(:id)",
        },
        {"name": "orders", "selected": True},
    ]
    refusal = review_stream_procedures(
        source, destination, contracts, ["customers", "orders"], sync_mode="full_refresh_append"
    )
    assert refusal == ""
    source_patch, dest_patch = patches_for_stream(contracts[0])
    assert source_patch is not None
    assert source_patch["procedure_call"] == "CALL public.get_customers()"
    assert dest_patch is not None
    assert dest_patch["dest_procedure_call"] == "CALL public.land_customer(:id)"
    assert patches_for_stream(contracts[1]) == (None, None)


def test_cdc_refuses_a_per_stream_call() -> None:
    source = {"type": "postgresql", "extra": {}}
    contracts = [
        {"name": "customers", "procedure_call": "CALL public.get_customers()"},
        {"name": "orders", "procedure_call": "CALL public.get_orders()"},
    ]
    refusal = review_stream_procedures(
        source, {"extra": {}}, contracts, ["customers", "orders"], sync_mode="cdc"
    )
    assert "CDC" in refusal
    assert "customers" in refusal


def test_partitioned_steps_transform_only_their_table() -> None:
    from services.shape_apply import build_shape_runner

    grouped, refusal = partition_shape_steps(
        {"steps": [_trim("email", "customers"), _trim("sku", "orders")]},
        ["customers", "orders"],
    )
    assert refusal == ""
    customers = build_shape_runner(
        {"steps": grouped["customers"]},
        source_columns=["id", "email"],
    )
    orders = build_shape_runner(
        {"steps": grouped["orders"]},
        source_columns=["id", "sku"],
    )
    assert customers is not None and orders is not None
    assert customers.records([{"id": 1, "email": " Ada@x.com "}])[0]["email"] == "Ada@x.com"
    assert orders.records([{"id": 10, "sku": " ab "}])[0]["sku"] == "ab"
    # The customers recipe was not validated against sku, so it cannot be the
    # orders writer. That is the silent-remap the partition exists to prevent.
    assert "sku" not in customers.recipe.input_columns


def test_preview_approves_the_whole_program_and_shows_one_table() -> None:
    from fastapi import HTTPException

    from src.routers.shape_router import _PreviewBody, _recipe_for_preview

    recipe = {"steps": [_trim("email", "customers"), _trim("sku", "orders")]}
    body = _PreviewBody(
        recipe=recipe,
        source_columns=["id", "email"],
        focus_table="customers",
        source_tables=["customers", "orders"],
        source_catalog={
            "customers": ["id", "email"],
            "orders": ["id", "sku"],
        },
    )
    applied, note, digest = _recipe_for_preview(body, ["id", "email"])
    assert digest == full_recipe_hash(recipe)
    assert applied.recipe_hash != digest
    assert "email" in applied.describe()
    assert "sku" not in applied.describe()
    assert "Preview shows customers" in note

    mixed = _PreviewBody(
        recipe={"steps": [_trim("email", "customers"), _trim("sku")]},
        source_tables=["customers", "orders"],
        source_catalog={"customers": ["id", "email"], "orders": ["id", "sku"]},
        focus_table="customers",
    )
    try:
        _recipe_for_preview(mixed, ["id", "email"])
        refused = False
    except HTTPException as exc:
        refused = True
        assert exc.status_code == 400
        assert "no source table" in str(exc.detail)
    assert refused


def test_destination_procedure_is_not_applied_to_an_unnamed_stream() -> None:
    destination = {
        "type": "postgresql",
        "extra": {
            "dest_write_mode": "procedure",
            "dest_procedure_call": "CALL public.land(:id)",
        },
    }
    contracts = [
        {"name": "customers", "selected": True},
        {"name": "orders", "selected": True},
    ]
    refusal = review_stream_procedures(
        {"extra": {}},
        destination,
        contracts,
        ["customers", "orders"],
        sync_mode="full_refresh_append",
    )
    assert "land" in refusal
    assert "orders" in refusal or "customers" in refusal


def test_select_extract_is_a_query_and_insert_is_a_dest_write() -> None:
    source = {"type": "postgresql", "extra": {}}
    destination = {"type": "postgresql", "extra": {}}
    contracts = [
        {
            "name": "customers",
            "source_read_mode": "procedure",
            "procedure_call": "SELECT id, email FROM customers WHERE active",
            "dest_write_mode": "procedure",
            "dest_procedure_call": "INSERT INTO customers (id, email) VALUES (:id, :email)",
        },
        {
            "name": "orders",
            "procedure_call": "SELECT * FROM public.get_orders()",
        },
    ]
    refusal = review_stream_procedures(
        source, destination, contracts, ["customers", "orders"], sync_mode="full_refresh_append"
    )
    assert refusal == ""
    source_patch, dest_patch = patches_for_stream(contracts[0])
    assert source_patch is not None
    assert source_patch["source_read_mode"] == "query"
    assert source_patch["source_query"].startswith("SELECT id, email")
    assert source_patch["procedure_call"] == ""
    assert dest_patch is not None
    assert dest_patch["dest_write_mode"] == "query"
    assert dest_patch["dest_query_sql"].startswith("INSERT INTO customers")
    assert dest_patch["dest_procedure_call"] == ""
    orders_source, orders_dest = patches_for_stream(contracts[1])
    assert orders_source is not None
    assert orders_source["source_read_mode"] == "procedure"
    assert orders_source["procedure_call"].startswith("SELECT * FROM public.get_orders")
    assert orders_dest is None


def test_a_write_on_the_source_and_a_select_on_the_dest_are_refused() -> None:
    source = {"type": "postgresql", "extra": {}}
    destination = {"type": "postgresql", "extra": {}}
    write_on_source = [
        {"name": "customers", "procedure_call": "INSERT INTO customers (id) VALUES (1)"},
        {"name": "orders"},
    ]
    refusal = review_stream_procedures(
        source, destination, write_on_source, ["customers", "orders"], sync_mode="full_refresh_append"
    )
    assert "writes rows" in refusal
    assert "customers" in refusal

    select_on_dest = [
        {"name": "customers", "dest_procedure_call": "SELECT id, email FROM customers"},
        {"name": "orders"},
    ]
    refusal = review_stream_procedures(
        source, destination, select_on_dest, ["customers", "orders"], sync_mode="full_refresh_append"
    )
    assert "SELECT is a source extract" in refusal


def test_unbound_source_parameter_is_refused_before_a_write() -> None:
    refusal = review_stream_procedures(
        {"type": "postgresql", "extra": {}},
        {"type": "postgresql", "extra": {}},
        [
            {"name": "customers", "procedure_call": "CALL public.get_customers(:since)"},
            {"name": "orders"},
        ],
        ["customers", "orders"],
        sync_mode="full_refresh_append",
    )
    assert "customers" in refusal
    assert "since" in refusal


def test_design_peek_uses_the_primary_streams_statement_only() -> None:
    contracts = [
        {"name": "customers", "source_query": "SELECT id, email FROM customers WHERE active"},
        {"name": "orders", "procedure_call": "CALL public.get_orders()"},
    ]
    patch = design_source_patch(contracts, ["customers", "orders"])
    assert patch is not None
    assert patch["source_read_mode"] == "query"
    assert patch["source_query"].startswith("SELECT id, email")
    assert "get_orders" not in patch["source_query"]
    assert design_source_patch(contracts, ["customers"]) is None
    assert design_source_patch(
        [{"name": "customers"}, {"name": "orders"}],
        ["customers", "orders"],
    ) is None


def test_inherited_map_is_kept_only_when_the_column_set_matches() -> None:
    customers = [
        {"source": "id", "target": "id"},
        {"source": "email", "target": "customer_email"},
    ]
    same, note = adopt_inherited_mappings(customers, ["email", "id"])
    assert note == ""
    assert same[1]["target"] == "customer_email"
    identity, note = adopt_inherited_mappings(customers, ["id", "customer_id", "amount"])
    assert "amount" in note
    assert "omits" in note
    assert {row["source"] for row in identity} == {"id", "customer_id", "amount"}
    assert all(row["source"] == row["target"] for row in identity)


def test_session_hooks_are_stripped_from_the_per_stream_patch() -> None:
    before, after = endpoint_session_hooks({
        "extra": {
            "dest_procedure_before": "CALL public.prep()",
            "dest_procedure_after": " CALL public.finish() ",
            "dest_procedure_call": "CALL public.land(:id)",
        }
    })
    assert before == "CALL public.prep()"
    assert after == "CALL public.finish()"
    stripped = strip_session_hooks({
        "dest_write_mode": "procedure",
        "dest_procedure_call": "CALL public.land(:id)",
    })
    assert stripped["dest_procedure_call"] == "CALL public.land(:id)"
    assert stripped["dest_procedure_before"] == ""
    assert stripped["dest_procedure_after"] == ""


def test_contract_lookup_folds_the_stream_name() -> None:
    found = contract_for_stream(
        [{"name": "Customers", "procedure_call": "CALL public.get_customers()"}],
        "customers",
    )
    assert found["procedure_call"] == "CALL public.get_customers()"
    assert contract_for_stream([{"name": "orders"}], "customers") == {}
