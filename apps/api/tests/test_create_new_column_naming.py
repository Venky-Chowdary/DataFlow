"""A CREATE TABLE proposal must land the operator's own column names.

Expanding a source name to its canonical semantic form (``qty`` → ``quantity``)
renamed columns nobody asked to rename: the destination the product itself
created no longer matched the source by name, and the next run of the same
unchanged route scored ``qty`` → ``quantity`` as a rename, held it for review
and had Execute refuse it — an unchanged route could never be re-run.
"""

from __future__ import annotations

import pytest

from services.mapping_pipeline import assert_mappings_executable, run_mapping_pipeline
from services.semantic_mapper import create_new_target_name

ABBREVIATED = ["qty", "cust_id", "order_amt", "txn_dt", "_id", "first_name"]


def _create_new(columns: list[str], dest: str = "mysql") -> dict[str, dict]:
    result = run_mapping_pipeline(
        columns,
        [],
        source_schemas=[
            {"name": c, "inferred_type": "VARCHAR(120)", "samples": ["a", "b"]}
            for c in columns
        ],
        use_llm=False,
        destination_db_type=dest,
        source_db_type="postgresql",
        destination_table_exists=False,
    )
    return {m["source"]: m for m in result["mappings"]}


@pytest.mark.parametrize("dest", ["mysql", "postgresql", "snowflake", "mongodb"])
def test_create_new_keeps_the_source_column_name(dest: str) -> None:
    rows = _create_new(ABBREVIATED, dest)
    for col in ABBREVIATED:
        assert rows[col]["target"] == col, f"{dest}: {col} was renamed to {rows[col]['target']}"


def test_canonical_form_stays_available_as_advisory_metadata() -> None:
    rows = _create_new(["qty"])
    assert rows["qty"]["semantic_name"] == "quantity"


def test_illegal_characters_are_repaired_by_the_identifier_owner() -> None:
    rows = _create_new(["order amt%"])
    assert rows["order amt%"]["target"] == "order_amt_"


def test_rerunning_an_unchanged_route_is_not_held_for_review() -> None:
    """Second pass maps against the table the first pass created."""
    created = _create_new(["qty", "amount"])
    dest_columns = [m["target"] for m in created.values()]
    result = run_mapping_pipeline(
        ["qty", "amount"],
        dest_columns,
        source_schemas=[
            {"name": "qty", "inferred_type": "INTEGER", "samples": ["1", "2"]},
            {"name": "amount", "inferred_type": "DECIMAL(12,2)", "samples": ["1.25"]},
        ],
        target_schemas=[
            {"name": "qty", "inferred_type": "BIGINT", "samples": []},
            {"name": "amount", "inferred_type": "DECIMAL(12,2)", "samples": []},
        ],
        use_llm=False,
        destination_db_type="mysql",
        source_db_type="postgresql",
        destination_table_exists=True,
    )
    mappings = result["mappings"]
    assert {m["source"]: m["target"] for m in mappings} == {"qty": "qty", "amount": "amount"}
    assert_mappings_executable(mappings)


def test_create_new_target_name_is_the_single_naming_owner() -> None:
    assert create_new_target_name("qty") == "qty"
    assert create_new_target_name("  Order Amt  ") == "Order_Amt"
    assert create_new_target_name("_id") == "_id"
