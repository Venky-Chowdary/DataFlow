"""A recurring run must notice that its source changed shape.

Schema drift is the largest single cause of pipeline incidents — 38% across a
published review of 50 postmortems — and the damaging half does not crash. A
column changes type, the load completes, and wrong values land: one postmortem
describes revenue silently doubling because a currency field moved from ISO
codes to a numeric enum that downstream logic multiplied by.

The same review found most affected teams already had a schema registry; it
simply was not enforced where the data moves. A schedule here remembered its
cursor but not the shape it read, so the drift rules this codebase already
implements had nothing to compare against and never fired on a recurring run.

These tests are about which changes stop a run and which must not. Blocking on
an unrelated new column is the false alarm that gets the check disabled, and a
disabled check is how the permissive pipelines in those postmortems got that way.
"""

from __future__ import annotations

import pytest

from services.source_schema_memory import (
    BLOCK,
    CLEAR,
    REVIEW,
    evaluate_source_drift,
    fingerprint_source,
    mapped_source_columns,
)

BASE = {
    "id": "BIGINT",
    "currency": "VARCHAR(3)",
    "amount": "DECIMAL(12,2)",
    "created_at": "TIMESTAMP",
}
MAPPINGS = [{"source": c, "target": c} for c in BASE]


def _verdict(current, mappings=None, policy="manual_review", previous=BASE):
    return evaluate_source_drift(
        previous_schema=previous,
        current_schema=current,
        mappings=mappings if mappings is not None else MAPPINGS,
        schema_policy=policy,
        dest_db="postgresql",
    )


def test_first_run_establishes_a_baseline_rather_than_failing():
    verdict = evaluate_source_drift(
        previous_schema=None, current_schema=BASE, mappings=MAPPINGS
    )
    assert verdict.verdict == CLEAR
    assert verdict.fingerprint


def test_unchanged_source_is_clear():
    assert _verdict(dict(BASE)).verdict == CLEAR


def test_type_change_on_a_read_column_blocks():
    """The revenue-doubling shape: same name, different type, still 'valid'."""
    verdict = _verdict({**BASE, "currency": "INTEGER"})
    assert verdict.verdict == BLOCK
    assert "currency" in verdict.summary
    # The operator has to be told what changed, not just that something did.
    assert "VARCHAR(3)" in verdict.summary and "INTEGER" in verdict.summary


def test_dropped_read_column_blocks():
    current = {k: v for k, v in BASE.items() if k != "currency"}
    verdict = _verdict(current)
    assert verdict.verdict == BLOCK
    assert "currency" in verdict.summary


def test_renamed_read_column_blocks_and_names_both_sides():
    current = {"id": "BIGINT", "currency_code": "VARCHAR(3)", "amount": "DECIMAL(12,2)",
               "created_at": "TIMESTAMP"}
    verdict = _verdict(current)
    assert verdict.verdict == BLOCK
    assert "currency" in verdict.summary and "currency_code" in verdict.summary


def test_narrowed_decimal_blocks():
    verdict = _verdict({**BASE, "amount": "DECIMAL(6,2)"})
    assert verdict.verdict == BLOCK
    assert "amount" in verdict.summary


def test_new_unmapped_column_does_not_block():
    """Failing a nightly load over a field nobody reads is the false alarm."""
    verdict = _verdict({**BASE, "region": "VARCHAR(20)"})
    assert verdict.verdict == REVIEW
    assert verdict.breaking == []
    assert verdict.additive


def test_pause_on_change_blocks_even_on_additive():
    verdict = _verdict({**BASE, "region": "VARCHAR(20)"}, policy="pause_on_change")
    assert verdict.verdict == BLOCK


def test_change_to_an_unread_column_does_not_block():
    """Only columns this transfer actually reads can break it."""
    previous = {**BASE, "internal_note": "VARCHAR(10)"}
    current = {**BASE, "internal_note": "INTEGER"}
    verdict = evaluate_source_drift(
        previous_schema=previous,
        current_schema=current,
        mappings=MAPPINGS,  # internal_note is not mapped
        dest_db="postgresql",
    )
    assert verdict.verdict != BLOCK


def test_an_omitted_column_counts_as_unread():
    previous = {**BASE, "legacy": "VARCHAR(10)"}
    current = {**BASE, "legacy": "INTEGER"}
    mappings = MAPPINGS + [
        {"source": "legacy", "target": "", "intentional_omit": True}
    ]
    assert evaluate_source_drift(
        previous_schema=previous,
        current_schema=current,
        mappings=mappings,
        dest_db="postgresql",
    ).verdict != BLOCK


def test_no_mappings_means_every_column_is_read():
    """An unmapped transfer carries the whole source, so any change counts."""
    verdict = evaluate_source_drift(
        previous_schema=BASE,
        current_schema={**BASE, "currency": "INTEGER"},
        mappings=[],
        dest_db="postgresql",
    )
    assert verdict.verdict == BLOCK


def test_fingerprint_moves_only_when_the_shape_moves():
    same = fingerprint_source(list(BASE), BASE)
    assert fingerprint_source(list(BASE), dict(BASE)) == same
    assert fingerprint_source(list(BASE), {**BASE, "currency": "INTEGER"}) != same


def test_mapped_columns_are_lower_cased_and_omissions_dropped():
    assert mapped_source_columns(
        [
            {"source": "Amount", "target": "amount"},
            {"source": "Legacy", "target": "", "intentional_omit": True},
            {"source": "", "target": "x"},
        ]
    ) == {"amount"}


@pytest.mark.parametrize("policy", ["manual_review", "propagate_columns", "type_locked"])
def test_breaking_change_blocks_under_every_policy(policy: str):
    """A type change on a read column is never something to auto-apply."""
    assert _verdict({**BASE, "currency": "INTEGER"}, policy=policy).verdict == BLOCK


def test_integer_alias_is_not_a_type_change():
    """INTEGER vs INT is the same logical type — string equality would false-alarm."""
    previous = {**BASE, "id": "INTEGER"}
    verdict = evaluate_source_drift(
        previous_schema=previous,
        current_schema={**previous, "id": "INT"},
        mappings=MAPPINGS,
        dest_db="postgresql",
    )
    assert verdict.verdict == CLEAR
    assert verdict.compatibility == "identical"


def test_mapped_drop_under_propagate_is_review_not_block():
    """Fivetran net-additive: dest keeps history; unattended propagate may continue."""
    current = {k: v for k, v in BASE.items() if k != "currency"}
    verdict = _verdict(current, policy="propagate_columns")
    assert verdict.verdict == REVIEW
    assert verdict.compatibility in {"backward", "full"}


def test_primary_key_change_blocks_even_without_a_column_key():
    """PK identity is the stream. The classified row has old/new keys, not column=."""
    previous = {
        "columns": {"id": "INTEGER", "sku": "VARCHAR", "email": "VARCHAR"},
        "primary_key": ["id"],
    }
    current = {
        "columns": {"id": "INTEGER", "sku": "VARCHAR", "email": "VARCHAR"},
        "primary_key": ["sku"],
    }
    mappings = [{"source": c, "target": c} for c in ("id", "sku", "email")]
    verdict = evaluate_source_drift(
        previous_schema=previous,
        current_schema=current,
        mappings=mappings,
        dest_db="postgresql",
    )
    assert verdict.verdict == BLOCK
    assert verdict.compatibility == "none"
    assert "primary key" in verdict.summary.lower()
