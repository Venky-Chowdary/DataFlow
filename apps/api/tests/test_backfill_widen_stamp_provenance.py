"""Map stamp provenance: a catalog echo is not an operator ceiling.

A destination column the source has outgrown must widen under backfill, while
an operator-authored ``target_type`` stays a hard ceiling that quarantines
overflow instead of silently altering the approved DDL.
"""

from __future__ import annotations

from connectors.writer_common import (
    effective_dest_types_under_backfill,
    stamp_is_operator_ceiling,
)


def _catalog_stamp(**over):
    return {
        "source": "amount",
        "target": "amount",
        "source_type": "DECIMAL(12,2)",
        "target_type": "DECIMAL(8,2)",
        "target_type_origin": "destination_catalog",
        **over,
    }


def test_catalog_echo_is_not_an_operator_ceiling():
    assert stamp_is_operator_ceiling(_catalog_stamp()) is False


def test_operator_edit_of_a_catalog_column_is_a_ceiling():
    assert stamp_is_operator_ceiling(_catalog_stamp(user_override=True)) is True


def test_stamp_without_recorded_provenance_is_a_ceiling():
    mapping = _catalog_stamp()
    mapping.pop("target_type_origin")
    assert stamp_is_operator_ceiling(mapping) is True


def test_unstamped_mapping_is_not_a_ceiling():
    assert stamp_is_operator_ceiling({"target": "amount"}) is False


def test_backfill_resolves_the_carrier_the_write_will_widen_to():
    out = effective_dest_types_under_backfill(
        {"id": "BIGINT", "amount": "DECIMAL(8,2)"},
        [_catalog_stamp()],
        backfill=True,
    )
    assert out["amount"] == "DECIMAL(12,2)"
    assert out["id"] == "BIGINT"


def test_operator_ceiling_survives_backfill():
    out = effective_dest_types_under_backfill(
        {"amount": "DECIMAL(8,2)"},
        [_catalog_stamp(user_override=True)],
        backfill=True,
    )
    assert out["amount"] == "DECIMAL(8,2)"


def test_without_backfill_the_live_carrier_stays_authoritative():
    out = effective_dest_types_under_backfill(
        {"amount": "DECIMAL(8,2)"},
        [_catalog_stamp()],
        backfill=False,
    )
    assert out["amount"] == "DECIMAL(8,2)"


def test_narrower_source_never_narrows_the_destination():
    out = effective_dest_types_under_backfill(
        {"amount": "DECIMAL(18,2)"},
        [_catalog_stamp(source_type="DECIMAL(8,2)")],
        backfill=True,
    )
    assert out["amount"] == "DECIMAL(18,2)"


def test_unmapped_destination_columns_are_untouched():
    out = effective_dest_types_under_backfill(
        {"other": "VARCHAR(10)"},
        [_catalog_stamp()],
        backfill=True,
    )
    assert out == {"other": "VARCHAR(10)"}
