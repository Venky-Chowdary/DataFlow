"""A create-new stamp projected from a sample must widen when the declared
source turns out not to fit it.

Map projects a destination carrier from a sampled profile. Validate and Run
judge the *declared* source type, which can be wider than that sample. When it
is, the earlier projection is DataFlow's own guess — not an operator decision —
so it is re-projected rather than used to refuse the transfer for a fidelity
collapse the product invented. An operator-approved narrowing still stands.
"""

from __future__ import annotations

from services.decision_kernel.invent import stamp_additive_mapping_types
from services.decision_kernel.type_invent import promote_create_new_capacity_stamp
from services.type_system import resolve_mapping_target_type


def test_sampled_decimal_stamp_widens_to_the_declared_source_scale() -> None:
    promoted = promote_create_new_capacity_stamp(
        "DECIMAL(8,4)", "NUMERIC(6,4)", "postgresql"
    )
    assert promoted.upper().startswith("NUMERIC(")
    assert promoted.upper() != "NUMERIC(6,4)"


def test_a_stamp_that_still_holds_the_source_is_left_alone() -> None:
    assert (
        promote_create_new_capacity_stamp("DECIMAL(6,4)", "NUMERIC(6,4)", "postgresql")
        == "NUMERIC(6,4)"
    )


def test_bounded_string_stamp_widens_for_a_wider_declaration() -> None:
    promoted = promote_create_new_capacity_stamp(
        "VARCHAR(4000)", "VARCHAR(64)", "mysql"
    )
    assert promoted.upper() != "VARCHAR(64)"


def test_an_operator_approved_narrowing_is_not_widened_back() -> None:
    rows = [
        {
            "source": "amount",
            "target": "amount",
            "source_type": "DECIMAL(8,4)",
            "target_type": "NUMERIC(6,4)",
            "create_new": True,
            "risk_acknowledged": True,
        }
    ]
    assert (
        resolve_mapping_target_type(rows[0], dest_db_type="postgresql").upper()
        == "NUMERIC(6,4)"
    )


def test_resolve_promotes_the_stamp_for_an_unacknowledged_row() -> None:
    row = {
        "source": "amount",
        "target": "amount",
        "source_type": "DECIMAL(8,4)",
        "target_type": "NUMERIC(6,4)",
        "create_new": True,
    }
    assert resolve_mapping_target_type(row, dest_db_type="postgresql").upper() != (
        "NUMERIC(6,4)"
    )


def test_an_existing_destination_column_keeps_its_own_type() -> None:
    row = {
        "source": "amount",
        "target": "amount",
        "source_type": "DECIMAL(8,4)",
        "target_type": "NUMERIC(6,4)",
    }
    assert (
        resolve_mapping_target_type(
            row,
            dest_db_type="postgresql",
            target_types={"amount": "NUMERIC(6,4)"},
        ).upper()
        == "NUMERIC(6,4)"
    )


def test_additive_stamping_carries_the_promotion() -> None:
    rows = [
        {
            "source": "amount",
            "target": "amount",
            "target_type": "NUMERIC(6,4)",
            "create_new": True,
        }
    ]
    stamped, _unstamped = stamp_additive_mapping_types(
        rows,
        dest_db="postgresql",
        source_types={"amount": "DECIMAL(8,4)"},
        live_dest_types={},
        dest_table_exists=False,
    )
    assert stamped[0]["target_type"].upper() != "NUMERIC(6,4)"


def test_integer_to_serial_invent_is_not_rewritten_to_bigint() -> None:
    """Identity polarity is not width. SERIAL must survive so Validate can refuse."""
    for stamp in (
        "SERIAL",
        "BIGSERIAL",
        "INTEGER GENERATED ALWAYS AS IDENTITY",
    ):
        assert (
            promote_create_new_capacity_stamp("INTEGER", stamp, "postgresql") == stamp
        )


def test_additive_stamping_keeps_serial_invent() -> None:
    rows = [
        {
            "source": "id",
            "target": "id",
            "source_type": "INTEGER",
            "target_type": "SERIAL",
            "create_new": True,
        }
    ]
    stamped, _unstamped = stamp_additive_mapping_types(
        rows,
        dest_db="postgresql",
        source_types={"id": "INTEGER"},
        live_dest_types={},
        dest_table_exists=False,
    )
    kept = str(stamped[0]["target_type"] or "").upper()
    assert "SERIAL" in kept
    assert kept != "BIGINT"
