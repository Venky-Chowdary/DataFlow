"""G19 — a full refresh may not silently redefine a declared destination column."""

from __future__ import annotations

from typing import Any

from services.dest_schema_replacement import (
    GATE_ID,
    build_dest_schema_replacement_gate,
    carrier_would_truncate,
    find_silent_replacements,
)


def _gate(**overrides: Any) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "mappings": [{"source": "amt_dec", "target": "amt_dec", "confidence": 0.99}],
        "source_column_types": {"amt_dec": "DECIMAL(20,9)"},
        "destination_column_types": {"amt_dec": "INTEGER"},
        "destination_table_exists": True,
        "dest_recreated": True,
        "destination_db_type": "postgresql",
    }
    kwargs.update(overrides)
    return build_dest_schema_replacement_gate(**kwargs)


def test_overwrite_onto_narrower_existing_column_blocks() -> None:
    gate = _gate()
    assert gate["id"] == GATE_ID
    assert gate["status"] == "block"
    assert "amt_dec" in gate["message"]
    replaced = gate["details"]["replacements"][0]
    assert replaced["declared_destination_type"].upper().startswith("INT")
    assert replaced["source_type"] == "DECIMAL(20,9)"


def test_widening_recreate_passes() -> None:
    gate = _gate(destination_column_types={"amt_dec": "NUMERIC(38,12)"})
    assert gate["status"] == "pass"
    assert gate["details"]["replacements"] == []


def test_identical_recreate_on_a_second_tick_passes() -> None:
    gate = _gate(destination_column_types={"amt_dec": "NUMERIC(20,9)"})
    assert gate["status"] == "pass"


def test_non_overwrite_mode_is_skipped() -> None:
    # Append keeps the live column, and G3 already refuses the narrowing there.
    gate = _gate(dest_recreated=False)
    assert gate["status"] == "skip"


def test_create_new_destination_is_skipped() -> None:
    gate = _gate(destination_table_exists=False)
    assert gate["status"] == "skip"


def test_unknown_destination_existence_is_skipped() -> None:
    gate = _gate(destination_table_exists=None)
    assert gate["status"] == "skip"


def test_schemaless_destination_is_skipped() -> None:
    gate = _gate(destination_db_type="mongodb")
    assert gate["status"] == "skip"


def test_case_folded_destination_column_is_matched() -> None:
    gate = _gate(destination_column_types={"AMT_DEC": "INTEGER"})
    assert gate["status"] == "block"


def test_omitted_column_is_not_a_replacement() -> None:
    gate = _gate(
        mappings=[
            {"source": "amt_dec", "target": "", "intentional_omit": True},
        ]
    )
    assert gate["status"] == "pass"


def test_signed_risk_contract_demotes_the_block_to_a_warning() -> None:
    from services.migration_risk_contract import create_migration_risk_contract

    contract = create_migration_risk_contract(
        column="amt_dec",
        source_type="DECIMAL(20,9)",
        destination_type="NUMERIC(20,9)",
        approved_by="cfo@bank.example",
        reason="Integer column predates the fractional amounts; finance signed off.",
        execution_policy="CAST_AND_CONTINUE",
        table="ledger",
    ).to_dict()
    gate = _gate(
        mappings=[
            {
                "source": "amt_dec",
                "target": "amt_dec",
                "confidence": 0.99,
                "risk_contract": contract,
            }
        ]
    )
    assert gate["status"] == "warn"
    assert gate["details"]["blocks_execute"] is False
    assert gate["details"]["risk_contract_cleared"][0]["target"] == "amt_dec"


def test_string_width_narrowing_is_a_replacement() -> None:
    found = find_silent_replacements(
        mappings=[{"source": "name_txt", "target": "name_txt"}],
        source_column_types={"name_txt": "VARCHAR(64)"},
        destination_column_types={"name_txt": "VARCHAR(8)"},
        destination_db_type="postgresql",
    )
    assert [f["target"] for f in found] == ["name_txt"]


def test_fidelity_only_pairs_are_not_replacements() -> None:
    # CHAR(36) → UUID is a judgement about meaning, and on the second tick of a
    # schedule the UUID column is one Datawrap itself created. G19 stays quiet.
    assert not carrier_would_truncate("CHAR(36)", "UUID", dest_db="sqlite")
    assert not find_silent_replacements(
        mappings=[{"source": "uid", "target": "uid"}],
        source_column_types={"uid": "CHAR(36)"},
        destination_column_types={"uid": "UUID"},
        destination_db_type="sqlite",
    )


def test_crm_overwrite_does_not_recreate_schema() -> None:
    from services.db_type_utils import dest_schema_is_recreated_on_overwrite

    assert dest_schema_is_recreated_on_overwrite("salesforce") is False
    assert dest_schema_is_recreated_on_overwrite("hubspot") is False
    assert dest_schema_is_recreated_on_overwrite("postgresql") is True
    gate = _gate(
        destination_db_type="salesforce",
        destination_column_types={"amount": "DECIMAL(18,2)"},
        source_column_types={"amt_dec": "DECIMAL(6,2)"},
        dest_recreated=False,
    )
    assert gate["status"] == "skip"
    assert carrier_would_truncate("BIGINT", "INTEGER", dest_db="postgresql")


def test_zero_scale_decimal_wider_than_int32_is_a_replacement() -> None:
    assert carrier_would_truncate("DECIMAL(20,0)", "INTEGER", dest_db="postgresql")
    assert not carrier_would_truncate("DECIMAL(4,0)", "INTEGER", dest_db="postgresql")
