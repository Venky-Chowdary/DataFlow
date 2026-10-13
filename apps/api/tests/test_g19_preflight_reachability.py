"""G19 names a carrier only when overwrite really DROP+CREATEs the table.

Relational overwrite empties the table and keeps its columns, so a narrower
INTEGER is a live write (G3/G6), not a silent replacement (G19). Warehouses
that still replace the object keep the G19 block.

Does not claim Track A 100K, CRM overwrite, or a signed-contract Execute.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.preflight_service import run_file_preflight  # noqa: E402

GATE = "g19_dest_schema_replacement"

_BASE = dict(
    columns=["amt_dec"],
    column_types={"amt_dec": "DECIMAL(20,9)"},
    row_count=3,
    mappings=[{"source": "amt_dec", "target": "amt_dec", "confidence": 0.99}],
    destination_connected=True,
    destination_can_create=True,
    source_connected=True,
    source_kind="database",
    source_format="postgresql",
    sync_mode="full_refresh_overwrite",
    sample_rows=[{"amt_dec": "123.456789000"}],
    destination_table_exists=True,
    destination_db_type="postgresql",
    destination_table="ledger",
    validation_mode="strict",
)


def _by_id(pf: dict) -> dict:
    return {g["id"]: g for g in pf["gates"]}


def _blocks(pf: dict, gate_id: str) -> dict:
    gate = _by_id(pf)[gate_id]
    assert gate["status"] == "block", gate
    return gate


def test_g19_skips_when_postgres_overwrite_keeps_the_integer() -> None:
    """Live INTEGER stays. G19 does not pretend the column will be replaced."""
    pf = run_file_preflight(
        **_BASE,
        destination_column_types={},
        destination_live_column_types={"amt_dec": "INTEGER"},
    )
    gate = _by_id(pf)[GATE]
    assert gate["status"] == "skip", gate
    assert "as declared" in gate["message"]
    assert pf["passed"] is False
    _blocks(pf, "g3_schema_contract")
    assert "amt_dec" in _by_id(pf)["g6_target_ddl"]["message"]


def test_g19_skips_when_validate_passes_the_live_integer() -> None:
    pf = run_file_preflight(
        **_BASE,
        destination_column_types={"amt_dec": "INTEGER"},
    )
    assert _by_id(pf)[GATE]["status"] == "skip"
    assert pf["passed"] is False
    _blocks(pf, "g6_target_ddl")


def test_live_integer_wins_over_a_planned_wider_type() -> None:
    """Planned NUMERIC(20,9) would fit. The standing INTEGER is the contract."""
    pf = run_file_preflight(
        **_BASE,
        destination_column_types={"amt_dec": "NUMERIC(20,9)"},
        destination_live_column_types={"amt_dec": "INTEGER"},
    )
    assert _by_id(pf)[GATE]["status"] == "skip"
    g6 = _blocks(pf, "g6_target_ddl")
    assert "INTEGER" in g6["message"].upper()


def test_g19_skips_on_append_so_g3_owns_the_live_narrowing() -> None:
    pf = run_file_preflight(
        **{**_BASE, "sync_mode": "full_refresh_append"},
        destination_column_types={"amt_dec": "INTEGER"},
    )
    assert _by_id(pf)[GATE]["status"] == "skip"


def test_snowflake_overwrite_still_blocks_when_it_replaces_a_narrow_column() -> None:
    pf = run_file_preflight(
        **{
            **_BASE,
            "columns": ["name"],
            "column_types": {"name": "VARCHAR(200)"},
            "mappings": [{"source": "name", "target": "name", "confidence": 0.99}],
            "sample_rows": [{"name": "x" * 40}],
            "destination_db_type": "snowflake",
        },
        destination_column_types={"name": "VARCHAR(10)"},
    )
    gate = _by_id(pf)[GATE]
    assert gate["status"] == "block", gate
    assert "name" in gate["message"]


def test_signed_continue_contract_demotes_g19_to_warn() -> None:
    """A signed contract demotes G19 only when the run really replaces the column."""
    from services.migration_risk_contract import create_migration_risk_contract

    contract = create_migration_risk_contract(
        column="name",
        source_type="VARCHAR(200)",
        destination_type="VARCHAR(200)",
        approved_by="cfo@bank.example",
        reason="The warehouse recreate widens the standing VARCHAR(10).",
        execution_policy="CAST_AND_CONTINUE",
        table="ledger",
    ).to_dict()
    pf = run_file_preflight(
        **{
            **_BASE,
            "columns": ["name"],
            "column_types": {"name": "VARCHAR(200)"},
            "sample_rows": [{"name": "x" * 40}],
            "destination_db_type": "snowflake",
            "mappings": [
                {
                    "source": "name",
                    "target": "name",
                    "confidence": 0.99,
                    "risk_contract": contract,
                }
            ],
        },
        destination_column_types={"name": "VARCHAR(10)"},
    )
    gate = _by_id(pf)[GATE]
    assert gate["status"] == "warn", gate
    assert gate["details"].get("blocks_execute") is False
