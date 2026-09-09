"""G19 must still see the doomed live carrier after overwrite type hygiene.

Execute clears ``destination_column_types`` so G3/G6 do not judge a table the
run is about to drop. G19 is the one gate that must keep those types — via
``destination_live_column_types``. Validate now stamps the same kwarg.

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


def test_g19_blocks_when_execute_cleared_dest_types_but_kept_live() -> None:
    pf = run_file_preflight(
        **_BASE,
        destination_column_types={},
        destination_live_column_types={"amt_dec": "INTEGER"},
    )
    gate = _by_id(pf)[GATE]
    assert gate["status"] == "block", gate
    assert "amt_dec" in gate["message"]
    assert pf["passed"] is False


def test_g19_blocks_when_validate_passes_live_types_as_destination_column_types() -> None:
    pf = run_file_preflight(
        **_BASE,
        destination_column_types={"amt_dec": "INTEGER"},
    )
    gate = _by_id(pf)[GATE]
    assert gate["status"] == "block", gate
    assert pf["passed"] is False


def test_g19_prefers_live_types_over_planned_recreate_ddl() -> None:
    """Planned NUMERIC(20,9) would pass G19; the standing INTEGER must win."""
    pf = run_file_preflight(
        **_BASE,
        destination_column_types={"amt_dec": "NUMERIC(20,9)"},
        destination_live_column_types={"amt_dec": "INTEGER"},
    )
    gate = _by_id(pf)[GATE]
    assert gate["status"] == "block", gate
    assert gate["details"]["replacements"][0]["declared_destination_type"].upper().startswith(
        "INT"
    )


def test_g19_skips_on_append_so_g3_owns_the_live_narrowing() -> None:
    pf = run_file_preflight(
        **{**_BASE, "sync_mode": "full_refresh_append"},
        destination_column_types={"amt_dec": "INTEGER"},
    )
    assert _by_id(pf)[GATE]["status"] == "skip"


def test_signed_continue_contract_demotes_g19_to_warn() -> None:
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
    kwargs = {
        **_BASE,
        "mappings": [
            {
                "source": "amt_dec",
                "target": "amt_dec",
                "confidence": 0.99,
                "risk_contract": contract,
            }
        ],
        "destination_column_types": {},
        "destination_live_column_types": {"amt_dec": "INTEGER"},
    }
    pf = run_file_preflight(**kwargs)
    gate = _by_id(pf)[GATE]
    assert gate["status"] == "warn", gate
    assert gate["details"].get("blocks_execute") is False
