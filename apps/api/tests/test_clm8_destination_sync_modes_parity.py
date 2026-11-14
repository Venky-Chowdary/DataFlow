"""CLM-8: the sync-mode picker offers what Validate accepts, per destination.

``/transfer/capabilities`` publishes ``destination_sync_modes`` and the web
picker (``availableSyncModes``) intersects with it. This test runs Validate's
own policy gates (``run_transfer_policy_gates``) for every destination and
every mode and requires the published list to match: a mode Validate refuses
for a destination-specific reason is not offered, and an offered mode is not
refused. PostgreSQL with the same contract is the control, so source-side or
contract findings never count as a destination refusal.

CDC is stricter than today's Validate: it also needs a destination that can
apply a DELETE (``cdc_deletes_supported``), because Redis and the SaaS writers
pass Validate and then fail at the first source DELETE (CLM-2).
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

CONTRACTS = [
    {"name": "t", "primary_key": ["id"], "cursor_field": "id", "cursor_semantics": "monotonic_sequence"}
]


def _blocks(dest: str, mode: str) -> set[str]:
    from services.preflight_transfer_policy import run_transfer_policy_gates

    logging.disable(logging.CRITICAL)
    try:
        out = run_transfer_policy_gates(
            sync_mode=mode,
            dest_type=dest,
            source_type="postgresql",
            source_kind="database",
            stream_contracts=CONTRACTS,
            source_columns=["id", "name"],
            validation_mode="strict",
        )
    finally:
        logging.disable(logging.NOTSET)
    gates = out if isinstance(out, list) else out.get("gates", [])
    return {
        f"{g['id']}: {g['message']}"
        for g in gates
        if str(g.get("status")).lower() in {"block", "fail", "blocked"}
    }


def _destinations() -> list[str]:
    from services.destination_sync_modes import capabilities_destination_sync_modes

    return sorted(capabilities_destination_sync_modes())


def test_capabilities_publish_a_sync_mode_list_for_every_destination() -> None:
    from src.routers.transfer_router import transfer_capabilities

    caps = transfer_capabilities()
    table = caps["destination_sync_modes"]
    for dest in caps["destination_databases"]:
        from src.transfer.connector_capabilities import resolve_driver_type

        assert dest in table or resolve_driver_type(dest) in table, dest
    assert table["kafka"]["sync_modes"] == ["full_refresh_append", "incremental_append"]
    # W2 wired the CDC delete LSN read for Redis (CLM-2), so Redis offers CDC;
    # destinations with no delete path (Salesforce, Milvus) still don't.
    assert "cdc" in table["redis"]["sync_modes"]
    assert "cdc" not in table["salesforce"]["sync_modes"]
    assert "cdc" not in table["milvus"]["sync_modes"]
    assert "cdc" not in table["csv"]["sync_modes"]
    assert set(table["postgresql"]["sync_modes"]) == {
        "full_refresh_overwrite", "full_refresh_append", "incremental_append",
        "incremental_deduped", "cdc", "scd2", "mirror",
    }
    # HubSpot "overwrite" upserts; the picker says so.
    assert "updates records by key" in table["hubspot"]["notes"]["full_refresh_overwrite"]
    assert "BigQuery" in table["bigquery"]["notes"]["scd2"]


@pytest.mark.parametrize("dest", _destinations())
def test_offered_modes_match_validate(dest: str) -> None:
    from services.cdc_destination_support import cdc_deletes_supported
    from services.destination_sync_modes import SYNC_MODE_IDS, capabilities_destination_sync_modes

    offered = set(capabilities_destination_sync_modes()[dest]["sync_modes"])
    for mode in SYNC_MODE_IDS:
        control = _blocks("postgresql", mode)
        refused_by_validate = bool(_blocks(dest, mode) - control)
        if mode == "cdc" and not refused_by_validate and not cdc_deletes_supported(dest):
            # CLM-2: Validate does not yet refuse this; the picker must not offer it.
            assert mode not in offered, dest
            continue
        assert (mode in offered) == (not refused_by_validate), (dest, mode, _blocks(dest, mode) - control)
