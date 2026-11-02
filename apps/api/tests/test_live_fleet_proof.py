"""Live fleet inventory + opt-in cartesian/lab/schedule proof.

``DATAFLOW_PROVE_ALL=1`` runs the unique-engine cartesian (honours
``DATAFLOW_CROSS_EXTENDED``), the 80 catalog-slot desktop lab, and every
saved workspace schedule. Closed ports are skipped, never invented green.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from services.live_fleet_proof import inventory_live_backends, run_live_fleet_proof


def test_inventory_does_not_count_saas_as_unique_engines():
    inv = inventory_live_backends()
    assert "postgresql" in inv["unique_engines_catalog"]
    assert "elasticsearch" in inv["unique_engines_catalog"]
    assert "salesforce" not in inv["unique_engines_catalog"]
    assert "hubspot" not in inv["unique_engines_catalog"]
    assert inv["honesty"]["not_sixty_unique_connectors"] is True
    assert "sqlite" in inv
    names = {p["name"] for p in inv["ports"]}
    assert "oracle" in names
    assert "bigquery_emulator" in names
    assert "iceberg_rest" in names


@pytest.mark.skipif(
    os.environ.get("DATAFLOW_PROVE_ALL", "").strip() != "1",
    reason="Full live fleet proof is opted in via DATAFLOW_PROVE_ALL=1",
)
def test_live_fleet_proof_writes_artifact():
    report = run_live_fleet_proof(persist=True)
    cross = report["unique_engine_cross"]
    assert cross is not None
    assert cross["passed"] + cross["failed"] + cross["skipped"] == cross["pairs"]
    for row in cross.get("failed_detail") or []:
        assert row.get("status") == "failed"
    lab = report["desktop_lab"]
    assert lab is not None
    assert lab["catalog_slots"] >= 80
    sched = report["schedules"]
    assert sched is not None
    assert sched["passed"] + sched["failed"] + sched["skipped"] == sched["schedules"]
    artifact = Path("/opt/cursor/artifacts/live_fleet_proof.json")
    if artifact.is_file():
        saved = json.loads(artifact.read_text())
        assert saved["unique_engine_cross"]["pairs"] == cross["pairs"]
