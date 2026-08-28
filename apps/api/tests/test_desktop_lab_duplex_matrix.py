"""Desktop lab: ≥80 catalog connectors exercised as source and dest.

``100%`` means every catalog slot in ``DESKTOP_LAB_CONNECTORS`` dest-wrote
and source-read the 2-row fixture. It does not mean 80 unique engines or
650+ live tiles.
"""

from __future__ import annotations

import json
from pathlib import Path

from services.desktop_lab import (
    DESKTOP_LAB_CONNECTORS,
    DESKTOP_LAB_MIN_DUPLEX,
    FIXTURE_ROWS,
    desktop_lab_catalog_ids,
    run_desktop_lab,
)


def test_desktop_lab_lists_at_least_80_unique_catalog_ids():
    ids = desktop_lab_catalog_ids()
    assert len(ids) >= DESKTOP_LAB_MIN_DUPLEX, len(ids)
    assert len(ids) == len(set(ids)), "duplicate catalog ids"
    unique = [row for row in DESKTOP_LAB_CONNECTORS if row["role"] == "unique_engine"]
    assert len(unique) >= 15, "lab must include a real unique-engine core"
    assert all("prove" not in row for row in DESKTOP_LAB_CONNECTORS)


def test_desktop_lab_duplex_matrix():
    report = run_desktop_lab(persist=True)
    assert report["catalog_slots"] >= DESKTOP_LAB_MIN_DUPLEX
    failed = report["failed_detail"]
    assert not failed, json.dumps(failed, indent=2)[:4000]
    assert report["skipped"] == 0, json.dumps(report["skipped_detail"], indent=2)[:2000]
    duplex = report["catalog_slots_duplex_passed"]
    unique = report["unique_engines_duplex_passed"]
    assert duplex == report["catalog_slots"], (
        f"desktop lab duplex-passed {duplex} of {report['catalog_slots']} "
        f"(unique engines {unique}); skipped={report['skipped']}"
    )
    assert report["one_hundred_percent"] is True
    assert report["catalog_slots_operations_passed"] == report["catalog_slots"]
    for row in report["connectors"]:
        assert row["duplex"] is True, row
        assert row["operations_ok"] is True, row
        assert row["map_status"] == "passed", row
        assert row["validate_status"] == "passed", row
        assert row["integrity_status"] == "passed", row
        assert row["silent_loss"] is False, row
        assert row["dest_rejected"] == 0, row
        assert row["dest_coerced"] == 0, row
        assert row["dest_rows"] == FIXTURE_ROWS, row
        assert row["source_rows"] == FIXTURE_ROWS, row
    artifact = Path("/opt/cursor/artifacts/desktop_lab_duplex.json")
    if artifact.is_file():
        saved = json.loads(artifact.read_text())
        assert saved["catalog_slots_duplex_passed"] == duplex
        assert saved["one_hundred_percent"] is True
