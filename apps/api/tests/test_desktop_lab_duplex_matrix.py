"""Desktop lab: ≥45 catalog connectors exercised as source and dest.

``100%`` means every catalog slot in ``DESKTOP_LAB_CONNECTORS`` was attempted
and every *available* backend passed both roles. It does not mean 45 unique
engines or 650+ live tiles.
"""

from __future__ import annotations

import json
from pathlib import Path

from services.desktop_lab import (
    DESKTOP_LAB_CONNECTORS,
    desktop_lab_catalog_ids,
    run_desktop_lab,
)


def test_desktop_lab_lists_at_least_45_unique_catalog_ids():
    ids = desktop_lab_catalog_ids()
    assert len(ids) >= 45, len(ids)
    assert len(ids) == len(set(ids)), "duplicate catalog ids"
    unique = [row for row in DESKTOP_LAB_CONNECTORS if row["role"] == "unique_engine"]
    assert len(unique) >= 15, "lab must include a real unique-engine core"


def test_desktop_lab_duplex_matrix():
    report = run_desktop_lab(persist=True)
    assert report["catalog_slots"] >= 45
    # Every slot that had a backend must pass both roles. Skips stay honest.
    failed = report["failed_detail"]
    assert not failed, json.dumps(failed, indent=2)[:4000]
    duplex = report["catalog_slots_duplex_passed"]
    unique = report["unique_engines_duplex_passed"]
    # The operator option is real only if a majority of slots duplex-pass.
    assert duplex >= 45, (
        f"desktop lab duplex-passed {duplex} of {report['catalog_slots']} "
        f"(unique engines {unique}); skipped={report['skipped']}"
    )
    artifact = Path("/opt/cursor/artifacts/desktop_lab_duplex.json")
    if artifact.is_file():
        saved = json.loads(artifact.read_text())
        assert saved["catalog_slots_duplex_passed"] == duplex
