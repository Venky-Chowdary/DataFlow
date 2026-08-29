"""Live type × sync × schema-shape matrix — named fixture only, not every type."""

from __future__ import annotations

import json
from pathlib import Path

from tests.desktop_lab_dimensions import run_desktop_lab_dimensions
from tests.typed_fidelity_helpers import require_ports


def test_desktop_lab_type_sync_schema_dimensions():
    require_ports(5432, 3306)
    report = run_desktop_lab_dimensions(persist=True)
    assert report["cells"] > 0
    assert report["passed"] + report["failed"] + report["skipped"] == report["cells"]
    assert report["honesty"]["not_every_sql_type"] is True
    assert "cdc" in report["honesty"]["sync_modes_not_claimed"]
    artifact = Path("/opt/cursor/artifacts/desktop_lab_dimensions.json")
    if artifact.is_file():
        saved = json.loads(artifact.read_text())
        assert saved["cells"] == report["cells"]
    typed = [c for c in report["results"] if c["kind"] == "typed_create_new"]
    assert typed, "typed cells missing"
    typed_failed = [c for c in typed if c["status"] != "passed"]
    assert not typed_failed, typed_failed

    sync_failed = [
        c for c in report["results"]
        if c["kind"] == "sync_two_run" and c["status"] == "failed"
    ]
    assert not sync_failed, sync_failed

    compatible = [
        c for c in report["results"]
        if c.get("name") == "dest_exists_compatible"
    ]
    assert compatible and all(c["status"] == "passed" for c in compatible), compatible

    # dest-exists DECIMAL→INT and extra source column are measured open
    # gaps on this fixture — do not invent a pass. The artifact records them.
