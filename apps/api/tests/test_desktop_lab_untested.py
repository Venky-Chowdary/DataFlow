"""Live desktop coverage for previously untested types / sync modes / engines."""

from __future__ import annotations

import json
from pathlib import Path

from tests.desktop_lab_untested import run_desktop_lab_untested
from tests.typed_fidelity_helpers import require_ports


def test_desktop_lab_untested_important_dimensions():
    require_ports(5432, 3306)
    report = run_desktop_lab_untested(persist=True)
    assert report["cells"] > 0
    assert report["passed"] + report["failed"] + report["skipped"] == report["cells"]
    assert report["honesty"]["not_every_sql_type"] is True
    assert "cdc" in report["honesty"]["sync_modes_measured"]
    assert "salesforce" in report["honesty"]["saas_omitted"]

    by_name = {c["name"]: c for c in report["results"]}
    pg_types = by_name.get("postgresql->postgresql")
    assert pg_types and pg_types["kind"] == "types_extended"
    assert pg_types["status"] == "passed", pg_types

    # At least one previously untested sync mode must have executed (pass or fail).
    sync = [c for c in report["results"] if c["kind"] in {"sync_extended", "cdc"}]
    assert sync, "extended sync / CDC cells missing"
    assert any(c["status"] != "skipped" for c in sync), sync

    artifact = Path("/opt/cursor/artifacts/desktop_lab_untested.json")
    if artifact.is_file():
        saved = json.loads(artifact.read_text())
        assert saved["cells"] == report["cells"]
