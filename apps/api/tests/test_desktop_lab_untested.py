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
    portable = by_name.get("portable_json_uuid_blob mysql->postgresql")
    assert portable and portable["status"] == "passed", portable
    invent = by_name.get("create_new_array_invent postgresql->postgresql")
    assert invent and invent["status"] == "passed", invent
    for name in (
        "incremental_deduped postgresql->postgresql",
        "mirror postgresql->postgresql",
        "reverse_etl postgresql->mysql",
        "mysql_binlog->sqlite",
        "postgresql_logical->postgresql",
        "postgresql->sqlite",
        "postgresql->sqlserver",
    ):
        cell = by_name.get(name)
        assert cell and cell["status"] == "passed", (name, cell)
    cdc = [c for c in report["results"] if c["kind"] == "cdc" and c["status"] == "passed"]
    assert cdc, "at least one live CDC cell must pass when binlog/logical is up"

    # At least one previously untested sync mode must have executed (pass or fail).
    sync = [c for c in report["results"] if c["kind"] in {"sync_extended", "cdc"}]
    assert sync, "extended sync / CDC cells missing"
    assert any(c["status"] != "skipped" for c in sync), sync

    artifact = Path("/opt/cursor/artifacts/desktop_lab_untested.json")
    if artifact.is_file():
        saved = json.loads(artifact.read_text())
        assert saved["cells"] == report["cells"]
