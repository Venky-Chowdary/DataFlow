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
    assert report["honesty"]["saas_local_stub_not_customer_org"] is True
    assert "salesforce" in report["honesty"]["saas_measured_local_stub"]
    assert report["honesty"]["catalog_tiles_are_not_transfer_live"] is True
    assert report["honesty"]["customer_tenant_warehouse_claimed"] is False
    assert report["honesty"]["cdc_exactly_once_claimed"] is False

    by_name = {c["name"]: c for c in report["results"]}
    portable = by_name.get("portable_json_uuid_blob mysql->postgresql")
    assert portable and portable["status"] == "passed", portable
    native = by_name.get("create_new_array_native postgresql->postgresql")
    assert native and native["status"] == "passed", native
    intarray = by_name.get("dest_exists_intarray postgresql->postgresql")
    assert intarray and intarray["status"] == "passed", intarray
    for name in (
        "dest_exists_native postgresql->postgresql",
        "incremental_deduped postgresql->postgresql",
        "mirror postgresql->postgresql",
        "scd2 postgresql->sqlite",
        "reverse_etl postgresql->mysql",
        "mysql_binlog->sqlite",
        "mysql_binlog_replay_at_least_once",
        "postgresql_logical->postgresql",
        "dest_only_not_null postgresql->postgresql",
        "postgresql->sqlite",
        "postgresql->sqlserver",
        "postgresql->oracle",
        "xml dest_exists postgresql->postgresql",
        "point dest_exists postgresql->postgresql",
        "nested_explode csv->postgresql",
        "geography dest_exists postgresql->postgresql",
        "reverse_etl_stub postgresql->salesforce",
        "reverse_etl_stub postgresql->hubspot",
        "reverse_etl_stub postgresql->stripe",
        "postgresql->gcs",
        "postgresql->adls",
        "production_sku_validate_honesty",
    ):
        cell = by_name.get(name)
        assert cell and cell["status"] == "passed", (name, cell)
    cdc = [c for c in report["results"] if c["kind"] == "cdc" and c["status"] == "passed"]
    assert cdc, "at least one live CDC cell must pass when binlog/logical is up"
    replay = by_name.get("mysql_binlog_replay_at_least_once")
    assert replay and replay.get("exactly_once_claimed") is False, replay
    bq = by_name.get("postgresql->bigquery")
    assert bq and bq["status"] in {"passed", "skipped"}, bq
    if bq["status"] == "skipped":
        assert bq.get("emulator_not_customer_tenant") is True, bq

    # At least one previously untested sync mode must have executed (pass or fail).
    sync = [c for c in report["results"] if c["kind"] in {"sync_extended", "cdc"}]
    assert sync, "extended sync / CDC cells missing"
    assert any(c["status"] != "skipped" for c in sync), sync

    artifact = Path("/opt/cursor/artifacts/desktop_lab_untested.json")
    if artifact.is_file():
        saved = json.loads(artifact.read_text())
        assert saved["cells"] == report["cells"]
