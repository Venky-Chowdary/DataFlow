"""D-05/06/09/11: point-of-failure errors name the fix."""

from __future__ import annotations

import pytest
from pydantic import ValidationError


def test_d05_egress_refusal_names_the_allowlist():
    from services.egress_guard import metadata_endpoint_refusal

    refusal = metadata_endpoint_refusal("127.0.0.1", allowlist=())
    assert refusal
    # Round 11 (B1): it names the setting a user can change, never a server variable.
    assert "workspace egress allowlist" in refusal
    assert "DATAFLOW_" not in refusal and "DATAWRAP_" not in refusal
    assert "127.0.0.0/8" in refusal


def test_d06_unknown_schema_policy_names_valid_values():
    from services.preflight_service import run_transfer_policy_gates

    gates = run_transfer_policy_gates(schema_policy="create_new")
    blocked = [g for g in gates if g.get("status") == "block"]
    assert blocked
    issues = " ".join(
        str(i) for g in blocked for i in g.get("details", {}).get("issues", [])
    )
    assert "create_new" in issues
    assert "manual_review" in issues
    assert "propagate_columns" in issues


def test_d09_export_error_names_exports_root():
    from services.file_export_append import land_export_bytes

    class _Dest:
        output_path = "/tmp/outside.csv"
        format = "csv"

    with pytest.raises(ValueError, match="exports folder") as err:
        land_export_bytes(
            _Dest(),
            b"a,b\n",
            job_id="j",
            sync_mode="full_refresh_overwrite",
            export_name="x.csv",
        )
    # Names the allowed root and an example, not just the rule.
    assert "inside the server exports folder (exports/)" in str(err.value)
    assert "orders.csv" in str(err.value)


def test_d11_invalid_interval_names_cron_option():
    from src.routers.schedules_router import ScheduleCreate

    with pytest.raises(ValidationError) as err:
        ScheduleCreate(
            name="s",
            source_connector_id="a",
            source_table="t",
            dest_connector_id="b",
            dest_table="u",
            interval="*/3 * * * *",
        )
    text = str(err.value)
    assert "cron" in text.lower()
    assert "hourly" in text


def test_d11_cron_interval_accepted():
    from src.routers.schedules_router import ScheduleCreate

    sched = ScheduleCreate(
        name="s",
        source_connector_id="a",
        source_table="t",
        dest_connector_id="b",
        dest_table="u",
        interval="cron",
        cron="*/30 * * * *",
    )
    assert sched.interval == "cron"
