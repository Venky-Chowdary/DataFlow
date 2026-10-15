"""Round 12 regressions.

R12-001: a refused append re-run says why and names ``force=true``.
R12-002: "every 7 minutes" is measured from the previous run. ``*/7`` leaves a
four-minute gap at the end of each hour, so that cron is not the compiler's
answer.

The still-open Round 11 items stay locked by ``test_qa_r11_defects`` and the
egress tests: replay does not replace landed rows, a Postgres NULL is not an
empty CSV cell, a plain S3 copy does not cast cells, and 172.16/12 plus
192.168/16 are refused before a dial.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from services.cron_schedule import next_run
from services.execution_engine_contract import (
    append_rerun_refusal_error,
    decide_job_rerun,
)
from services.schedule_store import (
    _validate_cadence,
    compute_next_run,
    rolling_minutes,
)
from src.ai.copilot.confirm_ack import _detail
from src.ai.copilot.schedule_cadence import describe_stored_cadence, parse_cadence


def test_refused_append_rerun_explains_why_and_names_force():
    decision = decide_job_rerun(
        status="completed",
        sync_mode="full_refresh_append",
        rows_committed=4,
    )
    assert decision["allowed"] is False
    message = append_rerun_refusal_error(decision["reason"])
    assert "4 row" in message
    assert "append" in message
    assert "force=true" in message

    class _Exc:
        detail = {
            "error": "Re-run refused",
            "reason": decision["reason"],
            "override": "Re-send with force=true to accept duplicate append rows.",
        }

    shown = _detail(_Exc())
    assert "4 row" in shown
    assert "force=true" in shown
    assert "Re-run refused" in shown


def test_every_seven_minutes_is_not_a_gapped_cron():
    spec = parse_cadence("every 7 minutes")
    assert spec.resolved
    assert spec.cron == ""
    assert spec.interval == "every_7m"
    assert "*/7" not in spec.description
    assert "previous run" in spec.description

    five = parse_cadence("every 5 minutes")
    assert five.cron == "*/5 * * * *"
    assert five.interval == "cron"

    start = datetime(2026, 1, 1, 10, 56, tzinfo=timezone.utc)
    rolled = datetime.fromisoformat(compute_next_run("every_7m", start))
    assert rolled - start == timedelta(minutes=7)
    gapped = next_run("*/7 * * * *", start, "UTC")
    assert gapped - start == timedelta(minutes=4)

    _validate_cadence("every_7m", "", "UTC", "full_refresh_append")
    assert rolling_minutes("every_7m") == 7
    assert rolling_minutes("every_60m") is None
    label = describe_stored_cadence("every_7m", "", "UTC")
    assert "7 minutes" in label
    assert "*/7" not in label
    uneven = describe_stored_cadence("cron", "*/7 * * * *", "UTC")
    assert "does not divide 60" in uneven


def test_private_172_and_192_ranges_are_refused_before_a_dial():
    from services.egress_guard import metadata_endpoint_refusal

    assert "172.16/12" in metadata_endpoint_refusal("172.16.0.1")
    assert "172.16/12" in metadata_endpoint_refusal("172.31.255.255")
    assert "192.168/16" in metadata_endpoint_refusal("192.168.0.1")
    assert "192.168/16" in metadata_endpoint_refusal("http://192.168.1.20/latest")
    assert metadata_endpoint_refusal("172.15.0.1") == ""
    assert metadata_endpoint_refusal("172.32.0.1") == ""
    assert metadata_endpoint_refusal("192.167.0.1") == ""


def test_plain_s3_copy_does_not_type_check_cells():
    """CopyObject moves the object. Column types are not a cast of its cells."""
    import inspect

    from services.copy_s3_s3 import copy_s3_to_s3

    source = inspect.getsource(copy_s3_to_s3)
    assert "build_mapped_rows" not in source
    assert "quarantine" not in source
    assert "s3_copy_object" in source
