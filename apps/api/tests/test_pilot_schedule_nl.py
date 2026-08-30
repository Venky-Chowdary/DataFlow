"""DataPilot can create a pipeline from words — and refuses the unsayable ones.

"Schedule users transfer nightly at 2 AM" used to answer that schedules need the
UI. It now resolves a route, plans it, requires an execute-grade preflight, and
stages a schedule the operator has to Confirm. These tests pin the parts where a
schedule could quietly mean something other than what was asked:

* cadence wording — an ambiguous zone, an uneven "every N days", or a nightly run
  with no time is a question, never a nearby cadence;
* routing — "schedule … nightly" stages a *schedule*, not one run, and a plain
  transfer sentence still stages one run;
* staging — nothing is created until Confirm, and semantics a schedule cannot
  carry (row limit, row filter, incremental with no watermark) are refused
  instead of dropped;
* confirming — the durable schedule is created exactly once, by the store, and a
  replayed ack returns the first outcome rather than a second pipeline.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from src.ai.copilot import schedule_tools
from src.ai.copilot.ack_ledger import get_ack_ledger
from src.ai.copilot.schedule_cadence import parse_cadence
from src.ai.copilot.tool_permissions import can_confirm_kind, is_tool_allowed
from src.ai.copilot.tools import infer_tools_from_message

# --------------------------------------------------------------------------
# Cadence
# --------------------------------------------------------------------------


def test_daily_at_a_time_becomes_a_cron_in_utc_and_says_it_assumed_utc():
    spec = parse_cadence("daily at 02:30")
    assert spec.resolved
    assert (spec.interval, spec.cron, spec.timezone) == ("daily", "30 2 * * *", "UTC")
    assert spec.timezone_assumed is True


def test_nightly_with_a_time_and_zone_is_resolved_exactly():
    spec = parse_cadence("nightly at 2 AM in Asia/Kolkata")
    assert (spec.interval, spec.cron, spec.timezone) == ("daily", "0 2 * * *", "Asia/Kolkata")
    assert spec.timezone_assumed is False


def test_nightly_with_no_time_is_asked_about_not_run_whenever_it_was_created():
    spec = parse_cadence("nightly")
    assert not spec.resolved
    assert "what time" in spec.question.lower()


def test_weekly_on_a_weekday_carries_the_day_and_the_zone():
    spec = parse_cadence("weekly on Monday at 09:00 America/New_York")
    assert (spec.interval, spec.cron, spec.timezone) == (
        "weekly",
        "0 9 * * 1",
        "America/New_York",
    )


def test_every_15_minutes_is_a_minute_cron_on_the_hourly_preset():
    spec = parse_cadence("every 15 minutes")
    assert (spec.interval, spec.cron) == ("hourly", "*/15 * * * *")


def test_monthly_needs_the_day_and_takes_it():
    assert not parse_cadence("monthly").resolved
    spec = parse_cadence("monthly on the 1st at 02:00 UTC")
    assert spec.cron == "0 2 1 * *"


def test_monthly_day_that_does_not_exist_every_month_is_refused():
    spec = parse_cadence("monthly on the 31st at 02:00 UTC")
    assert not spec.resolved
    assert "31" in spec.question


@pytest.mark.parametrize("zone", ["IST", "CST"])
def test_ambiguous_timezone_abbreviations_are_asked_about(zone: str):
    spec = parse_cadence(f"nightly at 2am {zone}")
    assert not spec.resolved
    assert "more than one timezone" in spec.question


def test_every_n_days_is_refused_rather_than_written_as_an_uneven_cron():
    spec = parse_cadence("every 10 days")
    assert not spec.resolved
    assert "cron" in spec.question.lower()


def test_explicit_five_field_cron_is_taken_as_given():
    spec = parse_cadence("cron 0 3 * * 1-5 in Europe/Paris")
    assert spec.cron == "0 3 * * 1-5"
    assert spec.timezone == "Europe/Paris"


def test_a_cron_that_is_not_five_fields_is_refused():
    assert not parse_cadence("cron 0 3 *").resolved


# --------------------------------------------------------------------------
# Routing: a recurring request is not one run
# --------------------------------------------------------------------------


def _routed(message: str) -> list[str]:
    return [name for name, _ in infer_tools_from_message(message)]


def _args(message: str, tool: str) -> dict[str, Any]:
    for name, args in infer_tools_from_message(message):
        if name == tool:
            return args
    raise AssertionError(f"{tool} not planned for {message!r}; got {_routed(message)}")


def test_scheduling_words_stage_a_schedule_not_a_single_run():
    assert "create_schedule" in _routed(
        "schedule transfer of users from Prod PG to Warehouse nightly at 2am UTC"
    )
    assert "start_transfer" not in _routed(
        "schedule transfer of users from Prod PG to Warehouse nightly at 2am UTC"
    )


def test_a_cadence_alone_is_enough_to_mean_recurring():
    assert "create_schedule" in _routed(
        "transfer users from Prod PG to Warehouse every 15 minutes"
    )


def test_a_plain_transfer_sentence_still_runs_once():
    assert _routed("transfer users from Prod PG to Warehouse") == ["start_transfer"]


def test_the_cadence_tail_is_not_read_as_the_destination_name():
    args = _args(
        "transfer users from SQL Server to Postgres nightly at 2am in Asia/Kolkata",
        "create_schedule",
    )
    assert args["dest_connector_name"].lower() == "postgres"
    assert args["source_table"] == "users"
    spec = parse_cadence(args["cadence"])
    assert (spec.cron, spec.timezone) == ("0 2 * * *", "Asia/Kolkata")


# --------------------------------------------------------------------------
# Staging: refusals that keep a schedule from meaning something else
# --------------------------------------------------------------------------


def _plan_stub(**plan: Any):
    """A plan_transfer stand-in returning an execute-cleared plan."""

    class _Result:
        success = True
        error = ""
        output = {
            "source": {"connector_id": "c-src", "connector_name": "Prod PG", "table": "users"},
            "destination": {
                "connector_id": "c-dst",
                "connector_name": "Warehouse",
                "table": "users",
            },
            "sync_mode": "full_refresh_append",
            "schema_policy": "manual_review",
            "validation_mode": "balanced",
            "engine_mappings": [{"source": "id", "destination": "id"}],
            "stream_contracts": [],
            "mapped_count": 1,
            "unmapped_source_columns": [],
            "preflight": {
                "passed": True,
                "run_id": "pf_1",
                "blockers": [],
                "transfer_decision": {"decision": "approve"},
            },
            "data_rules": {},
            **plan,
        }

    return _Result()


@pytest.fixture
def cleared_plan(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(schedule_tools, "plan_transfer", lambda **kw: _plan_stub())
    monkeypatch.setattr(schedule_tools, "_is_execute_cleared", lambda pf: True)


def test_cadence_question_is_returned_before_anything_is_planned(
    monkeypatch: pytest.MonkeyPatch,
):
    def _boom(**kw: Any):
        raise AssertionError("planning must not run for an unresolved cadence")

    monkeypatch.setattr(schedule_tools, "plan_transfer", _boom)
    result = schedule_tools.create_schedule(cadence="nightly", source_table="users")
    assert result.success is False
    assert "what time" in (result.error or "").lower()


def test_a_row_limit_cannot_become_a_schedule(cleared_plan):
    result = schedule_tools.create_schedule(cadence="daily at 02:00 UTC", limit=100)
    assert result.success is False
    assert "row limit" in (result.error or "")


def test_a_row_filter_cannot_be_silently_dropped_into_a_schedule(cleared_plan):
    # PipelineSchedule persists no filter, so accepting one would move every row.
    result = schedule_tools.create_schedule(
        cadence="daily at 02:00 UTC",
        source_filter={"column": "status", "operator": "eq", "value": "active"},
    )
    assert result.success is False
    assert "row filter" in (result.error or "")


def test_incremental_without_a_watermark_is_refused(cleared_plan):
    result = schedule_tools.create_schedule(
        cadence="hourly", sync_mode="incremental"
    )
    assert result.success is False
    assert "watermark" in (result.error or "")


def test_a_blocked_preflight_never_becomes_an_unattended_pipeline(
    monkeypatch: pytest.MonkeyPatch,
):
    blocked = _plan_stub(
        preflight={
            "passed": False,
            "run_id": "pf_2",
            "blockers": [{"id": "b1", "message": "dest column too narrow"}],
            "transfer_decision": {"decision": "blocked"},
        }
    )
    monkeypatch.setattr(schedule_tools, "plan_transfer", lambda **kw: blocked)
    result = schedule_tools.create_schedule(cadence="daily at 02:00 UTC")
    assert result.success is False
    assert "dest column too narrow" in (result.error or "")
    assert "pf_2" in (result.error or "")


def test_staging_creates_nothing_and_asks_for_confirm(cleared_plan, monkeypatch):
    import services.schedule_store as store

    monkeypatch.setattr(
        store,
        "create_schedule",
        lambda data: (_ for _ in ()).throw(AssertionError("staging must not create")),
    )
    result = schedule_tools.create_schedule(
        cadence="nightly at 2am in Asia/Kolkata",
        source_connector_name="Prod PG",
        source_table="users",
        dest_connector_name="Warehouse",
    )
    assert result.success is True
    out = result.output or {}
    assert out["requires_confirm"] is True
    assert out["risk"] == "mutate"
    assert out["preview"]["cron"] == "0 2 * * *"
    assert out["preview"]["timezone"] == "Asia/Kolkata"
    assert out["preview"]["preflight_run_id"] == "pf_1"
    # The staged payload stays server-side; only the ack id crosses the wire.
    staged = get_ack_ledger().peek(out["ack_id"]) or {}
    assert staged.get("kind") == "create_schedule"


def test_an_unstated_timezone_is_disclosed_in_the_preview(cleared_plan):
    result = schedule_tools.create_schedule(cadence="daily at 02:00")
    out = result.output or {}
    assert "timezone_note" in out["preview"]


# --------------------------------------------------------------------------
# Permission: schedule.manage, both at planning and at confirm time
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("role", "allowed"),
    [("viewer", False), ("operator", False), ("editor", True), ("admin", True)],
)
def test_only_schedule_managers_may_stage_or_confirm_a_schedule(role: str, allowed: bool):
    assert is_tool_allowed(role, "create_schedule") is allowed
    assert can_confirm_kind(role, "create_schedule") is allowed


def test_an_unlisted_ack_kind_can_never_be_confirmed():
    assert can_confirm_kind("admin", "delete_everything") is False


# --------------------------------------------------------------------------
# Confirm: the store creates it once, and a replay does not create a second
# --------------------------------------------------------------------------


class _Req:
    """Minimal stand-in for the confirm request body."""

    def __init__(self, ack_id: str) -> None:
        self.ack_id = ack_id
        self.actor = "owner"
        self.reason = "approved nightly users pipeline"


class _Http:
    class state:  # noqa: N801 - mirrors Starlette's request.state attribute
        pass

    headers: dict[str, str] = {}


def test_confirm_creates_the_schedule_once_through_the_store(monkeypatch):
    from src.routers import copilot_router
    import services.schedule_store as store

    created: list[dict[str, Any]] = []

    class _Sched:
        id = "sch-1"
        name = "users — every day at 02:00 UTC"
        interval = "daily"
        cron = "0 2 * * *"
        timezone = "UTC"
        sync_mode = "full_refresh_append"
        enabled = True
        next_run_at = "2026-08-18T02:00:00+00:00"

    def _create(data: dict[str, Any]):
        created.append(data)
        return _Sched()

    monkeypatch.setattr(store, "create_schedule", _create)
    monkeypatch.setattr(copilot_router, "_caller", lambda req: ("admin", "owner@example.com"))

    ack_id = get_ack_ledger().put(
        kind="create_schedule",
        payload={
            "name": "users — every day at 02:00 UTC",
            "source_connector_id": "c-src",
            "source_table": "users",
            "dest_connector_id": "c-dst",
            "dest_table": "users",
            "interval": "daily",
            "cron": "0 2 * * *",
            "timezone": "UTC",
            "sync_mode": "full_refresh_append",
            "mappings": [{"source": "id", "destination": "id"}],
            "enabled": True,
        },
        preview={"name": "users"},
    )

    first = asyncio.run(copilot_router.copilot_confirm(_Req(ack_id), _Http()))
    assert first["ok"] is True and first["idempotent"] is False
    assert first["schedule_id"] == "sch-1"
    assert first["next_run_at"] == "2026-08-18T02:00:00+00:00"
    assert len(created) == 1

    # Replaying the same approval must echo the first outcome, not create again.
    replay = asyncio.run(copilot_router.copilot_confirm(_Req(ack_id), _Http()))
    assert replay["idempotent"] is True
    assert replay["schedule_id"] == "sch-1"
    assert len(created) == 1


def test_a_store_refusal_is_surfaced_and_the_approval_stays_spendable(monkeypatch):
    from fastapi import HTTPException

    from src.routers import copilot_router
    import services.schedule_store as store

    def _refuse(data: dict[str, Any]):
        raise ValueError("Unsupported interval: fortnightly")

    monkeypatch.setattr(store, "create_schedule", _refuse)
    monkeypatch.setattr(copilot_router, "_caller", lambda req: ("admin", "owner@example.com"))

    ack_id = get_ack_ledger().put(
        kind="create_schedule",
        payload={"name": "x", "interval": "fortnightly"},
        preview={"name": "x"},
    )
    with pytest.raises(HTTPException) as exc:
        asyncio.run(copilot_router.copilot_confirm(_Req(ack_id), _Http()))
    assert "fortnightly" in str(exc.value.detail)
    # The claim was released, so the operator can fix the cadence and retry.
    assert (get_ack_ledger().peek(ack_id) or {}).get("kind") == "create_schedule"
