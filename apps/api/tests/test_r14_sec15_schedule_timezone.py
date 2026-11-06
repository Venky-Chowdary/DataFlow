"""Security 15 (QA r14): a schedule's time zone is an IANA zone for every cadence.

Round 8 validated the General settings zone; a schedule checked its zone only
when it had a cron expression, so a daily/hourly schedule saved
``timezone: "Mars/Olympus"``. ``schedule_store._validate_cadence`` — the one
check create and update both run — now uses the settings validator
(``workspace_store.validate_timezone``) for every cadence.
"""

from __future__ import annotations

import pytest

from services.schedule_store import _validate_cadence
from services.workspace_store import InvalidTimezone, validate_timezone


@pytest.mark.parametrize("interval,cron", [("daily", ""), ("hourly", ""), ("cron", "0 9 * * *")])
@pytest.mark.parametrize("zone", ["Mars/Olympus", "America/Atlantis", "../../etc/passwd", "Not A Zone"])
def test_invented_zone_is_refused_for_every_cadence(interval, cron, zone):
    with pytest.raises(ValueError, match="IANA|Unknown timezone"):
        _validate_cadence(interval, cron, zone, "full_refresh_overwrite")


@pytest.mark.parametrize("zone", ["UTC", "America/New_York", "Europe/Berlin", "Asia/Kolkata", ""])
def test_real_zones_and_blank_default_are_accepted(zone):
    assert _validate_cadence("daily", "", zone, "full_refresh_overwrite")


def test_settings_and_schedules_share_one_validator():
    with pytest.raises(InvalidTimezone):
        validate_timezone("Mars/Olympus")
    assert validate_timezone("America/New_York") == "America/New_York"


# ── Legacy rows (review finding): a stored invalid zone never blocks a write ──

from datetime import datetime, timezone  # noqa: E402

from r8_ent_harness import ent  # noqa: E402,F401


def _legacy(ws: str, *, zone: str = "Mars/Olympus", interval: str = "daily", cron: str = ""):
    """A row written before zones were validated (seeded around the validator)."""
    import uuid

    from services.schedule_store import PipelineSchedule, _load_all, _save_all

    sched = PipelineSchedule(
        id=f"legacy-{uuid.uuid4().hex[:8]}", name=f"legacy {uuid.uuid4().hex[:6]}",
        source_connector_id="c-src", source_table="t", dest_connector_id="c-dst",
        dest_table="t_out", interval=interval, cron=cron, timezone=zone, enabled=True,
        workspace_id=ws, mappings=[{"source": "id", "target": "id", "confidence": 1.0}],
    )
    _save_all(_load_all() + [sched])
    return sched


def test_store_pauses_and_updates_a_legacy_row_without_touching_its_zone(ent):
    from services.schedule_store import get_schedule, update_schedule

    ws = ent.workspace("Legacy tz store")
    for interval, cron in (("daily", ""), ("cron", "0 9 * * *")):
        sched = _legacy(ws, interval=interval, cron=cron)
        paused = update_schedule(sched.id, {"enabled": False})
        assert paused is not None and paused.enabled is False
        assert update_schedule(sched.id, {"name": f"renamed {sched.id}"}) is not None
        # Setting the zone, or changing the cadence, still asks for a real zone.
        with pytest.raises(ValueError, match="IANA"):
            update_schedule(sched.id, {"timezone": "Mars/Olympus"})
        with pytest.raises(ValueError, match="IANA"):
            update_schedule(sched.id, {"interval": "hourly", "cron": ""})
        assert update_schedule(sched.id, {"timezone": "Europe/Berlin"}).timezone == "Europe/Berlin"
        assert get_schedule(sched.id).name == f"renamed {sched.id}"


def test_route_disables_a_legacy_row_and_refuses_an_invented_zone(ent):
    ws = ent.workspace("Legacy tz route")
    sched = _legacy(ws)
    admin = ent.admin
    disabled = admin.patch(f"/api/v1/schedules/{sched.id}", json={"enabled": False},
                           headers={"X-Workspace-Id": ws})
    assert disabled.status_code == 200, disabled.text
    assert disabled.json()["enabled"] is False
    refused = admin.patch(f"/api/v1/schedules/{sched.id}", json={"timezone": "Mars/Olympus"},
                          headers={"X-Workspace-Id": ws})
    assert refused.status_code == 400, refused.text
    assert "IANA" in refused.text
    fixed = admin.patch(f"/api/v1/schedules/{sched.id}", json={"timezone": "America/New_York"},
                        headers={"X-Workspace-Id": ws})
    assert fixed.status_code == 200, fixed.text
    assert fixed.json()["timezone"] == "America/New_York"


def test_runner_backs_off_a_legacy_row(ent):
    from services.schedule_runner import _back_off_transient
    from services.schedule_store import get_schedule

    ws = ent.workspace("Legacy tz runner")
    sched = _legacy(ws)
    before = datetime.now(timezone.utc)
    _back_off_transient(sched.id, sched, "transient_source_error")
    due = datetime.fromisoformat(str(get_schedule(sched.id).next_run_at))
    assert due.replace(tzinfo=due.tzinfo or timezone.utc) > before


@pytest.fixture
def healthy_zones(monkeypatch):
    from services import schedule_store

    monkeypatch.setattr(schedule_store, "zone_database_healthy", lambda: True)


def test_scheduler_start_normalises_invalid_zones_to_utc_on_the_record(ent, healthy_zones):
    from services.schedule_store import get_schedule, normalize_invalid_timezones

    ws = ent.workspace("Legacy tz normalise")
    bad = _legacy(ws, interval="cron", cron="0 9 * * *")
    good = _legacy(ws, zone="Asia/Kolkata")
    repaired = normalize_invalid_timezones()
    assert repaired == [{"schedule_id": bad.id, "from": "Mars/Olympus", "to": "UTC", "paused": True}]
    after = get_schedule(bad.id)
    # S4b: a UTC cadence is a time nobody chose — the row is paused, with no next run,
    # and its history says why.
    assert after.timezone == "UTC" and after.enabled is False and after.next_run_at is None
    assert after.run_history[-1]["reason_code"] == "timezone_normalized"
    assert get_schedule(good.id).timezone == "Asia/Kolkata"
    assert normalize_invalid_timezones(force=True) == []  # idempotent
    events = [e for e in ent.audit(ws, action="schedule.timezone.normalized")]
    assert len(events) == 1 and events[0]["details"]["from"] == "Mars/Olympus"


def test_normalisation_runs_once_per_deployment_not_every_start(ent, healthy_zones):
    from services.schedule_store import get_schedule, normalize_invalid_timezones

    ws = ent.workspace("Legacy tz once")
    normalize_invalid_timezones()
    late = _legacy(ws)
    assert normalize_invalid_timezones() == []  # a second start does nothing
    assert get_schedule(late.id).timezone == "Mars/Olympus"


def test_an_incomplete_zone_database_rewrites_nothing(ent, monkeypatch):
    """S4a: on a host without backward links, Asia/Calcutta must not become UTC."""
    from services import schedule_store

    monkeypatch.setattr(schedule_store, "zone_database_healthy", lambda: False)
    ws = ent.workspace("Legacy tz unhealthy")
    alias = _legacy(ws, zone="Asia/Calcutta")
    assert schedule_store.normalize_invalid_timezones(force=True) == []
    assert schedule_store.get_schedule(alias.id).timezone == "Asia/Calcutta"


def test_a_patch_between_read_and_write_wins(ent, healthy_zones, monkeypatch):
    """S4c: the rewrite is conditional on the stored invalid value."""
    from services import schedule_store

    ws = ent.workspace("Legacy tz race")
    sched = _legacy(ws)
    stale = schedule_store.list_schedules()
    # A user fixes the zone after the normaliser read its list.
    assert schedule_store.update_schedule(sched.id, {"timezone": "Europe/Berlin"}) is not None
    monkeypatch.setattr(schedule_store, "list_schedules", lambda *a, **k: stale)
    assert schedule_store.normalize_invalid_timezones(force=True) == []
    after = schedule_store.get_schedule(sched.id)
    assert after.timezone == "Europe/Berlin" and after.enabled is True


def test_disabled_legacy_row_stays_disabled_with_no_next_run(ent, healthy_zones):
    from services.schedule_store import get_schedule, normalize_invalid_timezones, update_schedule

    ws = ent.workspace("Legacy tz disabled")
    sched = _legacy(ws)
    update_schedule(sched.id, {"enabled": False})
    repaired = normalize_invalid_timezones(force=True)
    assert repaired == [{"schedule_id": sched.id, "from": "Mars/Olympus", "to": "UTC", "paused": False}]
    after = get_schedule(sched.id)
    assert after.enabled is False and after.next_run_at is None
