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
