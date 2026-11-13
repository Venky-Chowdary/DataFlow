"""Run a pytest session as if the wall clock read a chosen instant.

    DATAFLOW_TEST_WALL_CLOCK=2026-11-13T00:02:20Z python -m pytest -p tests.helpers.wall_clock ...

Tests that seed "N minutes ago" and then ask about a calendar window ("today",
"this week") pass or fail by the time of day CI happens to run them. This
plugin moves the process clock — ``datetime.now`` / ``utcnow`` / ``today``
and ``time.time`` — by one fixed offset, so the clock still
ticks but starts at the instant given, and such a run is repeatable at
00:02 UTC or 23:59 UTC on any machine. Without the variable it does nothing.

It must load before the code under test imports ``datetime`` (``-p`` does
that), because ``from datetime import datetime`` binds the class at import.
``date`` is left alone: libraries subclass it with metaclasses of their own
(pydantic's ``ConstrainedDate``), which a stand-in class would break; pandas
is imported first for the same reason.
"""

from __future__ import annotations

import datetime as _dt
import os
import time as _time

_ENV = "DATAFLOW_TEST_WALL_CLOCK"
_RealDatetime = _dt.datetime
_real_time = _time.time


def _offset_from_env() -> _dt.timedelta | None:
    raw = (os.environ.get(_ENV) or "").strip()
    if not raw:
        return None
    target = _RealDatetime.fromisoformat(raw.replace("Z", "+00:00"))
    if target.tzinfo is None:
        target = target.replace(tzinfo=_dt.timezone.utc)
    return target - _RealDatetime.now(_dt.timezone.utc)


class _DatetimeMeta(type):
    """``isinstance(x, datetime)`` stays true for datetimes made before the patch."""

    def __instancecheck__(cls, obj):
        return isinstance(obj, _RealDatetime)


def install(offset: _dt.timedelta) -> None:
    """Shift every wall-clock read in this process by ``offset``."""

    class FakeDatetime(_RealDatetime, metaclass=_DatetimeMeta):
        @classmethod
        def now(cls, tz=None):
            moment = _RealDatetime.now(_dt.timezone.utc) + offset
            return moment.astimezone(tz) if tz is not None else moment.astimezone().replace(tzinfo=None)

        @classmethod
        def utcnow(cls):
            return (_RealDatetime.now(_dt.timezone.utc) + offset).replace(tzinfo=None)

        @classmethod
        def today(cls):
            return cls.now()

    _dt.datetime = FakeDatetime
    shift = offset.total_seconds()
    _time.time = lambda: _real_time() + shift


_OFFSET = _offset_from_env()
if _OFFSET is not None:
    # pandas' Cython ``NaT`` type subclasses ``datetime`` at import and recurses
    # until the C stack overflows if it first meets the stand-in class.
    try:
        import pandas  # noqa: F401
    except ImportError:
        pass
    install(_OFFSET)


def pytest_report_header(config):
    if _OFFSET is None:
        return None
    return f"wall clock shifted: now = {_dt.datetime.now(_dt.timezone.utc).isoformat(timespec='seconds')}"
