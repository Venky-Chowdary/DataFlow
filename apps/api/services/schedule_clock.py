"""The scheduler's clock: one seam for "now" in schedule_store and schedule_runner.

Every scheduler decision that depends on the present (is it due, when is the
next slot, has a claim gone stale) reads :func:`utc_now` unless the caller
passed ``now``. Tests freeze it with ``monkeypatch.setattr(schedule_clock,
"utc_now", lambda: FIXED)`` so an assertion such as "the next slot is in the
future" cannot flip when a run crosses a minute boundary.
"""

from __future__ import annotations

from datetime import datetime, timezone


def utc_now() -> datetime:
    """The present instant, timezone-aware UTC."""
    return datetime.now(timezone.utc)
