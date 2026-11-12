"""The first ``/api/v1/health`` of a test process is not a cold driver import (CI).

``aggregate_health`` reports driver availability by importing every bundled
connector driver (:func:`services.driver_bootstrap.driver_status`). The app
does that once at start-up (lifespan, ``ensure_platform_drivers``), so a
served request never pays for it. A test that drives the app without the
lifespan (an ASGI transport, D321's latency probe) paid whatever was still
unimported - seconds in a fresh process - and passed or failed on which test
files happened to run before it in its CI shard. The session imports them
once, as start-up does.
"""

from __future__ import annotations

import importlib.util
import sys


def test_every_installed_platform_driver_is_imported_before_any_test_runs():
    from services.driver_bootstrap import _platform_drivers

    cold = []
    for driver_id, module, _package in _platform_drivers():
        try:
            installed = importlib.util.find_spec(module) is not None
        except (ImportError, ValueError):
            installed = False
        if installed and module not in sys.modules:
            cold.append(f"{driver_id} ({module})")
    assert not cold, f"the first /health in this process would import: {', '.join(cold)}"
