"""Review UI2 #9: the schedule form's preview is checked against the scheduler.

apps/web/src/lib/__fixtures__/cron_next_runs.json holds the scheduler's own
next_run over a seeded random corpus; the web differential test replays it.
This fails when the scheduler changes and the fixture was not regenerated
(python scripts/export_cron_next_runs_fixture.py).
"""

from __future__ import annotations

import subprocess  # nosec B404 - runs the repo's own export script
import sys
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "export_cron_next_runs_fixture.py"


def test_the_web_preview_fixture_matches_the_scheduler_today():
    done = subprocess.run([sys.executable, str(SCRIPT), "--check"], capture_output=True, text=True, timeout=600)  # nosec B603
    assert done.returncode == 0, done.stderr
