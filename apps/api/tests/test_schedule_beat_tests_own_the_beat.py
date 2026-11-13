"""Tests that fire scheduler beats themselves run without the app's cadence loop.

CI, PR 426 api-tests (6/6): test_d119 called ``schedule_runner._run_due_schedules()``
while the TestClient lifespan's own loop was running. The loop fired the
schedule the instant the test made it due, the test's beat was skipped as
"already in progress", and the test saw ``started == 0``. The r5, r6, r10,
r11 and r12 schedule e2e tests drive beats the same way.

``tests/conftest.py`` now keeps ``SCHEDULE_LOOP`` off for every test module
whose source calls ``_run_due_schedules(`` (so a new test cannot reintroduce
the race), and offers the ``manual_schedule_beats`` fixture for a test that
drives beats some other way.
"""

from __future__ import annotations

import types


def test_a_module_that_drives_beats_runs_without_the_cadence_loop():
    from services.process_role import schedule_loop_enabled

    # This module drives no real beat, but its source names the call
    # (schedule_runner._run_due_schedules() above), which is what is detected.
    assert schedule_loop_enabled() is False


def test_detection_reads_the_test_module_source(tmp_path):
    from tests.conftest import _drives_schedule_beats

    driver = tmp_path / "test_driver.py"
    driver.write_text("from services import schedule_runner\n"
                      "def test_x():\n    schedule_runner._run_due_schedules()\n")
    other = tmp_path / "test_other.py"
    other.write_text("def test_y():\n    assert True\n")
    assert _drives_schedule_beats(types.SimpleNamespace(__file__=str(driver)))
    assert not _drives_schedule_beats(types.SimpleNamespace(__file__=str(other)))


def test_manual_schedule_beats_fixture(manual_schedule_beats):
    from services.process_role import schedule_loop_enabled

    assert schedule_loop_enabled() is False
