"""Under pytest the app's cadence loop is off unless a test asks for it.

CI, PR 426 api-tests (6/6): test_d119 called the scheduler beat itself while
the TestClient lifespan's own loop was running. The loop fired the schedule
the instant the test made it due, the test's beat was skipped as "already in
progress", and the test saw ``started == 0``.

The first fix detected beat-driving modules by grepping their source; the
final review showed a module that fires beats through an imported helper
(test_d347 uses test_r10's ``_schedule``) slipped through. Now
``tests/conftest.py`` keeps ``SCHEDULE_LOOP`` off for the whole session (an
operator-set value is respected), and a test that needs the real loop asks
for the ``schedule_loop_on`` fixture.
"""

from __future__ import annotations

import os


def test_the_loop_is_off_by_default_under_pytest():
    from services.process_role import schedule_loop_enabled

    # This module fires no beat and names no beat call.
    assert schedule_loop_enabled() is False


def test_a_module_that_beats_through_an_imported_helper_is_covered():
    from pathlib import Path

    from services.process_role import schedule_loop_enabled

    source = Path(__file__).with_name("test_d347_propagate_columns_mirror_scd2.py").read_text()
    assert "_run_due_schedules" + "(" not in source  # a source grep could not see it
    assert schedule_loop_enabled() is False


def test_schedule_loop_on_fixture(schedule_loop_on):
    from services.process_role import schedule_loop_enabled

    assert schedule_loop_enabled() is True


def test_the_session_default_is_set_only_when_unset():
    import tests.conftest as conftest

    assert conftest.SCHEDULE_LOOP_DEFAULT == "0"
    assert not hasattr(conftest, "_drives_schedule_beats")
    assert os.environ.get("DATAFLOW_SCHEDULE_LOOP") or os.environ.get("DATAWRAP_SCHEDULE_LOOP")
