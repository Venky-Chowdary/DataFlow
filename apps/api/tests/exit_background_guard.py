"""Rehearsal tests leave nothing running: extract jobs and verifications end with the test.

Imported by every exit test module (an autouse fixture). It depends on
``monkeypatch`` so it tears down first: the test's background work is joined
while the test's own stores and connectors are still patched in, and only then
are the patches undone. A rehearsal a test started and did not wait for used to
keep extracting and verifying after the test, against whatever the next test
had set up.
"""

from __future__ import annotations

import threading

import pytest

FEATURE_THREADS = ("exit-verify",)  # exit-verify-<rehearsal>, exit-verify-lease-<rehearsal>


def feature_threads() -> list[str]:
    return sorted(t.name for t in threading.enumerate() if t.is_alive() and t.name.startswith(FEATURE_THREADS))


@pytest.fixture(autouse=True)
def _no_exit_background_work_outlives_a_test(monkeypatch):
    yield
    from services import exit_rehearsal

    finished = exit_rehearsal.join_background(90)
    left = exit_rehearsal.background_work() + feature_threads()
    if left:
        exit_rehearsal.stop_background(10)
    assert finished and not left, f"rehearsal work still running after the test: {left}"
