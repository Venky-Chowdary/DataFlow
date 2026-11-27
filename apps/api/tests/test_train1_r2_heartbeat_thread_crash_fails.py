"""Review 2 item 16: a crashed reconcile-heartbeat thread fails its test.

test_execute_tracked_locale_data_csv_to_bigquery's job store lacked
``update_job_fields`` (real stores — MongoDB and the in-memory store — both
have it; the heartbeat has called it since #367), and the thread's
AttributeError only surfaced as a warning. The conftest hook now turns a
crash of any ``reconcile-heartbeat-*`` thread into a failure.
"""

from __future__ import annotations

import threading

import pytest

import tests.conftest as conftest


def test_a_crashed_heartbeat_thread_is_reported(monkeypatch):
    # The hook under test chains to a silent one here, so this test's own
    # autouse copy of the hook does not see the deliberate crash.
    monkeypatch.setattr(threading, "excepthook", lambda args: None)
    gen = conftest._fail_on_heartbeat_thread_crash.__wrapped__()
    next(gen)

    def _boom():
        raise AttributeError("'_FakeMongo' object has no attribute 'update_job_fields'")

    t = threading.Thread(target=_boom, name="reconcile-heartbeat-abcd1234")
    t.start()
    t.join()
    with pytest.raises(AssertionError, match="reconcile-heartbeat-abcd1234: AttributeError"):
        next(gen)


def test_other_threads_are_left_to_pytest():
    gen = conftest._fail_on_heartbeat_thread_crash.__wrapped__()
    next(gen)
    t = threading.Thread(target=lambda: None, name="some-worker")
    t.start()
    t.join()
    with pytest.raises(StopIteration):
        next(gen)


def test_real_job_stores_have_update_job_fields():
    from services.mongodb_service import MemoryMongoDBService, MongoDBService

    for store in (MongoDBService, MemoryMongoDBService):
        assert callable(getattr(store, "update_job_fields", None)), store
