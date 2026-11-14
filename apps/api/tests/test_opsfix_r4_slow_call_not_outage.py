"""A slow call is not an outage (PR 406 CI, shard 5).

16 writers on one tenant: one write ran past the 1 s control-plane bound on a
loaded runner, the NetworkTimeout was remembered as an outage, and for the
next 2 s every call in the process failed at once (deny-all ACL reads, refused
writes). Reproduced locally with the bound at 0.15 s: 2 slow calls became 123
failures. Only "no server can be reached" opens the outage window; one slow
call fails closed on its own. Writes (CAS retries, transactions) get their own,
longer bound; request-path reads keep the short one.
"""
from __future__ import annotations

import pytest

pymongo = pytest.importorskip("pymongo")
from pymongo import errors  # noqa: E402

from services import control_plane_bounds as cpb  # noqa: E402
from services.control_plane_store import ControlPlaneUnavailable  # noqa: E402


@pytest.fixture(autouse=True)
def _clean_outage():
    cpb.reset_outage_state()
    yield
    cpb.reset_outage_state()


def _fail_inside_deadline(exc: BaseException) -> None:
    with pytest.raises(ControlPlaneUnavailable):
        with cpb.deadline():
            raise exc


@pytest.mark.parametrize(
    "exc",
    [
        errors.NetworkTimeout("timed out"),
        errors.ExecutionTimeout("operation exceeded time limit", code=50),
        errors.WTimeoutError("waiting for replication timed out", code=64),
    ],
    ids=["socket timeout", "server maxTimeMS", "write concern timeout"],
)
def test_one_slow_call_fails_closed_without_opening_the_outage_window(exc):
    with cpb.deadline():  # a working database: an earlier call succeeded
        pass
    _fail_inside_deadline(exc)
    assert cpb.outage_retry_at() == 0.0
    with cpb.deadline():  # the next call is attempted, not refused at once
        pass


@pytest.mark.parametrize(
    "exc",
    [
        errors.ServerSelectionTimeoutError("no server available"),
        errors.AutoReconnect("connection refused"),
    ],
    ids=["no server reachable", "connection lost"],
)
def test_an_unreachable_database_opens_the_outage_window(exc):
    _fail_inside_deadline(exc)
    assert cpb.outage_retry_at() > 0.0
    with pytest.raises(ControlPlaneUnavailable, match="next attempt"):
        with cpb.deadline():
            pass


class _Recorder:
    """A pymongo.timeout stand-in that records the bound it was given."""

    def __init__(self, seen: list[float]):
        self.seen = seen

    def __call__(self, seconds):
        self.seen.append(seconds)
        return self

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False


class _Coll:
    name = "c"

    def find_one(self, *_a, **_k):
        return {}

    def count_documents(self, *_a, **_k):
        return 0

    def update_one(self, *_a, **_k):
        return None

    def insert_one(self, *_a, **_k):
        return None

    def find_one_and_update(self, *_a, **_k):
        return {}

    def delete_many(self, *_a, **_k):
        return None


def test_socket_timeouts_in_a_row_with_no_success_are_an_outage():
    """A database that stopped answering on open connections (a blackhole)."""
    for _ in range(cpb.SOCKET_TIMEOUTS_FOR_OUTAGE - 1):
        _fail_inside_deadline(errors.NetworkTimeout("timed out"))
        assert cpb.outage_retry_at() == 0.0
    _fail_inside_deadline(errors.NetworkTimeout("timed out"))
    assert cpb.outage_retry_at() > 0.0


def test_a_success_between_socket_timeouts_resets_the_count():
    for _ in range(cpb.SOCKET_TIMEOUTS_FOR_OUTAGE * 3):
        _fail_inside_deadline(errors.NetworkTimeout("timed out"))
        with cpb.deadline():
            pass
    assert cpb.outage_retry_at() == 0.0


def test_writes_get_the_write_bound_and_reads_the_short_one(monkeypatch):
    seen: list[float] = []
    monkeypatch.setattr(pymongo, "timeout", _Recorder(seen))
    coll = cpb.bounded(_Coll())
    coll.find_one({})
    coll.count_documents({})
    reads = list(seen)
    seen.clear()
    coll.update_one({}, {})
    coll.insert_one({})
    coll.find_one_and_update({}, {})
    coll.delete_many({})
    assert reads == [cpb.DEFAULT_TIMEOUT_SECONDS] * 2
    assert seen == [cpb.DEFAULT_WRITE_TIMEOUT_SECONDS] * 4
    assert cpb.DEFAULT_WRITE_TIMEOUT_SECONDS > cpb.DEFAULT_TIMEOUT_SECONDS


def test_the_write_bound_is_configurable_and_capped(monkeypatch):
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_WRITE_TIMEOUT_SECONDS", "7")
    assert cpb.write_timeout_seconds() == 7.0
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_WRITE_TIMEOUT_SECONDS", "999")
    assert cpb.write_timeout_seconds() == cpb.MAX_WRITE_TIMEOUT_SECONDS


def test_sixteen_callers_one_slow_call_only_that_call_fails():
    """One socket timeout among many concurrent calls: the others succeed."""
    import threading

    state = {"first": True}
    lock = threading.Lock()
    failures: list[BaseException] = []

    class _Slow(_Coll):
        def update_one(self, *_a, **_k):
            with lock:
                slow, state["first"] = state["first"], False
            if slow:
                raise errors.NetworkTimeout("timed out")

    coll = cpb.bounded(_Slow())

    def caller():
        for _ in range(25):
            try:
                coll.update_one({}, {})
            except BaseException as exc:  # noqa: BLE001 - every failure is the finding
                failures.append(exc)

    threads = [threading.Thread(target=caller) for _ in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(60)
    assert len(failures) == 1, [type(f).__name__ for f in failures[:5]]
