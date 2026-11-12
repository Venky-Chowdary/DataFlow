"""The read of a read-modify-write is part of the write (PR 406 CI, api-tests 6/6).

``test_sixteen_writers_on_one_tenant_lose_nothing_with_the_shipped_settings``
failed on a loaded runner: 16 writers on one tenant, and a grant was lost with
"Failed reading resource ACL store (deny-all until fixed): the control-plane
database did not answer within 1.0s". The write itself runs under the 3 s
write bound, but the read it needs (inside the compare-and-swap loop) ran
under the 1 s request-path read bound, so a shared store that was slow but
healthy turned an admin's grant into an error.

Inside a ``retrying`` read-modify-write every call runs under the write
bound; a read on the request path keeps the short one (a slow lookup still
fails closed quickly).
"""

from __future__ import annotations

import time
import uuid

import pytest

pymongo = pytest.importorskip("pymongo")

from services import (  # noqa: E402
    control_plane_bounds as cpb,
    control_plane_store,
    mongodb_service,
    resource_acl,
    shared_doc_store,
)

#: Between the read bound (1 s) and the write bound (3 s): a slow, healthy store.
LATENCY_SECONDS = 1.5


class _SlowCursor:
    """The real cursor; iterating it first waits LATENCY_SECONDS, as a loaded store would."""

    def __init__(self, cursor):
        self._cursor = cursor

    def sort(self, *args, **kwargs):
        self._cursor = self._cursor.sort(*args, **kwargs)
        return self

    def skip(self, n):
        self._cursor = self._cursor.skip(n)
        return self

    def limit(self, n):
        self._cursor = self._cursor.limit(n)
        return self

    def __iter__(self):
        time.sleep(LATENCY_SECONDS)
        return iter(self._cursor)


class _SlowFind:
    def __init__(self, coll):
        self._coll = coll

    def find(self, *args, **kwargs):
        return _SlowCursor(self._coll.find(*args, **kwargs))

    def __getattr__(self, name):
        return getattr(self._coll, name)


@pytest.fixture
def slow_acl_store(tmp_path, monkeypatch):
    svc = mongodb_service.MongoDBService("mongodb://localhost:27017/")
    if not svc.connect():
        pytest.skip("MongoDB not reachable on this runner")
    svc.db_name = f"opsfix_r5_{uuid.uuid4().hex[:10]}"
    try:
        monkeypatch.setattr(mongodb_service, "_mongodb_service", svc)
        monkeypatch.setattr(control_plane_store, "_remember_seen", lambda: None)
        monkeypatch.setenv("DATAFLOW_CONTROL_STORE_BACKEND", "mongo")
        monkeypatch.setenv("DATAFLOW_CONTROL_STORE_CACHE_SECONDS", "0")
        for prefix in ("DATAFLOW_", "DATAWRAP_"):
            monkeypatch.delenv(prefix + "CONTROL_STORE_TIMEOUT_SECONDS", raising=False)
            monkeypatch.delenv(prefix + "CONTROL_STORE_WRITE_TIMEOUT_SECONDS", raising=False)
        monkeypatch.setattr(resource_acl, "STORE_PATH", tmp_path / "resource_acls.jsonl")
        cpb.reset_outage_state()
        first = resource_acl.upsert_grant(  # indexes and the first record, at full speed
            tenant_id="t", resource_type="job", resource_id="r", principal="first@x.io", role="viewer"
        )
        real = shared_doc_store._collection

        def slow(name):
            coll = real(name)
            return None if coll is None else cpb.bounded(_SlowFind(coll._coll))

        monkeypatch.setattr(shared_doc_store, "_collection", slow)
        yield first
    finally:
        cpb.reset_outage_state()
        svc.client.drop_database(svc.db_name)
        svc.disconnect()


def test_the_bounds_leave_room_for_the_injected_latency():
    assert cpb.DEFAULT_TIMEOUT_SECONDS < LATENCY_SECONDS < cpb.DEFAULT_WRITE_TIMEOUT_SECONDS - 1.0


def test_a_slow_healthy_store_does_not_lose_a_grant(slow_acl_store):
    grant = resource_acl.upsert_grant(
        tenant_id="t", resource_type="job", resource_id="r", principal="second@x.io", role="editor"
    )
    assert grant.principal == "second@x.io"
    assert cpb.outage_retry_at() == 0.0


def test_a_slow_revoke_is_not_lost_either(slow_acl_store):
    assert resource_acl.revoke_grant(slow_acl_store.id, tenant_id="t") is True
    assert cpb.outage_retry_at() == 0.0


def test_a_request_path_read_keeps_the_short_bound(slow_acl_store):
    """Outside a write, the same slow read still fails closed after about 1 s."""
    started = time.monotonic()
    with pytest.raises(RuntimeError, match="did not answer within 1.0s"):
        resource_acl.list_grants(tenant_id="t", resource_type="job", resource_id="r")
    assert time.monotonic() - started < cpb.DEFAULT_WRITE_TIMEOUT_SECONDS


def test_write_scope_gives_reads_the_write_bound(monkeypatch):
    seen: list[float] = []

    class _Recorder:
        def __call__(self, seconds):
            seen.append(seconds)
            return self

        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

    monkeypatch.setattr(pymongo, "timeout", _Recorder())
    with cpb.deadline():
        pass
    with cpb.write_scope():
        with cpb.deadline():
            pass
        with cpb.write_scope():  # nested scopes (a retrying call inside another)
            pass
        with cpb.deadline():
            pass
    with cpb.deadline():
        pass
    assert seen == [
        cpb.DEFAULT_TIMEOUT_SECONDS,
        cpb.DEFAULT_WRITE_TIMEOUT_SECONDS,
        cpb.DEFAULT_WRITE_TIMEOUT_SECONDS,
        cpb.DEFAULT_TIMEOUT_SECONDS,
    ]
