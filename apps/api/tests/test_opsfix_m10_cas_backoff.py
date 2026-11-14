"""Ops review Medium 10: write contention on the shared control-plane stores.

A read-modify-write that loses the compare-and-swap is run again. The retry
waited a fixed, growing-by-5 ms interval with no jitter, so contending
replicas retried in lockstep, and a conflict after the last attempt reached
the user as a 500. Retries now back off exponentially with full jitter, and
a conflict that outlasts them is a 409 with a retry hint.
"""

from __future__ import annotations

import random
import threading
import uuid

import pytest

from services import control_plane_store, mongodb_service, resource_acl, shared_doc_store
from services.shared_doc_store import SharedDoc, SharedDocConflict

BASE = getattr(shared_doc_store, "BACKOFF_BASE_SECONDS", 0.01)
CAP = getattr(shared_doc_store, "BACKOFF_CAP_SECONDS", 0.5)


def _always_conflicting(tmp_path) -> SharedDoc:
    doc = SharedDoc(
        f"test_backoff_{uuid.uuid4().hex[:8]}",
        file_load=dict, file_save=lambda _data: None, file_path=lambda: tmp_path / "none.json",
    )
    return doc


def _sleeps(monkeypatch, tmp_path, rng) -> list[float]:
    slept: list[float] = []
    monkeypatch.setattr(shared_doc_store.time, "sleep", slept.append)
    monkeypatch.setattr(shared_doc_store, "_jitter", rng, raising=False)
    doc = _always_conflicting(tmp_path)

    @doc.retrying
    def lose():
        raise SharedDocConflict(doc.name)

    with pytest.raises(SharedDocConflict):
        lose()
    return slept


def test_retry_backoff_is_exponential(monkeypatch, tmp_path):
    slept = _sleeps(monkeypatch, tmp_path, lambda low, high: high)  # the top of each jitter window
    assert len(slept) == shared_doc_store._MAX_ATTEMPTS - 1
    caps = [min(CAP, BASE * 2**n) for n in range(len(slept))]
    assert slept == pytest.approx(caps)
    assert slept[3] == pytest.approx(8 * slept[0]), "the window doubles per attempt"


def test_retry_backoff_is_jittered(monkeypatch, tmp_path):
    first = _sleeps(monkeypatch, tmp_path, random.Random(1).uniform)
    second = _sleeps(monkeypatch, tmp_path, random.Random(2).uniform)
    assert first != second, "replicas that conflicted together must not retry in lockstep"
    for n, value in enumerate(first):
        assert 0 <= value <= BASE * 2**n


def test_conflict_after_the_last_retry_is_a_409_with_a_retry_hint():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from src import main

    handler = main.app.exception_handlers.get(SharedDocConflict)
    assert handler is not None, "a lost compare-and-swap reaches the user as a 500"
    app = FastAPI()
    app.add_exception_handler(SharedDocConflict, handler)

    @app.post("/thing")
    def thing():
        raise SharedDocConflict("resource_acls")

    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.post("/thing")
    assert response.status_code == 409
    assert response.headers.get("retry-after")
    assert response.json()["error"] == "concurrent_update"


class _NoLock:
    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False


def test_sixteen_writers_on_one_tenant_lose_nothing_with_the_shipped_settings(tmp_path, monkeypatch):
    """p3 at 16 x 25, all in the same tenant (one fence: the worst case), on live MongoDB."""
    svc = mongodb_service.MongoDBService("mongodb://localhost:27017/")
    if not svc.connect():
        pytest.skip("MongoDB not reachable on this runner")
    svc.db_name = f"opsfix_m10_{uuid.uuid4().hex[:10]}"
    try:
        monkeypatch.setattr(mongodb_service, "_mongodb_service", svc)
        monkeypatch.setattr(control_plane_store, "_remember_seen", lambda: None)
        monkeypatch.setenv("DATAFLOW_CONTROL_STORE_BACKEND", "mongo")
        monkeypatch.setenv("DATAFLOW_CONTROL_STORE_CACHE_SECONDS", "0")
        monkeypatch.setattr(resource_acl, "STORE_PATH", tmp_path / "resource_acls.jsonl")
        monkeypatch.setattr(resource_acl._DOC, "_lock", _NoLock())  # replicas share no process lock
        shared_doc_store.reset_outage_state()
        failures: list[BaseException] = []

        def replica(n: int) -> None:
            for k in range(25):
                try:
                    resource_acl.upsert_grant(
                        tenant_id="t", resource_type="job", resource_id="r", principal=f"p{n}-{k}@x.io", role="viewer"
                    )
                except BaseException as exc:  # noqa: BLE001 - every failure is the finding
                    failures.append(exc)

        threads = [threading.Thread(target=replica, args=(n,)) for n in range(16)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(300)
        assert failures == []
        assert len(resource_acl.list_grants(tenant_id="t")) == 400
    finally:
        svc.client.drop_database(svc.db_name)
        svc.disconnect()
