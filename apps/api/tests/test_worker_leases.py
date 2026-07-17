"""Unit tests for the distributed worker lease store."""

import pytest

from services.worker_leases import WorkerLeaseStore


@pytest.fixture
def store():
    s = WorkerLeaseStore("test-worker")
    yield s
    s.release("job-1")


def test_acquire_and_release(store: WorkerLeaseStore) -> None:
    assert store.acquire("job-1", ttl_seconds=60) is True
    assert store.is_held("job-1") is True
    store.release("job-1")
    assert store.is_held("job-1") is False


def test_acquire_rejected_when_other_worker_holds() -> None:
    worker_a = WorkerLeaseStore("worker-a")
    worker_b = WorkerLeaseStore("worker-b")

    assert worker_a.acquire("job-shared", ttl_seconds=60) is True
    assert worker_b.acquire("job-shared", ttl_seconds=60) is False
    assert worker_b.is_held("job-shared") is True

    worker_a.release("job-shared")
    worker_b.release("job-shared")


def test_heartbeat_extends_lease(store: WorkerLeaseStore) -> None:
    assert store.acquire("job-1", ttl_seconds=1) is True
    assert store.heartbeat("job-1", ttl_seconds=60) is True
    store.release("job-1")
