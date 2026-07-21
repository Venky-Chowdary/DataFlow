"""Worker fleet enqueue / claim wiring (no live Mongo required for disabled path)."""

from __future__ import annotations

import os
from unittest import mock

from services.worker_fleet import enqueue_job, fleet_enabled


def test_fleet_disabled_by_default(monkeypatch):
    monkeypatch.delenv("DATAFLOW_WORKER_FLEET", raising=False)
    assert fleet_enabled() is False


def test_fleet_enabled_flag(monkeypatch):
    monkeypatch.setenv("DATAFLOW_WORKER_FLEET", "1")
    assert fleet_enabled() is True


def test_enqueue_returns_false_without_mongo(monkeypatch):
    monkeypatch.setenv("DATAFLOW_WORKER_FLEET", "1")
    with mock.patch("services.worker_fleet._queue_coll", return_value=None):
        assert enqueue_job("job_x") is False


def test_run_transfer_async_enqueues_when_fleet_on(monkeypatch):
    monkeypatch.setenv("DATAFLOW_WORKER_FLEET", "1")
    from src.transfer.background import run_transfer_async
    from src.transfer.models import EndpointConfig, TransferRequest

    req = TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql"),
        destination=EndpointConfig(kind="database", format="postgresql"),
    )
    with mock.patch("services.worker_fleet.enqueue_job", return_value=True) as enq:
        fut = run_transfer_async("job_fleet_1", req)
        assert fut.result() is None
        enq.assert_called_once()
        assert enq.call_args[0][0] == "job_fleet_1"
