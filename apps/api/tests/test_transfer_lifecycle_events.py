"""Transfer lifecycle events: job.requested, transfer.run.*, job.cancel.requested.

Real SQLite → SQLite runs through ``execute_tracked`` (memory job store). The
events carry connector types, sync mode and counts — never table names.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from services.logging_config import ContextFilter, JsonFormatter, TextFormatter  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

TABLE = "cust_secret_ledger_q7"


class _Capture(logging.Handler):
    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.records: list[logging.LogRecord] = []
        self.addFilter(ContextFilter())

    def emit(self, record: logging.LogRecord) -> None:
        if getattr(record, "event", None):
            self.records.append(record)

    def named(self, prefix: str) -> list[logging.LogRecord]:
        return [r for r in self.records if r.getMessage().startswith(prefix)]


@pytest.fixture
def capture():
    handler = _Capture()
    loggers = [logging.getLogger(n) for n in ("dataflow.transfer", "src.routers.connectors_router")]
    levels = [log.level for log in loggers]
    for log in loggers:
        log.addHandler(handler)
        log.setLevel(logging.DEBUG)
    try:
        yield handler
    finally:
        for log, level in zip(loggers, levels):
            log.removeHandler(handler)
            log.setLevel(level)


def _secret_free(records) -> None:
    for record in records:
        for fmt in (JsonFormatter(), TextFormatter()):
            assert TABLE not in fmt.format(record)


def _seed(path: Path, n: int) -> None:
    conn = sqlite3.connect(str(path))
    with conn:
        conn.execute(f"CREATE TABLE {TABLE} (id INTEGER PRIMARY KEY, v TEXT)")
        conn.executemany(f"INSERT INTO {TABLE} VALUES (?, ?)", [(i, f"r{i}") for i in range(n)])
    conn.close()


def _request(src: Path, dst: Path, table: str = TABLE) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table=table),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table=TABLE),
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="strict",
        workspace_id="ws-life",
    )


@pytest.fixture
def fake(monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    return fake


def test_successful_run_emits_requested_started_succeeded(tmp_path, fake, capture):
    src, dst = tmp_path / "s.db", tmp_path / "d.db"
    _seed(src, 25)
    engine = UniversalTransferEngine()
    req = _request(src, dst)
    job_id = engine._create_pending_job(req)
    result = engine.execute_tracked(req, job_id)
    assert result.success, result.error

    (requested,) = capture.named("job.requested")
    assert (requested.outcome, requested.job_id, requested.workspace_id) == ("succeeded", job_id, "ws-life")
    (started,) = capture.named("transfer.run.started")
    (done,) = capture.named("transfer.run.succeeded")
    assert started.levelno == done.levelno == logging.INFO
    assert (done.source_type, done.destination_type, done.sync_mode) == (
        "sqlite", "sqlite", "full_refresh_overwrite",
    )
    assert done.records_transferred == 25
    assert done.duration_ms > 0 and done.job_id == job_id and done.resume is False
    _secret_free(capture.records)


def test_failed_run_result_emits_failed_at_error(tmp_path, fake, capture):
    src, dst = tmp_path / "s.db", tmp_path / "d.db"
    _seed(src, 3)
    engine = UniversalTransferEngine()
    req = _request(src, dst, table="missing_" + TABLE)
    job_id = engine._create_pending_job(req)
    result = engine.execute_tracked(req, job_id)
    assert not result.success
    (done,) = capture.named("transfer.run.failed")
    assert (done.outcome, done.levelno, done.records_transferred) == ("failed", logging.ERROR, 0)
    assert not capture.named("transfer.run.succeeded")
    _secret_free(capture.records)


@pytest.mark.parametrize(
    "exc, event, outcome, level",
    [
        (RuntimeError(TABLE), "transfer.run.failed", "errored", logging.ERROR),
        (asyncio.CancelledError(), "transfer.run.cancelled", "cancelled", logging.INFO),
    ],
)
def test_engine_exception_emits_and_reraises_same(tmp_path, fake, capture, monkeypatch, exc, event, outcome, level):
    engine = UniversalTransferEngine()

    def _boom(*a, **k):
        raise exc

    monkeypatch.setattr(engine, "_execute_tracked_inner", _boom)
    with pytest.raises(type(exc)) as info:
        engine.execute_tracked(_request(tmp_path / "s.db", tmp_path / "d.db"), "job-x")
    assert info.value is exc
    (record,) = capture.named(event)
    assert (record.outcome, record.levelno, record.error_type) == (outcome, level, type(exc).__name__)
    _secret_free(capture.records)


def _api_client(monkeypatch, mongo):
    import importlib

    from fastapi.testclient import TestClient

    from src.main import app

    router = importlib.import_module("src.routers.connectors_router")
    monkeypatch.setattr(router, "get_mongodb_service", lambda: mongo)
    monkeypatch.setattr("src.middleware.auth_middleware._auth_service.auth_required", lambda: False)
    return TestClient(app)


def test_cancel_request_events(monkeypatch, capture):
    from services.mongodb_service import MemoryMongoDBService

    mongo = MemoryMongoDBService()
    running = mongo.create_transfer_job({"_id": "life-cancel-1", "status": "running", "workspace_id": ""})
    done = mongo.create_transfer_job({"_id": "life-cancel-2", "status": "completed", "workspace_id": ""})
    with _api_client(monkeypatch, mongo) as client:
        assert client.post(f"/api/v1/connectors/jobs/{running}/cancel").status_code == 200
        assert client.post(f"/api/v1/connectors/jobs/{done}/cancel").status_code == 200
    first, second = capture.named("job.cancel.requested")
    assert (first.outcome, first.job_id, first.cdc_slot_released) == ("succeeded", running, False)
    assert (second.outcome, second.reason, second.status) == ("skipped", "already_terminal", "completed")


def test_retry_refused_and_forced_events(monkeypatch, capture):
    from tests.test_retry_duplicate_guard import _failed_append_job, _retry_via_api

    res, _ = _retry_via_api(monkeypatch, _failed_append_job())
    assert res.status_code == 409
    res, _ = _retry_via_api(monkeypatch, _failed_append_job(), force=True)
    assert res.status_code == 200, res.text
    (refused,) = capture.named("job.retry.refused")
    assert (refused.outcome, refused.levelno, refused.reason) == ("held", logging.INFO, "duplicate_risk")
    (requested,) = capture.named("job.retry.requested")
    assert (requested.job_id, requested.retry_of, requested.duplicate_risk_acknowledged) == (
        "new-job", "job-append", True,
    )
