"""Every persisted job status transition emits one ``job.status.changed`` event.

Failed terminal transitions log at ERROR, operator-recoverable holds at INFO,
blocked holds at WARNING. Progress ticks (same status) and refused writes emit
nothing, and the event never carries the job's error/message text.
"""

from __future__ import annotations

import json
import logging
import socket

import pytest

from services.logging_config import ContextFilter, JsonFormatter
from services.mongodb_service import (
    MemoryMongoDBService,
    _job_status_outcome,
    emit_job_status_changed,
)

SENTINEL = "SENTINEL-ROWVAL-91c2 postgresql://u:hunter2@db/x"


class _Capture(logging.Handler):
    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.records: list[logging.LogRecord] = []
        self.addFilter(ContextFilter())

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)

    def events(self) -> list[logging.LogRecord]:
        return [r for r in self.records if getattr(r, "event", "") == "job.status.changed"]


@pytest.fixture
def capture():
    log = logging.getLogger("services.mongodb_service")
    handler = _Capture()
    old_level = log.level
    log.addHandler(handler)
    log.setLevel(logging.DEBUG)
    try:
        yield handler
    finally:
        log.removeHandler(handler)
        log.setLevel(old_level)


def _assert_no_sentinel(records: list[logging.LogRecord]) -> None:
    fmt = JsonFormatter()
    for record in records:
        rendered = fmt.format(record)
        assert "SENTINEL-ROWVAL" not in rendered
        assert "hunter2" not in rendered
        json.loads(rendered)


def test_memory_backend_emits_one_event_per_transition(capture):
    mongo = MemoryMongoDBService()
    job_id = mongo.create_transfer_job({"name": "obs", "workspace_id": "ws-42"})
    assert mongo.update_job_status(job_id, "running", phase="writing")
    for n in range(5):
        assert mongo.update_job_status(job_id, "running", records_processed=n * 1000)
    assert mongo.update_job_status(job_id, "failed", error=SENTINEL, message=SENTINEL)

    events = capture.events()
    assert [(r.from_status, r.to_status) for r in events] == [
        ("pending", "running"),
        ("running", "failed"),
    ]
    started, failed = events
    assert started.outcome == "started" and started.levelno == logging.INFO
    assert failed.outcome == "failed" and failed.levelno == logging.ERROR
    for record in events:
        assert record.getMessage() == "job.status.changed"
        assert record.job_id == job_id
        assert record.workspace_id == "ws-42"
        assert record.from_status_source == "prior_read"
    _assert_no_sentinel(capture.records)


def test_refused_write_after_terminal_emits_nothing(capture):
    mongo = MemoryMongoDBService()
    job_id = mongo.create_transfer_job({"name": "obs"})
    assert mongo.update_job_status(job_id, "completed")
    before = len(capture.events())
    assert mongo.update_job_status(job_id, "running") is False
    assert len(capture.events()) == before
    assert capture.events()[-1].outcome == "succeeded"
    assert capture.events()[-1].workspace_id is None


@pytest.mark.parametrize(
    ("status", "outcome", "level"),
    [
        ("failed", "failed", logging.ERROR),
        ("cancelled", "cancelled", logging.INFO),
        ("completed", "succeeded", logging.INFO),
        ("completed_with_quarantine", "succeeded", logging.INFO),
        ("retrying", "retrying", logging.WARNING),
        ("needs_approval", "held", logging.INFO),
        ("blocked", "held", logging.WARNING),
        ("running", "started", logging.INFO),
        ("queued", "started", logging.INFO),
    ],
)
def test_status_outcome_and_level(capture, status, outcome, level):
    assert _job_status_outcome(status)[0] == outcome
    emit_job_status_changed("job-1", "pending", status, "ws-1")
    (record,) = capture.events()
    assert (record.outcome, record.levelno) == (outcome, level)


def test_same_status_is_not_a_transition(capture):
    emit_job_status_changed("job-1", "running", "running", "ws-1")
    assert capture.events() == []


def _mongo_up() -> bool:
    try:
        with socket.create_connection(("127.0.0.1", 27017), timeout=0.4):
            return True
    except OSError:
        return False


@pytest.mark.skipif(not _mongo_up(), reason="MongoDB not reachable on :27017")
def test_live_mongo_backend_emits_transition_with_workspace(capture):
    from bson import ObjectId

    from services.mongodb_service import MongoDBService

    svc = MongoDBService("mongodb://127.0.0.1:27017")
    job_oid = ObjectId()
    job_id = str(job_oid)
    try:
        svc.create_transfer_job(
            {"_id": job_oid, "name": "obs-live", "status": "pending", "workspace_id": "ws-live"}
        )
        assert svc.update_job_status(job_id, "running", operator_command=True)
        assert svc.update_job_status(job_id, "running", records_processed=10, operator_command=True)
        assert svc.update_job_status(job_id, "failed", error=SENTINEL, operator_command=True)
        events = capture.events()
        assert [(r.from_status, r.to_status, r.levelno) for r in events] == [
            ("pending", "running", logging.INFO),
            ("running", "failed", logging.ERROR),
        ]
        assert {r.workspace_id for r in events} == {"ws-live"}
        assert {r.job_id for r in events} == {job_id}
        _assert_no_sentinel(capture.records)
    finally:
        svc.get_database()["transfer_jobs"].delete_one({"_id": job_oid})
