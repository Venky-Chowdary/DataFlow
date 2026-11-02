"""Contract for the canonical operational event helper and log context."""

from __future__ import annotations

import asyncio
import json
import logging

import pytest

from services import ops_events
from services.logging_config import (
    ContextFilter,
    JsonFormatter,
    TextFormatter,
    current_job_id,
    job_log_context,
    log_context,
)
from services.ops_events import emit_event, operation, validate_event

SENTINEL = "SENTINEL-PII-7f3a"


class _Capture(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.records: list[logging.LogRecord] = []
        self.addFilter(ContextFilter())

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


@pytest.fixture()
def cap():
    logger = logging.getLogger("dataflow.test.ops_events")
    handler = _Capture()
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    try:
        yield logger, handler
    finally:
        logger.removeHandler(handler)
        logger.propagate = True


def _rendered(record: logging.LogRecord) -> str:
    return JsonFormatter().format(record) + "\n" + TextFormatter().format(record)


@pytest.mark.parametrize(
    "name", ["job.status.changed", "pilot.tool.executed", "gate8.verdict", "a1.b_2"]
)
def test_valid_event_names(name):
    validate_event(name, "succeeded")


@pytest.mark.parametrize(
    "name", ["job", "Job.status", "job.status changed", "job.7x", "job..x", "job.x.", "", None]
)
def test_invalid_event_names_rejected(name):
    with pytest.raises(ValueError):
        validate_event(name, "succeeded")


def test_invalid_outcome_rejected():
    with pytest.raises(ValueError):
        validate_event("job.status.changed", "done")


def test_invalid_name_degrades_to_warning_without_echoing_it(cap):
    logger, handler = cap
    emit_event(logger, f"job.{SENTINEL}", outcome="succeeded")
    (record,) = handler.records
    assert record.levelno == logging.WARNING
    assert record.getMessage() == ops_events.INVALID_EVENT
    assert record.contract_violation == "event_name"
    assert SENTINEL not in _rendered(record)


def test_invalid_outcome_degrades_to_errored_warning(cap):
    logger, handler = cap
    emit_event(logger, "job.status.changed", outcome="done", level=logging.INFO)
    (record,) = handler.records
    assert record.levelno == logging.WARNING
    assert record.outcome == "errored"
    assert record.contract_violation == "outcome"


@pytest.mark.parametrize(
    "outcome,level",
    [
        ("started", logging.INFO),
        ("succeeded", logging.INFO),
        ("skipped", logging.INFO),
        ("cancelled", logging.INFO),
        ("retrying", logging.WARNING),
        ("held", logging.WARNING),
        ("failed", logging.ERROR),
        ("errored", logging.ERROR),
    ],
)
def test_default_levels(cap, outcome, level):
    logger, handler = cap
    emit_event(logger, "job.status.changed", outcome=outcome)
    (record,) = handler.records
    assert record.levelno == level
    assert record.getMessage() == "job.status.changed"
    assert record.event == "job.status.changed"
    assert record.outcome == outcome


def test_caller_level_override(cap):
    logger, handler = cap
    emit_event(logger, "preflight.run.completed", outcome="held", level=logging.INFO)
    assert handler.records[0].levelno == logging.INFO


def test_error_logs_type_and_category_never_message_or_traceback(cap):
    logger, handler = cap
    err = RuntimeError(f"duplicate key value {SENTINEL} password=hunter2")
    emit_event(logger, "job.run.finished", outcome="failed", error=err)
    (record,) = handler.records
    assert record.error_type == "RuntimeError"
    assert record.error_category == "unknown"
    assert record.exc_info is None
    out = _rendered(record)
    assert SENTINEL not in out and "hunter2" not in out


def test_error_category_reuses_connection_loss_classifier():
    assert ops_events.error_category(TimeoutError()) == "timeout"
    assert ops_events.error_category(ConnectionResetError("connection reset by peer")) == (
        "connection_lost"
    )
    assert ops_events.error_category(ValueError("bad")) == "unknown"


def test_secret_fields_redacted_in_json_and_text(cap):
    logger, handler = cap
    emit_event(
        logger,
        "notification.delivery.failed",
        outcome="failed",
        password=SENTINEL,
        token=SENTINEL,
        endpoint=f"https://ops:{SENTINEL}@hooks.example.invalid/x",
        note=f"Authorization=Bearer {SENTINEL}",
    )
    (record,) = handler.records
    out = _rendered(record)
    assert SENTINEL not in out
    payload = json.loads(JsonFormatter().format(record))
    assert payload["password"] == "***"
    assert payload["msg"] == "notification.delivery.failed"


def test_non_scalars_and_reserved_keys_dropped_and_counted(cap):
    logger, handler = cap
    emit_event(
        logger,
        "quarantine.rows.persisted",
        outcome="succeeded",
        rows=[SENTINEL],
        sample={"email": SENTINEL},
        message="clobber",
        name="clobber",
        rows_quarantined=3,
        ok=True,
        ratio=0.5,
        missing=None,
    )
    (record,) = handler.records
    assert record.dropped_fields == 4
    assert record.rows_quarantined == 3 and record.ok is True and record.ratio == 0.5
    assert record.event == "quarantine.rows.persisted"
    assert record.name == "dataflow.test.ops_events"
    assert SENTINEL not in _rendered(record)


def test_long_strings_truncated(cap):
    logger, handler = cap
    emit_event(logger, "job.status.changed", outcome="succeeded", object_id="x" * 1000)
    assert len(handler.records[0].object_id) == ops_events.MAX_FIELD_CHARS


def test_never_raises_on_broken_logger(caplog):
    caplog.set_level(logging.WARNING, logger=ops_events.__name__)
    emit_event(None, "job.status.changed", outcome="succeeded")  # type: ignore[arg-type]
    assert any(r.getMessage() == ops_events.EMIT_FAILED_EVENT for r in caplog.records)


def test_operation_success_records_duration_and_result_fields(cap):
    logger, handler = cap
    with operation(logger, "rules.compile", emit_start=True, object_kind="workbook") as res:
        res["rule_count"] = 4
    started, done = handler.records
    assert started.getMessage() == "rules.compile.started"
    assert done.getMessage() == "rules.compile.succeeded"
    assert done.levelno == logging.INFO
    assert done.rule_count == 4 and done.object_kind == "workbook"
    assert isinstance(done.duration_ms, float) and done.duration_ms >= 0


def test_operation_reraises_identical_exception(cap):
    logger, handler = cap
    boom = KeyError(SENTINEL)
    with pytest.raises(KeyError) as info:
        with operation(logger, "rules.apply"):
            raise boom
    assert info.value is boom
    (record,) = handler.records
    assert record.getMessage() == "rules.apply.failed"
    assert record.levelno == logging.ERROR
    assert record.error_type == "KeyError"
    assert record.duration_ms >= 0
    assert SENTINEL not in _rendered(record)


@pytest.mark.parametrize(
    "interrupt", [asyncio.CancelledError(), KeyboardInterrupt(), GeneratorExit()]
)
def test_operation_cancellation_emits_cancelled_info_and_reraises_same(cap, interrupt):
    logger, handler = cap
    with pytest.raises(type(interrupt)) as info:
        with operation(logger, "transfer.run", object_kind="table") as res:
            res["rows_written"] = 7
            raise interrupt
    assert info.value is interrupt
    (record,) = handler.records
    assert record.getMessage() == "transfer.run.cancelled"
    assert record.outcome == "cancelled"
    assert record.levelno == logging.INFO
    assert record.error_type == type(interrupt).__name__
    assert record.rows_written == 7
    assert record.duration_ms >= 0


def test_log_context_stamps_and_restores(cap):
    logger, handler = cap
    with log_context(job_id="j1", workspace_id="w1", schedule_id="s1", run_id="r1",
                     attempt=2, phase="reconcile"):
        with log_context(phase="write"):
            emit_event(logger, "job.status.changed", outcome="succeeded")
        emit_event(logger, "job.status.changed", outcome="succeeded")
    emit_event(logger, "job.status.changed", outcome="succeeded")
    inner, outer, after = handler.records
    assert (inner.job_id, inner.workspace_id, inner.schedule_id, inner.run_id,
            inner.attempt, inner.phase) == ("j1", "w1", "s1", "r1", "2", "write")
    assert outer.phase == "reconcile"
    assert after.job_id == "" and after.workspace_id == "" and after.phase == ""
    payload = json.loads(JsonFormatter().format(inner))
    assert payload["workspace_id"] == "w1" and payload["attempt"] == "2"
    assert "workspace_id=w1" in TextFormatter().format(inner)


def test_log_context_ignores_unknown_keys():
    with log_context(job_id="j2", customer_email=SENTINEL):
        assert current_job_id() == "j2"
    assert current_job_id() == ""


def test_job_log_context_unchanged():
    with job_log_context("  j3 "):
        assert current_job_id() == "j3"
        with job_log_context(None):
            assert current_job_id() == ""
        assert current_job_id() == "j3"
    assert current_job_id() == ""


def test_reconcile_heartbeat_thread_keeps_job_context():
    import time

    from src.transfer.reconcile_heartbeat import reconcile_phase_heartbeat

    seen: list[tuple[str, bool]] = []

    class _Mongo:
        def update_job_status(self, job_id, status, **kwargs):
            import threading

            seen.append((current_job_id(), threading.current_thread().name.startswith(
                "reconcile-heartbeat")))
            return True

    with job_log_context("job-hb-1"):
        with reconcile_phase_heartbeat(_Mongo(), "job-hb-1", processed=10, total=10,
                                       interval_s=0.02):
            time.sleep(0.12)
    pulses = [job for job, in_thread in seen if in_thread]
    assert pulses, "expected at least one pulse from the heartbeat thread"
    assert set(pulses) == {"job-hb-1"}
