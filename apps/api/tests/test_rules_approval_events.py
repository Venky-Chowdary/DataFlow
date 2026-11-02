"""Business-rule compile/apply and schedule approval lifecycle events.

Counts, fingerprints, ids and finding codes only — never rule text, column
names, row values, the operator's reason, or the actor's identity.
"""

from __future__ import annotations

import logging

import pytest

import services.schedule_store as store
from services.logging_config import ContextFilter, JsonFormatter, TextFormatter
from services.rule_compiler import compile_rule_workbook
from services.rule_compiler.apply import apply_compiled_projection
from services.rule_compiler.ingest import RuleIngestError
from services.schedule_approvals import (
    approve_request,
    build_approval_request,
    open_approval_request,
    reject_request,
)
from services.standing_authorization import binding_from_schedule

COLUMN = "zz_secret_col"
VALUE = "ROWVALUE-SENTINEL-41"
ACTOR = "secret.actor@example.com"
REASON = "REASON-SENTINEL-77"
FINDING = "FINDING-SENTINEL-88"


class _Capture(logging.Handler):
    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.records: list[logging.LogRecord] = []
        self.addFilter(ContextFilter())

    def emit(self, record: logging.LogRecord) -> None:
        if getattr(record, "event", None):
            self.records.append(record)

    def named(self, name: str) -> list[logging.LogRecord]:
        return [r for r in self.records if r.getMessage() == name]


@pytest.fixture
def capture(tmp_path, monkeypatch):
    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(store, "_mongo_backend", lambda: None)
    handler = _Capture()
    loggers = [
        logging.getLogger(n)
        for n in (
            "services.rule_compiler.compile",
            "services.rule_compiler.apply",
            "services.schedule_approvals",
        )
    ]
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


def _clean(records) -> None:
    for record in records:
        for fmt in (JsonFormatter(), TextFormatter()):
            out = fmt.format(record)
            for secret in (COLUMN, VALUE, ACTOR, REASON, FINDING, "rules_secret.csv"):
                assert secret not in out, (secret, out)


def test_compile_and_apply_emit_counts_only(capture):
    csv = f"Source Column,Destination Column,Rule\n{COLUMN},{COLUMN}_out,Direct\n".encode()
    report = compile_rule_workbook(
        "rules_secret.csv", csv, source_columns=[COLUMN], dest_columns=[f"{COLUMN}_out"]
    )
    (compiled,) = capture.named("rules.compile.succeeded")
    assert compiled.levelno == logging.INFO
    assert (compiled.rule_count, compiled.executable) == (1, 1)
    assert compiled.report_fingerprint == report["report_fingerprint"]
    assert compiled.duration_ms >= 0

    applied = apply_compiled_projection(
        report, source_table="", rows=[{COLUMN: VALUE}], source_columns=[COLUMN]
    )
    (event,) = capture.named("rules.apply.succeeded")
    assert (event.written, event.refused, event.source_rows) == (
        applied["written"], applied["refused"], 1,
    )
    _clean(capture.records)


def test_compile_failure_emits_failed_and_reraises(capture):
    with pytest.raises(RuleIngestError):
        compile_rule_workbook("rules_secret.csv", b"")
    (event,) = capture.named("rules.compile.failed")
    assert (event.levelno, event.error_type) == (logging.ERROR, "RuleIngestError")
    _clean(capture.records)


def _parked():
    sched = store.create_schedule({
        "name": "n", "source_connector_id": "s", "source_table": "t",
        "dest_connector_id": "d", "dest_table": "T", "interval": "daily",
        "mappings": [{"source": "a", "target": "A"}],
    })
    request = build_approval_request(
        kind="source_drift", code="SOURCE_SCHEMA_DRIFT", finding=FINDING,
        corrective_action="confirm", binding=binding_from_schedule(sched),
    )
    open_approval_request(sched.id, request)
    return sched, request


def test_approval_requested_then_approved(capture):
    sched, request = _parked()
    open_approval_request(sched.id, request)
    first, second = capture.named("approval.requested")
    assert (first.outcome, first.levelno, first.schedule_id) == ("held", logging.INFO, sched.id)
    assert (first.approval_id, first.code) == (request["id"], "SOURCE_SCHEMA_DRIFT")
    assert (first.occurrences, second.occurrences) == (1, 2)
    approve_request(sched.id, request["id"], actor=ACTOR, reason=REASON)
    (approved,) = capture.named("approval.approved")
    assert (approved.outcome, approved.standing_authorization) == ("succeeded", False)
    _clean(capture.records)


def test_approval_rejected(capture):
    sched, request = _parked()
    reject_request(sched.id, request["id"], actor=ACTOR, reason=REASON)
    (rejected,) = capture.named("approval.rejected")
    assert (rejected.schedule_disabled, rejected.approval_id) == (True, request["id"])
    _clean(capture.records)
