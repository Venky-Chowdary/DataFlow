"""Gate-8 verdict, quarantine/replay and CDC cycle operational events.

Counts, verdicts, connector types and position *kind* only — never row values,
table/slot names, or the LSN/binlog/token value itself.
"""

from __future__ import annotations

import logging

import pytest

import services.quarantine_dlq as dlq
import src.transfer.cdc_transfer as cdc_transfer
import src.transfer.reconcile_step as reconcile_step
from services.logging_config import ContextFilter, JsonFormatter, TextFormatter
from src.transfer.models import EndpointConfig

VALUE = "ROWVALUE-SENTINEL-93"
TABLE = "zz_secret_table"
LSN = "0/DEADBEEF"


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
def capture():
    handler = _Capture()
    loggers = [
        logging.getLogger(n)
        for n in ("src.transfer.reconcile_step", "services.quarantine_dlq", "src.transfer.cdc_transfer")
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
            for secret in (VALUE, TABLE, LSN):
                assert secret not in out, (secret, out)


def _reconcile(monkeypatch, recon):
    def fake_impl(**_kwargs):
        if isinstance(recon, BaseException):
            raise recon
        return dict(recon)

    monkeypatch.setattr(reconcile_step, "_run_reconciliation_impl", fake_impl)
    monkeypatch.setattr(reconcile_step, "_apply_unique_rule_checks", lambda *a, **k: None)
    return reconcile_step.run_reconciliation(
        endpoint=EndpointConfig(format="postgresql", table=TABLE),
        records=[{"v": VALUE}], columns=["v"], rows_written=1,
        writer_checksum="x", dest_summary={},
    )


@pytest.mark.parametrize(
    "recon, verdict, outcome, level",
    [
        ({"passed": True, "source_rows": 1, "target_rows": 1, "checksum_match": True},
         "passed", "succeeded", logging.INFO),
        ({"passed": True, "verification_unavailable": True, "source_rows": 1},
         "unproven", "held", logging.WARNING),
        ({"passed": False, "source_rows": 2, "target_rows": 1, "message": VALUE},
         "failed", "failed", logging.ERROR),
    ],
)
def test_gate8_verdict_event(monkeypatch, capture, recon, verdict, outcome, level):
    result = _reconcile(monkeypatch, recon)
    assert result["passed"] is recon["passed"]
    (event,) = capture.named("gate8.verify.completed")
    assert (event.verdict, event.outcome, event.levelno) == (verdict, outcome, level)
    assert (event.destination_type, event.rows_written) == ("postgresql", 1)
    assert event.source_rows == recon.get("source_rows")
    _clean(capture.records)


def test_gate8_exception_emits_errored_and_reraises(monkeypatch, capture):
    boom = RuntimeError(f"driver said {VALUE}")
    with pytest.raises(RuntimeError) as info:
        _reconcile(monkeypatch, boom)
    assert info.value is boom
    (event,) = capture.named("gate8.verify.completed")
    assert (event.outcome, event.error_type) == ("errored", "RuntimeError")
    _clean(capture.records)


def test_quarantine_persist_emits_counts(monkeypatch, capture):
    import services.quarantine_row_contract as contract

    monkeypatch.setattr(dlq, "append_dlq_event", lambda **kw: {"rows": kw["rows"]})
    monkeypatch.setattr(contract, "normalize_quarantine_rows", lambda rows, **_: list(rows))
    monkeypatch.setattr(contract, "assert_quarantine_rows_contract", lambda *a, **k: None)
    out = dlq.persist_rejected_rows(
        job_id="job-q1", workspace_id="ws-1", connector=TABLE,
        rejected_details=[{"row": {"v": VALUE}, "reason": f"bad {VALUE}"}] * 3,
    )
    assert out["rows"] == 3
    (event,) = capture.named("quarantine.rows.persisted")
    assert (event.job_id, event.rows, event.chunks, event.outcome) == ("job-q1", 3, 1, "succeeded")
    _clean(capture.records)


def test_replay_recorded_emits_closure(capture):
    findings = [{"row": {"v": VALUE}, "retry_status": "open"}]
    dlq.record_replay(
        findings, child_rejected=[], gate8_passed=True, child_job_id="job-child",
        rows_written=1, rejected=0,
    )
    (event,) = capture.named("quarantine.replay.recorded")
    assert (event.child_job_id, event.gate8_passed, event.rows_written) == ("job-child", True, 1)
    assert event.closure_verdict
    _clean(capture.records)


@pytest.mark.parametrize(
    "src, has_position, kind",
    [("postgresql", True, "lsn"), ("mysql", True, "binlog"), ("sqlite", True, "cursor"),
     ("postgresql", False, "none")],
)
def test_cdc_cycle_event_counts_lag_and_position_kind(capture, src, has_position, kind):
    cdc_transfer._emit_cdc_cycle(
        job_id="job-c1", workspace_id="ws-1", src_type=src, dest_type="postgresql",
        inserts=3, updates=2, deletes=1,
        lag_fields={"cdc_lag_seconds": 1.5, "replication_lag_bytes": 128,
                    "cdc_lag_basis": "commit_ts", "cdc_slot_name": TABLE},
        delivery="at-least-once", has_position=has_position,
    )
    (event,) = capture.named("cdc.cycle.completed")
    assert (event.inserts, event.updates, event.deletes) == (3, 2, 1)
    assert (event.lag_seconds, event.lag_bytes, event.position_kind) == (1.5, 128, kind)
    assert event.levelno == logging.INFO
    _clean(capture.records)
