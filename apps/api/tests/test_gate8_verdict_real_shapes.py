"""``gate8.verify.completed`` classifies real Gate-8 report shapes.

Each case drives ``run_reconciliation`` through the production code path (only
the destination I/O is stubbed, as in the MX2-15 tests) so the event is judged
against the report the job itself acts on — not a hand-written dict.
"""

from __future__ import annotations

import logging
from unittest.mock import patch

import pytest

from services.logging_config import ContextFilter
from services.reconciliation import TargetSampleUnavailable
from src.transfer.models import EndpointConfig
from src.transfer.reconcile_step import run_reconciliation

_DEAD = TargetSampleUnavailable("Could not read destination sample: DBPROCESS is dead")


class _Capture(logging.Handler):
    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.records: list[logging.LogRecord] = []
        self.addFilter(ContextFilter())

    def emit(self, record: logging.LogRecord) -> None:
        if record.getMessage() == "gate8.verify.completed":
            self.records.append(record)


@pytest.fixture
def capture():
    handler = _Capture()
    log = logging.getLogger("src.transfer.reconcile_step")
    level = log.level
    log.addHandler(handler)
    log.setLevel(logging.DEBUG)
    with patch("services.error_handling.time.sleep", lambda _s: None):
        yield handler
    log.removeHandler(handler)
    log.setLevel(level)


def _reconcile(*, target=(1, "abc"), sample=None, source_count=True):
    records = [{"id": "1"}]
    summary = {"schema": "dbo", "table": "t"}
    if source_count:
        summary["source_row_count"] = len(records)
    with patch(
        "src.transfer.reconcile_step.resolve_connector_config",
        return_value={"type": "sqlserver", "database": "db", "schema": "dbo"},
    ), patch(
        "src.transfer.reconcile_step.verify_target", return_value=target,
    ), patch(
        "src.transfer.reconcile_step.read_target_sample",
        side_effect=sample if sample is not None else [[{"id": "1"}]],
    ):
        return run_reconciliation(
            endpoint=EndpointConfig(kind="database", format="sqlserver", database="db", table="t"),
            records=records, columns=["id"], rows_written=len(records),
            writer_checksum="abc", dest_summary=summary,
            mappings=[{"source": "id", "target": "id"}], validation_mode="strict",
        )


def test_unavailable_verification_is_a_hold_not_a_failure(capture):
    recon = _reconcile(sample=_DEAD)
    assert recon["passed"] is False and recon["verification_unavailable"] is True
    (event,) = capture.records
    assert (event.verdict, event.outcome, event.levelno) == ("unavailable", "held", logging.WARNING)
    assert event.job_outcome == "failed"


def test_unproven_failed_closed_report_is_a_hold_not_a_failure(capture):
    """Empty delta with no pre-write destination count: fails closed, unproven."""
    with patch(
        "src.transfer.reconcile_step.resolve_connector_config",
        return_value={"type": "sqlserver", "database": "db", "schema": "dbo"},
    ), patch("src.transfer.reconcile_step.verify_target", return_value=(5, "x")), patch(
        "src.transfer.reconcile_step.read_target_sample", side_effect=[[]],
    ):
        recon = run_reconciliation(
            endpoint=EndpointConfig(kind="database", format="sqlserver", database="db", table="t"),
            records=[], columns=["id"], rows_written=0, writer_checksum="",
            dest_summary={"schema": "dbo", "table": "t", "source_row_count": 0},
            mappings=[{"source": "id", "target": "id"}], validation_mode="strict",
        )
    assert recon["passed"] is False and recon["unproven"] is True
    assert not recon.get("verification_unavailable")
    (event,) = capture.records
    assert (event.verdict, event.outcome, event.levelno) == ("unproven", "held", logging.WARNING)
    assert event.job_outcome == "failed"


def test_export_unproven_by_design_is_skipped_at_info_not_held(capture):
    """A file export cannot be read back by design: ``skipped_readback`` is the
    existing recon signal. The job succeeds; the event must neither claim proof
    (``succeeded``) nor raise alert noise (WARNING)."""
    recon = run_reconciliation(
        endpoint=EndpointConfig(kind="file_export", format="csv", table="t"),
        records=[{"id": "1"}], columns=["id"], rows_written=1, writer_checksum="abc",
        dest_summary={"source_row_count": 1, "checksum": "abc"},
        mappings=[{"source": "id", "target": "id"}],
    )
    assert recon["passed"] is True and recon["unproven"] is True
    assert recon["skipped_readback"] is True
    (event,) = capture.records
    assert (event.verdict, event.outcome, event.levelno) == ("unproven", "skipped", logging.INFO)
    assert event.reason_code == "dest_not_verifiable"
    assert event.job_outcome == "succeeded"


def _pgvector(target):
    with patch(
        "src.transfer.reconcile_step.resolve_connector_config",
        return_value={"type": "pgvector", "database": "db", "schema": "public"},
    ), patch("src.transfer.reconcile_step.verify_target", return_value=target):
        return run_reconciliation(
            endpoint=EndpointConfig(kind="database", format="pgvector", database="db", table="t"),
            records=[{"id": "1"}], columns=["id"], rows_written=1, writer_checksum="abc",
            dest_summary={"schema": "public", "table": "t", "source_row_count": 1},
            mappings=[{"source": "id", "target": "id"}], validation_mode="strict",
        )


def test_opaque_vector_dest_with_measured_identity_is_skipped_at_info(capture):
    recon = _pgvector((1, "abc"))
    assert recon["passed"] is True and recon["skipped_readback"] is True
    (event,) = capture.records
    assert (event.outcome, event.levelno, event.reason_code) == (
        "skipped", logging.INFO, "dest_not_verifiable",
    )
    assert event.job_outcome == "succeeded"


def test_unreadable_vector_dest_that_fails_the_job_stays_held(capture):
    """Same by-design shape, but the identity read failed and the job fails:
    that is a hold, not an expected skip."""
    recon = _pgvector((-1, ""))
    assert recon["passed"] is False and recon["skipped_readback"] is True
    (event,) = capture.records
    assert (event.outcome, event.levelno) == ("held", logging.WARNING)
    assert getattr(event, "reason_code", None) is None
    assert event.job_outcome == "failed"


def test_measured_mismatch_is_failed_at_error(capture):
    recon = _reconcile(target=(2, "zzz"))
    assert recon["passed"] is False
    assert not recon.get("verification_unavailable") and not recon.get("unproven")
    (event,) = capture.records
    assert (event.verdict, event.outcome, event.levelno) == ("failed", "failed", logging.ERROR)
    assert event.job_outcome == "failed"
