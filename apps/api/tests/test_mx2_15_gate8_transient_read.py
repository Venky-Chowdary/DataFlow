"""QA MX2-15 — one transient destination read failed a committed job.

PostgreSQL -> SQL Server, 120,000 rows committed. During post-write Gate-8 the
destination blipped (tunnel rotation) and the job ended failed with
"Gate-8 sample compare unavailable ... DBPROCESS is dead ... The failed batch
was left in place because the undo could not run." No retry was attempted, and
the failure path tried to *delete* the committed batch because verification
could not read it.

Contract: transient verification reads are retried with backoff. A read that
still cannot run fails closed (never counted as proof) but is marked
``verification_unavailable`` and the committed rows are not undone.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from services.reconciliation import TargetSampleUnavailable
from src.transfer.models import EndpointConfig
from src.transfer.reconcile_step import run_reconciliation

_DBPROCESS_DEAD = TargetSampleUnavailable(
    "Could not read destination sample from 'sqlserver'.'QA_E2E_mx_pg_ms_big': "
    "(20047, b'DBPROCESS is dead or not enabled')"
)


@pytest.fixture(autouse=True)
def _no_backoff_sleep():
    with patch("services.error_handling.time.sleep", lambda _s: None):
        yield


def _endpoint() -> EndpointConfig:
    return EndpointConfig(kind="database", format="sqlserver", database="db", table="t")


def _reconcile(read_side_effect, *, records=None, deletes=None):
    records = [{"id": "1"}] if records is None else records
    summary = {"schema": "dbo", "table": "t", "source_row_count": len(records)}
    if deletes:
        summary["reconcile_deletes"] = deletes
    with patch(
        "src.transfer.reconcile_step.resolve_connector_config",
        return_value={"type": "sqlserver", "database": "db", "schema": "dbo"},
    ), patch(
        "src.transfer.reconcile_step.verify_target",
        return_value=(len(records), "abc"),
    ), patch(
        "src.transfer.reconcile_step.read_target_sample",
        side_effect=read_side_effect,
    ) as reader:
        report = run_reconciliation(
            endpoint=_endpoint(),
            records=records,
            columns=["id"],
            rows_written=len(records),
            writer_checksum="abc",
            dest_summary=summary,
            mappings=[{"source": "id", "target": "id"}],
            validation_mode="strict",
        )
    return report, reader


def test_one_transient_sample_read_is_retried_and_gate8_passes():
    report, reader = _reconcile([_DBPROCESS_DEAD, [{"id": "1"}]])
    assert reader.call_count == 2
    # The retried read reached Gate-8's value compare (the stub checksum is
    # unrelated to this defect and is judged separately).
    assert (report.get("sample_compare") or {}).get("passed") is True, report.get("message")
    assert "unavailable" not in (report.get("message") or "").lower()
    assert not report.get("verification_unavailable")


def test_persistent_transient_read_fails_closed_but_is_marked_unavailable():
    report, reader = _reconcile(_DBPROCESS_DEAD)
    assert reader.call_count >= 2
    assert report["passed"] is False
    assert report["verification_unavailable"] is True
    msg = report["message"].lower()
    assert "sample compare unavailable" in msg
    assert "attempt" in msg


def test_deterministic_read_failure_is_not_retried():
    denied = TargetSampleUnavailable("SELECT permission denied on object 't'")
    report, reader = _reconcile(denied)
    assert reader.call_count == 1
    assert report["passed"] is False
    assert not report.get("verification_unavailable")


def test_transient_delete_proof_read_is_retried():
    report, reader = _reconcile([_DBPROCESS_DEAD, []], records=[], deletes=["1"])
    assert reader.call_count == 2
    assert "delete proof unavailable" not in (report.get("message") or "").lower()


def test_unavailable_verification_does_not_undo_committed_rows(monkeypatch):
    import services.batch_undo as batch_undo
    import src.transfer.engine as engine

    def _boom(**_kw):
        raise AssertionError("committed rows must not be undone when verification could not run")

    monkeypatch.setattr(batch_undo, "undo_failed_batch_if_dest_was_empty", _boom)
    summary = {"target_rows_before": 0, "table": "t"}
    msg = engine._note_failed_batch_undo(
        SimpleNamespace(destination=_endpoint(), sync_mode="full_refresh_overwrite"),
        summary,
        "Gate-8 sample compare unavailable",
        recon={"passed": False, "verification_unavailable": True},
    )
    assert summary["partial_batch_undo"] == "retained"
    assert "committed" in msg.lower()
