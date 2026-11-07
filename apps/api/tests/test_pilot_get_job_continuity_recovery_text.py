"""Pilot names the Studio control, not raw MCP/REST calls, for a continuity failure.

Browser QA: "why did it fail" on a run whose destination table was dropped
answered with the engine sentence "Clear the cursor (MCP tool
clear_cursor_resnapshot, or POST /api/v1/ops/cdc-cursors/clear) ..." and
offered "Review column mappings" — neither is the operator's recovery. The
Run step's Rebuild with full refresh is.
"""
from __future__ import annotations

import pytest

from services.preflight_cursor_gate import operator_failure_text

_RESET = (
    "Destination reset since the last incremental run: the stored watermark '5' on "
    "'updated_at' claims those rows are already at rest, but the destination table is "
    "missing — resuming would skip the whole history and report success. Clear the "
    "cursor (MCP tool clear_cursor_resnapshot with cursor_key 'k', or POST "
    "/api/v1/ops/cdc-cursors/clear) to re-snapshot, or rebaseline the route (MCP tool "
    "rebaseline_transfer), or point the run at the destination that holds the history."
)


def _job(error: str, reason: str) -> dict:
    return {
        "_id": "job-1",
        "status": "failed",
        "error": error,
        "error_details": {"reason": reason},
        "transfer_request": {
            "source": {"table": "orders", "format": "postgresql"},
            "destination": {"table": "orders", "format": "sqlite"},
            "mappings": [],
            "sync_mode": "incremental_append",
        },
    }


def _get_job(monkeypatch, job: dict):
    monkeypatch.setattr(
        "src.ai.copilot.job_reads.read_transfer_job", lambda job_id: job, raising=True
    )
    from src.ai.copilot.tools import DataPilotTools

    res = DataPilotTools().execute("get_job", {"job_id": "job-1"})
    assert res.success is True, res.error
    return res.output


def test_cursor_reset_failure_drops_api_recovery_prose(monkeypatch):
    out = _get_job(monkeypatch, _job(_RESET, "cursor_destination_reset"))
    assert out["error"].endswith("report success.")
    assert "clear_cursor" not in out["error"]
    assert "POST" not in out["error"]
    labels = [r["label"] for r in out["suggested_remediations"]]
    assert any("Rebuild with full refresh" in label for label in labels), labels
    assert not any("mapping" in label.lower() for label in labels), labels


def test_continuity_mismatch_drops_rebaseline_prose():
    msg = (
        "Append continuity — the table no longer exists. Nothing was written. "
        "To recover: accept the removal (MCP tool rebaseline_transfer; audited)."
    )
    assert operator_failure_text(msg, "destination_continuity_mismatch") == (
        "Append continuity — the table no longer exists. Nothing was written."
    )


@pytest.mark.parametrize("reason", ["", "transfer_failed", None])
def test_other_failures_keep_their_full_message(monkeypatch, reason):
    out = _get_job(monkeypatch, _job(_RESET, reason or ""))
    assert out["error"] == _RESET
    labels = [r["label"] for r in out["suggested_remediations"]]
    assert "Review column mappings" in labels
