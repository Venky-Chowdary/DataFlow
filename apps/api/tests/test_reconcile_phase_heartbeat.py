"""Reconcile-phase heartbeat keeps live UI messaging fresh at 99%."""

from __future__ import annotations

import sys
import time
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
_SRC = _API_ROOT / "src"
for p in (str(_API_ROOT), str(_SRC)):
    if p not in sys.path:
        sys.path.insert(0, p)

from transfer.engine import _reconcile_phase_heartbeat  # noqa: E402


class _FakeMongo:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def update_job_status(self, job_id: str, status: str, **kwargs):
        self.calls.append({"job_id": job_id, "status": status, **kwargs})
        return True


def test_reconcile_heartbeat_sets_phase_and_pulses_message():
    mongo = _FakeMongo()
    with _reconcile_phase_heartbeat(
        mongo,
        "job-abc",
        processed=37_778,
        total=37_778,
        interval_s=0.05,
    ):
        time.sleep(0.14)

    assert mongo.calls, "expected at least the enter-reconcile update"
    first = mongo.calls[0]
    assert first["phase"] == "reconcile"
    assert first["progress_pct"] == 99
    assert first["records_processed"] == 37_778
    assert "reconcil" in (first.get("message") or "").lower()

    # At least one heartbeat pulse after the initial enter update.
    assert len(mongo.calls) >= 2
    assert all(c["phase"] == "reconcile" and c["progress_pct"] == 99 for c in mongo.calls)
    assert any("Reconciling data" in (c.get("message") or "") for c in mongo.calls[1:])
