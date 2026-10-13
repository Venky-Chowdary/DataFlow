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

from src.transfer.reconcile_heartbeat import reconcile_heartbeat_scope  # noqa: E402
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


def test_write_pass_heartbeat_does_not_claim_migration_proven():
    mongo = _FakeMongo()
    with _reconcile_phase_heartbeat(
        mongo,
        "job-file-1m",
        processed=1_000_000,
        total=1_000_000,
        interval_s=0.05,
        proof_kind="inline_write_pass",
    ):
        time.sleep(0.12)

    first = mongo.calls[0]["message"]
    assert "write-pass" in first.lower()
    assert "not migration_proven" in first.lower()
    assert any("write-pass" in (c.get("message") or "").lower() for c in mongo.calls[1:])


def test_multi_stream_heartbeat_does_not_call_the_job_total_checksum_proof():
    mongo = _FakeMongo()
    scope = reconcile_heartbeat_scope({
        "checksum_mode": "source_reread",
        "multi_stream": True,
        "table": "orders",
        "streams": [{"name": "customers"}, {"name": "orders"}],
    })
    with _reconcile_phase_heartbeat(
        mongo,
        "job-multi",
        processed=4,
        total=4,
        interval_s=0.05,
        **scope,
    ):
        time.sleep(0.12)

    first = mongo.calls[0]["message"]
    assert "4 rows written across 2 tables" in first
    assert "last stream (orders)" in first
    assert "not this total" in first
    assert "counts + checksum proof" not in first
    assert any("Reconciling data" in (c.get("message") or "") for c in mongo.calls[1:])
    assert any("not the whole job" in (c.get("message") or "") for c in mongo.calls[1:])


def test_checksum_publishes_hashed_rows_without_moving_the_job_counter():
    """The scan count moves. The written-row counter and the 99% pin do not."""
    from services.fingerprint_accumulator import FingerprintAccumulator

    mongo = _FakeMongo()
    with _reconcile_phase_heartbeat(
        mongo,
        "job-1m",
        processed=1_000_000,
        total=1_000_000,
        interval_s=0.05,
    ):
        acc = FingerprintAccumulator()
        for i in range(9000):
            acc.add(str(i), f"fp-{i}")
        acc.digest()
        time.sleep(0.12)

    pulses = [c.get("message") or "" for c in mongo.calls[1:]]
    assert any("9,000" in message and "row fingerprints" in message for message in pulses)
    assert any("has not stalled" in message for message in pulses)
    assert all(c["progress_pct"] == 99 for c in mongo.calls)
    assert all(c["records_processed"] == 1_000_000 for c in mongo.calls)
