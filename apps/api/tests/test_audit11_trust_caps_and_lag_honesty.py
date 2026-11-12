"""Audit finding 11 (M1, M2) — trust verdict caps and CDC lag honesty.

M1. The trust score is a weighted mean in which a missing factor's weight was
redistributed, so less evidence could score higher ("no Gate-8 report" 84
above "Gate-8 ran, unproven" 78), and ``row_fidelity_score`` was read before
``passed is False``: a failed Gate-8 with 0.999 fidelity on a completed job
scored 89, grade B, tone ok.

M2. Up to 1 MiB of unapplied log read as "0 s, caught up, ok" — also when the
consumer had been dead for hours — and ``merge_freshness_severity('ok',
'unknown')`` was ``ok``.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from services.cdc_lag_honesty import (
    CATCH_UP_BYTES,
    HEARTBEAT_STALE_SECONDS,
    LAG_BASIS_WAL_BYTES,
    fold_evidence_severity,
    merge_freshness_severity,
    observe_cdc_lag,
)
from services.job_trust import compute_job_trust

_FULL = {
    "passed": True, "phase": "post_write", "coverage": "full_checksum",
    "assurance_level": "full_checksum", "checksum_match": True,
    "source_checksum": "abc", "target_checksum": "abc",
    "source_checksum_provenance": "independent_source_reread",
    "identity_hash_aligned": True, "verification_verdict": "verified",
}
_SAMPLE = {"passed": True, "assurance_level": "sample"}
_WRITER_ACK = {"passed": True, "assurance_level": "writer_ack", "source_checksum": "a"}
_UNPROVEN = {"passed": True, "unproven": True, "assurance_level": "none"}


def _trust(status="completed", **job):
    return compute_job_trust({"status": status, "records_processed": 1000, **job})


# ── M1: verdict caps ────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "status", ["completed", "completed_with_warnings", "completed_with_quarantine", "failed"]
)
@pytest.mark.parametrize(
    "recon",
    [
        {"passed": False, "message": "checksum mismatch"},
        {"passed": False, "row_fidelity_score": 0.999},
        {"passed": False, "row_fidelity_score": 1},
        {"passed": False, "row_fidelity_score": 100, "assurance_level": "full_checksum"},
    ],
)
def test_failed_gate8_never_above_F(status, recon):
    trust = _trust(status, reconciliation=recon)
    assert trust["grade"] == "F" and trust["score"] < 55
    assert trust["tone"] == "danger"
    reconcile = next(f for f in trust["factors"] if f["id"] == "reconcile")
    assert reconcile["score"] <= 18


def test_trust_monotone_in_evidence():
    full = _trust(reconciliation=_FULL)["score"]
    sample = _trust(reconciliation=_SAMPLE)["score"]
    writer_ack = _trust(reconciliation=_WRITER_ACK)["score"]
    unproven = _trust(reconciliation=_UNPROVEN)["score"]
    no_report = _trust()["score"]
    failed = _trust(reconciliation={"passed": False})["score"]
    assert full >= sample >= writer_ack >= unproven >= no_report > failed, (
        full, sample, writer_ack, unproven, no_report, failed,
    )
    assert full >= 90


def test_no_report_never_outranks_ran_but_unproven():
    no_report = _trust()
    unproven = _trust(reconciliation=_UNPROVEN)
    assert no_report["score"] <= unproven["score"]
    # Neither is more than grade C, and neither reads as healthy.
    for trust in (no_report, unproven):
        assert trust["score"] < 80 and trust["tone"] != "ok"
    # Missing evidence still lowers confidence.
    assert no_report["confidence"] == "low"
    reconcile = next(f for f in no_report["factors"] if f["id"] == "reconcile")
    assert reconcile["present"] is False and reconcile["score"] is not None


def test_completed_with_warnings_is_terminal_not_in_progress():
    trust = _trust("completed_with_warnings", reconciliation=_FULL)
    completeness = next(f for f in trust["factors"] if f["id"] == "completeness")
    assert "progress" not in completeness["note"].lower()
    assert trust["tone"] != "muted"
    assert trust["score"] < _trust("completed", reconciliation=_FULL)["score"]


def test_a_verified_job_is_unchanged_by_the_caps():
    trust = _trust(reconciliation=_FULL)
    assert trust["grade"] == "A" and trust["tone"] == "ok"


# ── M2: lag honesty ─────────────────────────────────────────────────────────

_NOW = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)


def test_lag_not_zero_with_unapplied_bytes_and_stale_heartbeat():
    obs = observe_cdc_lag(
        last_event_commit_at=_NOW - timedelta(hours=3),
        last_heartbeat_at=_NOW - timedelta(hours=3),
        replication_lag_bytes=900_000,
        now=_NOW,
    )
    assert obs["caught_up"] is False
    assert obs["cdc_lag_seconds"] is None
    assert obs["freshness_severity"] == "unknown"
    assert obs["consumer_alive"] is False
    assert "heartbeat" in obs["cdc_lag_unknown_reason"]


def test_caught_up_needs_zero_unapplied_bytes_and_a_live_consumer():
    fresh = _NOW - timedelta(seconds=5)
    caught = observe_cdc_lag(last_heartbeat_at=fresh, replication_lag_bytes=0, now=_NOW)
    assert caught["caught_up"] is True and caught["cdc_lag_seconds"] == 0.0
    assert caught["freshness_severity"] == "ok" and caught["cdc_lag_basis"] == LAG_BASIS_WAL_BYTES

    # Unapplied log inside the jitter band: healthy, but never "0 s, caught up".
    band = observe_cdc_lag(
        last_event_commit_at=_NOW - timedelta(hours=2), last_heartbeat_at=fresh,
        replication_lag_bytes=4096, now=_NOW,
    )
    assert band["caught_up"] is False and band["cdc_lag_seconds"] is None
    assert band["freshness_severity"] == "ok"
    assert "4096" in band["cdc_lag_unknown_reason"]
    assert 4096 <= CATCH_UP_BYTES

    # Zero bytes read by a consumer that then went silent proves nothing now.
    stale = observe_cdc_lag(
        last_heartbeat_at=_NOW - timedelta(seconds=HEARTBEAT_STALE_SECONDS + 1),
        replication_lag_bytes=0, now=_NOW,
    )
    assert stale["caught_up"] is False and stale["cdc_lag_seconds"] is None
    assert stale["freshness_severity"] == "unknown"

    # No heartbeat on record: liveness is not proven, so not "caught up".
    unknown_liveness = observe_cdc_lag(replication_lag_bytes=0, now=_NOW)
    assert unknown_liveness["caught_up"] is False
    assert unknown_liveness["consumer_alive"] is None


def test_a_stale_heartbeat_never_hides_a_byte_alarm():
    obs = observe_cdc_lag(
        last_heartbeat_at=_NOW - timedelta(hours=1),
        replication_lag_bytes=128 * 1024 * 1024, now=_NOW,
    )
    assert obs["freshness_severity"] == "critical"


def test_unknown_outranks_ok_in_a_worst_of_merge():
    assert merge_freshness_severity("ok", "unknown") == "unknown"
    assert merge_freshness_severity("unknown", "ok", "ok") == "unknown"
    assert merge_freshness_severity("ok", "ok") == "ok"
    assert merge_freshness_severity("unknown", "warn") == "warn"
    assert merge_freshness_severity("ok", "critical", "unknown") == "critical"
    assert merge_freshness_severity() == "unknown"
    assert merge_freshness_severity(None, "") == "unknown"
    # Folding two readings of ONE pipeline: a reading with nothing to say
    # ("unknown") does not erase a proven one.
    assert fold_evidence_severity("unknown", "ok") == "ok"
    assert fold_evidence_severity("ok", "warn") == "warn"
    assert fold_evidence_severity() == "unknown"


def test_trust_freshness_does_not_call_unapplied_bytes_caught_up():
    behind = _trust(reconciliation=_FULL, replication_lag_bytes=900_000)
    note = next(f for f in behind["factors"] if f["id"] == "freshness")["note"]
    assert "caught up" not in note.lower() and "900,000" in note
    zero = _trust(reconciliation=_FULL, replication_lag_bytes=0)
    assert "caught up" in next(f for f in zero["factors"] if f["id"] == "freshness")["note"].lower()
