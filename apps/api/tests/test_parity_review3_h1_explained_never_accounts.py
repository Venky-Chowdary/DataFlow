"""Re-review 3, H1: an explained difference never accounts for a failed Gate-8.

The Gate-8 failure guard accepted any surviving "localizing" difference,
explained ones included: an L3 checksum mismatch that named nothing, next to
10 quarantined rows (or a witnessed concurrent update), read
``no_difference_found`` / ``explained`` with sign-off open. Only an
unexplained difference Gate-8 localized (or one an operator accepted) now
accounts for the failure.
"""

from __future__ import annotations

import pytest

from services.parity_report import build_parity_report

LAYERS = {"L1": {"passed": True}, "L3": {"passed": False, "details": {"source": "a", "target": "b"}}}


def _job(recon: dict, **extra) -> dict:
    job = {"_id": "j", "status": "completed", "reconciliation": recon,
           "destination_summary": {"table": "t", "primary_key_columns": ["id"], "sync_mode": "full_refresh_overwrite"}}
    job.update(extra)
    return job


@pytest.mark.parametrize("recon", [
    {"passed": False, "source_rows": 1000, "target_rows": 990, "rejected_rows": 10,
     "sample_compare": {"rows_compared": 1000, "mismatches": []}, "verification_ladder": {"layers": LAYERS}},
    {"passed": False, "source_rows": 1000, "target_rows": 1000,
     "concurrent_source_drift": {"count": 1, "sample_keys": [5]},
     "sample_compare": {"rows_compared": 1000, "mismatches": []}, "verification_ladder": {"layers": LAYERS}},
    {"passed": False, "source_rows": 1000, "target_rows": 990, "rejected_rows": 10, "culprit_census_clean": True,
     "verification_ladder": {"layers": LAYERS}},
], ids=["quarantine", "witness_drift", "census_clean_quarantine"])
def test_explained_differences_do_not_account_for_a_failed_gate8(recon):
    report = build_parity_report(_job(recon), reread=False)
    assert report["verdict"] == "unverifiable", report["verdict_reason"]
    assert "Gate-8 failed" in report["verdict_reason"]
    assert report["signoff"]["status"] == "blocked"
    assert any(f["action"] == "rerun" for f in report["fixes"])


def test_an_unexplained_localized_difference_still_accounts():
    report = build_parity_report(_job({
        "passed": False, "source_rows": 100, "target_rows": 100, "culprit_keys": {"changed": ["7"]},
        "verification_ladder": {"layers": LAYERS},
    }), reread=False)
    assert report["verdict"] == "unexplained"
    assert "Gate-8 failed" not in report["verdict_reason"]


def test_an_accepted_localized_difference_accounts():
    recon = {"passed": False, "source_rows": 100, "target_rows": 100, "culprit_keys": {"changed": ["7"]},
             "verification_ladder": {"layers": LAYERS}}
    did = build_parity_report(_job(recon), reread=False)["differences"][0]["id"]
    report = build_parity_report(_job(recon, parity={"version": 1, "acceptances": {
        did: {"by": "a@x", "note": "known", "keys": ["7"], "keyless_rows": 0, "rows": 1}}}), reread=False)
    assert report["verdict"] == "explained", report["verdict_reason"]
