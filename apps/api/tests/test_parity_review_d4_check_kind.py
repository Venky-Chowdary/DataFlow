"""Review D4: the parity check kind re-derived assurance instead of asking
``signed_proof_pack.classify_post_write_assurance`` (and the
``reconcile_coverage`` scopes), so it disagreed with the proof pack: a
pre-write preview or a writer acknowledgement that carried a sample read as a
"sampled" value comparison, and a CDC count-only scope as more than a count.
"""

from __future__ import annotations

import pytest

from services.parity_evidence import _check_kind


def _sample(**over) -> dict:
    recon = {"passed": True, "source_rows": 100, "target_rows": 100,
             "sample_compare": {"rows_compared": 50, "mismatches": []}}
    recon.update(over)
    return recon


@pytest.mark.parametrize("recon, kind", [
    (_sample(post_write_pending=True, phase="pre_write_simulation"), "none"),
    (_sample(preview=True), "none"),
    (_sample(coverage="writer_ack"), "count_only"),
    (_sample(checksum_scope="cdc_source_image_count"), "count_only"),
    (_sample(), "sampled"),
    ({"passed": True, "source_rows": 10, "target_rows": 10, "culprit_census_clean": True}, "key_diff"),
    ({"passed": True, "source_rows": 10, "target_rows": 10}, "count_only"),
])
def test_the_check_kind_follows_the_proof_classifier(recon, kind):
    assert _check_kind(recon) == kind


def test_a_preview_is_never_a_verdict_of_parity():
    from services.parity_report import build_parity_report

    job = {"_id": "j", "status": "completed",
           "reconciliation": _sample(post_write_pending=True, phase="pre_write_simulation"),
           "destination_summary": {"table": "t", "primary_key_columns": ["id"], "sync_mode": "full_refresh_overwrite"}}
    assert build_parity_report(job, reread=False)["verdict"] == "unverifiable"
