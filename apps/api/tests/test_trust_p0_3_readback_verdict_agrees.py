"""Trust audit P0-3 (F-3): a mirror / SCD2 read-back verdict agrees with its own confidence.

Regression for the read-only probe ``probe_readback.py``: a mirror job whose
Gate-8 digests matched returned ``no_difference_found`` while the confidence
block said "No value comparison ran for this job", and the web drew it as "No
difference in sample" with a bound that does not exist.

A value comparison did run: the destination digest is a streamed re-read of
every live row after the write (D394, proven live in
``test_trust_d394_mirror_readback_provenance.py``), compared with the digest of
the rows the run read. It is not an independent source re-read and a digest
names no row, so the report says exactly that and never "proven".
"""

from __future__ import annotations

import pytest

from services.parity_report import build_parity_report, compact_summary, public_report
from services.parity_summary import _cell


def _job(sync_mode: str = "mirror", **recon_over) -> dict:
    recon = {
        "passed": True, "source_rows": 10, "target_rows": 10,
        "source_checksum": "abc", "target_checksum": "abc", "checksum_match": True,
        "phase": "post_write_verified", "coverage": "readback",
    }
    recon.update(recon_over)
    return {
        "_id": "j1", "status": "completed",
        "request": {"sync_mode": sync_mode, "destination": {"table": "t"}},
        "destination_summary": {"sync_mode": sync_mode, "table": "t"},
        "reconciliation": recon,
    }


@pytest.mark.parametrize("mode", ["mirror", "full_refresh_mirror", "scd2"])
def test_probe_readback_verdict_and_confidence_agree(mode):
    report = public_report(build_parity_report(_job(mode), reread=False))
    assert report["verdict"] == "no_difference_found"
    conf = report["confidence"]
    assert conf["method"] == "readback"
    assert "No value comparison ran" not in conf["statement"]
    for text in ("re-read after the write", "not an independent re-read", "not proof"):
        assert text in conf["statement"], text
        assert text in report["verdict_reason"], text
    assert conf["upper_bound_rate"] is None  # no sample, so no bound is claimed
    assert conf["rows_checked"] == 10
    table = report["tables"][0]
    assert (table["check"], table["status"], table["proven_equal_pct"]) == ("readback", "match", None)
    # The signed verdict could only ever say "no difference found", never "proven".
    assert report["verdict"] != "proven"


def test_a_readback_run_is_never_drawn_as_proven_on_the_health_heatmap():
    report = build_parity_report(_job(), reread=False)
    summary = compact_summary(report)
    assert _cell(summary["tables"][0]) == "sampled"  # "no difference found, not proven"


def test_a_write_pass_digest_without_a_source_digest_stays_a_count():
    # No source digest: nothing was compared; the verdict says so and nothing can be signed.
    report = public_report(build_parity_report(_job(source_checksum=""), reread=False))
    assert report["tables"][0]["check"] == "count_only"
    assert report["verdict"] == "unverifiable"
    assert report["signoff"]["status"] == "blocked"
