"""Re-review 3, H3: accepting sampled differences never shrinks what the sample says about the rest.

Accepting the 50 truncations a 1,000-row sample found (of a 1,000,000-row
table) removed them from k, so the report read ``no_difference_found`` with
k = 0 ("at most 0.30%") — while the same defect rate applies to the rows
nobody looked at. And the reason called the operator's acceptances
"differences a rule explains". Accepted sampled rows now stay in k, and the
reason names acceptances as acceptances.
"""

from __future__ import annotations

from services.parity_report import build_parity_report


def _job(**extra) -> dict:
    mism = [{"key": str(i), "source": "note", "target": "note", "source_value": "abcdefghij" + str(i),
             "target_value": "abcdefghij"} for i in range(50)]
    job = {"_id": "h3", "status": "completed",
           "reconciliation": {"passed": True, "source_rows": 1_000_000, "target_rows": 1_000_000,
                              "sample_compare": {"rows_compared": 1000, "mismatches": mism}},
           "destination_summary": {"table": "t", "primary_key_columns": ["id"], "sync_mode": "full_refresh_overwrite"},
           "transfer_request": {"mappings": [{"source": "note", "target": "note", "target_type": "VARCHAR(10)"}]}}
    job.update(extra)
    return job


def test_accepted_sampled_rows_stay_in_k_and_are_named_as_accepted():
    did = build_parity_report(_job(), reread=False)["differences"][0]["id"]
    report = build_parity_report(_job(parity={"version": 1, "acceptances": {did: {
        "by": "a@x", "at": "t", "note": "ok", "keys": [str(i) for i in range(50)], "keyless_rows": 0, "rows": 50}}}),
        reread=False)
    assert report["confidence"]["differences_in_sample"] == 50
    assert "0.30%" not in report["confidence"]["statement"] + report["verdict_reason"]
    assert "accepted by an operator" in report["verdict_reason"]
    assert "rule explains" not in report["verdict_reason"]
