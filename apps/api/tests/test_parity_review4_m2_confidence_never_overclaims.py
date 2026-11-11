"""Re-review 4, finding 2 (Medium): the confidence statement never claims a clean full compare
on a report Gate-8 failed without naming the rows, or whose evidence was capped.

``unverifiable`` ("Gate-8 failed …") came with "Every row was compared and no row differs." under
"How sure is this?", in the Markdown export and in the Pilot tool.
"""

from __future__ import annotations

from services.parity_report import build_parity_report, public_report
from services.parity_signoff import render_markdown


def _job(recon: dict, **dest) -> dict:
    return {"_id": "j", "status": "completed", "reconciliation": recon,
            "destination_summary": {"table": "orders", "primary_key_columns": ["id"],
                                    "sync_mode": "full_refresh_overwrite", **dest}}


def test_gate8_failed_with_a_clean_census_does_not_say_no_row_differs():
    report = public_report(build_parity_report(
        _job({"passed": False, "source_rows": 500, "target_rows": 500, "culprit_census_clean": True}), reread=False))
    assert report["verdict"] == "unverifiable"
    statement = report["confidence"]["statement"]
    assert "no row differs" not in statement and "every difference is explained" not in statement
    assert "Gate-8 failed" in statement
    md = render_markdown(report, None)
    assert "no row differs" not in md


def test_capped_evidence_says_more_rows_may_differ():
    keys = [str(i) for i in range(32)]
    report = build_parity_report(_job({"passed": False, "source_rows": 5000, "target_rows": 5000,
                                       "culprit_keys": {"changed": keys}}), reread=False)
    statement = report["confidence"]["statement"]
    assert "at least 32" in statement and "more may differ" in statement, statement


def test_capped_evidence_whose_rows_are_all_accepted_is_not_called_explained():
    keys = [str(i) for i in range(32)]
    recon = {"passed": False, "source_rows": 5000, "target_rows": 5000, "culprit_keys": {"changed": keys}}
    did = build_parity_report(_job(recon), reread=False)["differences"][0]["id"]
    job = _job(recon)
    job["parity"] = {"version": 1, "acceptances": {did: {"by": "a@x", "note": "ok", "keys": keys,
                                                         "keyless_rows": 0, "rows": 32}}}
    statement = build_parity_report(job, reread=False)["confidence"]["statement"]
    assert "every difference is explained" not in statement
    assert "more may differ" in statement, statement


def test_a_clean_full_compare_keeps_its_sentence():
    report = build_parity_report(_job({"passed": True, "source_rows": 10, "target_rows": 10,
                                       "culprit_census_clean": True}), reread=False)
    assert report["confidence"]["statement"] == "Every row was compared and no row differs."
