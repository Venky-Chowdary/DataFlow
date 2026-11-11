"""Re-review finding 3 (HIGH): a sampled table is never "explained" in full.

A sampled table whose only differences were explained (CHAR padding in the
sample, a protected column) read ``explained`` — "1,000,000 row(s) differ and
every difference is explained" — and counted as fully proven on the dashboard,
although only 2,000 rows were compared. Any sampled table caps the verdict at
``no_difference_found`` (which states the bound), the heat cell is
``sampled``, and the run is not fully proven.
"""

from __future__ import annotations

import unicodedata

from services.parity_report import build_parity_report, compact_summary
from services.parity_summary import _cell


def _job(recon: dict, dest: dict | None = None, **over) -> dict:
    job = {"_id": "job-1", "status": "completed", "reconciliation": recon,
           "destination_summary": {"table": "orders", "primary_key_columns": ["id"],
                                   "sync_mode": "full_refresh_overwrite", **(dest or {})}}
    job.update(over)
    return job


def _sampled(mismatches: list[dict]) -> dict:
    return {"passed": True, "source_rows": 1_000_000, "target_rows": 1_000_000,
            "sample_compare": {"rows_compared": 2000, "mismatches": mismatches}}


def _assert_bounded(report: dict) -> None:
    # Re-review 3, H4: a Gate-8 sample is not random, so it states the rows it checked, no bound.
    assert report["verdict"] == "no_difference_found", report["verdict_reason"]
    assert "2,000 row(s) checked" in report["verdict_reason"]
    assert "%" not in report["verdict_reason"]
    assert "every difference is explained" not in report["verdict_reason"]
    assert report["signoff"]["status"] == "blocked"
    assert _cell(compact_summary(report)["tables"][0]) == "sampled"


def test_explained_padding_in_a_sample_is_a_bound_not_a_proof():
    report = build_parity_report(_job(
        _sampled([{"key": "1", "target": "code", "source_value": "ab", "target_value": "ab   "}]),
        transfer_request={"mappings": [{"source": "id", "target": "id"},
                                       {"source": "code", "target": "code", "target_type": "CHAR(5)"}]},
    ), reread=False)
    _assert_bounded(report)


def test_a_protected_column_on_a_sampled_table_is_a_bound_not_a_proof():
    report = build_parity_report(_job(
        _sampled([]), dest={"rows_written": 1_000_000},
        transfer_request={"mappings": [{"source": "id", "target": "id"},
                                       {"source": "email", "target": "email", "transform": "hash_pii"}]},
    ), reread=False)
    _assert_bounded(report)


def test_unicode_normal_form_in_a_sample_is_a_bound_not_a_proof():
    s, d = unicodedata.normalize("NFD", "café"), unicodedata.normalize("NFC", "café")
    report = build_parity_report(_job(_sampled([{"key": "1", "target": "name", "source_value": s,
                                                  "target_value": d}])), reread=False)
    _assert_bounded(report)


def test_a_sampled_run_is_not_counted_fully_proven(monkeypatch):
    from services import parity_summary

    report = build_parity_report(_job(_sampled([{"key": "1", "target": "code", "source_value": "ab",
                                                  "target_value": "ab   "}]),
                                      transfer_request={"mappings": [
                                          {"source": "id", "target": "id"},
                                          {"source": "code", "target": "code", "target_type": "CHAR(5)"}]}),
                                 reread=False)

    class Store:
        def list_jobs(self, limit, workspace_filter):
            return [{"_id": "job-1"}]

        def get_job(self, job_id):
            # A summary stamped for this job state (re-review 3, M4: an unstamped one is recomputed).
            return {"_id": "job-1", "status": "completed",
                    "parity": {"summary": compact_summary(report, {"status": "completed"})}}

    monkeypatch.setattr(parity_summary, "job_store", lambda: Store())
    summary = parity_summary.build_summary(None, limit=5)
    assert summary["kpis"]["fully_proven_pct"] == 0.0
    assert summary["heatmap"]["cells"] == [["sampled"]]
