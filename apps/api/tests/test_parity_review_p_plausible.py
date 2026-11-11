"""Review "plausible" items, each verified and fixed (P1-P4) or decided (P5).

P1 rows_skipped: every skipped row was "explained by the duplicate policy",
   even when the run recorded no duplicate policy at all. Now explained only
   when the run recorded one; otherwise the skipped rows are unexplained.
P2 a key absent on both sides at re-read was silently "resolved": a row the
   run lost and the source deleted since vanished from the report. Now it stays
   visible (unexplained: it cannot be re-checked) unless it was an extra row the
   destination no longer holds (that one really is resolved).
P3 a protected KEY column's values were shown in keys_sample, the re-sync
   preview and the pack. They now read "protected".
P4 the re-read had no time bound; it now runs under the connector executor's
   deadline (``run_with_deadline``) and a timeout records ``reread: failed``.
P5 multi-stream jobs whose extra streams were only counted stay unverifiable
   (decided; see the contract): nothing compared their values, so nothing
   about them can be signed.
"""

from __future__ import annotations

import time

from services.parity_report import build_parity_report


def _job(recon: dict, dest: dict | None = None, **over) -> dict:
    job = {"_id": "job-p", "status": "completed", "reconciliation": recon,
           "destination_summary": {"table": "orders", "primary_key_columns": ["id"],
                                   "sync_mode": "full_refresh_overwrite", **(dest or {})}}
    job.update(over)
    return job


def test_p1_skipped_rows_without_a_duplicate_policy_are_unexplained():
    report = build_parity_report(_job({"passed": True, "source_rows": 1000, "target_rows": 900, "rows_skipped": 100,
                                       "culprit_census_clean": True}), reread=False)
    assert report["verdict"] == "unexplained", report["verdict_reason"]
    assert report["parity"]["unexplained"] == 100


def test_p1_skipped_rows_under_a_recorded_duplicate_policy_are_explained():
    report = build_parity_report(_job(
        {"passed": True, "source_rows": 1000, "target_rows": 900, "rows_skipped": 100, "culprit_census_clean": True},
        {"duplicate_policy": "keep_first", "collapsed_duplicate_rows": 100},
    ), reread=False)
    assert report["verdict"] == "explained"
    skipped = [d for d in report["differences"] if d["cause"]["explained"] and d["kind"] == "missing_in_destination"]
    assert skipped and "keep_first" in skipped[0]["cause"]["detail"]


def _reread_job() -> dict:
    return _job({"passed": False, "source_rows": 100, "target_rows": 100,
                 "culprit_keys": {"missing_in_dest": ["13"], "extra_in_dest": ["77"]}},
                transfer_request={"source": {"kind": "database"}, "destination": {"kind": "database"},
                                  "mappings": [{"source": "id", "target": "id"}]})


def test_p2_a_lost_key_the_source_deleted_since_is_not_silently_resolved(monkeypatch):
    from services import parity_report

    monkeypatch.setattr(parity_report, "reread_cells", lambda _job, _ev, keys: {
        k: {"raw": None, "expected": None, "dest": None, "held_out": False} for k in keys})
    report = build_parity_report(_reread_job(), reread=True)
    lost = [d for d in report["differences"] if d["kind"] == "missing_in_destination"]
    assert lost and lost[0]["keys_sample"] == ["13"] and not lost[0]["cause"]["explained"], report["differences"]
    assert "cannot be re-checked" in lost[0]["cause"]["detail"]
    # The extra row the destination no longer holds is resolved for real.
    assert not [d for d in report["differences"] if d["kind"] == "extra_in_destination" and "77" in d["keys_sample"]]
    assert report["_tables_ev"][0].resolved_now == ["77"]


def test_p3_a_protected_key_column_never_shows_its_values():
    from services.parity_report import public_report

    job = _job({"passed": False, "source_rows": 100, "target_rows": 99, "culprit_keys": {"missing_in_dest": ["4111-1111"]}},
               transfer_request={"source": {"kind": "database", "table": "o"},
                                 "destination": {"kind": "database", "table": "o"},
                                 "mappings": [{"source": "id", "target": "id", "transform": "hash_pii"}]})
    report = build_parity_report(job, reread=False)
    text = str(public_report(report))
    assert "4111-1111" not in text, text
    missing = next(d for d in report["differences"] if d["kind"] == "missing_in_destination")
    assert missing["keys_sample"] == ["protected"]
    resync = next((f for f in report["fixes"] if f["action"] == "resync_keys"), None)
    assert resync is None or "4111" not in str(resync.get("preview"))


def test_p4_a_slow_reread_is_bounded(monkeypatch):
    from services import parity_report

    monkeypatch.setenv("DATAFLOW_PARITY_REREAD_TIMEOUT", "1")

    def slow(_job, _ev, keys):
        time.sleep(3)
        return {}

    monkeypatch.setattr(parity_report, "reread_cells", slow)
    started = time.monotonic()
    report = build_parity_report(_reread_job(), reread=True)
    assert time.monotonic() - started < 2.5
    assert report["evidence"]["reread"]["orders"]["status"] == "failed"
    assert "did not finish" in report["evidence"]["reread"]["orders"]["reason"]


def test_p5_count_only_streams_stay_unverifiable():
    report = build_parity_report(_job(
        {"passed": True, "source_rows": 10, "target_rows": 10, "culprit_census_clean": True},
        {"multi_stream": True, "streams": [{"name": "items", "row_accounting": {"source_count": 5, "dest_count": 5}}]},
    ), reread=False)
    assert report["verdict"] == "unverifiable"
    assert "items" in report["verdict_reason"] and report["signoff"]["status"] == "blocked"


def test_p4_slow_live_column_types_are_bounded(monkeypatch):
    from types import SimpleNamespace

    from services import parity_evidence, parity_fixes, value_readback
    from services.parity_evidence import TableEvidence

    monkeypatch.setenv("DATAFLOW_PARITY_REREAD_TIMEOUT", "1")
    endpoint = SimpleNamespace(format="postgresql", table="t", collection="")
    monkeypatch.setattr(parity_evidence, "_endpoints", lambda _job: (None, endpoint, endpoint))
    monkeypatch.setattr(value_readback, "_live_source_types", lambda *_a: time.sleep(3) or {})
    started = time.monotonic()
    live = parity_fixes._live_types({"transfer_request": {}}, TableEvidence("t", "key_diff", 1, 1), ["amount"])
    assert time.monotonic() - started < 2.5
    assert live["source"] == {} and live["dialect"] == ""
