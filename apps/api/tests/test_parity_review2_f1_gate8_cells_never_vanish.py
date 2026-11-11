"""Re-review finding 1 (CRITICAL): a Gate-8 mismatch must never vanish.

``reconciliation.py`` stores a sample mismatch's values cut at 120 characters
(as fingerprints). When the cells cannot be re-read (no saved route, a
composite key, a timeout) the classifier saw the stored cells as equal, the
difference was dropped, and the Gate-8 failure guard was satisfied by the mere
existence of a "localizing" observation — so a failed run read ``proven`` (or
``no_difference_found``) with sign-off open.
"""

from __future__ import annotations

from services.parity_report import build_parity_report

#: reconciliation.py keeps the first 120 characters of a stored sample value.
STORED_VALUE_LIMIT = 120


def _job(recon: dict, *, dest_types: dict | None = None) -> dict:
    job = {
        "_id": "j", "status": "completed", "reconciliation": recon,
        "destination_summary": {"table": "t", "primary_key_columns": ["id"], "sync_mode": "full_refresh_overwrite"},
    }
    if dest_types:
        job["transfer_request"] = {"mappings": [
            {"source": c, "target": c, "target_type": t} for c, t in dest_types.items()
        ]}
    return job


def _truncated_mismatch() -> list[dict]:
    src, dst = "x" * 200, "x" * 150  # the destination column cut the value at 150 characters
    return [{"row": "0", "key": "7", "source": "note", "target": "note",
             "source_value": src[:STORED_VALUE_LIMIT], "target_value": dst[:STORED_VALUE_LIMIT]}]


def test_sampled_mismatch_whose_stored_cells_read_equal_stays_unexplained():
    report = build_parity_report(_job({
        "passed": False, "source_rows": 1000, "target_rows": 1000,
        "sample_compare": {"rows_compared": 1000, "mismatches": _truncated_mismatch()},
    }), reread=False)
    assert report["verdict"] == "unexplained", report["verdict_reason"]
    note = [d for d in report["differences"] if d["column"] == "note"]
    assert note and note[0]["cause"]["explained"] is False
    assert "too short to classify" in note[0]["cause"]["detail"]
    assert report["signoff"]["status"] == "blocked"


def test_key_diff_mismatch_whose_stored_cells_read_equal_is_not_proven():
    recon = {
        "passed": False, "source_rows": 1000, "target_rows": 1000, "culprit_census_clean": False,
        "sample_compare": {"rows_compared": 1000, "mismatches": _truncated_mismatch()},
        "verification_ladder": {"layers": {"L5": {"passed": True, "details": {"pk_column": "id"}}}},
    }
    report = build_parity_report(_job(recon), reread=False)
    assert report["verdict"] not in {"proven", "no_difference_found", "explained"}, report["verdict_reason"]
    assert report["parity"]["unexplained"] >= 1
    assert report["signoff"]["status"] == "blocked"


def test_a_cut_stored_value_is_never_explained_from_its_prefix():
    # The full destination value is "a"*100 + " "*20 + "zzz": the stored 120
    # characters look like CHAR padding, but the value differs past them.
    report = build_parity_report(_job({
        "passed": False, "source_rows": 10, "target_rows": 10,
        "sample_compare": {"rows_compared": 10, "mismatches": [{
            "key": "1", "source": "code", "target": "code",
            "source_value": "a" * 100, "target_value": ("a" * 100 + " " * 20 + "zzz")[:STORED_VALUE_LIMIT],
        }]},
    }, dest_types={"id": "INTEGER", "code": "CHAR(130)"}), reread=False)
    code = [d for d in report["differences"] if d["column"] == "code"]
    assert code and code[0]["cause"]["explained"] is False, code
    assert report["verdict"] == "unexplained"


def test_an_uncut_stored_char_padding_is_still_explained():
    report = build_parity_report(_job({
        "passed": True, "source_rows": 10, "target_rows": 10,
        "sample_compare": {"rows_compared": 10, "mismatches": [{
            "key": "1", "source": "code", "target": "code", "source_value": "ab", "target_value": "ab   ",
        }]},
    }, dest_types={"id": "INTEGER", "code": "CHAR(5)"}), reread=False)
    code = [d for d in report["differences"] if d["column"] == "code"]
    assert code and code[0]["cause"]["code"] == "whitespace" and code[0]["cause"]["explained"] is True


def test_gate8_guard_counts_surviving_differences_not_observations(monkeypatch):
    # Gate-8 failed and named key 7; nothing that survives classification
    # accounts for it once its only observation is gone — the table cannot be
    # shown equal. (A fix's recorded re-check is what resolves a key.)
    from services import parity_report as pr

    job = _job({"passed": False, "source_rows": 100, "target_rows": 100,
                "culprit_keys": {"changed": ["7"]}})
    job["parity"] = {"version": 1, "resolved": {}}
    monkeypatch.setattr(pr, "_group", lambda ev, engine, acceptances: [])
    report = pr.build_parity_report(job, reread=False)
    assert report["verdict"] == "unverifiable", report["verdict_reason"]
    assert report["tables"][0]["proven_equal_pct"] is None
    assert "Gate-8 failed" in report["verdict_reason"]
    assert any(f["action"] == "rerun" for f in report["fixes"])


def test_keys_a_recorded_fix_resolved_still_read_proven():
    job = _job({"passed": False, "source_rows": 100, "target_rows": 100,
                "culprit_keys": {"changed": ["7"]}})
    job["parity"] = {"version": 2, "resolved": {"t": ["7"]}}
    report = build_parity_report(job, reread=False)
    assert report["verdict"] == "proven", report["verdict_reason"]
