"""Re-review finding 4 (HIGH): rows Gate-8 did not count are a lower bound.

An L2/L4 column failure, an unattributed L2 discrepancy or a protection-proof
leak names a column but no rows; it was counted as exactly one row (99.9999%
proven), and accepting that "1 row" made the report signable at 100%. Such a
difference is now "at least 1 row": the table's proven-equal share is unknown,
no accept is offered (a re-run counts the rows), and an acceptance cannot
cover it.
"""

from __future__ import annotations

from services.parity_report import build_parity_report


def _job(**over) -> dict:
    job = {
        "_id": "j", "status": "completed",
        "reconciliation": {
            "passed": False, "source_rows": 1_000_000, "target_rows": 1_000_000,
            "verification_ladder": {"layers": {
                "L2": {"passed": False, "details": {"mismatched_columns": ["amount"]}},
                "L5": {"passed": True, "details": {"pk_column": "id"}},
            }},
        },
        "destination_summary": {"table": "pay", "primary_key_columns": ["id"], "sync_mode": "full_refresh_overwrite"},
        "transfer_request": {"source": {"kind": "database", "table": "p"},
                             "destination": {"kind": "database", "table": "pay"}},
    }
    job.update(over)
    return job


def test_an_uncounted_column_failure_is_a_lower_bound():
    report = build_parity_report(_job(), reread=False)
    t = report["tables"][0]
    assert report["verdict"] == "unexplained"
    assert t["proven_equal_pct"] is None and report["parity"]["proven_equal_pct"] is None
    [diff] = report["differences"]
    assert diff["column"] == "amount" and diff["rows"] == 1 and diff["rows_at_least"] is True
    assert "at least" in report["verdict_reason"].lower()
    assert "at least" in report["confidence"]["statement"]


def test_no_accept_is_offered_for_uncounted_rows_only_a_rerun():
    report = build_parity_report(_job(), reread=False)
    actions = {f["action"] for f in report["fixes"]}
    assert "accept" not in actions
    assert "rerun" in actions


def test_an_acceptance_cannot_cover_uncounted_rows():
    did = build_parity_report(_job(), reread=False)["differences"][0]["id"]
    job = _job(parity={"version": 1, "acceptances": {did: {"by": "ops@x", "note": "ok", "keys": [],
                                                           "keyless_rows": 1, "rows": 1}}})
    report = build_parity_report(job, reread=False)
    assert report["verdict"] == "unexplained", report["verdict_reason"]
    assert report["parity"]["proven_equal_pct"] is None
    assert report["signoff"]["status"] == "blocked"


def test_a_leaked_protected_column_is_a_lower_bound():
    report = build_parity_report(_job(
        reconciliation={"passed": False, "source_rows": 100, "target_rows": 100, "culprit_census_clean": True,
                        "protection_proof": {"status": "failed", "leaked_columns": ["email"]}},
        transfer_request={"mappings": [{"source": "id", "target": "id"},
                                       {"source": "email", "target": "email", "transform": "hash_pii"}]},
    ), reread=False)
    leaked = [d for d in report["differences"] if d["column"] == "email" and not d["cause"]["explained"]]
    assert leaked and leaked[0]["rows_at_least"] is True
    assert report["tables"][0]["proven_equal_pct"] is None
    assert not any(f["action"] == "accept" and leaked[0]["id"] in f["difference_ids"] for f in report["fixes"])


def test_counted_differences_keep_exact_rows():
    report = build_parity_report(_job(reconciliation={
        "passed": False, "source_rows": 100, "target_rows": 100,
        "sample_compare": {"rows_compared": 100, "mismatches": [
            {"key": "5", "source": "amount", "target": "amount", "source_value": "10.1049", "target_value": "10.10"}]},
        "culprit_census_clean": False, "culprit_keys": {"changed": ["5"]},
    }), reread=False)
    assert all(d["rows_at_least"] is False for d in report["differences"])
    assert report["tables"][0]["proven_equal_pct"] == 99.0
