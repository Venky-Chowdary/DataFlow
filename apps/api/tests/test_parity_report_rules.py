"""Parity report rules on stored evidence alone (no database): verdicts, tables, confidence, fixes."""

from __future__ import annotations

import pytest

from services.parity_report import build_parity_report, public_report


def _job(recon: dict | None = None, *, status: str = "completed", dest: dict | None = None, **over) -> dict:
    job = {
        "_id": "job-1",
        "status": status,
        "reconciliation": recon if recon is not None else {},
        "destination_summary": {"table": "orders", "primary_key_columns": ["id"],
                                "sync_mode": "full_refresh_overwrite", **(dest or {})},
    }
    job.update(over)
    return job


def _report(job: dict) -> dict:
    return build_parity_report(job, reread=False)


@pytest.fixture()
def random_sample(monkeypatch):
    """A sample method that is a uniform random draw (none Gate-8 records today, re-review 3 H4):
    only such a sample supports the Clopper-Pearson bound these tests check."""
    from services import parity_fixes, parity_report

    monkeypatch.setattr(parity_report, "sample_bound_valid", lambda method: True)
    monkeypatch.setattr(parity_fixes, "sample_bound_valid", lambda method: True)


def _sample(n: int, mismatches: list[dict] | None = None, **extra) -> dict:
    return {"passed": not mismatches, "source_rows": n, "target_rows": n,
            "sample_compare": {"rows_compared": n, "mismatches": mismatches or []}, **extra}


def test_a_full_key_census_with_no_difference_is_proven():
    report = _report(_job({"passed": True, "source_rows": 500, "target_rows": 500, "culprit_census_clean": True}))
    assert report["verdict"] == "proven"
    assert report["tables"][0] | {} == {**report["tables"][0], "check": "key_diff", "status": "match",
                                         "proven_equal_pct": 100.0}
    assert report["confidence"]["method"] == "full"
    assert report["confidence"]["statement"] == "Every row was compared and no row differs."
    assert report["signoff"]["status"] == "open"
    assert report["fixes"] == []


def test_a_clean_sample_states_its_upper_bound(random_sample):
    report = _report(_job(_sample(2000)))
    t = report["tables"][0]
    # A clean sample bounds the rate; it is not proof (review finding 9).
    assert report["verdict"] == "no_difference_found" and t["check"] == "sampled"
    assert t["upper_bound_rate"] == pytest.approx(0.0014967, abs=1e-7)
    assert t["proven_equal_pct"] == pytest.approx(99.8503, abs=1e-3)
    conf = report["confidence"]
    assert conf["method"] == "sampled" and conf["sampled_rows"] == 2000 and conf["differences_in_sample"] == 0
    assert "With 95% confidence at most 0.15% of rows" in conf["statement"]


def test_differences_in_the_sample_raise_the_bound(random_sample):
    mismatches = [{"key": str(i), "source": "v", "target": "v", "source_value": f"x{i}", "target_value": f"y{i}"}
                  for i in range(5)]
    report = _report(_job(_sample(1000, mismatches)))
    assert report["verdict"] == "unexplained"
    assert report["confidence"]["differences_in_sample"] == 5
    assert report["confidence"]["upper_bound_rate"] == pytest.approx(0.0104841, abs=1e-6)
    assert report["tables"][0]["unexplained"] == 5
    diff = report["differences"][0]
    assert diff["cause"]["code"] == "unexplained" and diff["rows"] == 5
    assert diff["example"] == {"source": "x0", "destination": "y0"}


@pytest.mark.parametrize(
    ("job", "reason"),
    [
        (_job(_sample(10), status="running"), "has not finished"),
        (_job({}), "recorded no reconciliation"),
        (_job({"passed": True, "source_rows": 10, "target_rows": 10}), "Values were not compared for orders"),
    ],
)
def test_evidence_that_cannot_decide_is_unverifiable(job, reason):
    report = _report(job)
    assert report["verdict"] == "unverifiable"
    assert reason in report["verdict_reason"]
    assert report["signoff"]["status"] == "blocked"


def test_quarantined_rows_are_explained():
    recon = {"passed": True, "source_rows": 100, "target_rows": 97, "rejected_rows": 3,
             "culprit_census_clean": True}
    dest = {"rejected_details": [{"source_pk": "4"}, {"source_pk": "9"}, {"source_pk": "11"}]}
    report = _report(_job(recon, dest=dest))
    assert report["verdict"] == "explained"
    (diff,) = report["differences"]
    assert diff["cause"]["code"] == "quarantined" and diff["rows"] == 3
    assert sorted(diff["keys_sample"]) == ["11", "4", "9"]
    assert report["tables"][0]["proven_equal_pct"] == 100.0


def test_rows_already_in_an_appended_table_are_out_of_scope():
    recon = {"passed": True, "source_rows": 5, "target_rows": 15, "target_rows_before": 10,
             "culprit_census_clean": True}
    report = _report(_job(recon, dest={"sync_mode": "full_refresh_append"}))
    (diff,) = report["differences"]
    assert diff["kind"] == "extra_in_destination" and diff["rows"] == 10
    assert diff["cause"]["code"] == "out_of_scope_population"
    assert report["verdict"] == "explained"


def test_a_count_gap_without_keys_proposes_rerun_and_accept_only():
    report = _report(_job({"passed": False, "source_rows": 100, "target_rows": 98, "culprit_census_clean": True},
                          transfer_request={"source": {"kind": "database", "table": "o"},
                                            "destination": {"kind": "database", "table": "o"}}))
    (diff,) = report["differences"]
    assert diff["kind"] == "missing_in_destination" and diff["rows"] == 2 and diff["keys_sample"] == []
    assert [f["action"] for f in report["fixes"]] == ["rerun", "accept"]
    assert report["fixes"][0]["preview"] == "POST /api/v1/connectors/jobs/job-1/rerun"


def test_protected_columns_never_show_values():
    mismatch = {"key": "1", "source": "email", "target": "email", "source_value": "a@x.com", "target_value": "b"}
    job = _job(_sample(10, [mismatch]), transfer_request={
        "mappings": [{"source": "id", "target": "id"}, {"source": "email", "target": "email",
                                                          "transform": "hash_pii"}],
    })
    report = public_report(_report(job))
    email = [d for d in report["differences"] if d["column"] == "email"]
    assert email and all(d["protected"] and d["example"] is None for d in email)
    assert "a@x.com" not in str(report)


def test_concurrent_source_updates_and_duplicates():
    recon = {"passed": False, "source_rows": 10, "target_rows": 10, "culprit_census_clean": True,
             "concurrent_source_drift": {"count": 2, "sample_keys": [{"id": 3}, {"id": 4}]},
             "rule_checks": [{"column": "email", "status": "failed", "duplicates": 2}]}
    report = _report(_job(recon))
    codes = {(d["kind"], d["cause"]["code"]): d for d in report["differences"]}
    drift = codes[("value", "concurrent_source_update")]
    assert drift["rows"] == 2 and drift["keys_sample"] == ["3", "4"] and drift["cause"]["explained"]
    dup = codes[("duplicate_key", "unexplained")]
    assert dup["rows"] == 2 and dup["column"] == "email"
    assert report["verdict"] == "unexplained"


def test_multi_table_runs_mark_count_only_tables():
    recon = {"passed": True, "source_rows": 5, "target_rows": 5, "culprit_census_clean": True}
    dest = {"multi_stream": True, "streams": [
        {"name": "orders", "row_accounting": {"dest_count": 5, "source_count": 5}},
        {"name": "items", "row_accounting": {"dest_count": 9, "source_count": 9}},
    ]}
    report = _report(_job(recon, dest=dest))
    assert [t["name"] for t in report["tables"]] == ["orders", "items"]
    assert report["tables"][1]["check"] == "count_only" and report["tables"][1]["status"] == "unverifiable"
    assert report["confidence"]["method"] == "mixed"
    assert report["verdict"] == "unverifiable" and "items" in report["verdict_reason"]


def _keyed(n_keys: int, **over) -> dict:
    keys = [str(i) for i in range(n_keys)]
    return _job({"passed": False, "source_rows": max(n_keys, 1) + 10, "target_rows": max(n_keys, 1) + 10,
                 "culprit_keys": {"changed": keys}},
                transfer_request={"source": {"kind": "database", "table": "o"},
                                  "destination": {"kind": "database", "table": "o"}}, **over)


def test_resync_rules_key_limit_and_key_shape():
    ok = _report(_keyed(3))
    resync = next(f for f in ok["fixes"] if f["action"] == "resync_keys")
    assert resync["applicable"] and resync["preview"] == "upsert orders for id IN (0, 1, 2) — 3 key(s)"
    big = next(f for f in _report(_keyed(1001))["fixes"] if f["action"] == "resync_keys")
    assert not big["applicable"] and "1,000-key limit" in big["not_applicable_reason"]
    composite = _keyed(3)
    composite["destination_summary"]["primary_key_columns"] = ["region", "id"]
    fix = next(f for f in _report(composite)["fixes"] if f["action"] == "resync_keys")
    assert not fix["applicable"] and "not one known column" in fix["not_applicable_reason"]
    assert any(f["action"] == "rerun" for f in _report(composite)["fixes"])


def test_round_limit_dismissal_and_retry_ids():
    base = _report(_keyed(2))
    resync = next(f for f in base["fixes"] if f["action"] == "resync_keys")
    accept = next(f for f in base["fixes"] if f["action"] == "accept")
    used = {f"f_used{i}": {"id": f"f_used{i}", "action": "resync_keys", "status": "applied",
                           "applied_at": f"2026-01-0{i + 1}"} for i in range(3)}
    limited = _report(_keyed(2, parity={"fixes": used}))
    again = next(f for f in limited["fixes"] if f["action"] == "resync_keys" and f["status"] == "proposed")
    assert not again["applicable"] and "3 applied fix rounds" in again["not_applicable_reason"]
    assert next(f for f in limited["fixes"] if f["action"] == "accept")["applicable"]

    decided = {resync["id"]: {**resync, "status": "failed", "applied_at": "2026-01-01"},
               accept["id"]: {**accept, "status": "dismissed", "decided_at": "2026-01-02"}}
    later = _report(_keyed(2, parity={"fixes": decided}))
    ids = [(f["id"], f["status"]) for f in later["fixes"]]
    assert (resync["id"], "failed") in ids and (f"{resync['id']}-2", "proposed") in ids
    assert (accept["id"], "dismissed") in ids
    assert [i for i, s in ids if i == accept["id"]] == [accept["id"]]  # not proposed again


def test_an_accepted_difference_is_explained_and_names_the_approver():
    report = _report(_keyed(2))
    diff = report["differences"][0]
    # An acceptance names the rows it covers (review finding 6); one recorded without them covers nothing.
    scope = {"keys": list(diff["keys_sample"]), "keyless_rows": diff["rows"] - len(diff["keys_sample"])}
    accepted = _report(_keyed(2, parity={"acceptances": {diff["id"]: {"by": "o@x.com", "note": "fine", **scope}}}))
    cause = accepted["differences"][0]["cause"]
    assert cause["code"] == "accepted_by_operator" and cause["explained"]
    assert "o@x.com: fine" in cause["detail"] and "Diagnosed as" in cause["detail"]
    assert accepted["verdict"] == "explained" and accepted["signoff"]["status"] == "open"


def test_summary_heatmap_cells():
    from services.parity_summary import _cell

    assert _cell({"status": "match"}) == "proven"
    assert _cell({"status": "explained"}) == "explained"
    assert _cell({"status": "unexplained", "unexplained": 4, "rows_source": 1_000_000}) == "unexplained_small"
    assert _cell({"status": "unexplained", "unexplained": 40, "rows_source": 1_000_000}) == "unexplained_large"
    assert _cell({"status": "unverifiable"}) == "none" and _cell({}) == "none"
