"""Re-review 4, finding 1 (High): an L2/L4 column failure is not dropped because L5 named *a* key.

L5 compares only its ``focus_columns`` (L4's typed-fingerprint mismatches). L2
compares untyped aggregates, so it can fail on a column L4 and L5 call equal: a
TIMESTAMP source written into a Postgres DATE loses the time of day on every
row, L2 fails on that column, L4/L5 only see the one upper-cased name. Dropping
every aggregate column once L5 named key 5 made the report ``explained`` after
accepting key 5 (and ``proven`` after a re-sync resolved it), with sign-off open.

Only the key census (a whole-row hash) localizes every column; L5 localizes the
columns it compared.
"""

from __future__ import annotations

import pytest

from services.parity_report import build_parity_report
from services.verification_ladder import run_five_layer_verification

TYPES = {"id": "INTEGER", "name": "VARCHAR(40)", "amount": "NUMERIC(10,2)", "shipped_on": "DATE"}


def _ladder() -> dict:
    src, dst = [], []
    for i in range(1, 51):
        day = f"2024-01-{(i % 28) + 1:02d}"
        src.append({"id": str(i), "name": "Acme" if i == 5 else f"n{i}", "amount": "10.5", "shipped_on": f"{day} 10:30:00"})
        dst.append({"id": str(i), "name": "ACME" if i == 5 else f"n{i}", "amount": "10.5", "shipped_on": day})
    return run_five_layer_verification(
        source_rows=src, target_rows=dst, columns=list(TYPES), pk_column="id",
        dest_db_type="postgresql", dest_types=TYPES,
    )


def _job(ladder: dict, parity: dict | None = None, **recon) -> dict:
    maps = [{"source": c, "target": c, "target_type": t} for c, t in TYPES.items()]
    maps[3]["source_type"] = "TIMESTAMP"
    job = {
        "_id": "j", "status": "completed",
        "reconciliation": {"passed": False, "source_rows": 50, "target_rows": 50,
                           "verification_ladder": ladder, **recon},
        "destination_summary": {"table": "orders", "primary_key_columns": ["id"],
                                "sync_mode": "full_refresh_overwrite"},
        "transfer_request": {"mappings": maps},
    }
    if parity:
        job["parity"] = parity
    return job


def test_the_real_ladder_fails_l2_on_a_column_l5_never_compared():
    layers = _ladder()["layers"]
    assert "shipped_on" in layers["L2"]["details"]["mismatched_columns"]
    assert layers["L5"]["details"]["focus_columns"] == ["name"]


def _accepted(ladder: dict) -> dict:
    first = build_parity_report(_job(ladder), reread=False)
    did = next(d["id"] for d in first["differences"] if d["column"] == "name")
    return {"version": 1, "acceptances": {did: {"by": "ops@x", "note": "case change ok", "keys": ["5"],
                                                "keyless_rows": 0, "rows": 1}}}


@pytest.mark.parametrize("decision", ["accepted", "resolved"])
def test_an_l2_column_l5_did_not_compare_stays_unexplained(decision):
    ladder = _ladder()
    parity = _accepted(ladder) if decision == "accepted" else {"version": 2, "resolved": {"orders": ["5"]}}
    report = build_parity_report(_job(ladder, parity), reread=False)
    assert report["verdict"] == "unexplained", report["verdict_reason"]
    left = [d for d in report["differences"] if d["column"] == "shipped_on"]
    assert left and left[0]["rows_at_least"] and not left[0]["cause"]["explained"]
    assert report["signoff"]["status"] == "blocked"
    # The column L5 compared and named is still localized by its row, not repeated as an aggregate.
    assert not any(d["column"] == "name" and d["rows_at_least"] for d in report["differences"])


def test_a_key_census_name_still_localizes_every_column():
    ladder = _ladder()
    report = build_parity_report(_job(ladder, culprit_keys={"changed": ["9"]}), reread=False)
    assert not any(d["rows_at_least"] for d in report["differences"]), report["differences"]


def test_l5_without_a_recorded_focus_drops_only_named_columns():
    ladder = _ladder()
    ladder["layers"]["L5"]["details"].pop("focus_columns")
    report = build_parity_report(_job(ladder), reread=False)
    cols = {d["column"] for d in report["differences"] if d["rows_at_least"]}
    assert cols == {"shipped_on"}


def test_a_fix_resolving_the_unexplained_keys_still_settles_the_failure():
    """The keys a fix re-checked equal settle a failure they alone localized (contract 28); a column
    they cannot localize stays a difference of its own (above), so it never rides on that."""
    layers = {"L1": {"passed": True}, "L3": {"passed": False, "details": {"source": "a", "target": "b"}}}
    job = {
        "_id": "j", "status": "completed",
        "reconciliation": {"passed": False, "source_rows": 100, "target_rows": 100,
                           "culprit_keys": {"changed": ["7"]}, "verification_ladder": {"layers": layers}},
        "destination_summary": {"table": "t", "primary_key_columns": ["id"], "sync_mode": "full_refresh_overwrite"},
        "parity": {"version": 2, "resolved": {"t": ["7"]}},
    }
    report = build_parity_report(job, reread=False)
    assert report["verdict"] == "proven", report["verdict_reason"]
