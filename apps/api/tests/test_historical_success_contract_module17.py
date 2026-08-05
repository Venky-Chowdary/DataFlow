"""Module 17 — Historical success must be measured or explicitly unmeasured."""

from __future__ import annotations

from services.historical_success_contract import (
    measure_from_runs,
    stamp_mappings_historical_success,
    unmeasured_historical_success,
)


def test_unmeasured_never_invents_rate():
    u = unmeasured_historical_success()
    assert u["measured"] is False
    assert u["success_rate"] is None
    assert u["never_invented"] is True


def test_measure_from_runs_computes_rate():
    runs = [
        {"row_count": 100, "rejected_rows": 10},
        {"row_count": 100, "rejected_rows": 0},
    ]
    ev = measure_from_runs(runs)
    assert ev["measured"] is True
    assert ev["runs_observed"] == 2
    assert ev["rows_written_total"] == 200
    assert ev["rows_rejected_total"] == 10
    assert ev["success_rate"] == 0.95
    assert ev["scope"] == "route_load_history"


def test_empty_runs_unmeasured():
    ev = measure_from_runs([])
    assert ev["measured"] is False
    assert ev["success_rate"] is None


def test_zero_row_runs_do_not_invent():
    ev = measure_from_runs([{"row_count": 0, "rejected_rows": 0}])
    assert ev["measured"] is False
    assert ev["success_rate"] is None


def test_rejected_over_written_clamps_to_zero():
    ev = measure_from_runs([{"row_count": 10, "rejected_rows": 50}])
    assert ev["measured"] is True
    assert ev["success_rate"] == 0.0


def test_stamp_discards_legacy_float_invent():
    mappings = [{"source": "a", "target": "a", "historical_success": 0.99}]
    evidence = measure_from_runs([{"row_count": 50, "rejected_rows": 0}])
    out = stamp_mappings_historical_success(mappings, evidence)
    hs = out[0]["historical_success"]
    assert isinstance(hs, dict)
    assert hs["measured"] is True
    assert hs["success_rate"] == 1.0
    assert hs["never_invented"] is True
    assert hs["success_rate"] != 0.99 or hs["runs_observed"] >= 1
