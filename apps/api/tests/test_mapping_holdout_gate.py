"""CI gate: the mapper on the blind held-out benchmark (mapping_holdout_v1).

Floors and ceilings live in ``services.mapping_holdout_eval`` and are the
values measured on this fixture; they only move upward. The ``test`` split is
never used for tuning (see tests/test_mapping_holdout_leak.py).
"""

from __future__ import annotations

import logging

import pytest

from services.mapping_holdout_eval import (
    HOLDOUT_CEILINGS,
    HOLDOUT_FLOORS,
    evaluate,
    gate_failures,
    load_holdout,
    metrics,
    wilson_lower_bound,
)


@pytest.fixture(scope="module")
def results() -> dict[str, dict]:
    logging.disable(logging.WARNING)
    try:
        res = {s: evaluate(s) for s in ("dev", "test")}
    finally:
        logging.disable(logging.NOTSET)
    res["all"] = {"overall": metrics(res["dev"]["rows"] + res["test"]["rows"]), "rows": res["dev"]["rows"] + res["test"]["rows"]}
    return res


def test_split_is_table_level_and_stratified() -> None:
    doc = load_holdout()
    tables = doc["tables"]
    assert {t["split"] for t in tables} == {"dev", "test"}
    assert len({t["id"] for t in tables}) == len(tables)
    strata: dict[str, set[str]] = {}
    for t in tables:
        key = "real_world_standard" if t["bucket"] == "negatives" else t["bucket"]
        strata.setdefault(key, set()).add(t["split"])
    # Every stratum with two or more tables has both splits.
    assert all(v == {"dev", "test"} for v in strata.values()), strata
    n_test = sum(len(t["source"]) for t in tables if t["split"] == "test")
    n_dev = sum(len(t["source"]) for t in tables if t["split"] == "dev")
    assert n_test >= 120 and n_dev >= 90


@pytest.mark.parametrize("split", ["test", "dev", "all"])
def test_holdout_gate_floors_and_ceilings(results, split) -> None:
    overall = results[split]["overall"]
    assert not gate_failures(split, overall), (gate_failures(split, overall), overall)


@pytest.mark.parametrize("split", ["test", "dev", "all"])
def test_no_match_column_is_never_auto_mapped(results, split) -> None:
    bad = [(r["table"], r["source"], r["pred"]) for r in results[split]["rows"] if not r["gold"] and r["auto"]]
    assert bad == []


@pytest.mark.parametrize("split", ["test", "all"])
def test_auto_approve_precision_at_least_098(results, split) -> None:
    o = results[split]["overall"]
    assert o["auto_count"] > 0
    assert o["auto_precision"] >= 0.98, o


def test_gate_policy_is_strict() -> None:
    for split in ("test", "dev", "all"):
        assert HOLDOUT_FLOORS[split]["auto_precision"] >= 0.96
        assert HOLDOUT_CEILINGS[split]["neg_auto"] == 0
    assert HOLDOUT_FLOORS["test"]["auto_precision"] >= 0.98
    assert HOLDOUT_FLOORS["all"]["auto_precision"] >= 0.98


def test_wilson_lower_bound_is_reasonable() -> None:
    assert wilson_lower_bound(0, 0) is None
    assert 0.90 < wilson_lower_bound(73, 74) < 0.94
    assert wilson_lower_bound(10, 10) < 1.0


def test_evidence_claim_floors_match_the_gate() -> None:
    from services.evidence_pack import CLAIM_REGISTRY

    claim = next(c for c in CLAIM_REGISTRY if c["id"] == "holdout_mapping")
    for key in ("top1", "top3", "auto_precision"):
        assert claim["floors"][key] == HOLDOUT_FLOORS["test"][key]
    assert claim["floors"]["neg_auto_zero"] == 1
