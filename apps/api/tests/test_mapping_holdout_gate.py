"""CI gate: Map's mapping on the held-out fixture (mapping_holdout_v1).

Measured on the production path (run_mapping_pipeline, use_llm=False) and,
labelled scorer-level, on map_columns alone. Floors and ceilings live in
``services.mapping_holdout_eval`` at the measured values; they only move
upward. ``dev2`` is an inspected split (it was re-measured while tuning); the
blind number comes from a separate sealed fixture.
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path

import pytest

from services.mapping_holdout_eval import (
    HOLDOUT_CEILINGS,
    HOLDOUT_FLOORS,
    cluster_bootstrap,
    evaluate,
    gate_failures,
    load_holdout,
    metrics,
    wilson_lower_bound,
)


@pytest.fixture(scope="module")
def results() -> dict[str, dict[str, dict]]:
    logging.disable(logging.WARNING)
    out: dict[str, dict[str, dict]] = {}
    try:
        for engine in ("pipeline", "scorer"):
            res = {s: evaluate(s, engine=engine) for s in ("dev", "dev2")}
            rows = res["dev"]["rows"] + res["dev2"]["rows"]
            res["all"] = {"overall": metrics(rows), "rows": rows}
            out[engine] = res
    finally:
        logging.disable(logging.NOTSET)
    return out


def test_split_is_table_level_and_stratified() -> None:
    tables = load_holdout()["tables"]
    assert {t["split"] for t in tables} == {"dev", "dev2"}
    strata: dict[str, set[str]] = {}
    for t in tables:
        key = "real_world_standard" if t["bucket"] == "negatives" else t["bucket"]
        strata.setdefault(key, set()).add(t["split"])
    assert all(v == {"dev", "dev2"} for v in strata.values()), strata


@pytest.mark.parametrize("engine", ["pipeline", "scorer"])
@pytest.mark.parametrize("split", ["dev2", "dev", "all"])
def test_holdout_gate_floors_and_ceilings(results, engine, split) -> None:
    overall = results[engine][split]["overall"]
    assert HOLDOUT_FLOORS[engine][split], "floors must be set"
    assert not gate_failures(split, overall, engine), (gate_failures(split, overall, engine), overall)


@pytest.mark.parametrize("engine", ["pipeline", "scorer"])
@pytest.mark.parametrize("split", ["dev2", "dev", "all"])
def test_no_match_column_is_never_auto_mapped(results, engine, split) -> None:
    bad = [(r["table"], r["source"], r["pred"]) for r in results[engine][split]["rows"] if not r["gold"] and r["auto"]]
    assert bad == []


def test_pipeline_auto_rows_are_scored(results) -> None:
    # The gate measures what Map approves: pipeline-only promotions count too.
    o = results["pipeline"]["all"]["overall"]
    assert o["auto_count"] > 0 and o["auto_precision"] >= 0.98


def test_gate_policy_is_strict() -> None:
    for engine in ("pipeline", "scorer"):
        for split in ("dev2", "dev", "all"):
            assert HOLDOUT_FLOORS[engine][split]["auto_precision"] >= 0.98
            assert HOLDOUT_CEILINGS[engine][split]["neg_auto"] == 0


def test_cluster_bootstrap_resamples_tables(results) -> None:
    rows = results["scorer"]["dev2"]["rows"]
    ci = cluster_bootstrap(rows, reps=300)
    assert ci["tables"] == len({r["table"] for r in rows})
    lo, hi = ci["top1"]
    # Wider than a column-independent Wilson interval (about +/-9 points).
    assert hi - lo > 0.2
    assert ci == cluster_bootstrap(rows, reps=300)


def test_external_fixture_is_scored_without_splits(tmp_path) -> None:
    doc = load_holdout()
    table = dict(doc["tables"][0])
    table.pop("split")
    path = tmp_path / "external.json"
    path.write_text(json.dumps({"tables": [table], "destination": doc["destination"]}), encoding="utf-8")
    res = evaluate(None, path=path, engine="scorer")
    assert res["overall"]["n_sources"] == len(table["source"])


def test_external_fixture_cli_prints_aggregates_only(tmp_path) -> None:
    import subprocess
    import sys

    doc = load_holdout()
    table = {k: v for k, v in doc["tables"][0].items() if k != "split"}
    path = tmp_path / "sealed.json"
    path.write_text(json.dumps({"tables": [table], "destination": doc["destination"]}), encoding="utf-8")
    api = Path(__file__).resolve().parents[1]
    out = subprocess.run(
        [sys.executable, "scripts/run_mapping_holdout.py", "--fixture", str(path), "--engine", "scorer",
         "--bootstrap", "50"], cwd=api, capture_output=True, text=True, timeout=300, check=True,
    ).stdout
    for col in table["source"]:
        assert col["name"] not in out
    for gold in table["gold"].values():
        if gold:
            assert gold not in out


def test_wilson_lower_bound_is_reasonable() -> None:
    assert wilson_lower_bound(0, 0) is None
    assert 0.90 < wilson_lower_bound(73, 74) < 0.94


def test_evidence_claim_floors_match_the_gate() -> None:
    from services.evidence_pack import CLAIM_REGISTRY

    claim = next(c for c in CLAIM_REGISTRY if c["id"] == "holdout_mapping")
    for key in ("top1", "top3", "auto_precision"):
        assert claim["floors"][key] == HOLDOUT_FLOORS["pipeline"]["dev2"][key]
    assert claim["floors"]["neg_auto_zero"] == 1


def test_docs_page_claims_no_more_than_the_gate_measures(results) -> None:
    """docs/use/mapping-accuracy.md quotes the pipeline on dev2; it may not overstate it."""
    text = (Path(__file__).resolve().parents[3] / "docs" / "use" / "mapping-accuracy.md").read_text()
    o = results["pipeline"]["dev2"]["overall"]
    m = re.search(r"\*\*(\d+) of (\d+)\*\* auto-approved mappings were right", text)
    assert m, "auto-approve line missing"
    assert (int(m.group(1)), int(m.group(2))) == (o["auto_correct"], o["auto_count"])
    top1 = re.search(r"top-1\) \| \d+% \| (\d+)%", text)
    top3 = re.search(r"top-3\) \| \d+% \| (\d+)%", text)
    assert top1 and int(top1.group(1)) <= round(o["top1"] * 100)
    assert top3 and int(top3.group(1)) <= round(o["top3"] * 100)
    for phrase in ("not blind", "blind number is pending", "dev2"):
        assert phrase in text, phrase
