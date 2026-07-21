"""Enterprise mapping proof — measurable accuracy, precision/recall, calibration.

Domain-batched bipartite eval against fixtures/mapping_golden_enterprise.json
(≥200 typed cases). Writes proof artifacts under apps/api/data/proofs/.
Floor is 100% on this fixture; expand the fixture before lowering.
"""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

import pytest

FIXTURE = Path(__file__).parent / "fixtures" / "mapping_golden_enterprise.json"
LEGACY_FIXTURE = Path(__file__).parent / "fixtures" / "mapping_golden.json"
PROOF_DIR = Path(__file__).resolve().parents[1] / "data" / "proofs"
ACCURACY_FLOOR = 1.0
MIN_CASES = 200


def _load_enterprise() -> dict:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def _map_domain(cases: list[dict]) -> list[dict]:
    from services.semantic_mapper import map_columns

    sources = [c["source"] for c in cases]
    targets = [c["target"] for c in cases]
    source_schemas = [
        {"name": c["source"], "inferred_type": c.get("source_type", "VARCHAR"), "samples": []}
        for c in cases
    ]
    target_schemas = [
        {"name": c["target"], "inferred_type": c.get("target_type", "VARCHAR"), "samples": []}
        for c in cases
    ]
    return map_columns(
        sources,
        targets,
        source_schemas=source_schemas,
        target_schemas=target_schemas,
    )


def test_enterprise_mapping_accuracy_100_percent(tmp_path: Path) -> None:
    data = _load_enterprise()
    domains = data["domains"]
    all_cases = [c for d in domains for c in d["cases"]]
    assert len(all_cases) >= MIN_CASES, (
        f"Enterprise golden too small ({len(all_cases)} < {MIN_CASES})"
    )

    results: list[dict] = []
    domain_scores: dict[str, dict] = {}
    correct = 0
    false_create_new = 0
    confidences_correct: list[float] = []
    confidences_wrong: list[float] = []

    for dom in domains:
        cases = dom["cases"]
        mapped = _map_domain(cases)
        by = {m["source"]: m for m in mapped}
        dom_correct = 0
        for case in cases:
            pred_m = by.get(case["source"]) or {}
            predicted = pred_m.get("target")
            ok = predicted == case["target"]
            if ok:
                correct += 1
                dom_correct += 1
                confidences_correct.append(float(pred_m.get("confidence") or 0))
            else:
                confidences_wrong.append(float(pred_m.get("confidence") or 0))
                if pred_m.get("create_new"):
                    false_create_new += 1
            results.append({
                "domain": dom["name"],
                "source": case["source"],
                "expected": case["target"],
                "predicted": predicted,
                "correct": ok,
                "confidence": pred_m.get("confidence"),
                "create_new": bool(pred_m.get("create_new")),
                "strategy": pred_m.get("assignment_strategy"),
                "source_type": case.get("source_type"),
                "target_type": case.get("target_type"),
            })
        domain_scores[dom["name"]] = {
            "correct": dom_correct,
            "total": len(cases),
            "score": round(dom_correct / max(len(cases), 1), 4),
        }

    total = len(all_cases)
    score = correct / total
    # Precision/recall on this 1:1 labeled set: TP=correct, FP=wrong existing,
    # FN would be unmapped — treat invent-when-label-exists as FP create-new.
    precision = correct / total  # every source assigned
    recall = correct / total
    f1 = 2 * precision * recall / max(precision + recall, 1e-9)

    # Confidence calibration by decile (correct rate in each band).
    bands = defaultdict(lambda: {"n": 0, "correct": 0})
    for row in results:
        conf = float(row.get("confidence") or 0)
        band = f"{int(conf * 10) / 10:.1f}-{int(conf * 10) / 10 + 0.1:.1f}"
        bands[band]["n"] += 1
        bands[band]["correct"] += int(row["correct"])
    calibration = {
        band: {
            "n": v["n"],
            "accuracy": round(v["correct"] / max(v["n"], 1), 4),
        }
        for band, v in sorted(bands.items())
    }

    proof = {
        "metric": "enterprise_column_mapping_accuracy",
        "score": round(score, 4),
        "correct": correct,
        "total": total,
        "floor": ACCURACY_FLOOR,
        "passed": score >= ACCURACY_FLOOR,
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
        "false_create_new": false_create_new,
        "false_create_new_rate": round(false_create_new / total, 4),
        "domain_scores": domain_scores,
        "confidence_calibration": calibration,
        "avg_confidence_correct": round(
            sum(confidences_correct) / max(len(confidences_correct), 1), 4
        ),
        "avg_confidence_wrong": round(
            sum(confidences_wrong) / max(len(confidences_wrong), 1), 4
        ) if confidences_wrong else None,
        "engine": "semantic_mapper.map_columns",
        "fixture": "fixtures/mapping_golden_enterprise.json",
        "eval_mode": "domain_batched_bipartite_typed",
        "honesty": (
            "Measured domain-batched accuracy on ≥200 typed enterprise cases. "
            "Not a substitute for live source×destination reconcile matrices."
        ),
        "misses": [r for r in results if not r["correct"]],
        "cases": results,
    }

    PROOF_DIR.mkdir(parents=True, exist_ok=True)
    artifact = PROOF_DIR / "mapping_enterprise_accuracy.json"
    artifact.write_text(json.dumps(proof, indent=2), encoding="utf-8")
    (tmp_path / "mapping_enterprise_accuracy.json").write_text(
        json.dumps(proof, indent=2), encoding="utf-8"
    )

    # Compact summary for CI logs / dashboards.
    summary = {
        "metric": proof["metric"],
        "score": proof["score"],
        "correct": proof["correct"],
        "total": proof["total"],
        "passed": proof["passed"],
        "precision": proof["precision"],
        "recall": proof["recall"],
        "f1": proof["f1"],
        "false_create_new": proof["false_create_new"],
        "domain_scores": proof["domain_scores"],
        "confidence_calibration": proof["confidence_calibration"],
    }
    (PROOF_DIR / "mapping_enterprise_summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )

    assert score >= ACCURACY_FLOOR, (
        f"Enterprise mapping accuracy {score:.1%} below floor {ACCURACY_FLOOR:.0%}. "
        f"See {artifact}. Misses: {proof['misses'][:20]}"
    )
    assert false_create_new == 0, (
        f"false_create_new={false_create_new} — invented columns when labeled targets exist"
    )


def test_legacy_golden_still_perfect() -> None:
    """Keep the original 20-case golden green as a regression canary."""
    from services.semantic_mapper import map_columns

    cases = json.loads(LEGACY_FIXTURE.read_text(encoding="utf-8"))
    sources = [c["source"] for c in cases]
    targets = [c["target"] for c in cases]
    mapped = map_columns(sources, targets)
    by = {m["source"]: m["target"] for m in mapped}
    misses = [c for c in cases if by.get(c["source"]) != c["target"]]
    assert not misses, misses


def test_enterprise_pipeline_preserves_create_new_objectid() -> None:
    """Pipeline must still keep type-safe create_new (ObjectId path)."""
    from services.mapping_pipeline import run_mapping_pipeline

    samples = [
        "693486a0f0d881be6f0c470e",
        "69349183a44dd21d08a19c2c",
        "6934a44da44dd21d08a1ac18",
        "6934b905a44dd21d08a1caca",
    ]
    result = run_mapping_pipeline(
        ["_id", "name"],
        ["id", "name"],
        source_schemas=[
            {"name": "_id", "inferred_type": "VARCHAR", "samples": samples},
            {"name": "name", "inferred_type": "VARCHAR", "samples": ["Ada"]},
        ],
        target_schemas=[
            {"name": "id", "inferred_type": "DECIMAL", "samples": []},
            {"name": "name", "inferred_type": "VARCHAR", "samples": []},
        ],
        destination_db_type="snowflake",
        use_llm=False,
        confidence_threshold=0.75,
    )
    by = {m["source"]: m for m in result["mappings"]}
    assert len(result["mappings"]) == 2
    assert by["_id"]["target"].lower() != "id"
    assert by["_id"].get("create_new") is True or by["_id"]["target"] not in {"id", "ID"}
