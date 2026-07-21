"""Mapping accuracy eval harness — measurable proof, no invented percentages.

Runs the semantic mapper against golden source→target pairs and writes a
JSON proof artifact under apps/api/data/proofs/. Fail the suite if accuracy
drops below the floor (default 0.85). This is how we earn accuracy claims.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

FIXTURE = Path(__file__).parent / "fixtures" / "mapping_golden.json"
PROOF_DIR = Path(__file__).resolve().parents[1] / "data" / "proofs"
ACCURACY_FLOOR = 1.0  # Current 20-case fixture must be perfect; expand fixture before lowering.


def _load_cases() -> list[dict]:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def test_mapping_golden_accuracy_with_proof_artifact(tmp_path: Path) -> None:
    from services.semantic_mapper import map_columns

    cases = _load_cases()
    assert len(cases) >= 15, "golden fixture too small for a meaningful eval"

    sources = [c["source"] for c in cases]
    targets = list({c["target"] for c in cases})
    # Also include targets as a flat list preserving order for mapper
    target_list = [c["target"] for c in cases]

    mapped = map_columns(sources, target_list)
    by_source = {m["source"]: m.get("target") for m in mapped}

    results = []
    correct = 0
    for case in cases:
        predicted = by_source.get(case["source"])
        ok = predicted == case["target"]
        if ok:
            correct += 1
        results.append({
            "source": case["source"],
            "expected": case["target"],
            "predicted": predicted,
            "correct": ok,
        })

    score = correct / len(cases)
    proof = {
        "metric": "column_mapping_accuracy",
        "score": round(score, 4),
        "correct": correct,
        "total": len(cases),
        "floor": ACCURACY_FLOOR,
        "passed": score >= ACCURACY_FLOOR,
        "cases": results,
        "engine": "semantic_mapper.map_columns",
        "honesty": "Measured against fixtures/mapping_golden.json — not a marketing claim.",
    }

    out_dir = PROOF_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    artifact = out_dir / "mapping_golden_accuracy.json"
    artifact.write_text(json.dumps(proof, indent=2), encoding="utf-8")

    # Also write to tmp for CI isolation
    (tmp_path / "mapping_golden_accuracy.json").write_text(
        json.dumps(proof, indent=2), encoding="utf-8"
    )

    assert score >= ACCURACY_FLOOR, (
        f"Mapping accuracy {score:.1%} below floor {ACCURACY_FLOOR:.0%}. "
        f"See {artifact}. Misses: {[r for r in results if not r['correct']]}"
    )


def test_unsigned_bigint_normalizes_to_decimal() -> None:
    from services.type_system import ddl_type, normalize_logical_type

    assert normalize_logical_type("BIGINT UNSIGNED") == "decimal"
    assert normalize_logical_type("uint64") == "decimal"
    native = ddl_type("postgresql", "BIGINT UNSIGNED")
    assert "NUMERIC" in native.upper() or "DECIMAL" in native.upper()
