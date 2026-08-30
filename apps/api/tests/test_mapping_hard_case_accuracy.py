"""Hard-case mapping proof — dest-exists, collisions, identity/measure false-friends.

Synonym goldens can score 100% on easy abbreviation pairs and still auto-pin
``order_qty`` onto ``order_amt``. This fixture is the enterprise bar: pin the
true synonym, hold Map on false-friends, never invent CREATE when dest exists.
"""

from __future__ import annotations

import json
from pathlib import Path


FIXTURE = Path(__file__).parent / "fixtures" / "mapping_golden_hard_cases.json"
PROOF_DIR = Path(__file__).resolve().parents[1] / "data" / "proofs"
ACCURACY_FLOOR = 1.0


def _load() -> dict:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def _map(sources: list[str], targets: list[str], **kwargs):
    from services.semantic_mapper import map_columns

    return map_columns(sources, targets, **kwargs)


def test_mapping_hard_cases_with_proof_artifact(tmp_path: Path) -> None:
    data = _load()
    results: list[dict] = []
    correct = 0
    total = 0

    for case in data["pin"]:
        total += 1
        mapped = _map(case["sources"], case["targets"])
        by = {m["source"]: m for m in mapped}
        misses = []
        for src, expected in case["expect"].items():
            row = by.get(src) or {}
            if row.get("target") != expected:
                misses.append({"source": src, "expected": expected, "predicted": row.get("target")})
            if case.get("max_review") is False and row.get("requires_review") is True:
                misses.append({"source": src, "error": "unexpected_review", "row": row.get("reasoning")})
        ok = not misses
        correct += int(ok)
        results.append({"id": case["id"], "kind": "pin", "correct": ok, "misses": misses})

    for case in data["review"]:
        total += 1
        mapped = _map(case["sources"], case["targets"])
        assert mapped, case["id"]
        row = mapped[0]
        allowed = case.get("expect_target_in") or [case.get("expect_target")]
        ok = (
            row.get("target") in allowed
            and row.get("create_new") is not True
            and row.get("requires_review") is True
            and float(row.get("confidence") or 0) <= float(case["max_confidence"])
        )
        correct += int(ok)
        results.append({
            "id": case["id"],
            "kind": "review",
            "correct": ok,
            "predicted": row.get("target"),
            "confidence": row.get("confidence"),
            "requires_review": row.get("requires_review"),
            "create_new": row.get("create_new"),
            "reasoning": row.get("reasoning"),
        })

    for case in data["dest_honesty"]:
        total += 1
        mapped = _map(
            case["sources"],
            case["targets"],
            destination_table_exists=case.get("destination_table_exists"),
        )
        row = mapped[0]
        # Review is per case: an unread destination must be held, while a
        # proven-absent table is a lossless identity CREATE the operator
        # approves — holding that one turns every new table into contract spam.
        ok = (
            bool(row.get("create_new")) is bool(case["create_new"])
            and bool(row.get("requires_review")) is bool(case.get("requires_review", True))
        )
        if case.get("assignment_strategy"):
            ok = ok and row.get("assignment_strategy") == case["assignment_strategy"]
        if case.get("mapping_class"):
            ok = ok and row.get("mapping_class") == case["mapping_class"]
        correct += int(ok)
        results.append({
            "id": case["id"],
            "kind": "dest_honesty",
            "correct": ok,
            "create_new": row.get("create_new"),
            "strategy": row.get("assignment_strategy"),
            "confidence": row.get("confidence"),
        })

    score = correct / max(total, 1)
    proof = {
        "metric": "hard_case_mapping_accuracy",
        "score": round(score, 4),
        "correct": correct,
        "total": total,
        "floor": ACCURACY_FLOOR,
        "passed": score >= ACCURACY_FLOOR,
        "engine": "semantic_mapper.map_columns",
        "fixture": "fixtures/mapping_golden_hard_cases.json",
        "honesty": (
            "Measured on named hard cases (dest-exists, identifier collision, "
            "quantity≠amount, user≠customer, sku≠product_id). "
            "Not a marketing claim and not 100% of all enterprise schemas."
        ),
        "competitor_pain": [
            "Airbyte approve-all-myself can silently fail streams on column removal",
            "Fivetran rename = new column + stale old column",
            "Fivetran no-PK hashes all columns so any edit looks like delete+insert",
            "Case-fold collisions (UserID vs userid) skip or overwrite",
        ],
        "cases": results,
        "misses": [r for r in results if not r["correct"]],
    }
    PROOF_DIR.mkdir(parents=True, exist_ok=True)
    artifact = PROOF_DIR / "mapping_hard_case_accuracy.json"
    artifact.write_text(json.dumps(proof, indent=2), encoding="utf-8")
    (tmp_path / "mapping_hard_case_accuracy.json").write_text(
        json.dumps(proof, indent=2), encoding="utf-8"
    )
    assert score >= ACCURACY_FLOOR, (
        f"Hard-case mapping accuracy {score:.1%} below floor. "
        f"See {artifact}. Misses: {proof['misses']}"
    )


def test_schema_rename_is_semantic_not_type_only() -> None:
    """AMT drop + quantity add is drop+add, not a Fivetran-style rename."""
    from services.schema_drift import classify_schema_change

    report = classify_schema_change(
        {"id": "INTEGER", "AMT": "DECIMAL"},
        {"id": "INTEGER", "quantity": "DECIMAL"},
    )
    kinds = {c["kind"] for c in report["breaking"]} | {c["kind"] for c in report["additive"]}
    assert "rename" not in {c["kind"] for c in report["breaking"]}
    assert "drop" in kinds
    assert "add_column" in kinds

    real_rename = classify_schema_change(
        {"cust_id": "INTEGER", "AMT": "DECIMAL"},
        {"customer_id": "INTEGER", "amount": "DECIMAL"},
    )
    assert {c["kind"] for c in real_rename["breaking"]} == {"rename"}
    assert len(real_rename["renamed"]) == 2
