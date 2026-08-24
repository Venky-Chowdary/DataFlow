"""Map review_kind SSOT — false-friends stay classified, not generic review.

Airbyte schema review is all-or-nothing (airbyte#74892 / #78427). DataFlow
stamps a stable kind so Approve eligible cannot clear qty≠amt / user≠customer.
"""

from __future__ import annotations

import json
from pathlib import Path

from services.semantic_mapper import (
    classify_review_kind,
    map_columns,
    pair_mapping_authority,
)


PROOF_DIR = Path(__file__).resolve().parents[1] / "data" / "proofs"


def test_classify_review_kind_priority() -> None:
    assert (
        classify_review_kind(
            source="order_qty",
            target="order_amt",
            reason="Schematic index match · measure-kind mismatch — review required",
            requires_review=True,
        )
        == "measure_kind"
    )
    assert (
        classify_review_kind(
            source="user_id",
            target="customer_id",
            reason="entity qualifier conflict — review required",
            requires_review=True,
        )
        == "entity_identity"
    )
    assert (
        classify_review_kind(
            source="user_id",
            target="UserID",
            reason="Exact name match · destination identifier collision — review required",
            requires_review=True,
            dest_collisions={"UserID", "userid"},
        )
        == "dest_collision"
    )
    assert (
        classify_review_kind(
            source="sku",
            target="product_id",
            reason="identity leaf mismatch (sku≠id) — review required",
            requires_review=True,
        )
        == "identity_leaf"
    )
    assert (
        classify_review_kind(
            source="created_at",
            target="updated_at",
            reason="temporal polarity conflict — review required",
            requires_review=True,
        )
        == "temporal_polarity"
    )
    assert (
        classify_review_kind(
            source="id",
            target="id",
            reason="Exact name match",
            requires_review=False,
        )
        is None
    )


def test_map_columns_stamps_false_friend_kinds() -> None:
    qty = map_columns(["order_qty"], ["order_amt"])[0]
    assert qty["review_kind"] == "measure_kind"
    user = map_columns(["user_id"], ["customer_id"])[0]
    assert user["review_kind"] == "entity_identity"
    dest = map_columns(["user_id"], ["UserID", "userid"])[0]
    assert dest["review_kind"] == "dest_collision"
    auth = pair_mapping_authority("order_qty", "order_amt")
    assert auth["review_kind"] == "measure_kind"
    assert auth["requires_review"] is True


def test_review_kind_proof_artifact(tmp_path: Path) -> None:
    cases = [
        ("order_qty", ["order_amt"], "measure_kind"),
        ("user_id", ["customer_id"], "entity_identity"),
        ("user_id", ["UserID", "userid"], "dest_collision"),
        ("sku", ["product_id"], "identity_leaf"),
        ("created_at", ["updated_at"], "temporal_polarity"),
        ("cust_id", ["customer_id"], None),
    ]
    rows = []
    correct = 0
    for source, targets, expected in cases:
        row = map_columns([source], targets)[0]
        kind = row.get("review_kind")
        ok = kind == expected
        if expected is None:
            ok = kind is None and row.get("requires_review") is not True
        correct += int(ok)
        rows.append({
            "source": source,
            "targets": targets,
            "proposed_target": row.get("target"),
            "expected_kind": expected,
            "review_kind": kind,
            "requires_review": bool(row.get("requires_review")),
            "confidence": row.get("confidence"),
            "correct": ok,
        })
    proof = {
        "metric": "map_review_kind_ssot",
        "score": round(correct / len(cases), 4),
        "correct": correct,
        "total": len(cases),
        "floor": 1.0,
        "passed": correct == len(cases),
        "honesty": (
            "review_kind is stamped by map_columns. Measured on this named fixture only. "
            "Not a claim that Map is better than Airbyte schema review on all connectors."
        ),
        "cases": rows,
    }
    PROOF_DIR.mkdir(parents=True, exist_ok=True)
    artifact = PROOF_DIR / "map_review_kind_ssot.json"
    artifact.write_text(json.dumps(proof, indent=2), encoding="utf-8")
    (tmp_path / "map_review_kind_ssot.json").write_text(
        json.dumps(proof, indent=2), encoding="utf-8"
    )
    assert proof["passed"], proof
