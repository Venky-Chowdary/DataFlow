"""Source×destination family mapping smoke — typed enterprise cases × dest engines.

Proves the mapper stays at 100% when destination_db_type varies across the
major warehouse/db families used in Transfer Studio. This is algorithm proof,
not a live network reconcile.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

FIXTURE = Path(__file__).parent / "fixtures" / "mapping_golden_enterprise.json"
PROOF_DIR = Path(__file__).resolve().parents[1] / "data" / "proofs"

DEST_FAMILIES = (
    "snowflake",
    "postgresql",
    "mysql",
    "bigquery",
    "mongodb",
    "sqlserver",
)


def test_mapping_accuracy_across_destination_families(tmp_path: Path) -> None:
    from services.semantic_mapper import map_columns

    data = json.loads(FIXTURE.read_text(encoding="utf-8"))
    # Use ecommerce domain as representative typed batch.
    cases = next(d["cases"] for d in data["domains"] if d["name"] == "ecommerce")
    sources = [c["source"] for c in cases]
    targets = [c["target"] for c in cases]
    source_schemas = [
        {"name": c["source"], "inferred_type": c["source_type"], "samples": []}
        for c in cases
    ]
    target_schemas = [
        {"name": c["target"], "inferred_type": c["target_type"], "samples": []}
        for c in cases
    ]

    family_scores: dict[str, dict] = {}
    for dest in DEST_FAMILIES:
        mapped = map_columns(
            sources,
            targets,
            source_schemas=source_schemas,
            target_schemas=target_schemas,
            destination_db_type=dest,
        )
        by = {m["source"]: m["target"] for m in mapped}
        correct = sum(1 for c in cases if by.get(c["source"]) == c["target"])
        family_scores[dest] = {
            "correct": correct,
            "total": len(cases),
            "score": round(correct / len(cases), 4),
            "passed": correct == len(cases),
        }

    proof = {
        "metric": "mapping_accuracy_by_destination_family",
        "families": family_scores,
        "all_passed": all(v["passed"] for v in family_scores.values()),
        "fixture_domain": "ecommerce",
        "honesty": (
            "Algorithm proof with destination_db_type set — not a live "
            "connector introspect/reconcile matrix."
        ),
    }
    PROOF_DIR.mkdir(parents=True, exist_ok=True)
    artifact = PROOF_DIR / "mapping_dest_family_matrix.json"
    artifact.write_text(json.dumps(proof, indent=2), encoding="utf-8")
    (tmp_path / "mapping_dest_family_matrix.json").write_text(
        json.dumps(proof, indent=2), encoding="utf-8"
    )

    failed = [k for k, v in family_scores.items() if not v["passed"]]
    assert not failed, f"Destination families failed: {failed}. See {artifact}"
