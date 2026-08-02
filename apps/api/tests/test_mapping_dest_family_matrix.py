"""Source×destination family mapping smoke — typed enterprise cases × dest engines.

Proves the mapper stays at 100% across all enterprise domains when
destination_db_type varies across major warehouse/db families. Algorithm
proof only — not a live network reconcile.
"""

from __future__ import annotations

import json
from pathlib import Path


FIXTURE = Path(__file__).parent / "fixtures" / "mapping_golden_enterprise.json"
PROOF_DIR = Path(__file__).resolve().parents[1] / "data" / "proofs"

DEST_FAMILIES = (
    "snowflake",
    "postgresql",
    "mysql",
    "bigquery",
    "mongodb",
    "sqlserver",
    "oracle",
    "s3",
)


def test_mapping_accuracy_across_destination_families(tmp_path: Path) -> None:
    from services.semantic_mapper import map_columns

    data = json.loads(FIXTURE.read_text(encoding="utf-8"))
    assert len(data["domains"]) >= 5, "enterprise fixture must cover ecommerce/finance/healthcare/hr/logistics"

    domain_scores: dict[str, dict] = {}
    family_rollups: dict[str, dict[str, int]] = {
        dest: {"correct": 0, "total": 0} for dest in DEST_FAMILIES
    }

    for domain in data["domains"]:
        cases = domain["cases"]
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

        per_family: dict[str, dict] = {}
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
            per_family[dest] = {
                "correct": correct,
                "total": len(cases),
                "score": round(correct / len(cases), 4),
                "passed": correct == len(cases),
            }
            family_rollups[dest]["correct"] += correct
            family_rollups[dest]["total"] += len(cases)

        domain_scores[domain["name"]] = {
            "cases": len(cases),
            "families": per_family,
            "all_passed": all(v["passed"] for v in per_family.values()),
        }

    family_scores = {
        dest: {
            "correct": roll["correct"],
            "total": roll["total"],
            "score": round(roll["correct"] / roll["total"], 4) if roll["total"] else 0.0,
            "passed": roll["correct"] == roll["total"],
        }
        for dest, roll in family_rollups.items()
    }

    total_correct = sum(v["correct"] for v in family_scores.values())
    total_cells = sum(v["total"] for v in family_scores.values())

    proof = {
        "metric": "mapping_accuracy_by_destination_family",
        "domains": domain_scores,
        "families": family_scores,
        "aggregate": {
            "correct": total_correct,
            "total": total_cells,
            "score": round(total_correct / total_cells, 4) if total_cells else 0.0,
            "domain_count": len(domain_scores),
            "family_count": len(DEST_FAMILIES),
        },
        "all_passed": all(v["passed"] for v in family_scores.values())
        and all(v["all_passed"] for v in domain_scores.values()),
        "honesty": (
            "Algorithm proof with destination_db_type set across all enterprise "
            "domains — not a live connector introspect/reconcile matrix."
        ),
    }
    PROOF_DIR.mkdir(parents=True, exist_ok=True)
    artifact = PROOF_DIR / "mapping_dest_family_matrix.json"
    artifact.write_text(json.dumps(proof, indent=2), encoding="utf-8")
    (tmp_path / "mapping_dest_family_matrix.json").write_text(
        json.dumps(proof, indent=2), encoding="utf-8"
    )

    failed_domains = [k for k, v in domain_scores.items() if not v["all_passed"]]
    failed_families = [k for k, v in family_scores.items() if not v["passed"]]
    assert not failed_domains and not failed_families, (
        f"Mapping failures domains={failed_domains} families={failed_families}. See {artifact}"
    )


def test_enterprise_mapping_fixture_size() -> None:
    data = json.loads(FIXTURE.read_text(encoding="utf-8"))
    total = sum(len(d["cases"]) for d in data["domains"])
    assert total >= int(data.get("min_cases") or 200)
    assert total == int(data.get("total_cases") or total)
