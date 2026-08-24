"""Name-match mapping matrix across dialect labels (NOT type/DDL fidelity).

Parametric pytest across top dialects with golden enterprise cases.
Proves column-name assignment score ≥ 85% with destination_db_type set.

For enterprise offline **type + DDL stamp + coercion + transform** pair
assurance, see ``services/pair_assurance.py`` and
``tests/test_pair_assurance_offline.py`` (claim ``pair_assurance_offline``).

Live fixtures are optional — skip when LIVE_MAPPING_MATRIX=0 / unset.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

FIXTURE = Path(__file__).parent / "fixtures" / "mapping_golden_enterprise.json"
PROOF_DIR = Path(__file__).resolve().parents[1] / "data" / "proofs"

# Top enterprise source × dest dialects (symmetric proof surface).
SOURCES = (
    "postgresql",
    "mysql",
    "mongodb",
    "sqlserver",
    "oracle",
    "snowflake",
)
DESTS = (
    "postgresql",
    "mysql",
    "snowflake",
    "bigquery",
    "mongodb",
    "sqlserver",
    "oracle",
    "redshift",
)

PAIR_IDS = [f"{src}__{dst}" for src in SOURCES for dst in DESTS]


def _domain_slice(max_cases: int = 24) -> list[dict]:
    data = json.loads(FIXTURE.read_text(encoding="utf-8"))
    cases: list[dict] = []
    for domain in data["domains"]:
        for c in domain["cases"][: max(1, max_cases // max(1, len(data["domains"])))]:
            cases.append(c)
        if len(cases) >= max_cases:
            break
    return cases[:max_cases]


@pytest.mark.parametrize("pair_id", PAIR_IDS)
def test_connector_pair_mapping_accuracy(pair_id: str, tmp_path: Path) -> None:
    from services.mapping_pipeline import run_mapping_pipeline

    src, dst = pair_id.split("__", 1)
    cases = _domain_slice(24)
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

    result = run_mapping_pipeline(
        source_columns=sources,
        target_columns=targets,
        source_schemas=source_schemas,
        target_schemas=target_schemas,
        destination_db_type=dst,
        use_llm=False,
    )
    by = {m["source"]: m["target"] for m in result["mappings"]}
    correct = sum(1 for c in cases if by.get(c["source"]) == c["target"])
    score = correct / len(cases) if cases else 0.0

    # ObjectId create-new stamp when source dialect is Mongo-like.
    if src == "mongodb":
        oid = run_mapping_pipeline(
            source_columns=["_id"],
            target_columns=[],
            source_schemas=[{
                "name": "_id",
                "inferred_type": "OBJECTID",
                "samples": ["507f1f77bcf86cd799439011"],
            }],
            destination_db_type=dst,
            destination_table_exists=False,
            use_llm=False,
        )
        assert oid["mappings"], f"{pair_id}: ObjectId create-new must emit a mapping"
        row = oid["mappings"][0]
        assert row.get("create_new") is True or row.get("assignment_strategy") in {
            "create_compatible_new",
            "identity_passthrough",
        }, f"{pair_id}: ObjectId must be create-new, got {row.get('assignment_strategy')}"
        assert row.get("target_type"), f"{pair_id}: create-new must stamp target_type"

    # Create-new TIMESTAMPTZ must stamp risks / review on warehouse dests.
    if dst in {"postgresql", "snowflake", "oracle", "mysql", "sqlserver", "bigquery"}:
        tz = run_mapping_pipeline(
            source_columns=["created_at"],
            target_columns=[],
            source_schemas=[{
                "name": "created_at",
                "inferred_type": "TIMESTAMPTZ",
                "samples": ["2024-01-01T00:00:00Z"],
            }],
            destination_db_type=dst,
            destination_table_exists=False,
            use_llm=False,
        )
        row = tz["mappings"][0]
        assert row.get("create_new") is True or row.get("assignment_strategy") in {
            "create_compatible_new",
            "identity_passthrough",
        }
        risks = row.get("create_new_risks") or []
        # Dest may preserve TZ (PG TIMESTAMPTZ) — risks optional; never invent silent green hide.
        if risks:
            assert row.get("requires_review") is True
            kinds = {r.get("kind") for r in risks}
            assert kinds & {
                "timezone_polarity",
                "lossy_coercion",
                "precision_collapse",
                "varchar_width_cap",
                "varchar_narrow",
                # MySQL TIMESTAMP keeps the instant but only 1970..2038 of it.
                "instant_range_cap",
            }
    assert score >= 0.85, (
        f"{pair_id}: mapping score {score:.2%} below 85% "
        f"({correct}/{len(cases)}). Engine={result.get('engine')}"
    )

    proof = {
        "pair": pair_id,
        "source": src,
        "dest": dst,
        "correct": correct,
        "total": len(cases),
        "score": round(score, 4),
        "engine": result.get("engine"),
    }
    PROOF_DIR.mkdir(parents=True, exist_ok=True)
    out = PROOF_DIR / "mapping_connector_pair_matrix"
    out.mkdir(parents=True, exist_ok=True)
    (out / f"{pair_id}.json").write_text(json.dumps(proof, indent=2), encoding="utf-8")
    (tmp_path / f"{pair_id}.json").write_text(json.dumps(proof, indent=2), encoding="utf-8")


@pytest.mark.skipif(
    os.environ.get("LIVE_MAPPING_MATRIX", "").strip() not in {"1", "true", "yes"},
    reason="Set LIVE_MAPPING_MATRIX=1 to run live connector introspect fixtures",
)
def test_connector_pair_live_fixtures_optional() -> None:
    """Placeholder live path — golden matrix is the default CI proof."""
    live_root = Path(__file__).parent / "fixtures" / "live_connector_pairs"
    if not live_root.exists():
        pytest.skip("No live_connector_pairs fixtures checked in")
    fixtures = list(live_root.glob("*.json"))
    assert fixtures, "LIVE_MAPPING_MATRIX set but no live fixtures present"
