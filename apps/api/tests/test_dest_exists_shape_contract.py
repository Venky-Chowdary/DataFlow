"""Dest-exists shape contract — named matrix, not two anecdotal routes.

Closes competitor holes this product must not copy:
- Airbyte all-or-nothing / silent stream fail (airbyte#74892, #78427)
- Fivetran rename = add + stale
- AWS DMS dest-exists does not refresh extra columns
- dbt positional INSERT (dbt-databricks#1289)
- Informatica Snowflake jumble without column_mapping=name
"""

from __future__ import annotations

import json
from pathlib import Path

from services.destination_requirements_gate import build_mapping_contract_gates
from services.shape_contract import (
    GATE_ID,
    WRITE_BY_NAME,
    classify_dest_exists_shape,
    insert_sql_is_name_addressed,
    project_named_write,
)


FIXTURE = Path(__file__).resolve().parent / "fixtures" / "dest_exists_shape_matrix.json"
PROOF_DIR = Path(__file__).resolve().parents[1] / "data" / "proofs"


def _load_cases() -> list[dict]:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))["cases"]


def _classify(case: dict) -> dict:
    return classify_dest_exists_shape(
        destination_table_exists=case.get("table_exists"),
        source_columns=case.get("source") or [],
        dest_columns=case.get("dest") or [],
        mappings=case.get("mappings") or [],
        column_nullability=case.get("nullability") or {},
        column_defaults=case.get("defaults") or {},
        identity_columns=case.get("identity") or [],
        generated_columns=case.get("generated") or [],
    )


def test_dest_exists_shape_matrix() -> None:
    cases = _load_cases()
    rows = []
    correct = 0
    for case in cases:
        contract = _classify(case)
        ok = contract["shape"] == case["expect_shape"]
        ok = ok and contract["write_by"] == WRITE_BY_NAME
        if "expect_unaccounted" in case:
            ok = ok and len(contract["unaccounted_sources"]) == case["expect_unaccounted"]
        if "expect_unfilled" in case:
            ok = ok and len(contract["unfilled_required"]) == case["expect_unfilled"]
        if "expect_add" in case:
            ok = ok and contract["counts"]["add_proposed"] == case["expect_add"]
        if "expect_omit" in case:
            ok = ok and contract["counts"]["omit"] == case["expect_omit"]
        if "expect_pending" in case:
            ok = ok and contract["counts"]["pending"] == case["expect_pending"]
        if "expect_false_friend" in case:
            ok = ok and contract["counts"]["false_friend"] == case["expect_false_friend"]
        if "expect_dest_preserve" in case:
            ok = ok and contract["counts"]["dest_only_preserve"] == case["expect_dest_preserve"]
        if "source_row" in case:
            projected = project_named_write(
                source_row=case["source_row"],
                mappings=case.get("mappings") or [],
                dest_columns=case.get("dest") or [],
            )
            ok = ok and projected == case["expect_projection"]
            ok = ok and "extra" not in projected
            ok = ok and "updated_at" not in projected
        correct += int(ok)
        rows.append(
            {
                "id": case["id"],
                "note": case.get("note"),
                "expected": case["expect_shape"],
                "got": contract["shape"],
                "headline": contract["headline"],
                "correct": ok,
            }
        )
    proof = {
        "metric": "dest_exists_shape_contract",
        "score": round(correct / len(cases), 4),
        "correct": correct,
        "total": len(cases),
        "floor": 1.0,
        "passed": correct == len(cases),
        "honesty": (
            "Measured on dest_exists_shape_matrix.json only. "
            "Not a live warehouse matrix. CDC remains at-least-once upsert."
        ),
        "cases": rows,
    }
    PROOF_DIR.mkdir(parents=True, exist_ok=True)
    (PROOF_DIR / "dest_exists_shape_contract.json").write_text(
        json.dumps(proof, indent=2), encoding="utf-8"
    )
    assert proof["passed"], proof


def test_name_addressed_write_survives_source_reorder() -> None:
    """dbt-databricks#1289: adding a column that is not last must not shift values."""
    mappings = [
        {"source": "id", "target": "id"},
        {"source": "note", "target": "name"},
    ]
    dest = ["id", "name", "updated_at"]
    row_a = {"note": "hello", "id": 7, "extra": "x"}
    row_b = {"id": 7, "extra": "x", "note": "hello"}
    assert project_named_write(source_row=row_a, mappings=mappings, dest_columns=dest) == {
        "id": 7,
        "name": "hello",
    }
    assert project_named_write(source_row=row_b, mappings=mappings, dest_columns=dest) == {
        "id": 7,
        "name": "hello",
    }


def test_insert_sql_name_addressed_rejects_positional_values() -> None:
    named = "INSERT INTO t (id, name) VALUES (:id, :name)"
    positional = "INSERT INTO t VALUES (?, ?)"
    assert insert_sql_is_name_addressed(named) is True
    assert insert_sql_is_name_addressed(positional) is False
    mssql_stage = "INSERT INTO #df_mrg_1 ([id], [name]) VALUES (:id, :name)"
    assert insert_sql_is_name_addressed(mssql_stage) is True


def test_g15_does_not_duplicate_g13_g14_blockers() -> None:
    coverage, gates, blockers = build_mapping_contract_gates(
        source_columns=["id", "loyalty_tier"],
        mappings=[{"source": "id", "target": "id", "confidence": 0.99}],
        destination_table_exists=True,
        column_nullability={"id": False, "tenant_id": False},
        column_defaults={},
        identity_columns=[],
        generated_columns=[],
        dest_columns=["id", "tenant_id"],
    )
    by_id = {g["id"]: g for g in gates}
    assert "g13_source_coverage" in by_id
    assert "g14_destination_requirements" in by_id
    assert GATE_ID in by_id
    assert by_id[GATE_ID]["status"] != "block"
    assert coverage["shape_contract"]["shape"] == "overlap"
    assert coverage["shape_contract"]["write_by"] == WRITE_BY_NAME
    blocker_ids = {b["id"] for b in blockers}
    assert GATE_ID not in blocker_ids
    assert "g13_source_coverage" in blocker_ids
    assert "g14_destination_requirements" in blocker_ids
