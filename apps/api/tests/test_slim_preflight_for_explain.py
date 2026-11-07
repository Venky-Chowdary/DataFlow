"""POST /preflight/explain must not require the full 1M-row Validate payload."""

from __future__ import annotations

from services.validation_assistant import explain_validation, slim_preflight_for_explain


def _fat_preflight() -> dict:
    return {
        "passed": False,
        "run_id": "pf_explain_slim",
        "gates": [{"id": f"g{i}", "sample_rows": [["x"] * 50] * 100} for i in range(9)],
        "blockers": [
            {
                "id": "g3f_population_fit",
                "message": "DEP_TIME overflow",
                "details": {
                    "issues": ["overflow"] * 40,
                    "issues_detail": [{"source": "DEP_TIME", "column": "DEP_TIME"}],
                },
            }
        ],
        "population_fit": {
            "evidence": "partial",
            "rows_scanned": 88_629,
            "rows_total": 1_000_000,
            "unfit_rows": 12,
            "findings": [
                {
                    "source": "DEP_TIME",
                    "target": "DEP_TIME",
                    "target_type": "NUMBER(9,6)",
                    "unfit_rows": 12,
                    "example_values": [str(i) for i in range(200)],
                    "suggested_target_type": "NUMBER(15,11)",
                    "suggested_fix": "Widen CREATE",
                }
            ],
        },
        "proof_bundle": {"transfer_decision": {"decision": "review"}},
        "destination_table_exists": False,
    }


def test_slim_drops_gates_and_caps_examples():
    slim = slim_preflight_for_explain(_fat_preflight())
    assert "gates" not in slim
    assert slim["run_id"] == "pf_explain_slim"
    assert slim["blockers"][0]["id"] == "g3f_population_fit"
    examples = slim["population_fit"]["findings"][0]["example_values"]
    assert len(examples) == 3
    assert len(slim["blockers"][0]["details"]["issues"]) == 10


def test_explain_works_on_slimed_payload():
    result = explain_validation(_fat_preflight(), dest_kind="snowflake", use_llm=False)
    assert result["passed"] is False
    assert result["column_fixes"] or result["issues"]
    # Fat gates must not be required.
    slim = slim_preflight_for_explain(_fat_preflight())
    again = explain_validation(slim, dest_kind="snowflake", use_llm=False)
    assert again["passed"] is False
    assert again["summary"]
