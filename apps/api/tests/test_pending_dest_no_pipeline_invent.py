"""Partial Studio: pipeline must not invent target_type on pending_dest_schema."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_pipeline_pending_dest_leaves_target_type_empty():
    from services.mapping_pipeline import run_mapping_pipeline

    # Unknown existence + empty dest columns = pending (not create-new invent).
    result = run_mapping_pipeline(
        ["id", "amount"],
        [],
        source_schemas=[
            {"name": "id", "inferred_type": "INTEGER", "samples": ["1"]},
            {"name": "amount", "inferred_type": "DECIMAL", "samples": ["1.5"]},
        ],
        target_schemas=[],
        destination_db_type="postgresql",
        destination_table_exists=None,
        use_llm=False,
    )
    assert result["mappings"], "expected pending mappings"
    for m in result["mappings"]:
        assert m.get("assignment_strategy") == "pending_dest_schema", m
        assert not str(m.get("target_type") or "").strip(), (
            f"pipeline invented target_type={m.get('target_type')!r} under pending Studio"
        )
        assert m.get("requires_review") is True
        # Must not look like dest-proven preserve Ready.
        assert float(m.get("confidence") or 0) < 0.9


def test_schema_drift_decimal_scale_narrow():
    from services.schema_drift import classify_schema_change

    report = classify_schema_change(
        {"qty": "DECIMAL(10,4)"},
        {"qty": "DECIMAL(10,2)"},
        dest_db="postgresql",
    )
    kinds = {c["kind"] for c in report.get("breaking", [])}
    assert "narrow_type" in kinds, report


def test_country_full_name_not_iso_code():
    from services.semantic_analyzer import analyze_column

    out = analyze_column(
        "Country",
        "VARCHAR",
        samples=["Somalia", "Yemen", "South Sudan"],
    )
    assert out["semantic_role"] != "country_code"
    assert out["semantic_role"] == "description"
