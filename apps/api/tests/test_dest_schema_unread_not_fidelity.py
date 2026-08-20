"""An unread destination type is a catalog gap, never a fidelity verdict.

Regression (Snowflake → MySQL, new table): the destination schema never
loaded, so every column was stamped ``fidelity=cast`` with a source→source
type path. Map then printed ``VARCHAR(16777216) → VARCHAR(16777216) loses
fidelity`` on ten columns and demanded a Risk Contract that no signature
could clear, because there was no destination type to compare against.

A proven-absent destination table is the opposite case: it is create-new and
must receive real destination types.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.decision_kernel.findings import (  # noqa: E402
    FailureClass,
    findings_from_coercion_report,
    recommended_action_for_failure,
)
from services.mapping_pipeline import run_mapping_pipeline  # noqa: E402
from services.shape_contract import (  # noqa: E402
    FIDELITY_DEST_TYPE_UNREAD,
    SHAPE_CREATE_NEW,
    SHAPE_UNKNOWN,
)

_SOURCE_COLUMNS = ["employee_id", "age"]
_SOURCE_SCHEMAS = [
    {"name": "employee_id", "inferred_type": "VARCHAR(16777216)", "samples": ["EMP0000001"]},
    {"name": "age", "inferred_type": "BIGINT", "samples": ["34"]},
]


def _pipeline(table_exists: bool | None) -> dict:
    return run_mapping_pipeline(
        _SOURCE_COLUMNS,
        [],
        source_schemas=_SOURCE_SCHEMAS,
        target_schemas=[],
        source_db_type="snowflake",
        destination_db_type="mysql",
        destination_table_exists=table_exists,
        use_llm=False,
    )


def test_unproven_destination_is_unread_not_a_cast() -> None:
    result = _pipeline(None)
    assert result["mappings"]
    for m in result["mappings"]:
        assert m["assignment_strategy"] == "pending_dest_schema", m
        assert not str(m.get("target_type") or "").strip()
        assert m["fidelity"] == FIDELITY_DEST_TYPE_UNREAD, m
        # The false claim: a conversion verdict on a comparison nobody made.
        assert m["fidelity"] not in {"cast", "lossy_cast", "mutate"}
        assert "loses fidelity" not in str(m.get("fidelity_reason") or "").lower()
    assert result["shape_contract"]["shape"] == SHAPE_UNKNOWN


def test_proven_absent_destination_table_is_create_new_with_real_types() -> None:
    result = _pipeline(False)
    assert result["shape_contract"]["shape"] == SHAPE_CREATE_NEW
    by_source = {m["source"]: m for m in result["mappings"]}
    for src, mapping in by_source.items():
        assert mapping["assignment_strategy"] != "pending_dest_schema", mapping
        target_type = str(mapping.get("target_type") or "").strip()
        assert target_type, f"{src} left without a destination type on create-new"
        # A create-new MySQL column may not carry a Snowflake-only carrier.
        assert "16777216" not in target_type, mapping
        assert mapping.get("fidelity") != FIDELITY_DEST_TYPE_UNREAD, mapping


def test_coercion_probe_classifies_unread_dest_type_separately() -> None:
    from services.coercion_probe import analyze_coercion

    report = analyze_coercion(
        sample_rows=[{"employee_id": "EMP0000001"}],
        mappings=[{"source": "employee_id", "target": "employee_id", "target_type": ""}],
        source_types={"employee_id": "VARCHAR(16777216)"},
        dest_types={},
        dest_db_type="mysql",
    )
    col = report["by_source"]["employee_id"]
    # Still fail-closed …
    assert col["severity"] == "block"
    # … but named as the catalog gap it is, with no fidelity claim.
    assert col["fidelity_collapse"] is False
    assert col["dest_schema_unloaded"] is True
    assert col["failure_class"] == FailureClass.DEST_SCHEMA_UNLOADED.value
    assert report["dest_schema_unloaded_columns"] == ["employee_id"]

    findings = findings_from_coercion_report(report, dest_db="mysql")
    assert findings and findings[0]["failure_class"] == FailureClass.DEST_SCHEMA_UNLOADED.value
    assert findings[0]["blocking"] is True
    # Suggesting a target type here would invent the missing fact.
    assert not findings[0].get("suggested_target_type")
    action = recommended_action_for_failure(
        FailureClass.DEST_SCHEMA_UNLOADED, source="employee_id"
    )
    assert "Reload destination schema" in action
    assert "Risk Contract" not in action.replace(
        "no widen or Risk Contract applies", ""
    )
