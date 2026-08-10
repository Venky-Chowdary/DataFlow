"""Every source column is written, declared omitted, or blocks the run."""

from __future__ import annotations

from typing import Any

from services.mapping_constraints import (
    classify_source_coverage,
    detect_duplicate_targets,
    enforce_destination_constraints,
)
from services.preflight_service import run_file_preflight

SOURCE_COLUMNS = ["id"] + [f"c{i}" for i in range(1, 31)]
SOURCE_TYPES = {"id": "BIGINT", **{f"c{i}": "VARCHAR(64)" for i in range(1, 31)}}
DEST_TYPES = {"id": "BIGINT", **{f"c{i}": "VARCHAR(64)" for i in range(1, 21)}}
SAMPLE_ROWS = [
    {"id": i, **{f"c{j}": f"v{i}_{j}" for j in range(1, 31)}} for i in range(1, 6)
]


def _mapped() -> list[dict[str, Any]]:
    rows = [
        {
            "source": "id",
            "target": "id",
            "source_type": "BIGINT",
            "target_type": "BIGINT",
            "transform": "none",
            "confidence": 0.99,
        }
    ]
    rows += [
        {
            "source": f"c{i}",
            "target": f"c{i}",
            "source_type": "VARCHAR(64)",
            "target_type": "VARCHAR(64)",
            "transform": "none",
            "confidence": 0.97,
        }
        for i in range(1, 21)
    ]
    return rows


def _omissions() -> list[dict[str, Any]]:
    return [
        {
            "source": f"c{i}",
            "target": "",
            "source_type": "VARCHAR(64)",
            "target_type": "",
            "transform": "none",
            "confidence": 0.0,
            "intentional_omit": True,
        }
        for i in range(21, 31)
    ]


def _preflight(mappings: list[dict[str, Any]]) -> dict[str, Any]:
    return run_file_preflight(
        columns=SOURCE_COLUMNS,
        column_types=SOURCE_TYPES,
        row_count=len(SAMPLE_ROWS),
        mappings=mappings,
        sample_rows=SAMPLE_ROWS,
        source_kind="database",
        destination_db_type="postgresql",
        destination_table_exists=True,
        destination_column_types=DEST_TYPES,
        destination_can_write=True,
        destination_connected=True,
        sync_mode="incremental_append",
    )


def _gate(pf: dict[str, Any], gate_id: str) -> dict[str, Any]:
    return next(g for g in pf["gates"] if g.get("id") == gate_id)


def test_classify_counts_written_omitted_and_unaccounted():
    coverage = classify_source_coverage(SOURCE_COLUMNS, _mapped() + _omissions())

    assert coverage["complete"] is True
    assert coverage["omitted"] == [f"c{i}" for i in range(21, 31)]
    assert coverage["unaccounted"] == []


def test_classify_reports_columns_nobody_mentioned():
    coverage = classify_source_coverage(SOURCE_COLUMNS, _mapped())

    assert coverage["complete"] is False
    assert coverage["unaccounted"] == [f"c{i}" for i in range(21, 31)]


def test_thirty_into_twenty_without_omissions_blocks_and_names_the_columns():
    pf = _preflight(_mapped())

    gate = _gate(pf, "g13_source_coverage")
    assert gate["status"] == "block"
    assert gate["details"]["unaccounted_sources"] == [f"c{i}" for i in range(21, 31)]
    assert "c21" in gate["message"]
    assert pf["passed"] is False


def test_declared_omissions_pass_and_are_recorded_as_the_decision():
    pf = _preflight(_mapped() + _omissions())

    gate = _gate(pf, "g13_source_coverage")
    assert gate["status"] == "pass"
    assert gate["details"]["omitted_sources"] == [f"c{i}" for i in range(21, 31)]
    assert pf["passed"] is True


def test_declared_omissions_are_not_probed_as_transform_or_coercion_failures():
    pf = _preflight(_mapped() + _omissions())

    omitted = {f"c{i}" for i in range(21, 31)}
    for gate in pf["gates"]:
        issues = " ".join(
            str(x) for x in (gate.get("details") or {}).get("issues") or []
        )
        assert not (omitted & set(issues.split())), f"{gate['id']} probed an omission"
    for col in (pf.get("coercion_report") or {}).get("columns") or []:
        assert col.get("source") not in omitted


def test_row_with_a_source_but_no_target_does_not_account_for_the_column():
    """A half-written map row is not a decision — it must still block."""
    malformed = [dict(m) for m in _mapped()]
    malformed.append(
        {"source": "c21", "target": "", "source_type": "VARCHAR(64)", "confidence": 0.4}
    )
    coverage = classify_source_coverage(SOURCE_COLUMNS, malformed)

    assert "c21" in coverage["unaccounted"]
    assert _gate(_preflight(malformed), "g13_source_coverage")["status"] == "block"


def test_mapping_dropped_by_destination_constraints_becomes_unaccounted():
    """Constraint filtering removes the row; the source must not vanish with it."""
    kept, dropped, _invented = enforce_destination_constraints(
        [*_mapped(), {"source": "c21", "target": "not_in_destination", "confidence": 0.9}],
        list(DEST_TYPES),
    )

    assert dropped == ["c21"]
    assert "c21" in classify_source_coverage(SOURCE_COLUMNS, kept)["unaccounted"]


def test_low_confidence_mapping_dropped_from_the_active_map_blocks():
    kept, dropped, _invented = enforce_destination_constraints(
        [*_mapped(), {"source": "c21", "target": "c1", "confidence": 0.1}],
        list(DEST_TYPES),
        confidence_floor=0.55,
    )

    assert dropped == ["c21"]
    assert "c21" in classify_source_coverage(SOURCE_COLUMNS, kept)["unaccounted"]


def test_create_new_mapping_counts_as_written():
    """An authorized ADD COLUMN target carries the column — it is not a drop."""
    mappings = [
        *_mapped(),
        *_omissions()[1:],
        {
            "source": "c21",
            "target": "c21",
            "source_type": "VARCHAR(64)",
            "target_type": "VARCHAR(64)",
            "confidence": 0.95,
            "create_new": True,
        },
    ]
    coverage = classify_source_coverage(SOURCE_COLUMNS, mappings)

    assert coverage["unaccounted"] == []
    assert "c21" in coverage["written"]


def test_two_targets_fed_by_one_source_still_account_for_it_once():
    mappings = [
        *_mapped(),
        *_omissions()[1:],
        {
            "source": "c21",
            "target": "c20",
            "source_type": "VARCHAR(64)",
            "target_type": "VARCHAR(64)",
            "confidence": 0.9,
        },
    ]
    coverage = classify_source_coverage(SOURCE_COLUMNS, mappings)

    assert coverage["unaccounted"] == []
    assert coverage["written"].count("c21") == 1


def test_two_sources_writing_one_target_is_reported_as_a_duplicate():
    mappings = [
        *_mapped(),
        *_omissions()[1:],
        {
            "source": "c21",
            "target": "c20",
            "source_type": "VARCHAR(64)",
            "target_type": "VARCHAR(64)",
            "confidence": 0.9,
        },
    ]

    assert detect_duplicate_targets(mappings) == ["c20"]


def test_coverage_is_published_in_the_proof_bundle():
    pf = _preflight(_mapped() + _omissions())

    bundle = pf.get("proof_bundle") or {}
    assert bundle.get("source_coverage", {}).get("omitted") == [
        f"c{i}" for i in range(21, 31)
    ]
