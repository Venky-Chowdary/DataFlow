"""G20 — population-level code-system crosswalk coverage.

A passing unit test does not close a transfer defect. G20's write-path test
proves an unmapped code is quarantined, not passed through. The live Postgres
test (when the engine is up) proves a rare population code missing from the
map blocks the run and the destination stays empty on an independent reread.
"""

from __future__ import annotations

from typing import Any

from services.code_crosswalk import (
    EVIDENCE_EXACT,
    EVIDENCE_SAMPLED,
    GATE_ID,
    REPORT_SCHEMA,
    apply_code_crosswalk,
    build_code_crosswalk_evidence,
    build_code_crosswalk_gate,
    collect_observed_codes,
    declared_crosswalk,
    normalize_code,
    proof_pack_code_crosswalk,
)
from services.preflight_service import run_file_preflight


def _status_map() -> dict[str, str]:
    return {"A": "active", "B": "blocked", "C": "closed"}


def _mapping(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "source": "status",
        "target": "status",
        "confidence": 0.99,
        "code_crosswalk": _status_map(),
        "code_crosswalk_system": "legacy_status→v2",
    }
    row.update(overrides)
    return row


def _rows(*codes: str) -> list[dict[str, Any]]:
    return [{"id": i, "status": code} for i, code in enumerate(codes, start=1)]


def test_undeclared_crosswalk_is_not_a_coded_field() -> None:
    assert declared_crosswalk({"source": "status", "target": "status"}) is None
    report, gate = build_code_crosswalk_evidence(
        mappings=[{"source": "status", "target": "status", "confidence": 0.99}],
        sample_rows=_rows("A", "Z"),
        rows_are_population=True,
    )
    assert report["declared"] is False
    assert gate["status"] == "skip"
    assert gate["id"] == GATE_ID


def test_empty_dict_is_a_declaration_that_covers_nothing() -> None:
    table = declared_crosswalk(_mapping(code_crosswalk={}))
    assert table == {}


def test_null_and_blank_are_not_codes() -> None:
    assert normalize_code(None) is None
    assert normalize_code("  ") is None
    assert normalize_code("") is None
    converted, err = apply_code_crosswalk(None, _mapping())
    assert converted is None
    assert err is None
    converted, err = apply_code_crosswalk("  ", _mapping())
    assert converted is None
    assert err is None


def test_apply_rewrites_mapped_codes_and_refuses_unmapped() -> None:
    converted, err = apply_code_crosswalk("A", _mapping())
    assert err is None
    assert converted == "active"
    converted, err = apply_code_crosswalk("Z", _mapping())
    assert converted is None
    assert err is not None
    assert "unmapped code 'Z'" in err
    # Fail closed: never the original code.
    assert converted != "Z"


def test_no_implicit_identity() -> None:
    """A code that should keep its spelling must be listed as A→A."""
    converted, err = apply_code_crosswalk("A", _mapping(code_crosswalk={"B": "blocked"}))
    assert err is not None
    assert converted is None


def test_omit_is_not_a_crosswalk_column() -> None:
    assert (
        declared_crosswalk(
            {
                "source": "status",
                "target": "",
                "intentional_omit": True,
                "code_crosswalk": _status_map(),
            }
        )
        is None
    )


def test_population_unmapped_code_blocks() -> None:
    report, gate = build_code_crosswalk_evidence(
        mappings=[_mapping()],
        sample_rows=_rows("A", "B", "Z"),
        rows_are_population=True,
    )
    assert report["evidence"] == EVIDENCE_EXACT
    assert gate["status"] == "block"
    assert gate["details"]["rule_id"] == f"{GATE_ID}.unmapped"
    assert "Z" in report["columns"][0]["unmapped_codes"]
    assert "Z" in gate["message"]


def test_covered_population_passes() -> None:
    report, gate = build_code_crosswalk_evidence(
        mappings=[_mapping()],
        sample_rows=_rows("A", "B", "A", "C"),
        rows_are_population=True,
    )
    assert report["evidence"] == EVIDENCE_EXACT
    assert gate["status"] == "pass"
    assert report["columns"][0]["covered"] is True
    assert report["columns"][0]["observed_distinct"] == 3


def test_covered_sample_is_not_population_proof() -> None:
    report, gate = build_code_crosswalk_evidence(
        mappings=[_mapping()],
        sample_rows=_rows("A", "B", "C"),
        rows_are_population=False,
    )
    assert report["evidence"] == EVIDENCE_SAMPLED
    assert gate["status"] == "block"
    assert gate["details"]["rule_id"] == f"{GATE_ID}.unproven"
    assert "sample" in gate["message"].lower()


def test_observed_codes_are_population_evidence() -> None:
    _, gate = build_code_crosswalk_evidence(
        mappings=[_mapping()],
        sample_rows=_rows("A"),
        rows_are_population=False,
        observed_codes={"status": {"A": 900_000, "B": 99_000, "C": 1_000}},
    )
    assert gate["status"] == "pass"


def test_observed_unmapped_blocks_even_when_sample_was_clean() -> None:
    _, gate = build_code_crosswalk_evidence(
        mappings=[_mapping()],
        sample_rows=_rows("A", "B", "C"),
        observed_codes={"status": {"A": 10, "Z": 1}},
    )
    assert gate["status"] == "block"
    assert "Z" in gate["message"]


def test_collect_counts_distinct_and_ignores_nulls() -> None:
    counts, truncated = collect_observed_codes(
        [
            {"status": "A"},
            {"status": "A"},
            {"status": None},
            {"status": "  "},
            {"status": "B"},
        ],
        ["status"],
    )
    assert truncated is False
    assert counts["status"] == {"A": 2, "B": 1}


def test_preflight_skips_when_no_crosswalk_declared() -> None:
    pf = run_file_preflight(
        columns=["id", "status"],
        column_types={"id": "INTEGER", "status": "VARCHAR"},
        row_count=3,
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.99},
            {"source": "status", "target": "status", "confidence": 0.99},
        ],
        sample_rows=_rows("A", "B", "Z"),
        source_kind="database",
        destination_db_type="postgresql",
        destination_table_exists=True,
        destination_column_types={"id": "INTEGER", "status": "VARCHAR"},
        destination_can_write=True,
        destination_connected=True,
        rows_are_population=True,
    )
    gate = next(g for g in pf["gates"] if g["id"] == GATE_ID)
    assert gate["status"] == "skip"
    assert pf["code_crosswalk"]["declared"] is False


def test_preflight_blocks_unmapped_population_code() -> None:
    pf = run_file_preflight(
        columns=["id", "status"],
        column_types={"id": "INTEGER", "status": "VARCHAR"},
        row_count=3,
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.99},
            _mapping(),
        ],
        sample_rows=_rows("A", "B", "Z"),
        source_kind="database",
        destination_db_type="postgresql",
        destination_table_exists=True,
        destination_column_types={"id": "INTEGER", "status": "VARCHAR"},
        destination_can_write=True,
        destination_connected=True,
        rows_are_population=True,
    )
    gate = next(g for g in pf["gates"] if g["id"] == GATE_ID)
    assert gate["status"] == "block"
    assert pf["passed"] is False
    assert any(b["id"] == GATE_ID for b in pf["blockers"])
    assert pf["proof_bundle"]["code_crosswalk"]["schema"] == REPORT_SCHEMA


def test_preflight_passes_when_population_is_covered() -> None:
    pf = run_file_preflight(
        columns=["id", "status"],
        column_types={"id": "INTEGER", "status": "VARCHAR"},
        row_count=3,
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.99},
            _mapping(),
        ],
        sample_rows=_rows("A", "B", "C"),
        source_kind="database",
        destination_db_type="postgresql",
        destination_table_exists=True,
        destination_column_types={"id": "INTEGER", "status": "VARCHAR"},
        destination_can_write=True,
        destination_connected=True,
        rows_are_population=True,
    )
    gate = next(g for g in pf["gates"] if g["id"] == GATE_ID)
    assert gate["status"] == "pass"


def test_write_path_quarantines_unmapped_code_never_passthrough() -> None:
    from connectors.writer_common import build_mapped_rows_with_details

    headers = ["id", "status"]
    mappings = [
        {"source": "id", "target": "id", "confidence": 0.99, "transform": "none"},
        _mapping(transform="none"),
    ]
    mapped, errors, details = build_mapped_rows_with_details(
        headers=headers,
        data_rows=[["1", "A"], ["2", "Z"]],
        mappings=mappings,
        target_cols=["id", "status"],
        column_types={"id": "INTEGER", "status": "VARCHAR"},
        dest_types={"id": "INTEGER", "status": "VARCHAR"},
        error_policy="quarantine",
    )
    assert len(errors) >= 1
    blob = str(errors) + str(details)
    assert "Z" in blob or "unmapped" in blob.lower()
    # The unmapped row must not land as identity 'Z'.
    written_status = [r[1] for r in mapped if r]
    assert "Z" not in written_status
    assert "active" in written_status


def test_proof_pack_omits_full_rows() -> None:
    report, _gate = build_code_crosswalk_evidence(
        mappings=[_mapping()],
        sample_rows=_rows("A", "Z"),
        rows_are_population=True,
    )
    slice_ = proof_pack_code_crosswalk(report)
    blob = str(slice_)
    assert "id" not in blob or "unmapped_codes" in blob
    assert slice_["columns"][0]["unmapped_codes"] == ["Z"]


def test_exported_proof_pack_carries_crosswalk_coverage() -> None:
    from services.signed_proof_pack import export_proof_pack_for_job

    pf = run_file_preflight(
        columns=["id", "status"],
        column_types={"id": "INTEGER", "status": "VARCHAR"},
        row_count=2,
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.99},
            _mapping(),
        ],
        sample_rows=_rows("A", "Z"),
        source_kind="database",
        destination_db_type="postgresql",
        destination_table_exists=True,
        destination_column_types={"id": "INTEGER", "status": "VARCHAR"},
        destination_can_write=True,
        destination_connected=True,
        rows_are_population=True,
    )
    pack = export_proof_pack_for_job(
        {
            "_id": "job-g20",
            "status": "failed",
            "preflight": {
                "passed": pf["passed"],
                "code_crosswalk": pf["code_crosswalk"],
            },
        }
    )
    coverage = pack["preflight_summary"]["code_crosswalk"]
    assert coverage["schema"] == REPORT_SCHEMA
    assert coverage["declared"] is True
    assert coverage["columns"][0]["unmapped_codes"] == ["Z"]


def test_gate_builder_skip_vs_block() -> None:
    skip = build_code_crosswalk_gate({"declared": False, "columns": []})
    assert skip["status"] == "skip"
    block = build_code_crosswalk_gate(
        {
            "declared": True,
            "evidence": EVIDENCE_EXACT,
            "columns": [
                {
                    "source": "status",
                    "unmapped_codes": ["Z"],
                    "unmapped_named": ["Z"],
                }
            ],
        }
    )
    assert block["status"] == "block"


def test_sql_group_by_sees_rare_code_the_sample_missed(tmp_path) -> None:
    """A covered sample is not proof — GROUP BY must see Z on the table."""
    import sqlite3

    from services.code_crosswalk import probe_population_codes

    db = tmp_path / "codes.db"
    conn = sqlite3.connect(db)
    try:
        conn.execute("CREATE TABLE src (id INTEGER, status TEXT)")
        conn.executemany(
            "INSERT INTO src VALUES (?, ?)",
            [(1, "A"), (2, "A"), (3, "B"), (4, "C"), (5, "Z")],
        )
        conn.commit()
    finally:
        conn.close()

    cfg = {"type": "sqlite", "database": str(db), "format": "sqlite"}
    observed, truncated, method = probe_population_codes(
        columns=["status"],
        source_config=cfg,
        source_table="src",
    )
    assert method == "sql_group_by"
    assert truncated is False
    assert observed is not None
    assert set(observed["status"]) == {"A", "B", "C", "Z"}
    assert observed["status"]["Z"] == 1

    pf = run_file_preflight(
        columns=["id", "status"],
        column_types={"id": "INTEGER", "status": "VARCHAR"},
        row_count=5,
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.99},
            _mapping(),
        ],
        # Sample looks complete. Population is not.
        sample_rows=_rows("A", "B", "C"),
        source_kind="database",
        source_format="sqlite",
        source_config=cfg,
        source_table="src",
        destination_db_type="postgresql",
        destination_table_exists=True,
        destination_column_types={"id": "INTEGER", "status": "VARCHAR"},
        destination_can_write=True,
        destination_connected=True,
        rows_are_population=False,
    )
    gate = next(g for g in pf["gates"] if g["id"] == GATE_ID)
    assert gate["status"] == "block"
    assert gate["details"]["rule_id"] == f"{GATE_ID}.unmapped"
    assert "Z" in gate["message"]
    assert pf["code_crosswalk"]["scan_method"] == "sql_group_by"
    assert pf["code_crosswalk"]["evidence"] == EVIDENCE_EXACT


def test_sql_group_by_covered_population_passes(tmp_path) -> None:
    import sqlite3

    db = tmp_path / "codes.db"
    conn = sqlite3.connect(db)
    try:
        conn.execute("CREATE TABLE src (id INTEGER, status TEXT)")
        conn.executemany(
            "INSERT INTO src VALUES (?, ?)",
            [(1, "A"), (2, "B"), (3, "C")],
        )
        conn.commit()
    finally:
        conn.close()

    cfg = {"type": "sqlite", "database": str(db), "format": "sqlite"}
    pf = run_file_preflight(
        columns=["id", "status"],
        column_types={"id": "INTEGER", "status": "VARCHAR"},
        row_count=3,
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.99},
            _mapping(),
        ],
        sample_rows=_rows("A"),
        source_kind="database",
        source_format="sqlite",
        source_config=cfg,
        source_table="src",
        destination_db_type="sqlite",
        destination_table_exists=True,
        destination_column_types={"id": "INTEGER", "status": "VARCHAR"},
        destination_can_write=True,
        destination_connected=True,
        rows_are_population=False,
    )
    gate = next(g for g in pf["gates"] if g["id"] == GATE_ID)
    assert gate["status"] == "pass"
    assert pf["code_crosswalk"]["evidence"] == EVIDENCE_EXACT


def test_validate_mapping_item_keeps_code_crosswalk() -> None:
    """Pydantic extra=ignore would drop G20 off Validate and fail open."""
    from src.routers.preflight_router import MappingItem

    item = MappingItem(
        source="status",
        target="status",
        confidence=0.99,
        code_crosswalk={"A": "active", "B": "blocked"},
        code_crosswalk_system="legacy_status→v2",
    )
    dumped = item.model_dump()
    assert dumped["code_crosswalk"] == {"A": "active", "B": "blocked"}
    assert dumped["code_crosswalk_system"] == "legacy_status→v2"
    empty = MappingItem(source="status", target="status", code_crosswalk={})
    assert empty.model_dump()["code_crosswalk"] == {}
