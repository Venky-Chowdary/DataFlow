"""Wave AE — Transfer Studio honesty: contract PK + append uniqueness gates."""

from __future__ import annotations

import sys
from pathlib import Path

_API = Path(__file__).resolve().parents[1]
if str(_API) not in sys.path:
    sys.path.insert(0, str(_API))


def test_extract_contract_primary_key():
    from services.primary_key import extract_contract_primary_key

    assert extract_contract_primary_key(None) is None
    assert (
        extract_contract_primary_key(
            [{"name": "jobs", "selected": True, "primary_key": "job_uuid"}]
        )
        == "job_uuid"
    )
    assert (
        extract_contract_primary_key(
            [
                {"name": "a", "selected": False, "primary_key": "x"},
                {"name": "b", "selected": True, "primary_key": "order_id"},
            ]
        )
        == "order_id"
    )


def test_contract_primary_key_wins_over_inferred_id():
    from services.primary_key import resolve_identity_key

    mappings = [
        {"source": "id", "target": "id"},
        {"source": "job_uuid", "target": "job_uuid"},
        {"source": "name", "target": "name"},
    ]
    src, tgt = resolve_identity_key(
        mappings=mappings,
        source_columns=["id", "job_uuid", "name"],
        dest_kind="postgresql",
        purpose="uniqueness",
        contract_primary_key="job_uuid",
    )
    assert src == "job_uuid" and tgt == "job_uuid"


def test_g6_sql_skips_dupes_for_append():
    from services.preflight_service import run_file_preflight

    rows = [
        {"id": "a", "name": "1"},
        {"id": "a", "name": "2"},
    ]
    result = run_file_preflight(
        columns=["id", "name"],
        column_types={"id": "TEXT", "name": "TEXT"},
        row_count=2,
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.95},
            {"source": "name", "target": "name", "confidence": 0.95},
        ],
        destination_connected=True,
        source_connected=True,
        source_kind="database",
        sync_mode="full_refresh_append",
        sample_rows=rows,
        destination_db_type="postgresql",
        destination_table_exists=False,
        destination_can_create=True,
        destination_can_write=True,
        validation_mode="strict",
    )
    g6 = next(g for g in result["gates"] if g["id"] == "g6_target_ddl")
    assert g6["status"] == "pass", g6
    assert not any(
        "duplicate" in str(g.get("message", "")).lower()
        for g in result.get("blockers", [])
    )


def test_g6_sql_blocks_dupes_for_cdc_on_contract_pk():
    from services.preflight_service import run_file_preflight

    rows = [
        {"id": "1", "job_uuid": "u1", "name": "a"},
        {"id": "2", "job_uuid": "u1", "name": "b"},
    ]
    result = run_file_preflight(
        columns=["id", "job_uuid", "name"],
        column_types={"id": "TEXT", "job_uuid": "TEXT", "name": "TEXT"},
        row_count=2,
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.95},
            {"source": "job_uuid", "target": "job_uuid", "confidence": 0.95},
            {"source": "name", "target": "name", "confidence": 0.95},
        ],
        destination_connected=True,
        source_connected=True,
        source_kind="database",
        sync_mode="cdc",
        sample_rows=rows,
        destination_db_type="postgresql",
        destination_table_exists=False,
        destination_can_create=True,
        destination_can_write=True,
        validation_mode="strict",
        contract_primary_key="job_uuid",
    )
    g6 = next(g for g in result["gates"] if g["id"] == "g6_target_ddl")
    assert g6["status"] == "block", g6
    assert "job_uuid" in str(g6.get("message", "")).lower() or "job_uuid" in str(
        g6.get("details", {})
    )


def test_g6_sql_passes_cdc_when_contract_pk_unique_despite_id_dupes():
    from services.preflight_service import run_file_preflight

    rows = [
        {"id": "dup", "job_uuid": "u1", "name": "a"},
        {"id": "dup", "job_uuid": "u2", "name": "b"},
    ]
    result = run_file_preflight(
        columns=["id", "job_uuid", "name"],
        column_types={"id": "TEXT", "job_uuid": "TEXT", "name": "TEXT"},
        row_count=2,
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.95},
            {"source": "job_uuid", "target": "job_uuid", "confidence": 0.95},
            {"source": "name", "target": "name", "confidence": 0.95},
        ],
        destination_connected=True,
        source_connected=True,
        source_kind="database",
        sync_mode="cdc",
        sample_rows=rows,
        destination_db_type="postgresql",
        destination_table_exists=False,
        destination_can_create=True,
        destination_can_write=True,
        validation_mode="strict",
        contract_primary_key="job_uuid",
    )
    g6 = next(g for g in result["gates"] if g["id"] == "g6_target_ddl")
    assert g6["status"] == "pass", g6
    # Without contract PK, inferred ``id`` would have blocked.
    result_inferred = run_file_preflight(
        columns=["id", "job_uuid", "name"],
        column_types={"id": "TEXT", "job_uuid": "TEXT", "name": "TEXT"},
        row_count=2,
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.95},
            {"source": "job_uuid", "target": "job_uuid", "confidence": 0.95},
            {"source": "name", "target": "name", "confidence": 0.95},
        ],
        destination_connected=True,
        source_connected=True,
        source_kind="database",
        sync_mode="cdc",
        sample_rows=rows,
        destination_db_type="postgresql",
        destination_table_exists=False,
        destination_can_create=True,
        destination_can_write=True,
        validation_mode="strict",
    )
    g6_inf = next(g for g in result_inferred["gates"] if g["id"] == "g6_target_ddl")
    assert g6_inf["status"] == "block", g6_inf


def test_append_expectations_do_not_block_on_duplicate_id():
    from services.data_integrity import run_integrity_audit

    report = run_integrity_audit(
        source_columns=["id", "name"],
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.95},
            {"source": "name", "target": "name", "confidence": 0.95},
        ],
        sample_rows=[{"id": "a", "name": "1"}, {"id": "a", "name": "2"}],
        destination_db_type="postgresql",
        validation_mode="strict",
        sync_mode="full_refresh_append",
    )
    assert report.get("blocks_transfer") is False, report.get("issues")
    exp = next((c for c in report.get("checks", []) if c.get("check") == "expectations_suite"), None)
    assert exp is not None
    assert exp.get("blocks_transfer") is False
    assert exp.get("passed") is True


def test_cdc_expectations_still_block_on_duplicate_id():
    from services.data_integrity import run_integrity_audit

    report = run_integrity_audit(
        source_columns=["id", "name"],
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.95},
            {"source": "name", "target": "name", "confidence": 0.95},
        ],
        sample_rows=[{"id": "a", "name": "1"}, {"id": "a", "name": "2"}],
        destination_db_type="postgresql",
        validation_mode="strict",
        sync_mode="cdc",
    )
    assert report.get("blocks_transfer") is True
    assert any(
        "expect_column_unique" in str(i) or "duplicate" in str(i).lower()
        for i in report.get("issues", [])
    )


def test_append_full_preflight_passes_with_duplicate_id():
    from services.preflight_service import run_file_preflight

    rows = [{"id": "a", "name": "1"}, {"id": "a", "name": "2"}]
    result = run_file_preflight(
        columns=["id", "name"],
        column_types={"id": "TEXT", "name": "TEXT"},
        row_count=2,
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.95},
            {"source": "name", "target": "name", "confidence": 0.95},
        ],
        destination_connected=True,
        source_connected=True,
        source_kind="database",
        sync_mode="full_refresh_append",
        sample_rows=rows,
        destination_db_type="postgresql",
        destination_table_exists=True,
        destination_column_types={"id": "TEXT", "name": "TEXT"},
        destination_can_create=True,
        destination_can_write=True,
        validation_mode="strict",
    )
    g6 = next(g for g in result["gates"] if g["id"] == "g6_target_ddl")
    g9 = next(g for g in result["gates"] if g["id"] == "g9_data_integrity")
    assert g6["status"] == "pass", g6
    assert g9["status"] == "pass", g9
    assert result["passed"] is True, result.get("blockers")
