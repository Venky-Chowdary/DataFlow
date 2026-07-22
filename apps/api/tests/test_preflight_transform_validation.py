"""Preflight dry-run must validate the same transforms used by writers."""

from __future__ import annotations

import importlib.util
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]


def _load_preflight_service():
    path = _API_ROOT / "src" / "services" / "preflight_service.py"
    spec = importlib.util.spec_from_file_location("preflight_service_mod_unit", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def test_preflight_blocks_bad_transform_sample():
    service = _load_preflight_service()
    result = service.run_file_preflight(
        columns=["AMT"],
        column_types={"AMT": "DECIMAL"},
        row_count=1,
        mappings=[
            {
                "source": "AMT",
                "target": "payment_amount",
                "confidence": 0.98,
                "transform": "decimal",
            }
        ],
        destination_connected=True,
        sample_rows=[{"AMT": "not-a-number"}],
        estimated_bytes=128,
    )
    blockers = {b["id"]: b for b in result["blockers"]}
    assert "g5_dry_run" in blockers
    assert "Invalid decimal" in str(blockers["g5_dry_run"]["details"])


def test_preflight_surfaces_g5_and_g6_together():
    """Validate must not hide DDL (G6) behind integrity (G5) fail-fast."""
    service = _load_preflight_service()
    result = service.run_file_preflight(
        columns=["AMT", "note"],
        column_types={"AMT": "DECIMAL", "note": "VARCHAR"},
        row_count=2,
        mappings=[
            {
                "source": "AMT",
                "target": "payment_amount",
                "confidence": 0.98,
                "transform": "decimal",
                "target_type": "NUMBER(10,2)",
            },
            {
                "source": "note",
                "target": "missing_col",
                "confidence": 0.98,
                "target_type": "VARCHAR",
            },
        ],
        destination_connected=True,
        source_connected=True,
        sample_rows=[{"AMT": "not-a-number", "note": "x"}],
        destination_column_types={"payment_amount": "NUMBER(10,2)"},
        destination_table_exists=True,
        destination_can_create=False,
        destination_db_type="snowflake",
        backfill_new_fields=False,
        schema_policy="type_locked",
        estimated_bytes=128,
    )
    blocker_ids = {b["id"] for b in result["blockers"]}
    gate_ids = {g["id"] for g in result["gates"]}
    assert "g5_dry_run" in blocker_ids
    assert "g6_target_ddl" in gate_ids
    assert "g6_target_ddl" in blocker_ids or any(
        g["id"] == "g6_target_ddl" and g["status"] == "block" for g in result["gates"]
    )
