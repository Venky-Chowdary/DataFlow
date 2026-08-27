"""Execute after stamped Validate must not re-walk the source population."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))
_SRC = _API_ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))


def _artifact_hash() -> str:
    return "a" * 64


def test_reuse_requires_64_char_hash_and_operator_maps():
    from src.transfer.engine_identity import reuse_approved_validate_population_fit

    maps = [{"source": "a", "target": "a", "confidence": 0.93}]
    assert reuse_approved_validate_population_fit(
        SimpleNamespace(
            skip_preflight=False,
            approved_decision_artifact_hash=_artifact_hash(),
            mappings=maps,
        )
    )
    assert not reuse_approved_validate_population_fit(
        SimpleNamespace(
            skip_preflight=False,
            approved_decision_artifact_hash="short",
            mappings=maps,
        )
    )
    assert not reuse_approved_validate_population_fit(
        SimpleNamespace(
            skip_preflight=True,
            approved_decision_artifact_hash=_artifact_hash(),
            mappings=maps,
        )
    )
    assert not reuse_approved_validate_population_fit(
        SimpleNamespace(
            skip_preflight=False,
            approved_decision_artifact_hash=_artifact_hash(),
            mappings=[],
        )
    )


def test_confirm_copy_does_not_say_validating_again():
    from src.transfer.engine_identity import execute_preflight_progress_message

    msg = execute_preflight_progress_message(
        SimpleNamespace(
            skip_preflight=False,
            approved_decision_artifact_hash=_artifact_hash(),
            mappings=[{"source": "a", "target": "a"}],
        )
    )
    assert "Confirming approved Validate" in msg
    assert "Validating mapping and schema" not in msg


def test_skip_population_fit_does_not_iterate_source_rows():
    from services.preflight_service import run_file_preflight

    walked = {"n": 0}

    def _rows():
        walked["n"] += 1
        yield {"dep_time": "1234.5", "id": "1"}
        walked["n"] += 1
        yield {"dep_time": "9999.999", "id": "2"}

    result = run_file_preflight(
        columns=["id", "dep_time"],
        column_types={"id": "TEXT", "dep_time": "NUMBER"},
        row_count=1_000_000,
        mappings=[
            {
                "source": "id",
                "target": "id",
                "confidence": 0.95,
                "target_type": "VARCHAR",
            },
            {
                "source": "dep_time",
                "target": "dep_time",
                "confidence": 0.95,
                "target_type": "NUMBER(15,11)",
            },
        ],
        destination_connected=True,
        source_connected=True,
        source_kind="file",
        source_format="csv",
        sync_mode="full_refresh_append",
        sample_rows=[{"id": "1", "dep_time": "1234.5"}],
        destination_db_type="snowflake",
        destination_table_exists=False,
        destination_can_create=True,
        destination_can_write=True,
        destination_column_types={
            "id": "VARCHAR",
            "dep_time": "NUMBER(15,11)",
        },
        validation_mode="strict",
        population_rows=_rows(),
        rows_are_population=True,
        skip_population_fit=True,
    )
    assert walked["n"] == 0
    fit = next(g for g in result["gates"] if g["id"] == "g3f_population_fit")
    assert fit["details"].get("reused_from_validate") is True
    assert fit["details"].get("truncated_reason") == "reused_validate"
    assert "re-walk" in str(fit.get("message") or "").lower()
    assert fit["status"] != "block"


def test_without_skip_still_walks_population():
    from services.preflight_service import run_file_preflight

    walked = {"n": 0}

    def _rows():
        walked["n"] += 1
        yield {"name": "alice"}

    run_file_preflight(
        columns=["name"],
        column_types={"name": "TEXT"},
        row_count=1,
        mappings=[
            {
                "source": "name",
                "target": "name",
                "confidence": 0.95,
                "target_type": "VARCHAR(8)",
            }
        ],
        destination_connected=True,
        source_connected=True,
        source_kind="file",
        source_format="csv",
        sync_mode="full_refresh_append",
        sample_rows=[{"name": "alice"}],
        destination_db_type="postgresql",
        destination_table_exists=True,
        destination_can_create=True,
        destination_can_write=True,
        destination_column_types={"name": "VARCHAR(8)"},
        validation_mode="strict",
        population_rows=_rows(),
        rows_are_population=True,
        skip_population_fit=False,
    )
    assert walked["n"] >= 1
